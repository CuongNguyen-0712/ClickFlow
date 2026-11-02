"""Kafka Structured Streaming -> C4/C5 Delta, or the finite sample mode."""
import argparse
import json
import logging
import os
from pathlib import Path
import signal
from threading import Event
from functools import reduce
from operator import or_
from urllib.parse import urlsplit, urlunsplit

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import load
from src.common.schemas import SOURCE_COLUMNS
from src.common.spark_csv import same_rows, select_sample
from src.common.spark_session import get_spark
from src.storage.init_lake import contract_schema, lake_paths, table_contract


def start_kafka_stream(spark, config, *, available_now=False):
    from functools import partial
    from src.common.metrics import StreamMetrics
    from src.monitoring.stream_listener import attach_listener
    from src.storage.deduplication import deduplicate_valid_records
    from src.storage.quality_rules import classify_records
    from src.storage.stream_config import resolve_kafka_config
    from src.storage.stream_sink import prepare_stream_storage, process_batch

    settings = resolve_kafka_config(config)
    if spark.conf.get('spark.sql.session.timeZone') != 'UTC':
        raise ValueError('C4/C5 require Spark session timezone UTC')
    prepare_stream_storage(spark, settings, checkpoint_layout={
        'operator': 'dropDuplicatesWithinWatermark', 'keys': ['event_id'],
        'event_time': 'event_time', 'watermark': settings['watermark'],
        'routing': 'valid_union_invalid_v1',
        'shuffle_partitions': int(spark.conf.get('spark.sql.shuffle.partitions')),
    })
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', settings['bootstrap_servers'])
           .option('subscribe', settings['topic'])
           .option('startingOffsets', settings['starting_offsets'])
           .option('failOnDataLoss', 'true')
           .option('maxOffsetsPerTrigger', settings['max_offsets_per_trigger']).load()
           .select(F.col('value').cast('string').alias('payload'),
                   F.col('partition').alias('kafka_partition'),
                   F.col('offset').alias('kafka_offset'))
           .withColumn('ingested_at', F.current_timestamp()))
    # Errors bypass stateful filtering; null/out-of-range timestamps must reach C5.
    classified = deduplicate_valid_records(classify_records(raw), settings['watermark'])
    metrics = StreamMetrics(settings['query_name'], port=settings['metrics_port'])
    listener = attach_listener(spark, metrics)
    try:
        writer = (classified.writeStream.queryName(settings['query_name'])
                  .option('checkpointLocation', settings['checkpoint'])
                  .foreachBatch(partial(process_batch, settings=settings, metrics=metrics)))
        writer = writer.trigger(availableNow=True) if available_now else writer.trigger(
            processingTime=f"{settings['trigger_seconds']} seconds")
        query = writer.start()
        print(json.dumps({'event': 'kafka_stream_started', 'topic': settings['topic'],
                          'checkpoint': settings['checkpoint'], 'query_id': str(query.id),
                          'run_id': str(query.runId), 'driver_pid': os.getpid(),
                          'metrics_port': metrics.server.server_port}), flush=True)
        return query, metrics, listener
    except BaseException:
        spark.streams.removeListener(listener)
        metrics.close()
        raise


def close_kafka_stream(spark, query, metrics, listener):
    try:
        if query.isActive:
            query.stop()
    finally:
        spark.streams.removeListener(listener)
        metrics.close()


def write_sample_bronze(spark, sample, target, *, expected_rows=100000):
    source_columns = [name for name, _ in SOURCE_COLUMNS]
    contract = table_contract('bronze')
    schema = contract_schema(spark, contract)
    columns = [field.name for field in schema]
    # Sample metadata is explicitly synthetic; no Kafka offset or latency claim.
    bronze = (sample.withColumn('event_id', F.sha2(F.to_json(
        F.struct(*source_columns), {'ignoreNullFields': 'false'}), 256))
        .withColumn('kafka_partition', F.lit(-1).cast('int'))
        .withColumn('kafka_offset', F.lit(-1).cast('bigint'))
        .withColumn('produced_at', F.lit(None).cast('timestamp'))
        .withColumn('ingested_at', F.current_timestamp())
        .withColumn('event_date', F.to_date('event_time'))
        .select(*columns).cache())
    try:
        count = bronze.count()
        if count != expected_rows:
            raise ValueError(f'Need {expected_rows} distinct sample rows, got {count}')
        required = [F.col(field.name).isNull() for field in schema if not field.nullable]
        if bronze.filter(reduce(or_, required)).limit(1).count():
            raise ValueError('Sample contains null in a required C4 column')
        if bronze.select('event_id').distinct().count() != count:
            raise ValueError('Sample event_id must be unique')
        target = target.rstrip('/')
        (DeltaTable.createIfNotExists(spark).location(target).addColumns(schema)
         .partitionedBy(*contract['partition_by']).execute())
        table = DeltaTable.forPath(spark, target)
        existing = table.toDF()
        signature = lambda value: [(f.name, f.dataType, f.nullable) for f in value]
        if (signature(existing.schema) != signature(schema)
                or table.detail().first().partitionColumns != contract['partition_by']):
            raise ValueError('Existing sample table has a different C4 schema or partition')
        compare_columns = [name for name in columns if name != 'ingested_at']
        reused = existing.limit(1).count() > 0
        if reused:
            if not same_rows(existing, bronze, compare_columns):
                raise ValueError('Existing sample table contains different data; choose a new sample_bronze_path')
        else:
            (bronze.repartition('event_date').write.format('delta').mode('append')
             .partitionBy('event_date').option('txnAppId', 'clickflow-sample-bronze-v1')
             .option('txnVersion', 0).save(target))
        restored = spark.read.format('delta').load(target)
        actual = restored.count()
        if actual != expected_rows or not same_rows(restored, bronze, compare_columns):
            raise AssertionError('Bronze read-back changed the sample row count or values')
        result = {'event': 'sample_bronze_verified', 'path': target, 'rows': actual,
                  'schema': restored.schema.simpleString(), 'partition_by': ['event_date'],
                  'reused': reused, 'source': 'sample', 'master': spark.sparkContext.master}
        print(json.dumps(result))
        return result
    finally:
        bronze.unpersist()


def run_pipeline(spark, config, *, available_now=False, stop_requested=None):
    stream = config['stream']
    source = stream['source']
    if source == 'kafka':
        query, metrics, listener = start_kafka_stream(spark, config, available_now=available_now)
        try:
            stop_requested = stop_requested or Event()
            while not stop_requested.is_set():
                if query.awaitTermination(1):
                    break
        finally:
            close_kafka_stream(spark, query, metrics, listener)
        return
    if source != 'sample':
        raise ValueError(f'Unsupported stream.source: {source}')
    rows = stream.get('sample_rows', 100000)
    target = urlsplit(stream['sample_bronze_path'])
    if target.scheme != 's3a' or not target.netloc:
        raise ValueError('stream.sample_bronze_path must be an s3a:// URI')
    bucket = urlsplit(lake_paths()['bronze']).netloc
    target = urlunsplit(('s3a', bucket, target.path, '', '')).rstrip('/')
    if target == lake_paths()['bronze'].rstrip('/'):
        raise ValueError('Sample mode requires a separate trial table from lake.bronze')
    sample = select_sample(spark, stream['sample_path'], rows=rows,
                           day=stream.get('sample_day', '2019-11-01'))
    return write_sample_bronze(spark, sample, target, expected_rows=rows)


def main():
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    logging.getLogger('src.common.metrics').setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=['sample', 'kafka'], help='Override stream.source for this run')
    parser.add_argument('--config', type=Path, help='Optional pipeline YAML path')
    parser.add_argument('--available-now', action='store_true', help='Drain currently available Kafka offsets and exit')
    args = parser.parse_args()
    if args.config:
        import yaml
        config = yaml.safe_load(args.config.read_text(encoding='utf-8'))
    else:
        config = load()
    if args.source:
        config = {**config, 'stream': {**config['stream'], 'source': args.source}}
    spark = get_spark('stream-to-bronze-' + config['stream']['source'])
    spark.sparkContext.setLogLevel('WARN')
    stop_requested = Event()
    previous_handlers = {}
    if config['stream']['source'] == 'kafka':
        for number in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[number] = signal.signal(number, lambda *_: stop_requested.set())
    try:
        run_pipeline(spark, config, available_now=args.available_now, stop_requested=stop_requested)
    finally:
        try:
            spark.stop()
        finally:
            for number, handler in previous_handlers.items():
                signal.signal(number, handler)


if __name__ == '__main__':
    main()
