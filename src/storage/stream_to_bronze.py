"""Configured sample -> C4 Delta trial table; Kafka streaming is a later task."""
import argparse
import json
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


def run_pipeline(spark, config):
    stream = config['stream']
    source = stream['source']
    if source == 'kafka':
        raise NotImplementedError('Kafka streaming/quality routing is not implemented yet; use stream.source=sample')
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=['sample', 'kafka'], help='Override stream.source for this run')
    args = parser.parse_args()
    config = load()
    if args.source:
        config = {**config, 'stream': {**config['stream'], 'source': args.source}}
    spark = get_spark('stream-to-bronze-' + config['stream']['source'])
    spark.sparkContext.setLogLevel('WARN')
    try:
        run_pipeline(spark, config)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
