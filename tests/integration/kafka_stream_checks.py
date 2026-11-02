"""Retained Kafka/S3 integration case, run each stage in a NEW Spark driver.

Stages fault -> resume -> append -> verify preserve all topics and objects.
"""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import time
from urllib.request import urlopen
from uuid import uuid4

from delta.tables import DeltaTable
from src.common.spark_session import get_spark
from src.storage.stream_config import resolve_kafka_config
from src.storage.stream_to_bronze import start_kafka_stream, close_kafka_stream
from src.storage import stream_sink


def setup_case():
    import yaml
    from src.common.config import load

    run_id = uuid4().hex[:12]
    directory = Path('/opt/app/data/raw') / ('kafka-stream-test-' + run_id)
    directory.mkdir()
    config = deepcopy(load())
    config['kafka']['topic'] = 'clickflow-stream-test-' + run_id
    root = 's3a://lake/_checkpoints/integration-kafka/' + run_id
    config['lake'].update(bronze=root + '/bronze/events', quarantine=root + '/quarantine/events')
    config['stream'].update(source='kafka', checkpoint_path=root + '/checkpoint', metrics_port=18002,
                            query_name='stream-test-' + run_id, max_offsets_per_trigger=1000)
    config_path = directory / 'pipeline.yaml'
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))
    case = {'directory': str(directory), 'config': str(config_path),
            'topic': config['kafka']['topic'], 'run_id': run_id, 'case': str(directory / 'case.json')}
    Path(case['case']).write_text(json.dumps(case, indent=2))
    print(json.dumps(case), flush=True)


def event(event_id='event-a', **updates):
    result = {'event_time': '2019-11-01T00:00:01Z', 'event_type': 'view',
              'product_id': 1003461, 'category_id': 2053013555631882655,
              'category_code': 'electronics.smartphone', 'brand': None,
              'price': 489.07, 'user_id': 520088904, 'user_session': None,
              'event_id': event_id, 'source_file': '2019-Nov.csv',
              'produced_at': '2026-11-01T00:00:00Z'}
    result.update(updates)
    return result


def send(spark, settings, messages):
    jvm = spark._jvm
    properties = jvm.java.util.Properties()
    for key, value in {'bootstrap.servers': settings['bootstrap_servers'], 'acks': 'all',
                       'enable.idempotence': 'true',
                       'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
                       'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer'}.items():
        properties.setProperty(key, value)
    producer = jvm.org.apache.kafka.clients.producer.KafkaProducer(properties)
    positions = []
    try:
        for partition, payload in messages:
            if isinstance(payload, dict):
                payload = json.dumps(payload, separators=(',', ':'))
            record = jvm.org.apache.kafka.clients.producer.ProducerRecord(
                settings['topic'], jvm.java.lang.Integer(partition), '520088904', payload)
            metadata = producer.send(record).get(60, jvm.java.util.concurrent.TimeUnit.SECONDS)
            positions.append({'partition': metadata.partition(), 'offset': metadata.offset(), 'payload': payload})
        producer.flush()
    finally:
        producer.close()
    return positions


def read_report(case, stage):
    return json.loads(Path(case['directory'], stage + '.json').read_text())


def snapshot(spark, settings):
    bronze = spark.read.format('delta').load(settings['bronze'])
    quarantine = spark.read.format('delta').load(settings['quarantine'])
    records = {row.event_id: row.asDict() for row in bronze.collect()}
    bad = [row.asDict() for row in quarantine.collect()]
    return {'bronze_rows': bronze.count(), 'quarantine_rows': len(bad),
            'bronze': records, 'quarantine': bad,
            'versions': {name: DeltaTable.forPath(spark, settings[name]).history(1).first().version
                         for name in ('bronze', 'quarantine')}}


def run(case, stage):
    import yaml
    config = yaml.safe_load(Path(case['config']).read_text())
    settings = resolve_kafka_config(config)
    spark = get_spark('kafka-stream-check-' + stage)
    spark.sparkContext.setLogLevel('WARN')
    report = {'stage': stage, 'topic': settings['topic'], 'checkpoint': settings['checkpoint']}
    try:
        if stage == 'fault':
            report['positions'] = send(spark, settings, [
                (0, event()), (1, event('bad-price', price=-1)), (1, '{ broken JSON  '),
                (0, event(produced_at='2026-11-01T01:00:00Z')),
            ])
        elif stage == 'append':
            report['positions'] = send(spark, settings, [
                (0, event('event-b', event_time='2019-11-01T00:00:02Z')),
                (1, event(produced_at='2026-11-01T02:00:00Z')),
                (1, event(price=999)),
                (0, event('bad-time', event_time='2019-12-01T00:00:00Z')),
                (1, event('bad-type', event_type='refund')),
                (0, event('bad-id', user_id=None)), (1, None),
                (0, event('bad-price', price=-1)),
            ])
        if stage in ('fault', 'resume', 'append', 'verify'):
            original_merge = stream_sink.merge_insert_only
            if stage == 'fault':
                def interrupted_merge(spark, source, target, keys):
                    if target == settings['quarantine']:
                        raise RuntimeError('INJECTED_AFTER_BRONZE_COMMIT')
                    return original_merge(spark, source, target, keys)
                stream_sink.merge_insert_only = interrupted_merge
            query, metrics, listener = start_kafka_stream(spark, config, available_now=True)
            try:
                failed = False
                try:
                    if not query.awaitTermination(300):
                        raise AssertionError('Stream test exceeded 300 seconds')
                except Exception as error:
                    if stage != 'fault' or 'INJECTED_AFTER_BRONZE_COMMIT' not in str(error):
                        raise
                    failed = True
                assert failed == (stage == 'fault')
                expected_metric = 'clickflow_stream_failures_total' if failed else 'clickflow_stream_batches_total'
                deadline = time.monotonic() + 15
                while stage != 'verify' and (metrics.registry.get_sample_value(expected_metric) or 0) < 1 and time.monotonic() < deadline:
                    time.sleep(0.1)
                body = urlopen(f'http://127.0.0.1:{metrics.server.server_port}/metrics', timeout=5).read().decode()
                assert 'clickflow_stream_sink_rows_total' in body
                if stage != 'verify':
                    assert (metrics.registry.get_sample_value(expected_metric) or 0) >= 1
                report['query_id'], report['run_id'] = str(query.id), str(query.runId)
                report['metrics_scraped'] = True
                report['metrics'] = body
            finally:
                close_kafka_stream(spark, query, metrics, listener)
                stream_sink.merge_insert_only = original_merge
        state = snapshot(spark, settings)
        report.update(state)
        if stage == 'fault':
            assert state['bronze_rows'] == 1 and state['quarantine_rows'] == 0
        elif stage == 'resume':
            previous = read_report(case, 'fault')
            assert state['bronze_rows'] == 1 and state['quarantine_rows'] == 2
            assert report['query_id'] == previous['query_id']
            assert report['run_id'] != previous['run_id']
            assert json.loads(json.dumps(state['bronze'], default=str)) == previous['bronze']
            assert state['bronze']['event-a']['category_id'] == 2053013555631882655
        elif stage in ('append', 'verify'):
            previous = read_report(case, 'resume')
            assert state['bronze_rows'] == 2 and state['quarantine_rows'] == 7
            assert json.loads(json.dumps(state['bronze']['event-a'], default=str)) == previous['bronze']['event-a']
            assert {row['error_reason'] for row in state['quarantine']} == {
                'parse_error', 'bad_event_time', 'bad_event_type', 'missing_id', 'negative_price'}
            assert len({(r['kafka_partition'], r['kafka_offset']) for r in state['quarantine']}) == 7
            for item in read_report(case, 'fault')['positions'] + read_report(case, 'append')['positions'] if stage == 'verify' else []:
                matching = [r for r in state['quarantine'] if r['kafka_partition'] == item['partition'] and r['kafka_offset'] == item['offset']]
                if matching:
                    assert matching[0]['payload'] == item['payload']
        if stage == 'verify':
            # An existing checkpoint/sink pair must not silently adopt another topic.
            wrong = deepcopy(settings)
            wrong['topic'] += '-other'
            try:
                stream_sink.prepare_stream_storage(spark, wrong)
                raise AssertionError('Source binding mismatch was accepted')
            except ValueError as error:
                assert 'identity mismatch' in str(error)
            appended = read_report(case, 'append')
            assert report['query_id'] == appended['query_id']
            assert report['run_id'] != appended['run_id']
            assert state['versions'] == appended['versions']
            report['binding_mismatch_rejected'] = True
            report['committed_checkpoint_restart_verified'] = True
        output = Path(case['directory'], stage + '.json')
        with output.open('x') as stream:
            json.dump(report, stream, indent=2, default=str)
        print(json.dumps({key: value for key, value in report.items()
                          if key not in ('metrics', 'positions', 'bronze', 'quarantine')}, default=str), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case')
    parser.add_argument('--stage', choices=['setup', 'fault', 'resume', 'append', 'verify'], required=True)
    args = parser.parse_args()
    if args.stage == 'setup':
        setup_case()
    else:
        if not args.case:
            parser.error('--case is required for test driver stages')
        run(json.loads(Path(args.case).read_text()), args.stage)
