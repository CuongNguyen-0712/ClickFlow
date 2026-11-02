"""Real Kafka rule-5 manifests, state recovery and watermark boundary checks.

Run setup, seed, then recover in separate processes. All artifacts are retained.
"""
import argparse
from copy import deepcopy
import json
import logging
from pathlib import Path
import time
from urllib.request import urlopen
from uuid import uuid4

from src.common.config import load
from src.common.spark_session import get_spark
from src.sources.fault_injector import inject_duplicates
from src.storage.stream_config import resolve_kafka_config
from src.storage.stream_to_bronze import start_kafka_stream, close_kafka_stream
from tests.integration.kafka_stream_checks import event, send, snapshot


def setup():
    import yaml
    run_id = uuid4().hex[:12]
    folder = Path('/opt/app/data/raw') / ('watermark-dedup-test-' + run_id)
    folder.mkdir()
    config = deepcopy(load())
    root = 's3a://lake/_checkpoints/watermark-dedup-test-' + run_id
    config['kafka']['topic'] = 'watermark-dedup-test-' + run_id
    config['lake'].update(bronze=root + '/bronze/events', quarantine=root + '/quarantine/events')
    config['stream'].update(source='kafka', checkpoint_path=root + '/checkpoint', metrics_port=18003,
                            query_name='watermark-dedup-' + run_id, max_offsets_per_trigger=6)
    (folder / 'pipeline.yaml').write_text(yaml.safe_dump(config, sort_keys=False))
    case = {'directory': str(folder), 'config': str(folder / 'pipeline.yaml'),
            'topic': config['kafka']['topic'], 'case': str(folder / 'case.json')}
    (folder / 'case.json').write_text(json.dumps(case, indent=2))
    print(json.dumps(case), flush=True)


def write_json(path, value):
    with path.open('x') as output:
        json.dump(value, output, indent=2, default=str)


def count(metrics, suffix):
    return int(metrics.registry.get_sample_value('clickflow_stream_' + suffix + '_total') or 0)


def run_query(spark, config, manifest):
    query, metrics, listener = start_kafka_stream(spark, config, available_now=True)
    try:
        assert query.awaitTermination(300), 'Timed out draining Kafka fixture'
        progress = query.recentProgress
        last = query.lastProgress
        deadline = time.monotonic() + 15
        expected_drops = manifest['expected_duplicates_dropped']
        expected_late = manifest.get('expected_late_records_dropped', 0)
        while time.monotonic() < deadline:
            observed_batch = metrics.registry.get_sample_value('clickflow_stream_batch_id') or 0
            if (count(metrics, 'duplicates_dropped') == expected_drops
                    and count(metrics, 'late_records_dropped') == expected_late
                    and (last is None or observed_batch >= last['batchId'])):
                break
            time.sleep(0.1)
        operators = [op for item in progress for op in item.get('stateOperators', [])]
        actual_drops = sum(int(op.get('customMetrics', {}).get('numDroppedDuplicateRows', 0)) for op in operators)
        actual_late = sum(int(op.get('numRowsDroppedByWatermark', 0)) for op in operators)
        assert actual_drops == count(metrics, 'duplicates_dropped') == expected_drops, (actual_drops, expected_drops, progress)
        assert actual_late == count(metrics, 'late_records_dropped') == expected_late, (actual_late, expected_late, progress)
        assert all(op['operatorName'] == 'dedupeWithinWatermark' for op in operators)
        body = urlopen(f'http://127.0.0.1:{metrics.server.server_port}/metrics', timeout=5).read().decode()
        assert 'clickflow_stream_duplicates_dropped_total' in body
        return {'query_id': str(query.id), 'run_id': str(query.runId),
                'duplicates_dropped': actual_drops, 'late_records_dropped': actual_late,
                'durable_duplicates_ignored': count(metrics, 'duplicates_ignored'),
                'state_rows_removed': sum(int(op.get('numRowsRemoved', 0)) for op in operators),
                'progress': progress, 'metrics_http_verified': True}
    finally:
        close_kafka_stream(spark, query, metrics, listener)


def run(case, stage):
    import yaml
    config = yaml.safe_load(Path(case['config']).read_text())
    folder = Path(case['directory'])
    settings = resolve_kafka_config(config)
    spark = get_spark('watermark-dedup-' + stage)
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.shuffle.partitions', 2)
    try:
        if stage == 'seed':
            base = [event(f'manifest-event-{index}', event_time=f'2019-11-01T00:00:0{index}Z') for index in range(5)]
            injected, manifest = inject_duplicates(base, duplicate_ratio=2.0, seed=42)
            invalid = [event('invalid-null', event_time=None), event('invalid-future', event_time='2030-01-01T00:00:00Z')]
            manifest.update(phase='seed', expected_bronze_rows=5, expected_quarantine_rows=2,
                            expected_late_records_dropped=0)
            write_json(folder / 'injected.json', injected)
            write_json(folder / 'seed-manifest.json', manifest)
            positions = send(spark, settings, [(index % 2, body) for index, body in enumerate(injected + invalid)])
            write_json(folder / 'seed-positions.json', positions)
            result = run_query(spark, config, manifest)
            state = snapshot(spark, settings)
            assert state['bronze_rows'] == 5 and state['quarantine_rows'] == 2
            assert len(state['bronze']) == 5
            assert all(row['error_reason'] == 'bad_event_time' for row in state['quarantine'])
            assert all(item.get('eventTime', {}).get('max', '2019') < '2020' for item in result['progress'])
            report = {**result, **state, 'manifest': manifest}
            write_json(folder / 'seed-report.json', report)
            print(json.dumps({'event': 'dedup_seed_verified', 'duplicates_dropped': result['duplicates_dropped'],
                              'bronze_rows': 5, 'quarantine_rows': 2, 'case': case['case']}), flush=True)
        elif stage == 'within-hour':
            previous = json.loads((folder / 'final-report.json').read_text())
            manifest = {'phase': 'same_id_new_timestamp_within_hour', 'emitted_records': 1,
                        'expected_duplicates_dropped': 1, 'expected_late_records_dropped': 0}
            write_json(folder / 'within-hour-manifest.json', manifest)
            send(spark, settings, [(0, event('manifest-event-0', event_time='2019-11-01T03:30:00Z'))])
            result = run_query(spark, config, manifest)
            state = snapshot(spark, settings)
            assert state['bronze_rows'] == 6 and state['quarantine_rows'] == 4
            assert result['durable_duplicates_ignored'] == 0
            assert result['query_id'] == previous['seed']
            total = previous['duplicates_dropped'] + result['duplicates_dropped']
            assert total == 26
            offsets = result['progress'][-1]['sources'][0]['endOffset']
            offsets = json.loads(offsets) if isinstance(offsets, str) else offsets
            emitted = sum(offsets[settings['topic']].values())
            assert emitted == 39, offsets
            acceptance = {'event': 'watermark_dedup_acceptance_verified', 'case': case['case'],
                          'emitted_records': emitted, 'bronze_rows': 6, 'quarantine_rows': 4,
                          'expected_duplicates_dropped': 26, 'duplicates_dropped': total,
                          'late_records_dropped': 2, 'durable_duplicates_ignored': 1,
                          'changed_timestamp_within_hour_verified': True,
                          'state_restored_in_new_driver': True, 'state_expiry_verified': True,
                          'invalid_bypass_verified': True, 'metrics_http_verified': True,
                          'artifacts_retained': True, 'within_hour': result}
            assert sum(acceptance[key] for key in ('bronze_rows', 'quarantine_rows', 'duplicates_dropped',
                                                  'late_records_dropped', 'durable_duplicates_ignored')) == 39
            write_json(folder / 'acceptance-report.json', acceptance)
            print(json.dumps({key: value for key, value in acceptance.items() if key != 'within_hour'}), flush=True)
        else:
            seed = json.loads((folder / 'seed-report.json').read_text())
            injected = json.loads((folder / 'injected.json').read_text())
            replay_manifest = {**seed['manifest'], 'phase': 'replay', 'expected_duplicates_dropped': len(injected)}
            write_json(folder / 'replay-manifest.json', replay_manifest)
            send(spark, settings, [(index % 2, body) for index, body in enumerate(injected)])
            replay = run_query(spark, config, replay_manifest)
            state = snapshot(spark, settings)
            assert state['bronze_rows'] == 5 and state['quarantine_rows'] == 2
            assert json.loads(json.dumps(state['bronze'], default=str)) == seed['bronze']
            assert replay['query_id'] == seed['query_id'] and replay['run_id'] != seed['run_id']
            write_json(folder / 'replay-report.json', {**replay, **state})

            # Advance beyond two hours so Spark can evict the original ID state.
            anchor = event('advance-anchor', event_time='2019-11-01T03:00:00Z')
            send(spark, settings, [(0, anchor)])
            advance = run_query(spark, config, {'expected_duplicates_dropped': 0})
            assert advance['state_rows_removed'] >= 5, advance
            assert snapshot(spark, settings)['bronze_rows'] == 6
            write_json(folder / 'advance-report.json', advance)

            boundary_manifest = {'phase': 'watermark_boundary', 'expected_duplicates_dropped': 0,
                                 'expected_late_records_dropped': 2, 'expected_durable_ignored': 1,
                                 'expected_bronze_rows': 6, 'expected_quarantine_rows': 4}
            write_json(folder / 'boundary-manifest.json', boundary_manifest)
            positions = send(spark, settings, [
                (0, event('manifest-event-0', event_time='2019-11-01T03:01:00Z')),
                (1, event('late-unique', event_time='2019-11-01T00:15:00Z')),
                (0, event('manifest-event-1', event_time='2019-11-01T00:00:01Z')),
                (1, event('invalid-null', event_time=None)),
                (0, event('invalid-future', event_time='2030-01-01T00:00:00Z')),
            ])
            write_json(folder / 'boundary-positions.json', positions)
            boundary = run_query(spark, config, boundary_manifest)
            final = snapshot(spark, settings)
            assert final['bronze_rows'] == 6 and final['quarantine_rows'] == 4
            assert boundary['durable_duplicates_ignored'] == 1
            assert 'late-unique' not in final['bronze']
            assert final['bronze']['manifest-event-0']['event_time'] == state['bronze']['manifest-event-0']['event_time']
            assert all(row['error_reason'] == 'bad_event_time' for row in final['quarantine'])
            known_positions = json.loads((folder / 'seed-positions.json').read_text()) + positions
            for row in final['quarantine']:
                source = next(item for item in known_positions if item['partition'] == row['kafka_partition'] and item['offset'] == row['kafka_offset'])
                encoded = json.dumps(source['payload'], separators=(',', ':')) if isinstance(source['payload'], dict) else source['payload']
                assert row['payload'] == encoded
            expected_total = seed['manifest']['expected_duplicates_dropped'] + replay_manifest['expected_duplicates_dropped']
            actual_total = seed['duplicates_dropped'] + replay['duplicates_dropped']
            assert actual_total == expected_total == 25
            report = {'event': 'watermark_dedup_verified', 'case': case['case'],
                      'expected_duplicates_dropped': expected_total, 'duplicates_dropped': actual_total,
                      'late_records_dropped': boundary['late_records_dropped'],
                      'bronze_rows': 6, 'unique_event_ids': len(final['bronze']), 'quarantine_rows': 4,
                      'state_restored_in_new_driver': True, 'state_expiry_verified': True,
                      'durable_guard_after_expiry_verified': True, 'invalid_bypass_verified': True,
                      'seed': seed['query_id'], 'replay': replay, 'advance': advance, 'boundary': boundary,
                      'artifacts_retained': True}
            write_json(folder / 'final-report.json', report)
            print(json.dumps({key: value for key, value in report.items() if key not in ('replay', 'advance', 'boundary')}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    logging.getLogger('src.common.metrics').setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=['setup', 'seed', 'recover', 'within-hour'], required=True)
    parser.add_argument('--case')
    args = parser.parse_args()
    if args.stage == 'setup':
        setup()
    else:
        if not args.case:
            parser.error('--case is required for driver stages')
        run(json.loads(Path(args.case).read_text()), args.stage)
