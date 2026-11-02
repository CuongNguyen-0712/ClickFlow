"""Exercise all four quality rules, real Delta routing and retry-safe counters."""
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

from src.common.config import load
from src.common.metrics import StreamMetrics
from src.common.spark_session import get_spark
from src.storage.quality_rules import classify_records
from src.storage.stream_config import resolve_kafka_config
from src.storage.stream_sink import prepare_stream_storage, process_batch


spark = get_spark('quality-rules-integration')
spark.sparkContext.setLogLevel('WARN')
metrics = StreamMetrics('quality-rules-integration', port=0)
try:
    run_id = uuid4().hex
    config = deepcopy(load())
    root = config['lake']['checkpoints'].rstrip('/') + '/quality-rules-test-' + run_id
    config['lake'].update(bronze=root + '/bronze/events', quarantine=root + '/quarantine/events')
    config['stream']['checkpoint_path'] = root + '/checkpoint'
    config['kafka']['topic'] = 'quality-fixture-' + run_id
    settings = resolve_kafka_config(config)
    prepare_stream_storage(spark, settings)
    base = {'event_id': 'quality-valid', 'event_time': '2019-11-01T00:00:00Z',
            'event_type': 'view', 'product_id': 100, 'category_id': 2053013555631882655,
            'category_code': None, 'brand': None, 'price': 12.5, 'user_id': 200,
            'user_session': None, 'source_file': '2019-Nov.csv', 'produced_at': '2026-11-01T00:00:00Z'}
    changes = [None, '{ broken JSON  ', {'category_id': 'bad'}, {'event_time': None},
               {'event_time': '2019-12-01T00:00:00Z'}, {'event_type': 'refund'},
               {'product_id': None, 'price': -1}, {'user_id': None}, {'price': -1}]
    payloads = [change if isinstance(change, str) else json.dumps({**base, **(change or {})})
                for change in changes]
    raw = spark.createDataFrame(
        [(body, 0, offset, datetime(2026, 11, 1)) for offset, body in enumerate(payloads)],
        'payload string, kafka_partition int, kafka_offset bigint, ingested_at timestamp')
    classified = classify_records(raw)
    process_batch(classified, 0, settings, metrics)
    expected_errors = {'parse_error': 2, 'bad_event_time': 2, 'bad_event_type': 1,
                       'missing_id': 2, 'negative_price': 1}
    expected_rules = {1: 2, 2: 2, 3: 1, 4: 3}

    def counters():
        return {rule: metrics.registry.get_sample_value(
            'clickflow_stream_quality_rule_violations_total', {'rule': str(rule)}) for rule in range(1, 5)}

    assert counters() == expected_rules
    bronze = spark.read.format('delta').load(settings['bronze'])
    quarantine = spark.read.format('delta').load(settings['quarantine'])
    assert bronze.count() == 1
    valid = bronze.first()
    assert valid.category_id == 2053013555631882655
    assert valid.category_code is valid.brand is valid.user_session is None
    bad = quarantine.collect()
    assert len(bad) == 8
    assert {r.error_reason: r['count'] for r in quarantine.groupBy('error_reason').count().collect()} == expected_errors
    assert all(row.payload == payloads[row.kafka_offset] for row in bad)
    assert all(str(row.ingest_date) == '2026-11-01' for row in bad)
    http_metrics = urlopen(f'http://127.0.0.1:{metrics.server.server_port}/metrics', timeout=5).read().decode()
    for rule, count in expected_rules.items():
        assert f'clickflow_stream_quality_rule_violations_total{{rule="{rule}"}} {float(count)}' in http_metrics
    # Replay identical Kafka coordinates: no new rows or committed-rule counts.
    process_batch(classified, 0, settings, metrics)
    assert counters() == expected_rules
    assert spark.read.format('delta').load(settings['bronze']).count() == 1
    assert spark.read.format('delta').load(settings['quarantine']).count() == 8
    report = {'event': 'quality_rules_verified', 'run_id': run_id,
              'bronze_rows': 1, 'quarantine_rows': 8, 'error_counts': expected_errors,
              'rule_counts': expected_rules, 'retry_counts_unchanged': True,
              'optional_nulls_valid': True, 'raw_payload_preserved': True,
              'metrics_http_verified': True, 'paths': settings,
              'artifact_policy': 'retained'}
    destination = Path('/opt/app/data/raw') / ('quality-rules-verification-' + run_id + '.json')
    with destination.open('x') as output:
        json.dump(report, output, indent=2)
    print(json.dumps({**report, 'report': str(destination)}), flush=True)
finally:
    metrics.close()
    spark.stop()
