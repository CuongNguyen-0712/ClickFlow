"""Check signed exact percentiles, NULL coverage and post-warmup AddFiles."""
from datetime import datetime, timedelta, date
import json
from uuid import uuid4

from src.common.config import ROOT, load
from src.common.spark_session import get_spark
from src.monitoring.latency import latency_summary, file_statistics


spark = get_spark('latency-metric-checks')
spark.sparkContext.setLogLevel('WARN')
run_id = uuid4().hex
path = load()['lake']['checkpoints'].rstrip('/') + '/latency-metrics-' + run_id
schema = 'event_id string, produced_at timestamp, ingested_at timestamp, event_date date'
base = datetime(2026, 11, 1)
try:
    (spark.createDataFrame([('warmup-0', base, base, date(2019, 11, 1))], schema).coalesce(1)
     .write.format('delta').partitionBy('event_date').save(path))
    before = file_statistics(spark, path)
    rows = [('data-' + str(index), base, base + timedelta(milliseconds=delta), date(2019, 11, 1))
            for index, delta in enumerate([-1, 1, 2, 4])]
    rows.extend([('data-null-producer', None, base, date(2019, 11, 1)),
                 ('data-null-ingestion', base, None, date(2019, 11, 1))])
    (spark.createDataFrame(rows, schema).coalesce(1).write.format('delta').mode('append')
     .partitionBy('event_date').save(path))
    stats = latency_summary(spark, path, event_id_prefix='data-')
    assert stats['rows'] == 6 and stats['unique_event_ids'] == 6 and stats['measured_pairs'] == 4
    assert stats['p50_latency_ms'] == 1.5
    assert abs(stats['p95_latency_ms'] - 3.7) < 1e-10
    assert stats['negative_latency_rows'] == 1 and stats['min_latency_ms'] == -1
    assert stats['produced_at_null_rows'] == stats['ingested_at_null_rows'] == 1
    files = file_statistics(spark, path, after_version=before['file_snapshot_version'])
    assert before['active_files_after'] == 1
    assert files['active_files_after'] == 2 and files['files_created'] == files['small_files_created'] == 1
    empty = latency_summary(spark, path, event_id_prefix='absent-')
    assert empty['rows'] == 0 and empty['p50_latency_ms'] is empty['p95_latency_ms'] is None
    output = ROOT / 'data' / 'raw' / f'e3-latency-metrics-{run_id}.json'
    report = {'event': 'latency_metrics_verified', 'path': path, 'latency': stats, 'files': files,
              'empty': empty, 'artifacts_retained': True}
    with output.open('x') as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps({**report, 'report': str(output)}), flush=True)
finally:
    spark.stop()
