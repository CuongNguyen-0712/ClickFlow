"""Exact signed produced_at -> ingested_at latency and Delta file metrics."""
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
from statistics import fmean, stdev


def aggregate_trials(rows, *, require_complete=True):
    """Mean/SD of per-run percentiles, never pooled event percentiles."""
    if not rows:
        raise ValueError('No measured trials')
    groups, seen, hashes, sizes, environments = defaultdict(list), set(), set(), set(), set()
    for row in rows:
        try:
            key = (row['trigger_seconds'], row['speed'], row['repetition'])
            if (any(type(value) is not int or value <= 0 for value in key) or key in seen
                    or key[0] not in (5, 10, 30) or key[1] not in (100, 1000)):
                raise ValueError('Invalid or duplicate trial identity')
            seen.add(key)
            count_fields = ('expected_rows', 'rows', 'unique_event_ids', 'delivered_records',
                'produced_at_null_rows', 'ingested_at_null_rows', 'negative_latency_rows',
                'quarantine_rows', 'duplicates_dropped', 'late_records_dropped',
                'durable_duplicates_ignored', 'files_created', 'small_files_created')
            if any(type(row[name]) is not int or row[name] < 0 for name in count_fields):
                raise ValueError('Counts must be nonnegative integers')
            if not row['expected_rows'] or any(row[name] != row['expected_rows'] for name in
                    ('rows', 'unique_event_ids', 'delivered_records')):
                raise ValueError('Lost or duplicated measured records')
            if any(row[name] for name in ('produced_at_null_rows', 'ingested_at_null_rows',
                    'quarantine_rows', 'duplicates_dropped', 'late_records_dropped', 'durable_duplicates_ignored')):
                raise ValueError('A measured record was rejected or excluded')
            if row['small_files_created'] > row['files_created'] or row['negative_latency_rows'] > row['rows']:
                raise ValueError('Inconsistent subset count')
            for name in ('p50_latency_ms', 'p95_latency_ms', 'actual_speed'):
                if isinstance(row[name], bool) or not isinstance(row[name], (int, float)) or not math.isfinite(row[name]):
                    raise ValueError(f'Non-finite measurement: {name}')
            if row['p95_latency_ms'] < row['p50_latency_ms'] or row['actual_speed'] <= 0:
                raise ValueError('Invalid percentile/pace')
            if type(row['pace_target_met']) is not bool or not row['workload_sha256']:
                raise ValueError('Missing pace/workload identity')
            if row['pace_target_met'] != (0.9 <= row['actual_speed'] / row['speed'] <= 1.1):
                raise ValueError('Pace flag contradicts observed speed')
            hashes.add(row['workload_sha256'])
            sizes.add(row['expected_rows'])
            environments.add(row['experiment_fingerprint'])
            groups[key[:2]].append(row)
        except (KeyError, TypeError) as error:
            raise ValueError('Incomplete measured trial') from error
    if len(hashes) != 1 or len(sizes) != 1 or len(environments) != 1:
        raise ValueError('Trials use different input cohorts or experiment configurations')
    if require_complete and (set(groups) != {(t, s) for t in (5, 10, 30) for s in (100, 1000)}
                             or any(len(group) < 3 for group in groups.values())):
        raise ValueError('E3 requires at least three trials for all six configurations')
    summaries = []
    for (trigger, speed), group in sorted(groups.items()):
        result = {'trigger_seconds': trigger, 'speed': speed, 'repetitions': len(group),
                  'rows_per_trial': group[0]['expected_rows'],
                  'negative_latency_rows_total': sum(r['negative_latency_rows'] for r in group),
                  'pace_target_misses': sum(not r['pace_target_met'] for r in group)}
        for field in ('p50_latency_ms', 'p95_latency_ms', 'files_created', 'small_files_created', 'actual_speed'):
            values = [r[field] for r in group]
            result[field + '_mean'] = fmean(values)
            result[field + '_sd'] = stdev(values) if len(values) > 1 else 0.0
        summaries.append(result)
    return summaries


def latency_summary(spark, table_path, *, event_id_prefix=None):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    version = int(DeltaTable.forPath(spark, table_path).history(1).first().version)
    frame = spark.read.format('delta').option('versionAsOf', version).load(table_path)
    if event_id_prefix is not None:
        frame = frame.where(F.col('event_id').startswith(event_id_prefix))
    frame = frame.withColumn('latency_ms',
        (F.unix_micros('ingested_at') - F.unix_micros('produced_at')) / F.lit(1000.0))
    row = frame.agg(F.count('*').alias('rows'), F.countDistinct('event_id').alias('unique_event_ids'),
        F.count('latency_ms').alias('measured_pairs'),
        F.sum(F.col('produced_at').isNull().cast('long')).alias('produced_at_null_rows'),
        F.sum(F.col('ingested_at').isNull().cast('long')).alias('ingested_at_null_rows'),
        F.sum((F.col('latency_ms') < 0).cast('long')).alias('negative_latency_rows'),
        F.min('latency_ms').alias('min_latency_ms'), F.max('latency_ms').alias('max_latency_ms'),
        F.expr('percentile(latency_ms, array(0.5, 0.95))').alias('percentiles')).first().asDict()
    percentiles = row.pop('percentiles') or [None, None]
    for name in ('produced_at_null_rows', 'ingested_at_null_rows', 'negative_latency_rows'):
        row[name] = int(row[name] or 0)
    return {**row, 'p50_latency_ms': percentiles[0], 'p95_latency_ms': percentiles[1],
            'delta_version': version, 'latency_definition': 'signed batch-start ingested_at minus produced_at; milliseconds'}


def file_statistics(spark, table_path, *, after_version=None, small_file_bytes=16 * 1024 * 1024):
    from pyspark.sql import DataFrame, functions as F

    snapshot = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession, table_path).update(False, spark._jvm.scala.Option.empty())
    version = int(snapshot.version())
    active = DataFrame(snapshot.allFiles().toDF(), spark)
    active_stats = active.agg(F.count('*').alias('active_files_after'),
                              F.sum('size').alias('active_bytes_after')).first().asDict()
    result = {**{key: int(value or 0) for key, value in active_stats.items()},
              'file_snapshot_version': version, 'small_file_threshold_bytes': small_file_bytes}
    if after_version is None:
        return result
    if type(after_version) is not int or not -1 <= after_version <= version:
        raise ValueError('Invalid file-count baseline version')
    if after_version == version:
        return {**result, 'files_created': 0, 'small_files_created': 0, 'bytes_created': 0}
    logs = [f'{table_path.rstrip("/")}/_delta_log/{index:020d}.json'
            for index in range(after_version + 1, version + 1)]
    actions = spark.read.json(logs)
    if 'add' not in actions.columns:
        return {**result, 'files_created': 0, 'small_files_created': 0, 'bytes_created': 0}
    generated = actions.where(F.col('add').isNotNull()).select(F.col('add.size').alias('size'))
    stats = generated.agg(F.count('*').alias('files_created'), F.sum('size').alias('bytes_created'),
        F.sum((F.col('size') < small_file_bytes).cast('long')).alias('small_files_created')).first().asDict()
    return {**result, **{key: int(value or 0) for key, value in stats.items()}}


def main():
    from src.common.spark_session import get_spark
    from src.storage.init_lake import lake_paths

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table-path')
    parser.add_argument('--event-id-prefix')
    parser.add_argument('--after-version', type=int)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    spark = get_spark('latency-profile')
    try:
        path = args.table_path or lake_paths()['bronze']
        report = {**latency_summary(spark, path, event_id_prefix=args.event_id_prefix),
                  **file_statistics(spark, path, after_version=args.after_version), 'table_path': path}
        if args.output:
            with args.output.open('x', encoding='utf-8') as stream:
                json.dump(report, stream, indent=2)
        print(json.dumps(report), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
