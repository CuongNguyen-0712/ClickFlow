"""Closed-partition ZORDER and retention-checked, preview-first Delta VACUUM."""
import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from src.common.config import ROOT, load


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Dates must use YYYY-MM-DD')
    return date.fromisoformat(value)


@dataclass(frozen=True)
class MaintenancePlan:
    table_path: str
    day: str
    closed_before: str
    retention_hours: int = 720
    time_travel_hours: int = 720
    max_stream_lag_hours: int = 168
    max_transaction_hours: int = 24
    target_file_size_bytes: int = 128 * 1024 * 1024
    small_file_size_bytes: int = 16 * 1024 * 1024

    def __post_init__(self):
        target, boundary = _day(self.day), _day(self.closed_before)
        if not target < boundary <= datetime.now(timezone.utc).date():
            raise ValueError('day must precede closed_before, which cannot be after today UTC')
        if not isinstance(self.table_path, str) or any(
                ord(char) < 32 or char == '`' for char in self.table_path):
            raise ValueError('Invalid Delta S3A table path')
        uri = urlsplit(self.table_path)
        if (uri.scheme != 's3a' or not uri.netloc or not uri.path.strip('/')
                or uri.query or uri.fragment):
            raise ValueError('Expected s3a://bucket/table/path without query or fragment')
        object.__setattr__(self, 'table_path', self.table_path.rstrip('/'))
        for name in ('retention_hours', 'time_travel_hours', 'max_stream_lag_hours',
                     'max_transaction_hours', 'target_file_size_bytes', 'small_file_size_bytes'):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f'{name} must be a positive integer')
        if (self.retention_hours < max(168, self.time_travel_hours)
                or self.retention_hours <= max(self.max_stream_lag_hours, self.max_transaction_hours)):
            raise ValueError('Retention must cover time travel and exceed stream lag / transaction windows')
        if self.small_file_size_bytes > self.target_file_size_bytes:
            raise ValueError('Small-file threshold cannot exceed target file size')


def resolve_plan(config, *, day, closed_before, table_path=None, retention_hours=None):
    options = dict(config.get('maintenance', {}))
    if table_path is None:
        uri = urlsplit(config['lake']['bronze'])
        table_path = urlunsplit((uri.scheme, os.environ.get('LAKE_BUCKET') or uri.netloc,
                                uri.path, uri.query, uri.fragment))
    if retention_hours is not None:
        options['retention_hours'] = retention_hours
    return MaintenancePlan(table_path=table_path, day=day, closed_before=closed_before, **options)


def _interval_hours(value):
    """Parse fixed-duration Delta properties; fail closed on calendar months."""
    if not isinstance(value, str):
        raise ValueError('Unsupported Delta retention interval')
    units = {'week': 168, 'day': 24, 'hour': 1, 'minute': Decimal(1) / 60,
             'second': Decimal(1) / 3600, 'millisecond': Decimal(1) / 3600000,
             'microsecond': Decimal(1) / 3600000000}
    text = re.sub(r'^interval\s+', '', value.strip().lower())
    tokens = re.findall(r'(\d+(?:\.\d+)?)\s*(weeks?|days?|hours?|minutes?|seconds?|milliseconds?|microseconds?)', text)
    remainder = re.sub(r'\d+(?:\.\d+)?\s*(?:weeks?|days?|hours?|minutes?|seconds?|milliseconds?|microseconds?)', '', text)
    if not tokens or remainder.strip():
        raise ValueError(f'Unsupported Delta retention interval: {value}')
    hours = sum((Decimal(amount) * units[unit.rstrip('s')] for amount, unit in tokens), Decimal(0))
    if hours <= 0:
        raise ValueError('Delta retention interval must be positive')
    return hours


def validate_table_retention(plan, properties):
    deleted = _interval_hours(properties.get('delta.deletedFileRetentionDuration', 'interval 1 week'))
    logs = _interval_hours(properties.get('delta.logRetentionDuration', 'interval 30 days'))
    if plan.retention_hours < deleted:
        raise ValueError('Refusing to shorten existing delta.deletedFileRetentionDuration')
    if logs < plan.time_travel_hours:
        raise ValueError('delta.logRetentionDuration does not cover required time travel')


def sql_commands(plan, *, execute_vacuum=False):
    if type(execute_vacuum) is not bool:
        raise ValueError('execute_vacuum must be an explicit boolean')
    table = f'delta.`{plan.table_path}`'
    return {'optimize': f"OPTIMIZE {table} WHERE event_date = DATE '{plan.day}' ZORDER BY (user_id, product_id)",
            'vacuum': f'VACUUM {table} RETAIN {plan.retention_hours} HOURS' + ('' if execute_vacuum else ' DRY RUN')}


def active_file_snapshot(spark, plan):
    """Pinned Delta 3.2.1 snapshot metadata: active AddFiles, not a physical listing."""
    from pyspark.sql import DataFrame, functions as F

    snapshot = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession, plan.table_path).update(False, spark._jvm.scala.Option.empty())
    files = DataFrame(snapshot.allFiles().toDF(), spark)

    def stats(frame):
        row = frame.agg(F.count('*').alias('files'), F.sum('size').alias('bytes'),
                        F.sum(F.when(F.col('size') < plan.small_file_size_bytes, 1).otherwise(0)).alias('small_files'),
                        F.min('size').alias('min_bytes'), F.max('size').alias('max_bytes'),
                        F.avg('size').alias('avg_bytes')).first().asDict()
        return {key: (value if value is not None else 0) for key, value in row.items()}

    return {'version': int(snapshot.version()), 'table': stats(files),
            'partition': stats(files.where(F.col('partitionValues')['event_date'] == plan.day))}


def run_maintenance(spark, plan, *, execute_vacuum=False):
    from delta.tables import DeltaTable

    commands = sql_commands(plan, execute_vacuum=execute_vacuum)
    if spark.conf.get('spark.sql.session.timeZone') != 'UTC':
        raise ValueError('Maintenance requires Spark session timezone UTC')
    safety = 'spark.databricks.delta.retentionDurationCheck.enabled'
    if spark.conf.get(safety, 'true').lower() != 'true':
        raise ValueError(f'{safety} must remain true')
    table = DeltaTable.forPath(spark, plan.table_path)
    detail = table.detail().first().asDict()
    if detail['partitionColumns'] != ['event_date']:
        raise ValueError('Maintenance requires a table partitioned only by event_date')
    types = {field.name: field.dataType.simpleString() for field in table.toDF().schema}
    if any(types.get(name) != kind for name, kind in
           [('event_date', 'date'), ('user_id', 'bigint'), ('product_id', 'bigint')]):
        raise ValueError('Expected C4 event_date date, user_id bigint, product_id bigint')
    validate_table_retention(plan, detail['properties'])
    before = active_file_snapshot(spark, plan)
    report = {'event': 'delta_maintenance', 'run_id': uuid4().hex,
              'started_at': datetime.now(timezone.utc).isoformat(), 'plan': asdict(plan),
              'before': before, 'vacuum': {'mode': 'skipped', 'executed': False}}
    print(json.dumps({**report, 'phase': 'before'}), flush=True)
    if not before['partition']['files']:
        report.update(status='skipped_empty_partition', after=before, elapsed_seconds=0.0)
        print(json.dumps(report), flush=True)
        return report
    started = time.monotonic()
    key = 'spark.databricks.delta.optimize.maxFileSize'
    previous = spark.conf.get(key, None)
    try:
        spark.conf.set(key, str(plan.target_file_size_bytes))
        optimized = [row.asDict(recursive=True) for row in spark.sql(commands['optimize']).collect()]
    finally:
        if previous is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, previous)
    after = active_file_snapshot(spark, plan)
    report.update(status='optimized', after=after, optimize_sql=commands['optimize'], optimize_metrics=optimized,
                  files_reduced=before['partition']['files'] - after['partition']['files'],
                  small_files_reduced=before['partition']['small_files'] - after['partition']['small_files'])
    print(json.dumps({**report, 'phase': 'after_optimize'}, default=str), flush=True)
    # VACUUM is TABLE scoped even though OPTIMIZE used one partition predicate.
    preview_sql = sql_commands(plan)['vacuum']
    candidates = spark.sql(preview_sql)
    report['vacuum'] = {'mode': 'dry_run', 'scope': 'table', 'executed': False,
                        'retention_hours': plan.retention_hours, 'preview_sql': preview_sql,
                        'candidate_paths': candidates.count(),
                        'candidate_path_sample': [row[0] for row in candidates.limit(20).collect()]}
    if execute_vacuum:
        print(json.dumps({'event': 'vacuum_execute_requested', **report['vacuum'],
                          'execute_sql': commands['vacuum']}), flush=True)
        spark.sql(commands['vacuum']).collect()
        report['vacuum'].update(mode='executed', executed=True, execute_sql=commands['vacuum'])
    report['elapsed_seconds'] = round(time.monotonic() - started, 3)
    print(json.dumps(report, default=str), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day', required=True, help='Closed event_date, YYYY-MM-DD UTC')
    parser.add_argument('--closed-before', required=True, help='Exclusive date before which writers/backfills are closed')
    parser.add_argument('--table-path', help='Override configured bronze path, e.g. an E5 fixture')
    parser.add_argument('--retention-hours', type=int, help='May increase but cannot bypass retention guards')
    parser.add_argument('--config', type=Path, help='Optional pipeline YAML')
    parser.add_argument('--report', type=Path, help='New JSON file; existing reports are never overwritten')
    parser.add_argument('--execute-vacuum', action='store_true',
                        help='PHYSICALLY DELETE expired files after OPTIMIZE/preview; requires operator approval')
    args = parser.parse_args()
    if args.config:
        import yaml
        config = yaml.safe_load(args.config.read_text(encoding='utf-8'))
    else:
        config = load()
    plan = resolve_plan(config, day=args.day, closed_before=args.closed_before,
                        table_path=args.table_path, retention_hours=args.retention_hours)
    output = args.report or ROOT / 'data' / 'raw' / f'maintenance-{plan.day}-{uuid4().hex}.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    # Reserve the artifact before any table side effects, refusing overwrite.
    with output.open('x', encoding='utf-8') as stream:
        from src.common.spark_session import get_spark
        spark = get_spark('delta-maintenance')
        spark.sparkContext.setLogLevel('WARN')
        try:
            report = run_maintenance(spark, plan, execute_vacuum=args.execute_vacuum)
            json.dump(report, stream, indent=2, default=str)
            print(json.dumps({'event': 'maintenance_report_written', 'report': str(output)}), flush=True)
        except Exception as error:
            json.dump({'status': 'failed', 'plan': asdict(plan), 'error': str(error)}, stream, indent=2)
            raise
        finally:
            spark.stop()


if __name__ == '__main__':
    main()
