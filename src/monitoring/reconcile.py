"""Read-only, snapshot-pinned daily C1/C4/C5 SQL reconciliation for Processing."""
import argparse
import csv
from datetime import date
import json
from pathlib import Path
import re
from urllib.parse import urlsplit
from uuid import uuid4

from src.common.config import ROOT, load

REPORT_COLUMNS = ('event_date', 'rows_source', 'rows_bronze', 'rows_quarantine',
                  'duplicates_dropped', 'match')


def _date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Expected YYYY-MM-DD')
    return date.fromisoformat(value)


def _table_path(value):
    if not isinstance(value, str) or any(ord(char) < 32 or char == '`' for char in value):
        raise ValueError('Invalid Delta path')
    uri = urlsplit(value)
    if uri.scheme != 's3a' or not uri.netloc or not uri.path.strip('/') or uri.query or uri.fragment:
        raise ValueError('Expected s3a://bucket/table/path')
    return value.rstrip('/')


def _integer(value, name, maximum=2**63 - 1):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(f'{name} must be a nonnegative integer <= {maximum}')
    return value


def load_queries(path):
    text = Path(path).read_text(encoding='utf-8')
    sections = re.split(r'^-- name: ([a-z][a-z0-9_]*)\s*$', text, flags=re.MULTILINE)
    queries = {}
    for name, sql in zip(sections[1::2], sections[2::2]):
        if name in queries or not sql.strip():
            raise ValueError('Duplicate or empty named SQL query')
        queries[name] = sql.strip().removesuffix(';').rstrip()
    if not queries:
        raise ValueError(f'No named SQL queries in {path}')
    return queries


def render_query(sql, *, day, bronze_path, bronze_version):
    values = {'day': str(_date(day)), 'bronze_path': _table_path(bronze_path),
              'bronze_version': str(_integer(bronze_version, 'bronze_version'))}
    def substitute(match):
        if match[1] not in values:
            raise ValueError(f'Unknown SQL parameter: {match[1]}')
        return values[match[1]]
    return re.sub(r'\$\{([^}]+)\}', substitute, sql)


def audit_inputs(manifest, *, day, source_path, bronze_path, quarantine_path, sample=False):
    _date(day)
    missing = {'counters': (None, None, None, False), 'quarantine_dates': [], 'evidence': 'missing'}
    if sample:
        if manifest is not None:
            raise ValueError('Sample mode cannot use a Kafka audit manifest')
        return {**missing, 'counters': (0, 0, 0, True), 'evidence': 'finite_sample_batch'}
    if manifest is None:
        return missing
    if not isinstance(manifest, dict):
        raise ValueError('Audit manifest must be an object')
    for name, value in [('source_path', source_path), ('bronze_path', bronze_path),
                        ('quarantine_path', quarantine_path)]:
        if manifest.get(name) != value.rstrip('/'):
            raise ValueError(f'Audit scope mismatch for {name}')
    counters, seen = missing['counters'], set()
    days = manifest.get('days', [])
    if not isinstance(days, list):
        raise ValueError('Audit days must be a list')
    for item in days:
        if not isinstance(item, dict):
            raise ValueError('Audit day must be an object')
        event_date = str(_date(item.get('event_date')))
        if event_date in seen:
            raise ValueError('Duplicate audit event_date')
        seen.add(event_date)
        counts = tuple(_integer(item.get(key), key) for key in
                       ('duplicates_dropped', 'late_records_dropped', 'durable_duplicates_ignored'))
        if event_date == day:
            counters = (*counts, True)
    mappings, seen = [], set()
    raw_mappings = manifest.get('quarantine_dates', [])
    if not isinstance(raw_mappings, list):
        raise ValueError('quarantine_dates must be a list')
    for item in raw_mappings:
        if not isinstance(item, dict):
            raise ValueError('Quarantine mapping must be an object')
        key = (_integer(item.get('kafka_partition'), 'kafka_partition', 2**31 - 1),
               _integer(item.get('kafka_offset'), 'kafka_offset'))
        if key in seen:
            raise ValueError('Duplicate quarantine partition/offset')
        seen.add(key)
        mappings.append((*key, _date(item.get('event_date'))))
    return {'counters': counters, 'quarantine_dates': mappings, 'evidence': 'manifest'}


def run_verification(spark, source, *, day, source_path, bronze_path, quarantine_path,
                     manifest=None, sample=False, ingest_from=None, ingest_to=None):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from src.common.schemas import QUARANTINE_TABLE_COLUMNS, ddl

    _date(day)
    bronze_path, quarantine_path = _table_path(bronze_path), _table_path(quarantine_path)
    if spark.conf.get('spark.sql.session.timeZone') != 'UTC':
        raise ValueError('Processing queries require timezone UTC')
    if ingest_from is not None:
        _date(ingest_from)
    if ingest_to is not None:
        _date(ingest_to)
    if ingest_from and ingest_to and ingest_from > ingest_to:
        raise ValueError('ingest_from must not be after ingest_to')
    evidence = audit_inputs(manifest, day=day, source_path=source_path, bronze_path=bronze_path,
                            quarantine_path=quarantine_path, sample=sample)
    table = DeltaTable.forPath(spark, bronze_path)
    if table.detail().first().partitionColumns != ['event_date']:
        raise ValueError('C4 must be partitioned by event_date')
    bronze_version = int(table.history(1).first().version)
    if sample:
        quarantine_version = None
        quarantine = spark.createDataFrame([], ddl(QUARANTINE_TABLE_COLUMNS))
    else:
        table = DeltaTable.forPath(spark, quarantine_path)
        if table.detail().first().partitionColumns != ['ingest_date']:
            raise ValueError('C5 must be partitioned by ingest_date')
        quarantine_version = int(table.history(1).first().version)
        quarantine = spark.read.format('delta').option('versionAsOf', quarantine_version).load(quarantine_path)
    if ingest_from:
        quarantine = quarantine.where(F.col('ingest_date') >= F.lit(ingest_from).cast('date'))
    if ingest_to:
        quarantine = quarantine.where(F.col('ingest_date') <= F.lit(ingest_to).cast('date'))
    mapping = spark.createDataFrame(evidence['quarantine_dates'],
        'kafka_partition int, kafka_offset bigint, mapped_event_date date')
    parsed = F.to_date(F.try_to_timestamp(F.get_json_object(F.col('payload'), '$.event_time')))
    ambiguous = F.col('error_reason').isin('parse_error', 'bad_event_time')
    joined = quarantine.join(mapping, ['kafka_partition', 'kafka_offset'], 'left')
    conflicts = (~ambiguous & F.col('mapped_event_date').isNotNull()
                 & ~F.col('mapped_event_date').eqNullSafe(parsed))
    if joined.where(conflicts).limit(1).count():
        raise ValueError('Quarantine date mapping conflicts with validated payload event_time')
    attributed = (joined.withColumn('source_event_date',
                                     F.when(ambiguous, F.col('mapped_event_date')).otherwise(parsed))
                  .drop('mapped_event_date'))
    attributed.createOrReplaceTempView('verify_quarantine')
    spark.createDataFrame([(_date(day), *evidence['counters'])],
        'event_date date, duplicates_dropped bigint, late_records_dropped bigint, '
        'durable_duplicates_ignored bigint, evidence_complete boolean').createOrReplaceTempView('verify_audit')
    # Read CSV once; retain unassignable source timestamps so they cannot disappear into a match.
    scoped_source = source.where((F.to_date('event_time') == F.lit(day).cast('date'))
                                 | F.col('event_time').isNull()).cache()
    scoped_source.createOrReplaceTempView('verify_source')
    params = {'day': day, 'bronze_path': bronze_path, 'bronze_version': bronze_version}
    try:
        queries = {**load_queries(ROOT / 'queries' / 'verify.sql'),
                   **load_queries(ROOT / 'queries' / 'sample_queries.sql')}
        results = {}
        for name, sql in queries.items():
            frame = spark.sql(render_query(sql, **params))
            results[name] = [row.asDict(recursive=True) for row in frame.collect()]
            print(json.dumps({'event': 'processing_query_completed', 'query': name,
                              'result_rows': len(results[name])}), flush=True)
        return {'event': 'processing_verification', 'parameters': params,
                'source_path': source_path, 'quarantine_path': quarantine_path,
                'quarantine_version': quarantine_version, 'sample': sample,
                'ingest_from': ingest_from, 'ingest_to': ingest_to,
                'audit_evidence': evidence['evidence'], 'results': results}
    finally:
        scoped_source.unpersist()


def main():
    from src.common.spark_csv import read_source_csv
    from src.common.spark_session import get_spark
    from src.storage.init_lake import lake_paths

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day', default='2019-11-01')
    parser.add_argument('--sample', action='store_true', help='Finite sample CSV/C4; no Kafka drops or C5 stage')
    parser.add_argument('--source-path', help='Closed C1 CSV cohort; default configured month or sample file')
    parser.add_argument('--bronze-path')
    parser.add_argument('--quarantine-path')
    parser.add_argument('--audit-manifest', type=Path, help='Path-bound, per-day measured drop counts and C5 dates')
    parser.add_argument('--ingest-from', help='Optional inclusive C5 ingest_date bound, UTC')
    parser.add_argument('--ingest-to', help='Optional inclusive C5 ingest_date bound, UTC')
    parser.add_argument('--report', type=Path, help='New JSON report; same stem CSV contains six required columns')
    args = parser.parse_args()
    _date(args.day)
    config, paths = load(), lake_paths()
    source_path = args.source_path or (config['stream']['sample_path'] if args.sample else
                                     paths['raw_csv'].rstrip('/') + '/' + config['simulator']['files'][0])
    bronze_path = _table_path(args.bronze_path or (config['stream']['sample_bronze_path'] if args.sample else paths['bronze']))
    quarantine_path = _table_path(args.quarantine_path or paths['quarantine'])
    manifest = json.loads(args.audit_manifest.read_text(encoding='utf-8')) if args.audit_manifest else None
    # Validate external evidence before starting Spark or reading the large source file.
    audit_inputs(manifest, day=args.day, source_path=source_path, bronze_path=bronze_path,
                 quarantine_path=quarantine_path, sample=args.sample)
    output = args.report or ROOT / 'data' / 'raw' / f'processing-verify-{args.day}-{uuid4().hex}.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    csv_path = output.with_suffix('.csv')
    if output == csv_path:
        raise ValueError('--report must have a .json extension')
    with output.open('x', encoding='utf-8') as report_file, csv_path.open('x', encoding='utf-8', newline='') as csv_file:
        spark = None
        try:
            spark = get_spark('processing-verification')
            spark.sparkContext.setLogLevel('WARN')
            report = run_verification(spark, read_source_csv(spark, source_path), day=args.day,
                source_path=source_path, bronze_path=bronze_path, quarantine_path=quarantine_path,
                manifest=manifest, sample=args.sample, ingest_from=args.ingest_from, ingest_to=args.ingest_to)
            json.dump(report, report_file, indent=2, default=str)
            writer = csv.writer(csv_file)
            writer.writerow(REPORT_COLUMNS)
            for row in report['results']['reconciliation']:
                writer.writerow([str(row[name]).lower() if isinstance(row[name], bool) else row[name]
                                 for name in REPORT_COLUMNS])
            matched = all(row['match'] for row in report['results']['reconciliation'])
            print(json.dumps({'event': 'processing_report_written', 'match': matched,
                              'report': str(output), 'csv': str(csv_path)}, default=str), flush=True)
        except Exception as error:
            json.dump({'event': 'processing_verification_failed', 'error': str(error)}, report_file, indent=2)
            raise
        finally:
            if spark is not None:
                spark.stop()
    if not matched:
        raise SystemExit(2)


if __name__ == '__main__':
    main()
