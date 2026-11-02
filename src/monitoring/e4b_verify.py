"""Exact E4b producer/C4 comparison and existing Processing daily reconciliation."""
import argparse
import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path

from src.common.schemas import SOURCE_COLUMNS, ddl
from src.monitoring.e4b_failover import save_json, validate_receipts


def verify(spark, directory):
    from src.common.spark_csv import read_source_csv
    from src.monitoring.reconcile import REPORT_COLUMNS, run_verification

    case = json.loads((directory / 'case.json').read_text())
    cohort = json.loads((directory / 'cohort.json').read_text())
    receipts = validate_receipts(cohort, [json.loads(line) for line in
                                        (directory / 'deliveries.jsonl').read_text().splitlines()])
    if hashlib.sha256((directory / 'source.csv').read_bytes()).hexdigest() != case['source_sha256']:
        raise ValueError('Source CSV hash changed')
    expected_rows = []
    for source, receipt in zip(cohort, receipts):
        item = dict(source)
        item['event_time'] = datetime.fromisoformat(item['event_time'].replace('Z', '+00:00')).replace(tzinfo=None)
        values = [item[name] for name, _ in SOURCE_COLUMNS]
        produced = datetime.fromisoformat(receipt['produced_at'].replace('Z', '+00:00')).replace(tzinfo=None)
        expected_rows.append((*values, source['event_id'], receipt['partition'], receipt['offset'], produced,
                              item['event_time'].date()))
    expected_schema = SOURCE_COLUMNS + [('event_id', 'string'), ('kafka_partition', 'int'),
        ('kafka_offset', 'bigint'), ('produced_at', 'timestamp'), ('event_date', 'date')]
    expected = spark.createDataFrame(expected_rows, ddl(expected_schema)).cache()
    bronze = spark.read.format('delta').load(case['bronze_path']).cache()
    quarantine = spark.read.format('delta').load(case['quarantine_path'])
    names = [name for name, _ in expected_schema]
    source = read_source_csv(spark, case['source_path']).cache()
    source_names = [name for name, _ in SOURCE_COLUMNS]
    missing = expected.select('event_id').join(bronze.select('event_id'), 'event_id', 'left_anti').count()
    extra = bronze.select('event_id').join(expected.select('event_id'), 'event_id', 'left_anti').count()
    count, distinct = bronze.count(), bronze.select('event_id').distinct().count()
    mismatched = expected.select(*names).exceptAll(bronze.select(*names)).count()
    unexpected = bronze.select(*names).exceptAll(expected.select(*names)).count()
    csv_mismatches = (source.select(*source_names).exceptAll(expected.select(*source_names)).count()
                      + expected.select(*source_names).exceptAll(source.select(*source_names)).count())
    gate = json.loads((directory / 'gate.json').read_text())
    prior = spark.read.format('delta').option('versionAsOf', gate['bronze_version']).load(case['bronze_path']).cache()
    prior_count = prior.count()
    changed = prior.exceptAll(bronze.select(*prior.columns)).count()
    quarantine_count = quarantine.count()
    exact = (count == distinct == case['rows'] == len(receipts) and missing == extra == mismatched
             == unexpected == csv_mismatches == quarantine_count == changed == 0
             and prior_count == gate['driver_inserted_rows'] and prior_count > 1000)
    # The finite source contains each event/offset once. Replay attempts are not
    # additional source messages; counters from retries cannot enter this balance.
    manifest = {key: case[key] for key in ('source_path', 'bronze_path', 'quarantine_path')}
    manifest.update(days=[{'event_date': day, 'duplicates_dropped': 0, 'late_records_dropped': 0,
                          'durable_duplicates_ignored': 0} for day in case['days']],
                    evidence={'unique_source_receipts': len(receipts), 'exact_source_c4_comparison': exact,
                              'replay_attempt_metrics': 'recovery-metrics.prom'})
    if not exact:
        save_json(directory / 'verification-failure.json', {'missing': missing, 'extra': extra,
            'rows_bronze': count, 'distinct': distinct, 'mismatched': mismatched, 'unexpected': unexpected,
            'csv_mismatches': csv_mismatches, 'quarantine': quarantine_count,
            'prekill_rows': prior_count, 'changed_preexisting_rows': changed})
        raise AssertionError('Exact source/C4/historical comparison failed')
    save_json(directory / 'audit-manifest.json', manifest)
    reconciliation = []
    for day in case['days']:
        report = run_verification(spark, source, day=day, source_path=case['source_path'],
            bronze_path=case['bronze_path'], quarantine_path=case['quarantine_path'], manifest=manifest)
        save_json(directory / f'reconcile-{day}.json', report)
        reconciliation.extend(report['results']['reconciliation'])
    passed = exact and len(reconciliation) == len(case['days']) and all(r['match'] is True for r in reconciliation)
    result = {'passed': passed, 'rows_source': len(receipts), 'rows_bronze': count, 'distinct_event_ids': distinct,
              'duplicate_ids': count-distinct, 'missing': missing, 'extra': extra, 'field_mismatches': mismatched,
              'csv_mismatches': csv_mismatches, 'quarantine': quarantine_count, 'prekill_version': gate['bronze_version'],
              'prekill_rows_unchanged': prior_count, 'changed_preexisting_rows': changed,
              'reconciliation': reconciliation,
              'scope': 'Controlled real 100k sample subset; timestamps shifted over two dates; no injected bad rows'}
    save_json(directory / 'verification.json', result)
    with (directory / 'reconcile.csv').open('x', encoding='utf-8', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(reconciliation)
    prior.unpersist()
    source.unpersist()
    bronze.unpersist()
    expected.unpersist()
    if not passed:
        raise AssertionError('Daily reconciliation mismatch')
    return result


def main():
    from src.common.spark_session import get_spark
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    spark = get_spark('e4b-verification')
    spark.sparkContext.setLogLevel('WARN')
    try:
        print(json.dumps(verify(spark, args.directory), default=str), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
