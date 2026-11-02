import csv
import json
from pathlib import Path

import pytest

from src.common.schemas import SOURCE_COLUMNS
from src.monitoring import e5_storage as storage
from src.monitoring import e5_report as report


def source_file(tmp_path, rows):
    path = tmp_path / 'source.csv'
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow([name for name, _ in SOURCE_COLUMNS])
        for day, user in rows:
            writer.writerow([day + ' 00:00:00 UTC', 'view', 123, 2053013554658804075,
                             '', '', 12.50, user, ''])
    return path


def test_extract_preserves_real_dates_int64_nulls_and_multiplicity(tmp_path):
    source = source_file(tmp_path, [('2019-11-01', 5)] * 3 + [('2019-11-02', 7)] * 2)
    target = tmp_path / 'cohort.csv'
    result = storage.extract_cohort(source, target, rows_per_day=2)
    assert result['rows'] == 4 and result['scanned_rows'] == 5
    assert result['rows_by_day'] == {'2019-11-01': 2, '2019-11-02': 2}
    with target.open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 4 and rows[0] == rows[1]
    assert rows[0]['category_id'] == '2053013554658804075'
    assert rows[0]['category_code'] == rows[0]['brand'] == rows[0]['user_session'] == ''


def test_extract_refuses_missing_day_and_existing_output(tmp_path):
    source = source_file(tmp_path, [('2019-11-01', 5)] * 2)
    with pytest.raises(ValueError, match='quota'):
        storage.extract_cohort(source, tmp_path / 'partial.csv', rows_per_day=2)
    with pytest.raises(FileExistsError):
        storage.extract_cohort(source, source, rows_per_day=2)


@pytest.mark.parametrize('value', [0, -1, True, 1.5])
def test_extract_rejects_invalid_quota(tmp_path, value):
    with pytest.raises(ValueError):
        storage.extract_cohort(source_file(tmp_path, []), tmp_path / 'out.csv', rows_per_day=value)


def test_variant_matrix_and_balanced_trials():
    variants = storage.variant_specs()
    assert len(variants) == 8 and len({v['variant_id'] for v in variants}) == 8
    trials = storage.trial_schedule(seed=42)
    assert len(trials) == 72 and trials == storage.trial_schedule(seed=42)
    assert len({(r['variant_id'], r['query'], r['repetition']) for r in trials}) == 72
    for repetition in (1, 2, 3):
        assert sum(r['repetition'] == repetition for r in trials) == 24


def test_queries_all_read_data_and_filter_the_partition():
    queries = storage.query_sql(day='2019-11-01', user_id=7, product_id=123)
    assert len(queries) == 3
    assert all("event_date = DATE '2019-11-01'" in sql for sql in queries.values())
    assert 'COUNT(DISTINCT user_id)' in queries['event_type_totals']
    assert 'DECIMAL' in queries['product_activity']
    assert 'user_id = 7' in queries['user_product_lookup']
    with pytest.raises(ValueError):
        storage.query_sql(day="2019-11-01' OR 1=1", user_id=7, product_id=123)


def test_pruning_needs_runtime_scan_reduction_and_real_filter():
    scan = {'numFiles': 8, 'filesSize': 100, 'partition_filters': '[event_date = 2019-11-01]'}
    assert storage.pruning_evidence([scan], total_files=16, partitioned=True)['verified']
    assert not storage.pruning_evidence([{**scan, 'numFiles': 16}], total_files=16, partitioned=True)['verified']
    assert not storage.pruning_evidence([{**scan, 'partition_filters': '[]'}], total_files=16, partitioned=True)['verified']
    assert not storage.pruning_evidence([], total_files=16, partitioned=True)['verified']


def full_trials():
    return [{**cell, 'elapsed_ms': 100.0 + cell['repetition'], 'result_sha256': 'same-' + cell['query'],
             'expected_sha256': 'same-' + cell['query'], 'scans': [{'numFiles': 1}], 'verified': True}
            for cell in storage.trial_schedule()]


def test_summary_requires_all_72_valid_measurements():
    trials = full_trials()
    summary = report.aggregate_trials(trials)
    assert len(summary) == 24 and all(row['repetitions'] == 3 for row in summary)
    assert all(row['median_ms'] == 102.0 for row in summary)
    for bad in (trials[:-1], trials + [trials[0]],
                [{**trials[0], 'elapsed_ms': float('nan')}] + trials[1:],
                [{**trials[0], 'result_sha256': 'different'}] + trials[1:]):
        with pytest.raises(ValueError):
            report.aggregate_trials(bad)


def test_result_digest_ignores_row_order_but_preserves_duplicates():
    assert storage.result_digest([{'a': 1}, {'a': 2}]) == storage.result_digest([{'a': 2}, {'a': 1}])
    assert storage.result_digest([{'a': 1}, {'a': 1}]) != storage.result_digest([{'a': 1}])


def report_fixture(tmp_path):
    variants = storage.variant_specs()
    for name in ('trials', 'plans', 'snapshots'):
        (tmp_path / name).mkdir()
    for spec in variants:
        spec.update(multiset_verified=True, version=1 if spec['format'] == 'delta' else None,
                    active_data_files=2 if spec['partitioned'] else 1, active_data_bytes=200,
                    physical_total_files=3, physical_total_bytes=300,
                    snapshot_evidence='snapshots/' + spec['variant_id'] + '.json')
        (tmp_path / spec['snapshot_evidence']).write_text(json.dumps({'active_files': [
            {'size': 200 // spec['active_data_files']} for _ in range(spec['active_data_files'])]}))
        for query in storage.QUERIES:
            (tmp_path / 'plans' / f"{spec['variant_id']}--{query}.txt").write_text('Scan with runtime evidence')
    (tmp_path / 'variants.json').write_text(json.dumps(variants))
    (tmp_path / 'manifest.json').write_text(json.dumps({'workload': {'rows': 4,
        'rows_by_day': {'2019-11-01': 2, '2019-11-02': 2}},
        'expected_result_hashes': {q: 'same-' + q for q in storage.QUERIES}}))
    specs = {s['variant_id']: s for s in variants}
    for number, trial in enumerate(full_trials(), 1):
        spec = specs[trial['variant_id']]
        scans = [{'numFiles': 1, 'filesSize': 100, 'partition_filters': '[event_date = 2019-11-01]'
                   if spec['partitioned'] else '[]'}]
        trial.update(trial_number=number, result_rows=1, scans=scans,
                     pruning=storage.pruning_evidence(scans, total_files=spec['active_data_files'],
                                                      partitioned=spec['partitioned']))
        (tmp_path / 'trials' / f'{number:02d}.json').write_text(json.dumps(trial))


def test_reports_can_rebuild_into_new_directory_without_touching_measurements(tmp_path):
    report_fixture(tmp_path)
    original = (tmp_path / 'trials/01.json').read_bytes()
    first = report.build_reports(tmp_path, output=tmp_path / 'report-v1')
    first_csv = (tmp_path / 'report-v1/comparison.csv').read_bytes()
    second = report.build_reports(tmp_path, output=tmp_path / 'report-v2')
    assert first['passed'] and second['passed']
    assert (tmp_path / 'trials/01.json').read_bytes() == original
    assert (tmp_path / 'report-v1/comparison.csv').read_bytes() == first_csv
    assert (tmp_path / 'report-v2/storage-comparison.png').is_file()
    assert '(../trials/)' in (tmp_path / 'report-v2/README.md').read_text()


def test_report_rejects_stale_pruning_flag_and_missing_plans(tmp_path):
    report_fixture(tmp_path)
    path = next(p for p in (tmp_path / 'trials').glob('*.json') if json.loads(p.read_text())['pruning']['verified'])
    original = path.read_text()
    trial = json.loads(original)
    trial['scans'][0]['numFiles'] = trial['pruning']['total_active_files']
    path.write_text(json.dumps(trial))
    with pytest.raises(ValueError, match='pruning'):
        report.build_reports(tmp_path)
    path.write_text(original)
    plan = next((tmp_path / 'plans').glob('*.txt'))
    plan.write_text('')
    with pytest.raises(ValueError, match='plan'):
        report.build_reports(tmp_path)
