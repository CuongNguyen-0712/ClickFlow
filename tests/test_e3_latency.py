"""Measured configurations and aggregation must not hide missing/invalid trials."""
from copy import deepcopy
import csv
from types import SimpleNamespace

import pytest

from src.monitoring import latency
from src.monitoring import e3_benchmark as e3


def trial_rows():
    rows = []
    for trigger in (5, 10, 30):
        for speed in (100, 1000):
            for rep in (1, 2, 3):
                rows.append(dict(trigger_seconds=trigger, speed=speed, repetition=rep,
                    expected_rows=100000, rows=100000, unique_event_ids=100000,
                    produced_at_null_rows=0, ingested_at_null_rows=0,
                    p50_latency_ms=100.0 * rep, p95_latency_ms=1000.0 * rep,
                    negative_latency_rows=0, quarantine_rows=0, duplicates_dropped=0,
                    late_records_dropped=0, durable_duplicates_ignored=0,
                    delivered_records=100000, files_created=10-rep, small_files_created=10-rep,
                    workload_sha256='fixture-sha', experiment_fingerprint='fixture-config',
                    actual_speed=float(speed), pace_target_met=True))
    return rows


def test_balanced_matrix_has_three_independent_repetitions_per_cell():
    rows = e3.build_trial_matrix(seed=42)
    assert len(rows) == 18
    assert {(r['trigger_seconds'], r['speed'], r['repetition']) for r in rows} == {
        (trigger, speed, rep) for trigger in (5, 10, 30) for speed in (100, 1000) for rep in (1, 2, 3)}
    assert [r['repetition'] for r in rows[:6]] == [1] * 6
    assert e3.build_trial_matrix(seed=42) == rows
    assert e3.build_trial_matrix(seed=7) != rows


@pytest.mark.parametrize('reps', [0, 1, 2, True, 3.0])
def test_full_e3_rejects_fewer_than_three_or_invalid_repetitions(reps):
    with pytest.raises(ValueError):
        e3.build_trial_matrix(repetitions=reps)


def test_summary_is_mean_of_run_percentiles_with_sample_sd():
    summary = latency.aggregate_trials(trial_rows())
    assert len(summary) == 6
    cell = next(r for r in summary if r['trigger_seconds'] == 5 and r['speed'] == 100)
    assert cell['repetitions'] == 3
    assert cell['p50_latency_ms_mean'] == 200.0
    assert cell['p50_latency_ms_sd'] == 100.0
    assert cell['p95_latency_ms_mean'] == 2000.0
    assert cell['p95_latency_ms_sd'] == 1000.0
    assert cell['files_created_mean'] == 8.0
    assert cell['files_created_sd'] == 1.0


def test_missing_cell_or_repetition_cannot_be_published_as_complete():
    with pytest.raises(ValueError):
        latency.aggregate_trials(trial_rows()[:-1])
    with pytest.raises(ValueError):
        latency.aggregate_trials([r for r in trial_rows() if r['speed'] == 100])


def test_duplicate_repetition_cannot_inflate_the_sample_size():
    rows = trial_rows()
    rows[-1] = deepcopy(rows[-2])
    with pytest.raises(ValueError):
        latency.aggregate_trials(rows)


@pytest.mark.parametrize('key,bad', [
    ('rows', 99999), ('unique_event_ids', 99999), ('delivered_records', 99999),
    ('produced_at_null_rows', 1), ('ingested_at_null_rows', 1),
    ('quarantine_rows', 1), ('duplicates_dropped', 1), ('late_records_dropped', 1),
    ('durable_duplicates_ignored', 1), ('p50_latency_ms', None),
    ('p95_latency_ms', float('nan')), ('p95_latency_ms', float('inf')),
    ('files_created', -1), ('small_files_created', 999), ('workload_sha256', 'other'),
    ('experiment_fingerprint', 'different-spark-or-producer-config'),
    ('pace_target_met', False),
])
def test_incomparable_lost_or_invalid_data_cannot_be_aggregated(key, bad):
    rows = trial_rows()
    rows[0][key] = bad
    with pytest.raises(ValueError):
        latency.aggregate_trials(rows)


def test_negative_timestamp_differences_are_exposed_not_clipped():
    rows = trial_rows()
    rows[0].update(p50_latency_ms=-10.0, negative_latency_rows=100)
    summary = latency.aggregate_trials(rows)
    assert summary[0]['negative_latency_rows_total'] == 100
    assert summary[0]['p50_latency_ms_mean'] == pytest.approx(490 / 3)


def test_input_pacing_shortfall_is_reported_not_silently_renamed():
    rows = trial_rows()
    rows[0].update(actual_speed=70.0, pace_target_met=False)
    assert latency.aggregate_trials(rows)[0]['pace_target_misses'] == 1


def test_workload_preserves_source_values_sorts_time_and_exact_int64(tmp_path):
    path = tmp_path / 'source.csv'
    columns = ['event_time', 'event_type', 'product_id', 'category_id', 'category_code',
               'brand', 'price', 'user_id', 'user_session']
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerow(['2019-11-01 15:35:09 UTC', 'view', 3, '2053013555631882655', '', '', 4.5, 9, ''])
        writer.writerow(['2019-11-01 15:35:08 UTC', 'cart', 4, '2053013555631882655', 'electronics', '', 2.5, 10, ''])
    rows, metadata = e3.load_workload(path, limit=2)
    assert rows[0]['event_time'] == '2019-11-01T15:35:08Z'
    assert rows[1]['category_id'] == 2053013555631882655
    assert rows[1]['brand'] is None and rows[1]['category_code'] is None
    assert metadata['event_span_seconds'] == 1.0
    assert len(metadata['sha256']) == 64


@pytest.mark.parametrize('kind', ['ready', 'no_partitions', 'leader_pending', 'isr_pending', 'wrong_rf'])
def test_topic_readiness_requires_broker_metadata_not_only_create_ack(kind):
    parts = {index: SimpleNamespace(replicas=[1, 2, 3], isrs=[1, 2, 3], leader=1) for index in range(6)}
    if kind == 'no_partitions':
        parts = {}
    elif kind == 'leader_pending':
        parts[0].leader = -1
    elif kind == 'isr_pending':
        parts[0].isrs = [1, 2]
    elif kind == 'wrong_rf':
        parts[0].replicas = [1, 2]
    assert e3.topic_ready(SimpleNamespace(error=None, partitions=parts)) is (kind == 'ready')


def test_derived_reports_rebuild_after_interruption_preserving_raw_trial(tmp_path):
    from src.monitoring.e3_report import write_results
    (tmp_path / 'trials.csv').write_text('interrupted export')
    (tmp_path / 'trial-01.json').write_text('immutable evidence')
    write_results(tmp_path, trial_rows())
    assert (tmp_path / 'trial-01.json').read_text() == 'immutable evidence'
    with (tmp_path / 'summary.csv').open() as stream:
        summary = list(csv.DictReader(stream))
    assert len(summary) == 6
    assert all(row['repetitions'] == '3' for row in summary)
    assert all((tmp_path / name).stat().st_size > 100 for name in
               ('latency.png', 'latency.svg', 'tradeoff.png', 'tradeoff.svg'))


@pytest.mark.parametrize('now,trigger,phase,want', [
    (44.955, 30, 1/6, 65.0), (44.955, 30, 0.5, 45.0),
    (45.0, 30, 0.5, 75.0), (33.0, 5, 0.5, 37.5),
])
def test_emission_phase_uses_spark_epoch_grid(now, trigger, phase, want):
    assert e3.next_phase_time(now, trigger, phase) == pytest.approx(want)


def test_pipeline_and_contract_changes_invalidate_resume_identity(tmp_path):
    for relative in e3.IMPLEMENTATION_FILES:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('original')
    before = e3.implementation_hashes(tmp_path)
    (tmp_path / 'src/storage/quality_rules.py').write_text('changed classifier')
    changed_code = e3.implementation_hashes(tmp_path)
    assert changed_code['src/storage/quality_rules.py'] != before['src/storage/quality_rules.py']
    (tmp_path / 'contracts/event.schema.json').write_text('changed C3')
    changed_contract = e3.implementation_hashes(tmp_path)
    assert changed_contract['contracts/event.schema.json'] != before['contracts/event.schema.json']


@pytest.mark.parametrize('changed', ['trigger_seconds', 'repetition', 'workload_sha256',
                                     'experiment_fingerprint', 'session_id', 'row_count'])
def test_cached_trial_must_match_this_session_cell_and_workload(changed):
    row = {**trial_rows()[0], 'session_id': 'session-a'}
    cell = {'trigger_seconds': 5, 'speed': 100, 'repetition': 1}
    if changed == 'row_count':
        for name in ('expected_rows', 'rows', 'unique_event_ids', 'delivered_records'):
            row[name] = 50000
    else:
        row[changed] = {'trigger_seconds': 10, 'repetition': 2, 'workload_sha256': 'other-input',
                        'experiment_fingerprint': 'other-code', 'session_id': 'session-b'}[changed]
    with pytest.raises(ValueError):
        e3.validate_cached_trial(row, cell, {'sha256': 'fixture-sha', 'rows': 100000},
                                 fingerprint='fixture-config', session_id='session-a')
