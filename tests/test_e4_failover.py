import json
from datetime import datetime, timedelta, timezone

import pytest

from src.monitoring import e4b_failover as e4
from src.monitoring import terminal_recording as recording


def test_controller_can_record_production_events_without_name_collision(capsys):
    e4.emit('first_query', event='kafka_stream_started', query_id='query')
    result = json.loads(capsys.readouterr().out)
    assert result['event'] == 'first_query'
    assert result['source_event'] == 'kafka_stream_started'
    assert result['query_id'] == 'query'


def test_cohort_spans_two_days_without_late_or_duplicate_ids():
    base = datetime(2019, 11, 1, 15, tzinfo=timezone.utc)
    rows = [{'event_time': (base + timedelta(seconds=i)).isoformat(), 'user_id': i}
            for i in range(10)]
    result = e4.build_cohort(rows, 'e4b-case')
    assert {r['event_time'][:10] for r in result} == {'2019-11-01', '2019-11-02'}
    assert len({r['event_id'] for r in result}) == 10
    assert result == e4.build_cohort(rows, 'e4b-case')
    assert result[0]['event_time'][:10] != result[1]['event_time'][:10]
    assert {r['user_id'] for r in result} == set(range(10))


def test_checkpoint_offsets_are_strict_and_topic_bound():
    raw = 'v1\n{"batchWatermarkMs":0}\n' + json.dumps({'topic': {'0': 5, '1': 8}})
    assert e4.parse_offsets(raw, 'topic') == {'0': 5, '1': 8}


def test_checkpoint_mirrored_union_sources_are_not_double_counted():
    offset = json.dumps({'topic': {'0': 5, '1': 8}})
    raw = 'v1\n{"batchWatermarkMs":0}\n' + offset + '\n' + offset
    assert e4.parse_offsets(raw, 'topic') == {'0': 5, '1': 8}


def test_checkpoint_mirrored_union_sources_must_agree():
    with pytest.raises(ValueError):
        e4.parse_offsets('v1\n{}\n{"topic":{"0":5}}\n{"topic":{"0":6}}', 'topic')


@pytest.mark.parametrize('raw', [
    'v2\n{}\n{"topic":{"0":1}}', 'v1\n{}\n{"other":{"0":1}}',
    'v1\n{}\n{"topic":{"0":-1}}', 'v1\n{}\n{"topic":{"0":true}}',
    'v1\n{}\n{"topic":{"x":1}}', 'v1\n{}\n{"topic":{}}',
    'v1\n{}\n{"topic":{"0":1}}\n{"other":{"0":2}}',
])
def test_invalid_checkpoint_cannot_look_drained(raw):
    with pytest.raises(ValueError):
        e4.parse_offsets(raw, 'topic')


@pytest.mark.parametrize('pid,pgid,owner,cmd', [
    (1, 1, 77, 'org.apache.spark.deploy.SparkSubmit /driver.py --config /case.yaml'),
    (77, 77, 77, 'org.apache.spark.deploy.SparkSubmit /driver.py --config /case.yaml'),
    (88, 7, 77, 'org.apache.spark.deploy.SparkSubmit /driver.py --config /case.yaml'),
    (88, 88, 77, 'org.apache.spark.deploy.master.Master /driver.py --config /case.yaml'),
    (88, 88, 77, 'org.apache.spark.deploy.SparkSubmit /other.py --config /case.yaml'),
    (88, 88, 77, 'org.apache.spark.deploy.SparkSubmit /driver.py --config /other.yaml'),
])
def test_kill_guard_rejects_unsafe_targets(pid, pgid, owner, cmd):
    with pytest.raises(ValueError):
        e4.validate_target(pid, pgid, owner, cmd.split(), '/driver.py', '/case.yaml')


def test_kill_guard_accepts_only_owned_spark_submit():
    e4.validate_target(88, 88, 77,
        ['java', 'org.apache.spark.deploy.SparkSubmit', '/driver.py', '--config', '/case.yaml'],
        '/driver.py', '/case.yaml')


def test_receipts_prove_unique_acknowledged_source_records():
    cohort = [{'event_id': 'a'}, {'event_id': 'b'}]
    rows = [{'index': i, 'event_id': r['event_id'], 'partition': 0, 'offset': i,
             'produced_at': '2026-11-01T00:00:00Z'} for i, r in enumerate(cohort)]
    assert e4.validate_receipts(cohort, rows) == rows
    for bad in (rows[:1], rows + [rows[0]], [rows[0], {**rows[1], 'offset': 0}],
                [rows[0], {**rows[1], 'event_id': 'wrong'}]):
        with pytest.raises(ValueError):
            e4.validate_receipts(cohort, bad)


def test_video_frames_cannot_show_future_output():
    events = [[0, 'o', 'start\n'], [5, 'o', 'kill\n'], [10, 'o', 'PASS\n']]
    assert recording.visible_text(events, 4) == 'start\n'
    assert recording.visible_text(events, 5) == 'start\nkill\n'
    assert recording.visible_text(events, 10) == 'start\nkill\nPASS\n'
