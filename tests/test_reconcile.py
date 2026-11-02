"""Reconciliation must not manufacture drops or accept mismatched evidence."""
from datetime import date

import pytest

from src.monitoring import reconcile


SCOPE = {'source_path': '/opt/app/data/sample/events_sample_100k.csv',
         'bronze_path': 's3a://lake/bronze/events',
         'quarantine_path': 's3a://lake/quarantine/events'}


def manifest(**changes):
    result = {**SCOPE, 'days': [{'event_date': '2019-11-01', 'duplicates_dropped': 3,
                              'late_records_dropped': 2, 'durable_duplicates_ignored': 1}],
              'quarantine_dates': [{'kafka_partition': 0, 'kafka_offset': 4,
                                    'event_date': '2019-11-01'}]}
    result.update(changes)
    return result


def audit(value=None, **changes):
    scope = {**SCOPE, 'day': '2019-11-01', **changes}
    return reconcile.audit_inputs(value, **scope)


def test_missing_evidence_remains_unknown():
    actual = audit()
    assert actual['counters'] == (None, None, None, False)
    assert actual['quarantine_dates'] == []


def test_sample_batch_has_no_streaming_drops_or_quarantine():
    assert audit(sample=True)['counters'] == (0, 0, 0, True)
    with pytest.raises(ValueError):
        audit(manifest(), sample=True)


def test_manifest_returns_independent_counts_and_source_dates():
    actual = audit(manifest())
    assert actual['counters'] == (3, 2, 1, True)
    assert actual['quarantine_dates'] == [(0, 4, date(2019, 11, 1))]


@pytest.mark.parametrize('key', list(SCOPE))
def test_manifest_must_bind_every_input_and_output_path(key):
    with pytest.raises(ValueError, match='scope'):
        audit(manifest(**{key: '/different/cohort'}))


def test_missing_day_is_not_implicitly_zero():
    assert audit(manifest(days=[]))['counters'] == (None, None, None, False)


def test_manifest_duplicate_days_are_rejected():
    value = manifest()
    value['days'].append(dict(value['days'][0]))
    with pytest.raises(ValueError):
        audit(value)


@pytest.mark.parametrize('key', ['duplicates_dropped', 'late_records_dropped', 'durable_duplicates_ignored'])
@pytest.mark.parametrize('bad', [-1, True, 1.5, '3', None, 2**63])
def test_invalid_counts_are_rejected(key, bad):
    value = manifest()
    value['days'][0][key] = bad
    with pytest.raises(ValueError):
        audit(value)


@pytest.mark.parametrize('key,bad', [('kafka_partition', -1), ('kafka_partition', 2**31),
                                    ('kafka_offset', -1), ('kafka_offset', True),
                                    ('kafka_offset', 2**63), ('event_date', '2019-02-30')])
def test_invalid_quarantine_mapping_rejected(key, bad):
    value = manifest()
    value['quarantine_dates'][0][key] = bad
    with pytest.raises(ValueError):
        audit(value)


def test_duplicate_mapping_keys_cannot_multiply_quarantine_rows():
    value = manifest()
    value['quarantine_dates'].append(dict(value['quarantine_dates'][0]))
    with pytest.raises(ValueError):
        audit(value)


@pytest.mark.parametrize('day', ['20191101', "2019-11-01' OR true", '2019-02-30', None])
def test_bad_days_rejected_before_sql(day):
    with pytest.raises(ValueError):
        reconcile.render_query('SELECT 1', day=day, bronze_path=SCOPE['bronze_path'], bronze_version=1)


@pytest.mark.parametrize('path', ['s3a://lake/', 's3a://lake/table` WHERE true',
                                  's3a://lake/table\n', 's3a://lake/table?x=1'])
def test_bad_paths_rejected_before_sql(path):
    with pytest.raises(ValueError):
        reconcile.render_query('SELECT 1', day='2019-11-01', bronze_path=path, bronze_version=1)


def test_query_rendering_preserves_date_predicate_and_pinned_version():
    actual = reconcile.render_query(
        "SELECT * FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version} WHERE event_date = DATE '${day}'",
        day='2019-11-01', bronze_path=SCOPE['bronze_path'] + '/', bronze_version=7)
    assert actual == ("SELECT * FROM delta.`s3a://lake/bronze/events` VERSION AS OF 7 "
                      "WHERE event_date = DATE '2019-11-01'")


@pytest.mark.parametrize('version', [-1, True, '7', 1.5])
def test_untrusted_delta_version_cannot_become_sql(version):
    with pytest.raises(ValueError):
        reconcile.render_query('SELECT 1', day='2019-11-01', bronze_path=SCOPE['bronze_path'], bronze_version=version)


def test_unknown_template_parameter_is_not_left_unresolved():
    with pytest.raises(ValueError):
        reconcile.render_query('SELECT ${unknown}', day='2019-11-01', bronze_path=SCOPE['bronze_path'], bronze_version=1)


def test_named_queries_keep_sql_semicolons_inside_literals(tmp_path):
    path = tmp_path / 'queries.sql'
    path.write_text("-- name: example\nSELECT ';' AS value;\n-- name: other\nSELECT 2;\n")
    actual = reconcile.load_queries(path)
    assert actual == {'example': "SELECT ';' AS value", 'other': 'SELECT 2'}


def test_duplicate_query_names_are_rejected(tmp_path):
    path = tmp_path / 'queries.sql'
    path.write_text('-- name: same\nSELECT 1;\n-- name: same\nSELECT 2;\n')
    with pytest.raises(ValueError):
        reconcile.load_queries(path)
