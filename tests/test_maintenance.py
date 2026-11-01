"""Guard against optimizing open dates or silently shortening retention."""
import pytest

from src.storage import maintenance


def plan(**changes):
    values = dict(table_path='s3a://lake/bronze/events/', day='2019-11-01',
                  closed_before='2019-11-02')
    values.update(changes)
    return maintenance.MaintenancePlan(**values)


def test_partition_predicate_and_safe_vacuum_default():
    commands = maintenance.sql_commands(plan())
    assert commands['optimize'] == (
        "OPTIMIZE delta.`s3a://lake/bronze/events` WHERE event_date = DATE '2019-11-01' "
        'ZORDER BY (user_id, product_id)')
    assert commands['vacuum'] == 'VACUUM delta.`s3a://lake/bronze/events` RETAIN 720 HOURS DRY RUN'


def test_physical_vacuum_requires_explicit_opt_in():
    commands = maintenance.sql_commands(plan(), execute_vacuum=True)
    assert commands['vacuum'] == 'VACUUM delta.`s3a://lake/bronze/events` RETAIN 720 HOURS'


@pytest.mark.parametrize('flag', ['false', 'true', 1, None])
def test_execute_vacuum_does_not_accept_truthy_nonboolean_flags(flag):
    with pytest.raises(ValueError):
        maintenance.sql_commands(plan(), execute_vacuum=flag)


@pytest.mark.parametrize('day,closed_before', [
    ('2019-11-02', '2019-11-02'), ('2019-11-03', '2019-11-02'),
    ('2099-01-01', '2099-01-02'), ('20191101', '2019-11-02'),
    ("2019-11-01' OR true", '2019-11-02'), ('2019-02-30', '2019-11-02'),
    ('2019-11-01', '2099-01-01'), ('2019-11-01', None),
])
def test_open_future_and_malformed_dates_rejected(day, closed_before):
    with pytest.raises(ValueError):
        plan(day=day, closed_before=closed_before)


@pytest.mark.parametrize('path', [
    's3a://lake/', 's3a://lake', '/tmp/table', 's3://lake/bronze/events',
    's3a://lake/bronze/events`', 's3a://lake/bronze/events\n',
    's3a://lake/bronze/events?x=1', 's3a://lake/bronze/events#x',
])
def test_invalid_or_injectable_table_paths_rejected(path):
    with pytest.raises(ValueError):
        plan(table_path=path)


@pytest.mark.parametrize('changes', [
    {'retention_hours': 0}, {'retention_hours': 167}, {'retention_hours': True},
    {'retention_hours': 168.0}, {'retention_hours': '168'},
    {'retention_hours': 168, 'time_travel_hours': 720},
    {'max_stream_lag_hours': 720}, {'max_transaction_hours': 720},
    {'target_file_size_bytes': 0}, {'small_file_size_bytes': -1},
    {'small_file_size_bytes': 256 * 1024 * 1024}, {'time_travel_hours': -1},
])
def test_unsafe_or_invalid_retention_and_file_sizes_rejected(changes):
    with pytest.raises(ValueError):
        plan(**changes)


def test_retention_accepts_longer_time_travel_window():
    value = plan(retention_hours=720, time_travel_hours=720)
    maintenance.validate_table_retention(value, {
        'delta.deletedFileRetentionDuration': 'interval 30 days',
        'delta.logRetentionDuration': 'interval 4 weeks 2 days',
    })
    assert 'RETAIN 720 HOURS DRY RUN' in maintenance.sql_commands(value)['vacuum']


def test_existing_table_retention_cannot_be_shortened():
    with pytest.raises(ValueError, match='deletedFileRetentionDuration'):
        maintenance.validate_table_retention(plan(), {
            'delta.deletedFileRetentionDuration': 'interval 60 days',
        })


def test_short_log_retention_cannot_claim_required_time_travel():
    with pytest.raises(ValueError, match='logRetentionDuration'):
        maintenance.validate_table_retention(plan(), {'delta.logRetentionDuration': 'interval 1 day'})


@pytest.mark.parametrize('value', ['interval 1 month', 'garbage', 'interval -1 day', 'interval 0 days'])
def test_unrecognized_or_nonpositive_table_interval_rejected(value):
    with pytest.raises(ValueError):
        maintenance.validate_table_retention(plan(), {'delta.deletedFileRetentionDuration': value})


def test_default_table_properties_are_safe():
    maintenance.validate_table_retention(plan(), {})


def test_config_and_bucket_override_apply_to_maintenance(monkeypatch):
    monkeypatch.setenv('LAKE_BUCKET', 'test-lake')
    config = {'lake': {'bronze': 's3a://lake/bronze/events/'},
              'maintenance': {'retention_hours': 720, 'time_travel_hours': 720}}
    value = maintenance.resolve_plan(config, day='2019-11-01', closed_before='2019-11-02')
    assert value.table_path == 's3a://test-lake/bronze/events'
    assert value.retention_hours == 720
    explicit = maintenance.resolve_plan(config, day='2019-11-01', closed_before='2019-11-02',
                                        table_path='s3a://other/test', retention_hours=744)
    assert explicit.table_path == 's3a://other/test'
    assert explicit.retention_hours == 744
