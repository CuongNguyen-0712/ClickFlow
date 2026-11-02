from copy import deepcopy

import pytest

from src.common.config import load
from src.storage.stream_config import resolve_kafka_config


def test_defaults_use_internal_kafka_and_minio_checkpoint():
    result = resolve_kafka_config(load())
    assert result['topic'] == 'ecom.events.raw'
    assert result['bootstrap_servers'] == 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
    assert result['checkpoint'] == 's3a://lake/_checkpoints/kafka_to_delta_watermark_v1'
    assert result['trigger_seconds'] == 10
    assert result['max_offsets_per_trigger'] == 500000
    assert result['starting_offsets'] == 'earliest'


def test_bucket_override_applies_to_outputs_and_checkpoint(monkeypatch):
    monkeypatch.setenv('LAKE_BUCKET', 'other-lake')
    result = resolve_kafka_config(load())
    assert result['bronze'] == 's3a://other-lake/bronze/events'
    assert result['quarantine'] == 's3a://other-lake/quarantine/events'
    assert result['checkpoint'] == 's3a://other-lake/_checkpoints/kafka_to_delta_watermark_v1'


@pytest.mark.parametrize('field,value', [('trigger_seconds', 0), ('max_offsets_per_trigger', -1),
                                         ('metrics_port', 65536)])
def test_invalid_stream_limits_are_rejected(field, value):
    config = deepcopy(load())
    config['stream'][field] = value
    with pytest.raises(ValueError):
        resolve_kafka_config(config)


def test_checkpoint_cannot_live_inside_output_table():
    config = deepcopy(load())
    config['stream']['checkpoint_path'] = config['lake']['bronze'] + 'checkpoint'
    with pytest.raises(ValueError, match='overlap'):
        resolve_kafka_config(config)
