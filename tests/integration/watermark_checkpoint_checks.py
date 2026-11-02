"""Reject stateless checkpoints and changed dedup state plans; retain artifacts."""
from copy import deepcopy
from uuid import uuid4

from src.common.config import load
from src.common.spark_session import get_spark
from src.storage.stream_config import resolve_kafka_config
from src.storage.stream_sink import prepare_stream_storage


spark = get_spark('watermark-checkpoint-check')
spark.sparkContext.setLogLevel('WARN')
try:
    settings = resolve_kafka_config(deepcopy(load()))
    root = settings['checkpoint'] + '-plan-check-' + uuid4().hex
    settings.update(bronze=root + '/bronze', quarantine=root + '/quarantine', checkpoint=root + '/legacy')
    prepare_stream_storage(spark, settings)
    plan = {'operator': 'dropDuplicatesWithinWatermark', 'keys': ['event_id'],
            'event_time': 'event_time', 'watermark': '1 hour', 'routing': 'valid_union_invalid_v1',
            'shuffle_partitions': 8}
    try:
        prepare_stream_storage(spark, settings, checkpoint_layout=plan)
    except ValueError as error:
        assert 'identity mismatch' in str(error)
    else:
        raise AssertionError('A stateless checkpoint was accepted by the stateful plan')
    fresh = {**settings, 'checkpoint': root + '/stateful'}
    prepare_stream_storage(spark, fresh, checkpoint_layout=plan)
    prepare_stream_storage(spark, fresh, checkpoint_layout=plan)
    try:
        prepare_stream_storage(spark, fresh, checkpoint_layout={**plan, 'watermark': '2 hours'})
    except ValueError as error:
        assert 'identity mismatch' in str(error)
    else:
        raise AssertionError('Changed state watermark was accepted with the same checkpoint')
    print('Watermark checkpoint plan guards verified: ' + root, flush=True)
finally:
    spark.stop()
