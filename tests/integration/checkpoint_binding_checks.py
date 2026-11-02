"""An unrelated unbound checkpoint must never be adopted. Keep all artifacts."""
from copy import deepcopy
from uuid import uuid4

from src.common.config import load
from src.common.spark_session import get_spark
from src.storage.stream_config import resolve_kafka_config
from src.storage.stream_sink import _binding, prepare_stream_storage


spark = get_spark('checkpoint-binding-check')
spark.sparkContext.setLogLevel('WARN')
try:
    settings = resolve_kafka_config(deepcopy(load()))
    root = settings['checkpoint'] + '-binding-test-' + uuid4().hex
    settings.update(bronze=root + '/bronze', quarantine=root + '/quarantine', checkpoint=root + '/checkpoint')
    _binding(spark, settings['checkpoint'] + '/commits/0', {'simulated_existing_commit': 0})
    try:
        prepare_stream_storage(spark, settings)
    except ValueError as error:
        assert 'unbound checkpoint' in str(error).lower(), str(error)
        print('Existing unbound checkpoint rejected; all artifacts retained: ' + root, flush=True)
    else:
        raise AssertionError('An existing unbound checkpoint was silently adopted')
finally:
    spark.stop()
