"""Resolve and validate Kafka stream settings without importing Spark."""
import os
from urllib.parse import urlsplit, urlunsplit


def resolve_kafka_config(config):
    stream, lake, kafka = config['stream'], config['lake'], config['kafka']
    bucket = os.environ.get('LAKE_BUCKET') or lake['bucket']

    def uri(value):
        parts = urlsplit(value)
        if parts.scheme != 's3a' or not parts.netloc or not parts.path.strip('/') or parts.query or parts.fragment:
            raise ValueError('Streaming storage paths must be s3a://bucket/prefix URIs')
        return urlunsplit(('s3a', bucket, parts.path.rstrip('/'), '', ''))

    result = {
        'topic': kafka['topic'],
        'bootstrap_servers': kafka['bootstrap_servers_internal'],
        'bronze': uri(lake['bronze']),
        'quarantine': uri(lake['quarantine']),
        'checkpoint': uri(stream.get('checkpoint_path', lake['checkpoints'].rstrip('/') + '/kafka_to_delta')),
        'trigger_seconds': stream.get('trigger_seconds', 10),
        'max_offsets_per_trigger': stream.get('max_offsets_per_trigger', 500000),
        'starting_offsets': stream.get('starting_offsets', 'earliest'),
        'watermark': stream.get('watermark', '1 hour'),
        'metrics_port': stream.get('metrics_port', 8002),
        'query_name': stream.get('query_name', 'clickflow-kafka-to-delta'),
    }
    for name in ('trigger_seconds', 'max_offsets_per_trigger'):
        value = result[name]
        if type(value) is not int or value < 1:
            raise ValueError(f'stream.{name} must be a positive integer')
    if type(result['metrics_port']) is not int or not 0 <= result['metrics_port'] <= 65535:
        raise ValueError('stream.metrics_port must be an integer from 0 to 65535')
    if result['starting_offsets'] not in ('earliest', 'latest'):
        raise ValueError('stream.starting_offsets must be earliest or latest')
    if not all(isinstance(result[key], str) and result[key].strip()
               for key in ('topic', 'bootstrap_servers', 'watermark', 'query_name')):
        raise ValueError('Kafka topic, brokers, watermark and query_name must be nonempty strings')
    if ',' in result['topic']:
        raise ValueError('One source topic is required for C5 partition/offset identity')
    paths = [result[key] + '/' for key in ('bronze', 'quarantine', 'checkpoint')]
    if any(a.startswith(b) or b.startswith(a) for index, a in enumerate(paths) for b in paths[index + 1:]):
        raise ValueError('Output and checkpoint paths must not overlap')
    return result
