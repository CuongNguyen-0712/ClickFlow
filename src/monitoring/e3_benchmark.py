"""Controlled real-CSV Kafka replay benchmark for E3; retains every artifact."""
import argparse
from copy import deepcopy
import csv
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
from pathlib import Path
import random
import time
from uuid import uuid4

from src.common.config import ROOT, load
from src.common.schemas import SOURCE_COLUMNS
from src.monitoring.latency import aggregate_trials, file_statistics, latency_summary

IMPLEMENTATION_FILES = (
    'src/storage/stream_to_bronze.py', 'src/storage/quality_rules.py',
    'src/storage/deduplication.py', 'src/storage/stream_sink.py', 'src/storage/stream_config.py',
    'src/common/schemas.py', 'src/common/metrics.py', 'src/common/spark_session.py',
    'src/monitoring/stream_listener.py', 'src/monitoring/latency.py',
    'src/monitoring/e3_benchmark.py', 'src/monitoring/e3_report.py',
    'contracts/event.schema.json', 'contracts/bronze_events.yaml',
    'contracts/quarantine_events.yaml', 'configs/spark-defaults.conf',
)


def implementation_hashes(root=ROOT):
    return {relative: hashlib.sha256((Path(root) / relative).read_bytes()).hexdigest()
            for relative in IMPLEMENTATION_FILES}


def next_phase_time(now, trigger, phase):
    """Spark ProcessingTimeExecutor aligns to epoch multiples, not first-batch time."""
    if type(trigger) is not int or trigger <= 0 or not math.isfinite(now) or not 0 < phase < 1:
        raise ValueError('Invalid trigger phase')
    target = (math.floor(now / trigger) + phase) * trigger
    return target + trigger if target <= now else target


def validate_cached_trial(row, cell, workload, *, fingerprint, session_id):
    aggregate_trials([row], require_complete=False)
    if (any(row[name] != cell[name] for name in ('trigger_seconds', 'speed', 'repetition'))
            or row['workload_sha256'] != workload['sha256']
            or row['expected_rows'] != workload['rows']
            or row['experiment_fingerprint'] != fingerprint or row.get('session_id') != session_id):
        raise ValueError('Cached trial does not belong to this session/cell/workload/implementation')


def build_trial_matrix(*, repetitions=3, seed=42):
    if type(repetitions) is not int or repetitions < 3:
        raise ValueError('Full E3 requires at least three repetitions')
    rng, trials = random.Random(seed), []
    for rep in range(1, repetitions + 1):
        block = [(trigger, speed) for trigger in (5, 10, 30) for speed in (100, 1000)]
        rng.shuffle(block)
        trials.extend({'trigger_seconds': trigger, 'speed': speed, 'repetition': rep}
                      for trigger, speed in block)
    return trials


def load_workload(path, *, limit=100000):
    if type(limit) is not int or limit < 2:
        raise ValueError('At least two source rows required')
    path = Path(path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    rows, seen = [], set()
    with path.open(encoding='utf-8', newline='') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != [name for name, _ in SOURCE_COLUMNS]:
            raise ValueError('Source header does not match C1')
        for raw in reader:
            if len(rows) == limit:
                break
            row = {}
            for name, kind in SOURCE_COLUMNS:
                value = raw[name]
                if kind == 'timestamp':
                    value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                elif kind == 'bigint':
                    value = int(value)
                    if not -(2**63) <= value < 2**63:
                        raise ValueError('Source ID exceeds int64')
                elif kind == 'double':
                    value = float(value)
                    if not math.isfinite(value) or value < 0:
                        raise ValueError('Invalid source price')
                else:
                    value = value or None
                row[name] = value
            key = tuple(row.values())
            if key in seen:
                raise ValueError('Use a distinct source cohort')
            seen.add(key)
            rows.append(row)
    if len(rows) != limit:
        raise ValueError(f'Need {limit} source rows, got {len(rows)}')
    rows.sort(key=lambda row: row['event_time'])
    first, last = rows[0]['event_time'], rows[-1]['event_time']
    span = (last - first).total_seconds()
    if not 0 < span < 3600:
        raise ValueError('Controlled source cohort must span >0 and <1h to avoid watermark loss')
    for row in rows:
        row['event_time'] = row['event_time'].strftime('%Y-%m-%dT%H:%M:%SZ')
    return rows, {'path': str(path), 'sha256': sha, 'rows': len(rows),
                  'first_event_time': first.isoformat(), 'last_event_time': last.isoformat(),
                  'event_span_seconds': span}


def _json_file(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, default=str)


def _templates(rows, prefix, source_file):
    return [(json.dumps({**row, 'event_id': prefix + str(index), 'source_file': source_file},
                        separators=(',', ':'))[:-1] + ',"produced_at":"').encode('utf-8')
            for index, row in enumerate(rows)]


def _publish(producer, topic, rows, templates, speed):
    state = {'delivered_records': 0, 'errors': [], 'offsets': {}, 'buffer_full_retries': 0}

    def delivered(error, message):
        if error is not None:
            state['errors'].append(str(error))
            return
        state['delivered_records'] += 1
        part, offset = str(message.partition()), message.offset()
        bounds = state['offsets'].setdefault(part, [offset, offset])
        bounds[0], bounds[1] = min(bounds[0], offset), max(bounds[1], offset)

    first_event = datetime.fromisoformat(rows[0]['event_time'].replace('Z', '+00:00'))
    offsets = [(datetime.fromisoformat(row['event_time'].replace('Z', '+00:00')) - first_event).total_seconds()
               for row in rows]
    started, max_lag, last_enqueued = time.monotonic(), 0.0, None
    first_produced = None
    for index, (row, template, offset) in enumerate(zip(rows, templates, offsets)):
        target = started + (offset / speed if speed else 0)
        if speed and target > time.monotonic():
            time.sleep(max(0, target - time.monotonic()))
        now = datetime.now(timezone.utc)
        if first_produced is None:
            first_produced = now.isoformat()
        payload = template + now.isoformat(timespec='microseconds').replace('+00:00', 'Z').encode() + b'"}'
        while True:
            try:
                producer.produce(topic, key=str(row['user_id']), value=payload, on_delivery=delivered)
                break
            except BufferError:
                state['buffer_full_retries'] += 1
                producer.poll(0.01)
        last_enqueued = time.monotonic()
        max_lag = max(max_lag, (last_enqueued - target) * 1000) if speed else 0.0
        if index % 200 == 0:
            producer.poll(0)
    pending = producer.flush(60)
    if pending or state['errors'] or state['delivered_records'] != len(rows):
        raise RuntimeError(f'Producer delivery failure: {state}, pending={pending}')
    duration = max(last_enqueued - started, 0.000001)
    state.update(emit_duration_seconds=duration, first_produced_at=first_produced,
                 last_enqueued_at=now.isoformat(), max_pacing_lag_ms=max_lag,
                 target_emit_duration_seconds=offsets[-1] / speed if speed else 0.0,
                 actual_speed=offsets[-1] / duration if speed else None)
    return state


def _metric(metrics, name, labels=None):
    return int(metrics.registry.get_sample_value('clickflow_stream_' + name, labels or {}) or 0)


def topic_ready(metadata):
    return (metadata is not None and metadata.error is None and len(metadata.partitions) == 6
            and all(len(part.replicas) == 3 and len(part.isrs) == 3 and part.leader >= 0
                    for part in metadata.partitions.values()))


def _wait_committed(query, metrics, *, total, topic, timeout):
    deadline, last_notice = time.monotonic() + timeout, 0.0
    while time.monotonic() < deadline:
        error = query.exception()
        if error is not None:
            raise RuntimeError(str(error))
        if not query.isActive:
            raise RuntimeError('Continuous query terminated before the workload drained')
        progress = query.lastProgress
        sink = _metric(metrics, 'sink_rows_total', {'destination': 'bronze'})
        end = (progress or {}).get('sources', [{}])[0].get('endOffset') or {}
        consumed = sum(end.get(topic, {}).values())
        if sink >= total and consumed >= total:
            if sink != total or consumed != total:
                raise RuntimeError('Unexpected records in isolated trial')
            return progress
        if _metric(metrics, 'sink_rows_total', {'destination': 'quarantine'}):
            raise RuntimeError('Source workload entered quarantine')
        now = time.monotonic()
        if now - last_notice > 15:
            print(json.dumps({'event': 'e3_wait', 'topic': topic, 'committed_rows': sink,
                              'consumed_offsets': consumed, 'expected': total}), flush=True)
            last_notice = now
        time.sleep(0.5)
    raise TimeoutError(f'Timed out waiting for {total} committed records')


def run_trial(spark, base_config, rows, workload, spec, *, session_id, experiment_fingerprint,
              warmup_rows=200, timeout=300, repetitions=3):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    from prometheus_client import generate_latest
    from src.storage.init_lake import lake_paths
    from src.storage.stream_to_bronze import start_kafka_stream, close_kafka_stream

    trial_id = uuid4().hex[:12]
    topic = f'e3-{session_id[:12]}-{trial_id}'
    root = lake_paths()['checkpoints'].rstrip('/') + '/e3/' + session_id + '/' + trial_id
    config = deepcopy(base_config)
    config['kafka']['topic'] = topic
    config['lake'].update(bronze=root + '/bronze/events', quarantine=root + '/quarantine/events')
    config['stream'].update(source='kafka', checkpoint_path=root + '/checkpoint',
        trigger_seconds=spec['trigger_seconds'], metrics_port=0, query_name='e3-' + trial_id)
    print(json.dumps({'event': 'e3_trial_resources', 'topic': topic, 'root': root}), flush=True)
    brokers = config['kafka']['bootstrap_servers_internal']
    admin = AdminClient({'bootstrap.servers': brokers})
    admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=3,
                                 config={'min.insync.replicas': '2'})])[topic].result(60)
    deadline = time.monotonic() + 60
    while True:
        metadata = admin.list_topics(topic=topic, timeout=10).topics.get(topic)
        if topic_ready(metadata):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f'Topic {topic} is not ready with6 partitions/RF3/full ISR')
        time.sleep(0.25)
    source_file = base_config['simulator']['files'][0]
    prefix = trial_id + '-data-'
    templates = _templates(rows, prefix, source_file)
    warm = rows[:min(warmup_rows, len(rows))]
    warm_templates = _templates(warm, trial_id + '-warmup-', source_file)
    producer = Producer({'bootstrap.servers': brokers, **base_config['producer'],
                         'queue.buffering.max.messages': 200000, 'queue.buffering.max.kbytes': 131072})
    producer.list_topics(topic=topic, timeout=60)
    query, metrics, listener = start_kafka_stream(spark, config)
    try:
        warm_delivery = _publish(producer, topic, warm, warm_templates, 0)
        warm_progress = _wait_committed(query, metrics, total=len(warm), topic=topic, timeout=timeout)
        baseline = file_statistics(spark, config['lake']['bronze'])
        baseline_version = baseline['file_snapshot_version']
        phase = (spec['repetition'] - 0.5) / repetitions
        now = time.time()
        trigger = spec['trigger_seconds']
        target = next_phase_time(now, trigger, phase)
        print(json.dumps({'event': 'e3_phase_wait', **spec, 'seconds': target-now, 'phase': phase}), flush=True)
        time.sleep(max(0, target-time.time()))
        measurement_after_batch = (query.lastProgress or warm_progress)['batchId']
        emission = _publish(producer, topic, rows, templates, spec['speed'])
        _wait_committed(query, metrics, total=len(warm)+len(rows), topic=topic, timeout=timeout)
        visible_at = datetime.now(timezone.utc).isoformat()
        progress = list(query.recentProgress)
        raw_metrics = generate_latest(metrics.registry).decode()
        query_id, run_id = str(query.id), str(query.runId)
    finally:
        close_kafka_stream(spark, query, metrics, listener)
    stats = latency_summary(spark, config['lake']['bronze'], event_id_prefix=prefix)
    files = file_statistics(spark, config['lake']['bronze'], after_version=baseline_version)
    quarantine_rows = spark.read.format('delta').load(config['lake']['quarantine']).count()
    observed_phase = (datetime.fromisoformat(emission['first_produced_at']).timestamp() % trigger) / trigger
    result = {**spec, **stats, **files, 'trial_id': trial_id, 'session_id': session_id, 'expected_rows': len(rows),
              'workload_sha256': workload['sha256'], 'delivered_records': emission['delivered_records'],
              'experiment_fingerprint': experiment_fingerprint,
              'quarantine_rows': quarantine_rows, 'warmup_rows': len(warm),
              'duplicates_dropped': _metric(metrics, 'duplicates_dropped_total'),
              'late_records_dropped': _metric(metrics, 'late_records_dropped_total'),
              'durable_duplicates_ignored': _metric(metrics, 'duplicates_ignored_total'),
              'actual_speed': emission['actual_speed'],
              'pace_target_met': 0.9 <= emission['actual_speed'] / spec['speed'] <= 1.1,
              'emit_duration_seconds': emission['emit_duration_seconds'],
              'target_emit_duration_seconds': emission['target_emit_duration_seconds'],
              'max_pacing_lag_ms': emission['max_pacing_lag_ms'], 'phase_fraction': phase,
              'observed_phase_fraction': observed_phase, 'buffer_full_retries': emission['buffer_full_retries'],
              'measured_commit_batches': sum(p['batchId'] > measurement_after_batch
                  and p.get('numInputRows', 0) > 0 for p in progress),
              'trigger_overruns': sum(p['batchId'] > measurement_after_batch and p.get('numInputRows', 0) > 0
                  and p.get('durationMs', {}).get('triggerExecution', 0) > trigger*1000 for p in progress),
              'evidence': {'topic': topic, 'paths': {'bronze': config['lake']['bronze'],
                  'quarantine': config['lake']['quarantine'], 'checkpoint': config['stream']['checkpoint_path']},
                  'query_id': query_id, 'run_id': run_id, 'warmup_delivery': warm_delivery,
                  'baseline': baseline, 'emission': emission, 'measurement_after_batch_id': measurement_after_batch,
                  'all_rows_visible_at': visible_at,
                  'recent_progress': progress, 'prometheus': raw_metrics}}
    aggregate_trials([result], require_complete=False)
    return result


def main():
    from importlib.metadata import version
    from src.common.spark_session import get_spark
    from src.monitoring.e3_report import write_results

    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    logging.getLogger('src.common.metrics').setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source', type=Path)
    parser.add_argument('--rows', type=int, default=100000)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--warmup-rows', type=int, default=200)
    parser.add_argument('--timeout-seconds', type=int, default=300)
    parser.add_argument('--pilot', action='store_true', help='One5s/100x trial; explicitly incomplete E3')
    parser.add_argument('--resume', action='store_true', help='Resume only missing trial reports in this same output')
    args = parser.parse_args()
    config = load()
    rows, workload = load_workload(args.source or config['stream']['sample_path'], limit=args.rows)
    if args.warmup_rows < 1 or args.timeout_seconds < 60:
        raise ValueError('Positive warmup and timeout>=60s required')
    matrix = ([{'trigger_seconds': 5, 'speed': 100, 'repetition': 1}] if args.pilot
              else build_trial_matrix(repetitions=args.repetitions, seed=args.seed))
    spec = {'workload': workload, 'matrix': matrix, 'pilot': args.pilot,
            'warmup_rows': args.warmup_rows, 'seed': args.seed,
            'repetitions': args.repetitions, 'timeout_seconds': args.timeout_seconds,
            'fixed_pipeline': {'producer': config['producer'],
                'brokers': config['kafka']['bootstrap_servers_internal'],
                'source_file': config['simulator']['files'][0],
                'watermark': config['stream']['watermark'],
                'max_offsets_per_trigger': config['stream']['max_offsets_per_trigger']},
            'implementation_sha256': implementation_hashes()}
    output = args.output
    if args.resume:
        manifest = json.loads((output / 'manifest.json').read_text(encoding='utf-8'))
        if manifest['specification'] != spec:
            raise ValueError('Resume source/configuration differs from the recorded experiment')
        if (output / 'completed.json').exists():
            print(json.dumps({'event': 'e3_already_complete', 'output': str(output)}), flush=True)
            return
    else:
        output.mkdir(parents=True, exist_ok=False)
        manifest = {'session_id': uuid4().hex, 'started_at': datetime.now(timezone.utc).isoformat(),
                    'specification': spec, 'artifact_policy': 'retained; no cleanup or maintenance'}
        _json_file(output / 'manifest.json', manifest)
    spark = get_spark('e3-latency-benchmark')
    spark.sparkContext.setLogLevel('WARN')
    try:
        environment = {'python': __import__('sys').version, 'spark': spark.version,
                       'delta': version('delta-spark'), 'confluent_kafka': version('confluent-kafka'),
                       'matplotlib': version('matplotlib'),
                       'spark_conf': {key: spark.conf.get(key, None) for key in
                           ('spark.master', 'spark.sql.shuffle.partitions', 'spark.cores.max',
                            'spark.executor.cores', 'spark.executor.memory', 'spark.driver.memory')},
                       'kafka_partitions': 6, 'replication_factor': 3, 'min_insync_replicas': 2,
                       'producer_config': config['producer'],
                       'stream_watermark': config['stream']['watermark'],
                       'max_offsets_per_trigger': config['stream']['max_offsets_per_trigger']}
        environment_path = output / 'environment.json'
        if environment_path.exists():
            if json.loads(environment_path.read_text(encoding='utf-8')) != environment:
                raise ValueError('Resume Spark/dependency environment differs')
        else:
            _json_file(environment_path, environment)
        fingerprint = hashlib.sha256(json.dumps({'spec': spec, 'environment': environment},
                                                sort_keys=True).encode()).hexdigest()
        cached = {}
        for index, cell in enumerate(matrix, 1):
            destination = output / f'trial-{index:02d}.json'
            if destination.exists():
                record = json.loads(destination.read_text(encoding='utf-8'))
                validate_cached_trial(record, cell, workload, fingerprint=fingerprint, session_id=manifest['session_id'])
                cached[index] = record
        results = []
        for index, cell in enumerate(matrix, 1):
            destination = output / f'trial-{index:02d}.json'
            if index in cached:
                result = cached[index]
            else:
                print(json.dumps({'event': 'e3_trial_started', 'index': index, 'total': len(matrix), **cell}), flush=True)
                try:
                    result = run_trial(spark, config, rows, workload, cell,
                        session_id=manifest['session_id'], experiment_fingerprint=fingerprint,
                        warmup_rows=args.warmup_rows,
                        timeout=args.timeout_seconds, repetitions=1 if args.pilot else args.repetitions)
                    validate_cached_trial(result, cell, workload, fingerprint=fingerprint, session_id=manifest['session_id'])
                    _json_file(destination, result)
                except Exception as error:
                    _json_file(output / f'failed-{index:02d}-{uuid4().hex[:8]}.json',
                               {'index': index, 'cell': cell, 'error': str(error), 'artifacts_retained': True})
                    raise
            results.append(result)
            print(json.dumps({'event': 'e3_trial_verified', 'index': index, 'total': len(matrix),
                **{key: result[key] for key in ('trigger_seconds', 'speed', 'repetition', 'rows',
                   'p50_latency_ms', 'p95_latency_ms', 'files_created', 'actual_speed', 'negative_latency_rows')}}), flush=True)
        summary = write_results(output, results, complete=not args.pilot)
        _json_file(output / 'completed.json', {'full_e3_complete': not args.pilot,
            'verified_trials': len(results), 'summary': summary, 'environment': environment,
            'completed_at': datetime.now(timezone.utc).isoformat()})
        print(json.dumps({'event': 'e3_completed', 'full_e3_complete': not args.pilot,
                          'verified_trials': len(results), 'output': str(output)}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
