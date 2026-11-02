"""Isolated E4b crash/recovery benchmark controller and evidence helpers."""
import argparse
from collections import deque
from copy import deepcopy
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
from urllib.parse import urlsplit
from urllib.request import urlopen

from src.common.config import ROOT, load
from src.common.schemas import SOURCE_COLUMNS
from src.monitoring.e3_benchmark import load_workload, topic_ready


def save_json(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, default=str)


def emit(event_name, **values):
    if 'event' in values:
        values['source_event'] = values.pop('event')
    print(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'event': event_name, **values}), flush=True)


def build_cohort(rows, case_id):
    times = [datetime.fromisoformat(row['event_time'].replace('Z', '+00:00')) for row in rows]
    span = (max(times) - min(times)).total_seconds()
    if not 0 < span < 3600:
        raise ValueError('Cohort must span >0 and <1 hour')
    shift = datetime(2019, 11, 2, tzinfo=timezone.utc) - timedelta(seconds=max(1, span // 2)) - min(times)
    groups = [deque(), deque()]
    for row, stamp in zip(rows, times):
        item = {**row, 'event_time': (stamp + shift).strftime('%Y-%m-%dT%H:%M:%SZ')}
        item['event_id'] = hashlib.sha256((case_id + json.dumps(item, sort_keys=True)).encode()).hexdigest()
        groups[item['event_time'][:10] == '2019-11-02'].append(item)
    result = []
    while any(groups):
        for group in groups:
            if group:
                result.append(group.popleft())
    if len({row['event_id'] for row in result}) != len(result) or not all(
            any(r['event_time'][:10] == day for r in result) for day in ('2019-11-01', '2019-11-02')):
        raise ValueError('Need distinct records covering both days')
    return result


def parse_offsets(raw, topic):
    lines = raw.strip().splitlines()
    if len(lines) not in (3, 4) or lines[0] != 'v1':
        raise ValueError('Expected Spark v1 Kafka offset log with one or two mirrored sources')
    json.loads(lines[1])
    observed = []
    for line in lines[2:]:
        sources = json.loads(line)
        if not isinstance(sources, dict) or set(sources) != {topic}:
            raise ValueError('Checkpoint source topic mismatch')
        offsets = sources[topic]
        if not isinstance(offsets, dict) or not offsets or any(
            not isinstance(part, str) or not part.isdecimal() or type(offset) is not int or offset < 0
            for part, offset in offsets.items()
        ):
            raise ValueError('Invalid Kafka offsets')
        observed.append(offsets)
    # The production valid/invalid union has two logical KafkaSource entries.
    # Both describe the same topic cohort. Never sum them or mask disagreement.
    if any(offsets != observed[0] for offsets in observed[1:]):
        raise ValueError('Mirrored Kafka source offsets disagree')
    return observed[0]


def validate_target(pid, pgid, controller_pid, command, script, config):
    if (pid <= 1 or pid == controller_pid or pgid != pid
            or 'org.apache.spark.deploy.SparkSubmit' not in command or script not in command
            or '--config' not in command or command[command.index('--config') + 1:] != [config]):
        raise ValueError('Refusing to signal a process without the owned Spark driver identity')


def validate_receipts(cohort, receipts):
    if len(receipts) != len(cohort):
        raise ValueError('Incomplete producer delivery receipts')
    ordered = sorted(receipts, key=lambda row: row['index'])
    seen = set()
    for index, (source, row) in enumerate(zip(cohort, ordered)):
        key = row['partition'], row['offset']
        if (type(row['index']) is not int or row['index'] != index
                or row['event_id'] != source['event_id'] or key in seen
                or any(type(n) is not int or n < 0 for n in key)):
            raise ValueError('Conflicting delivery receipt')
        datetime.fromisoformat(row['produced_at'].replace('Z', '+00:00'))
        seen.add(key)
    return ordered


class Checkpoint:
    """Read-only native object observations; never creates a competing Spark job."""
    def __init__(self, client, uri, topic):
        parts = urlsplit(uri)
        self.client, self.bucket, self.prefix, self.topic = client, parts.netloc, parts.path.strip('/'), topic

    def read(self, suffix):
        from minio.error import S3Error
        try:
            response = self.client.get_object(self.bucket, self.prefix + '/' + suffix)
        except S3Error as error:
            if error.code in ('NoSuchKey', 'NoSuchObject'):
                return None
            raise
        try:
            return response.read().decode()
        finally:
            response.close()
            response.release_conn()

    def snapshot(self):
        commits = [int(obj.object_name.rsplit('/', 1)[-1]) for obj in self.client.list_objects(
            self.bucket, prefix=self.prefix + '/commits/') if obj.object_name.rsplit('/', 1)[-1].isdigit()]
        batch = max(commits) if commits else None
        raw = None if batch is None else self.read(f'offsets/{batch}')
        if batch is not None and raw is None:
            raise RuntimeError('Committed batch has no offset log')
        metadata, identity = self.read('metadata'), self.read('_clickflow_stream_identity.json')
        offsets = {} if batch is None else parse_offsets(raw, self.topic)
        return {'batch_id': batch, 'offsets': offsets, 'consumed': sum(offsets.values()),
                'query_id': json.loads(metadata)['id'] if metadata else None,
                'identity_sha256': hashlib.sha256(identity.encode()).hexdigest() if identity else None}


def wait_until(description, check, *, timeout=600, process=None):
    deadline, next_notice = time.monotonic() + timeout, 0
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'Driver exited {process.returncode} while waiting for {description}')
        value = check()
        if value:
            return value
        if time.monotonic() >= next_notice:
            emit('waiting', for_condition=description)
            next_notice = time.monotonic() + 15
        time.sleep(.5)
    raise TimeoutError(description)


class Driver:
    def __init__(self, directory, script, config, log_name):
        self.script, self.config = str(script), str(config)
        self.log = directory / log_name
        self.handle = self.log.open('x', encoding='utf-8')
        self.command = ['/opt/spark/bin/spark-submit', '--master', 'spark://spark-master:7077',
                        self.script, '--config', self.config]
        emit('start_driver', command=self.command, log=log_name)
        self.process = subprocess.Popen(self.command, stdout=self.handle, stderr=subprocess.STDOUT,
                                        start_new_session=True)

    def started(self):
        for line in self.log.read_text(encoding='utf-8', errors='replace').splitlines():
            if line.startswith('{'):
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if item.get('event') == 'kafka_stream_started':
                    return item
        return None

    def send(self, sig):
        if self.process.poll() is not None:
            raise RuntimeError('Owned driver already exited')
        pid = self.process.pid
        command = Path(f'/proc/{pid}/cmdline').read_bytes().decode().strip('\0').split('\0')
        validate_target(pid, os.getpgid(pid), os.getpid(), command, self.script, self.config)
        members = []
        for path in Path('/proc').iterdir():
            if path.name.isdecimal():
                try:
                    if os.getpgid(int(path.name)) == pid:
                        members.append(int(path.name))
                except ProcessLookupError:
                    pass
        evidence = {'jvm_pid': pid, 'pgid': pid, 'group_members': members,
                    'command': command, 'signal': sig.name}
        emit('signal_driver', **evidence)
        # Java owns the process group. Graceful stop goes to the Python entrypoint,
        # whose production handler stops the query before stopping Spark.
        if sig == signal.SIGTERM:
            python_pid = (self.started() or {}).get('driver_pid')
            if not python_pid or os.getpgid(python_pid) != pid:
                raise RuntimeError('Cannot identify the owned Python driver for graceful shutdown')
            os.kill(python_pid, sig)
        else:
            os.killpg(pid, sig)
        return evidence

    def wait(self, timeout=120):
        code = self.process.wait(timeout=timeout)
        self.handle.close()
        emit('driver_exit', log=self.log.name, returncode=code)
        return code


class Publisher:
    def __init__(self, directory, config, cohort):
        from confluent_kafka import Producer
        self.producer = Producer({'bootstrap.servers': config['kafka']['bootstrap_servers_internal'],
                                  **config['producer']})
        self.topic, self.cohort = config['kafka']['topic'], cohort
        self.receipts, self.errors = [], []
        self.output = (directory / 'deliveries.jsonl').open('x', encoding='utf-8')

    def publish(self, start, stop, rate=0):
        began = time.monotonic()
        try:
            for index in range(start, stop):
                if rate:
                    time.sleep(max(0, began + (index-start) / rate - time.monotonic()))
                row = self.cohort[index]
                produced = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
                payload = {**row, 'source_file': '2019-Nov.csv', 'produced_at': produced}

                def delivered(error, message, index=index, row=row, produced=produced):
                    if error:
                        self.errors.append(str(error))
                        return
                    receipt = {'index': index, 'event_id': row['event_id'], 'produced_at': produced,
                               'partition': message.partition(), 'offset': message.offset()}
                    self.output.write(json.dumps(receipt) + '\n')
                    self.output.flush()
                    self.receipts.append(receipt)

                while True:
                    try:
                        self.producer.produce(self.topic, key=str(row['user_id']), value=json.dumps(payload),
                                              on_delivery=delivered)
                        break
                    except BufferError:
                        self.producer.poll(.1)
                self.producer.poll(0)
            if self.producer.flush(60) or self.errors:
                raise RuntimeError(f'Kafka delivery failure: {self.errors}')
            emit('producer_wave_acknowledged', start=start, stop=stop, total_acknowledged=len(self.receipts))
        except BaseException as error:
            self.errors.append(repr(error))
            raise


def run(directory, *, rows=20000):
    import yaml
    from confluent_kafka.admin import AdminClient, NewTopic
    from minio import Minio

    if rows < 10000:
        raise ValueError('E4b requires >=10000 rows for seed, gated wave and live tail')
    case_id, config = directory.name, deepcopy(load())
    original, workload = load_workload(ROOT / 'data/sample/events_sample_100k.csv', limit=rows)
    cohort = build_cohort(original, case_id)
    base = f"s3a://{os.environ.get('LAKE_BUCKET', config['lake']['bucket'])}/_checkpoints/e4b/{case_id}"
    config['kafka']['topic'] = case_id
    config['lake'].update(bronze=base + '/bronze/events', quarantine=base + '/quarantine/events')
    config['stream'].update(source='kafka', checkpoint_path=base + '/checkpoint', trigger_seconds=5,
                            max_offsets_per_trigger=5000, metrics_port=0, query_name=case_id)
    config_path = directory / 'pipeline.yaml'
    with config_path.open('x', encoding='utf-8') as stream:
        yaml.safe_dump(config, stream)
    save_json(directory / 'cohort.json', cohort)
    csv_path = directory / 'source.csv'
    with csv_path.open('x', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=[name for name, _ in SOURCE_COLUMNS])
        writer.writeheader()
        for row in cohort:
            source = {name: row[name] for name, _ in SOURCE_COLUMNS}
            source['event_time'] = datetime.fromisoformat(source['event_time'].replace('Z', '+00:00')).strftime(
                '%Y-%m-%d %H:%M:%S UTC')
            writer.writerow(source)
    endpoint = urlsplit(config['lake']['endpoint'])
    client = Minio(endpoint.netloc, access_key=os.environ['AWS_ACCESS_KEY_ID'],
                   secret_key=os.environ['AWS_SECRET_ACCESS_KEY'], secure=endpoint.scheme == 'https')
    source_uri = base + '/input/source.csv'
    source_parts = urlsplit(source_uri)
    client.fput_object(source_parts.netloc, source_parts.path.strip('/'), str(csv_path), content_type='text/csv')
    specification = {'case_id': case_id, 'workload': workload, 'rows': rows, 'source_path': source_uri,
                     'source_sha256': hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                     'bronze_path': config['lake']['bronze'], 'quarantine_path': config['lake']['quarantine'],
                     'checkpoint': config['stream']['checkpoint_path'], 'topic': case_id,
                     'days': sorted({r['event_time'][:10] for r in cohort}),
                     'fault_window': 'Delta callback committed, Spark checkpoint batch not committed'}
    save_json(directory / 'case.json', specification)
    emit('e4b_case', **specification)
    admin = AdminClient({'bootstrap.servers': config['kafka']['bootstrap_servers_internal']})
    admin.create_topics([NewTopic(case_id, num_partitions=6, replication_factor=3,
                                 config={'min.insync.replicas': '2'})])[case_id].result(60)
    wait_until('topic six partitions / three replicas / full ISR', lambda: topic_ready(
        admin.list_topics(topic=case_id, timeout=10).topics.get(case_id)), timeout=60)
    checkpoint = Checkpoint(client, specification['checkpoint'], case_id)
    publisher = Publisher(directory, config, cohort)
    drivers, tail = [], None
    try:
        first = Driver(directory, ROOT / 'src/monitoring/e4b_driver.py', config_path, 'before-driver.log')
        drivers.append(first)
        before_identity = wait_until('first query started', first.started, process=first.process)
        emit('first_query', **before_identity)
        publisher.publish(0, 1000)
        def drained(total):
            state = checkpoint.snapshot()
            return state if state['consumed'] == total else None
        seed = wait_until('seed checkpoint committed:1000', lambda: drained(1000), process=first.process)
        save_json(directory / 'seed-checkpoint.json', seed)
        (directory / 'arm-fault').touch(exist_ok=False)
        publisher.publish(1000, 6000)
        def gated():
            path = directory / 'gate.json'
            if not path.exists():
                return None
            try:
                return json.loads(path.read_text())
            except json.JSONDecodeError:
                return None
        gate = wait_until('Delta commit / checkpoint gap', gated, process=first.process)
        emit('fault_gate', **gate)
        tail = threading.Thread(target=publisher.publish, args=(6000, rows, 250), name='e4b-live-publisher')
        tail.start()
        wait_until('live tail acknowledged >=1000', lambda: len(publisher.receipts) >= 7000, timeout=60)
        if not tail.is_alive() or publisher.errors:
            raise RuntimeError('Publisher must still be active at the kill')
        before = checkpoint.snapshot()
        if checkpoint.read(f"commits/{gate['batch_id']}") is not None:
            raise RuntimeError('Fault window closed before kill')
        if checkpoint.read(f"offsets/{gate['batch_id']}") is None:
            raise RuntimeError('Fault batch lacks offset evidence')
        killed = {'checkpoint': before, 'gate': gate, 'acknowledged_at_kill': len(publisher.receipts),
                  'producer_alive_at_kill': tail.is_alive(), **first.send(signal.SIGKILL)}
        killed['returncode'] = first.wait()
        if killed['returncode'] != -signal.SIGKILL:
            raise RuntimeError('SIGKILL was not observed')
        save_json(directory / 'kill.json', killed)
        emit('killed_mid_ingestion', **killed)
        time.sleep(2)
        emit('outage_producer_continues', acknowledged=len(publisher.receipts))
        recovered = Driver(directory, ROOT / 'src/storage/stream_to_bronze.py', config_path, 'after-driver.log')
        drivers.append(recovered)
        after_identity = wait_until('recovery query started', recovered.started, process=recovered.process)
        if (before_identity['query_id'] != after_identity['query_id']
                or before_identity['run_id'] == after_identity['run_id']):
            raise RuntimeError('Expected same query ID and fresh run ID after checkpoint recovery')
        emit('recovered_query', **after_identity)
        tail.join(timeout=180)
        if tail.is_alive() or publisher.errors:
            raise RuntimeError('Publisher did not finish successfully')
        validate_receipts(cohort, publisher.receipts)
        after = wait_until(f'all {rows} source offsets committed', lambda: drained(rows),
                           process=recovered.process)
        if (before['query_id'] != after['query_id'] or before['identity_sha256'] is None
                or before['identity_sha256'] != after['identity_sha256']
                or checkpoint.read(f"commits/{gate['batch_id']}") is None):
            raise RuntimeError('Checkpoint identity/replay commit validation failed')
        metrics = urlopen(f"http://127.0.0.1:{after_identity['metrics_port']}/metrics", timeout=10).read()
        (directory / 'recovery-metrics.prom').write_bytes(metrics)
        save_json(directory / 'recovery.json', {'before_query': before_identity, 'after_query': after_identity,
            'checkpoint': after, 'acknowledged': len(publisher.receipts), 'same_checkpoint_identity': True})
        emit('recovery_offsets_committed', **after)
        recovered.send(signal.SIGTERM)
        if recovered.wait() != 0:
            raise RuntimeError('Recovery did not shut down cleanly')
        command = ['/opt/spark/bin/spark-submit', str(ROOT / 'src/monitoring/e4b_verify.py'),
                   '--directory', str(directory)]
        emit('verify_start', command=command)
        with (directory / 'verify-driver.log').open('x', encoding='utf-8') as output:
            completed = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=900)
        if completed.returncode:
            raise RuntimeError('Verification failed; inspect verify-driver.log')
        report = json.loads((directory / 'verification.json').read_text())
        for row in report['reconciliation']:
            emit('daily_reconciliation', **row)
        if not report['passed']:
            raise RuntimeError('E4b verification mismatch')
        emit('E4B_PASS', rows=rows, missing=report['missing'], duplicate_ids=report['duplicate_ids'],
             prekill_rows_unchanged=report['prekill_rows_unchanged'], days=specification['days'])
        return report
    finally:
        if tail is not None and tail.is_alive():
            tail.join(timeout=180)
        publisher.output.close()
        for driver in drivers:
            if driver.process.poll() is None:
                driver.send(signal.SIGTERM)
                driver.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--rows', type=int, default=20000)
    args = parser.parse_args()
    run(args.directory, rows=args.rows)


if __name__ == '__main__':
    main()
