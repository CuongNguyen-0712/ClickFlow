"""Read-only NumPy/raw-Delta-log audit after the E3 measurement window."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

import numpy as np
from pyspark.sql import functions as F

from src.common.config import ROOT
from src.common.spark_session import get_spark

parser = argparse.ArgumentParser()
parser.add_argument('--directory', required=True, type=Path)
args = parser.parse_args()
manifest = json.loads((args.directory / 'manifest.json').read_text())
complete = json.loads((args.directory / 'completed.json').read_text())
assert complete['full_e3_complete'] is True and complete['verified_trials'] == 18
files = sorted(args.directory.glob('trial-*.json'))
assert len(files) == 18
for relative, digest in manifest['specification']['implementation_sha256'].items():
    assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == digest
spark = get_spark('e3-independent-audit')
spark.sparkContext.setLogLevel('WARN')
verified = []
try:
    for path in files:
        trial = json.loads(path.read_text())
        table = trial['evidence']['paths']['bronze']
        prefix = trial['trial_id'] + '-data-'
        records = (spark.read.format('delta').option('versionAsOf', trial['delta_version']).load(table)
                   .where(F.col('event_id').startswith(prefix))
                   .select('event_id', 'produced_at', 'ingested_at').collect())
        assert len(records) == len({r.event_id for r in records}) == 100000
        assert all(r.produced_at is not None and r.ingested_at is not None for r in records)
        values = np.array([(r.ingested_at-r.produced_at).total_seconds()*1000 for r in records])
        p50, p95 = np.percentile(values, [50, 95], method='linear')
        assert abs(float(p50)-trial['p50_latency_ms']) < 0.000001
        assert abs(float(p95)-trial['p95_latency_ms']) < 0.000001
        assert int((values < 0).sum()) == trial['negative_latency_rows']
        emission = trial['evidence']['emission']
        assert sum(high-low+1 for low, high in emission['offsets'].values()) == 100000
        assert sum(high+1 for low, high in emission['offsets'].values()) == 100000+trial['warmup_rows']
        adds = []
        for version in range(trial['evidence']['baseline']['file_snapshot_version']+1,
                             trial['file_snapshot_version']+1):
            uri = spark._jvm.org.apache.hadoop.fs.Path(f'{table}/_delta_log/{version:020d}.json')
            stream = uri.getFileSystem(spark._jsc.hadoopConfiguration()).open(uri)
            try:
                text = bytes(stream.readAllBytes()).decode()
            finally:
                stream.close()
            adds.extend(action['add'] for action in map(json.loads, text.splitlines()) if 'add' in action)
        assert len(adds) == trial['files_created']
        assert sum(a['size'] < 16*1024*1024 for a in adds) == trial['small_files_created']
        assert sum(a['size'] for a in adds) == trial['bytes_created']
        verified.append({'trial_id': trial['trial_id'], 'trigger_seconds': trial['trigger_seconds'],
                         'speed': trial['speed'], 'repetition': trial['repetition'],
                         'rows': len(records), 'p50_latency_ms': float(p50), 'p95_latency_ms': float(p95),
                         'files_created': len(adds), 'pace_target_met': trial['pace_target_met']})
        print(json.dumps({'event': 'e3_independent_trial_verified', **verified[-1]}), flush=True)
    assert Counter((r['trigger_seconds'], r['speed']) for r in verified) == {
        (trigger, speed): 3 for trigger in (5, 10, 30) for speed in (100, 1000)}
    output = args.directory / 'independent-audit.json'
    with output.open('x') as stream:
        json.dump({'event': 'e3_independent_audit_verified', 'verified_trials': 18,
                   'verified_rows': 1800000, 'percentiles_method': 'NumPy linear on collected UTC datetimes',
                   'file_count_method': 'direct JSON AddFile parse after warmup baseline',
                   'code_hashes_unchanged': True, 'trials': verified, 'read_only': True}, stream, indent=2)
    print(json.dumps({'event': 'e3_independent_audit_verified', 'report': str(output)}), flush=True)
finally:
    spark.stop()
