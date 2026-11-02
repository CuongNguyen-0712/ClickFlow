"""Validate and present E5 storage/query measurements."""
import argparse
from collections import defaultdict
import csv
import json
import math
import os
from pathlib import Path
from statistics import mean, median, stdev


def aggregate_trials(trials):
    from src.monitoring.e5_storage import trial_schedule
    required = {(r['variant_id'], r['query'], r['repetition']) for r in trial_schedule()}
    groups, seen, hashes = defaultdict(list), set(), defaultdict(set)
    for row in trials:
        try:
            key = row['variant_id'], row['query'], row['repetition']
            elapsed = row['elapsed_ms']
            if (key not in required or key in seen or type(row['repetition']) is not int
                    or type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0
                    or row['verified'] is not True or not row['scans']
                    or not row['result_sha256'] or row['result_sha256'] != row['expected_sha256']):
                raise ValueError('Invalid/repeated E5 trial or result mismatch')
            seen.add(key)
            hashes[row['query']].add(row['result_sha256'])
            groups[key[:2]].append(row)
        except (KeyError, TypeError) as error:
            raise ValueError('Incomplete E5 trial') from error
    if seen != required or any(len(values) != 1 for values in hashes.values()):
        raise ValueError('E5 requires all72 matched trials from the same cohort')
    return [{'variant_id': variant, 'query': query, 'repetitions': len(rows),
             'median_ms': median(r['elapsed_ms'] for r in rows),
             'mean_ms': mean(r['elapsed_ms'] for r in rows),
             'sd_ms': stdev(r['elapsed_ms'] for r in rows),
             'min_ms': min(r['elapsed_ms'] for r in rows), 'max_ms': max(r['elapsed_ms'] for r in rows)}
            for (variant, query), rows in sorted(groups.items())]


def _csv(path, rows):
    with path.open('x', encoding='utf-8', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_reports(directory, *, output=None):
    from src.monitoring.e5_storage import DAYS, QUERIES, emit, pruning_evidence, save_json, variant_specs
    directory = Path(directory)
    destination = Path(output) if output is not None else directory
    manifest = json.loads((directory / 'manifest.json').read_text())
    variants = json.loads((directory / 'variants.json').read_text())
    trials = [json.loads(path.read_text()) for path in sorted((directory / 'trials').glob('*.json'))]
    summary = aggregate_trials(trials)
    if (len(variants) != 8 or {v['variant_id'] for v in variants} != {v['variant_id'] for v in variant_specs()}
            or any(v.get('multiset_verified') is not True for v in variants)):
        raise ValueError('Eight typed-multiset-verified variants required')
    if any(row['expected_sha256'] != manifest['expected_result_hashes'][row['query']] for row in trials):
        raise ValueError('Trial hashes differ from manifest cohort')
    by_variant = {v['variant_id']: v for v in variants}
    for trial in trials:
        spec = by_variant[trial['variant_id']]
        recalculated = pruning_evidence(trial['scans'], total_files=spec['active_data_files'],
                                         partitioned=spec['partitioned'])
        if recalculated != trial['pruning'] or (spec['partitioned'] and not recalculated['verified']):
            raise ValueError('Inconsistent or missing partition-pruning proof')
    for spec in variants:
        snapshot_path = directory / spec['snapshot_evidence']
        if not snapshot_path.is_file():
            raise ValueError('Missing snapshot evidence')
        snapshot = json.loads(snapshot_path.read_text())
        if (len(snapshot['active_files']) != spec['active_data_files']
                or sum(f['size'] for f in snapshot['active_files']) != spec['active_data_bytes']):
            raise ValueError('Snapshot file/byte totals disagree')
        for query in QUERIES:
            path = directory / 'plans' / f"{spec['variant_id']}--{query}.txt"
            if not path.is_file() or not path.stat().st_size:
                raise ValueError('Missing or empty query plan')
    outputs = ('storage.csv', 'query-summary.csv', 'comparison.csv', 'query-trials.csv', 'pruning.csv',
               'storage-comparison.png', 'query-comparison.png', 'README.md', 'verification.json')
    if output is not None:
        destination.mkdir(exist_ok=False)
    if any((destination / name).exists() for name in outputs):
        raise FileExistsError('Reports already exist; rebuild with --output NEW_DIRECTORY, preserving all inputs')
    storage = [{key: spec[key] for key in ('variant_id', 'format', 'partitioned', 'stage', 'version',
                   'active_data_files', 'active_data_bytes', 'physical_total_files', 'physical_total_bytes')}
               for spec in variants]
    combined = []
    for spec in storage:
        row = dict(spec)
        for query in QUERIES:
            stats = next(r for r in summary if r['variant_id'] == spec['variant_id'] and r['query'] == query)
            row[query + '_median_ms'] = stats['median_ms']
            row[query + '_sd_ms'] = stats['sd_ms']
        combined.append(row)
    pruning = [{'variant_id': row['variant_id'], 'query': row['query'], 'repetition': row['repetition'],
                **row['pruning']} for row in trials]
    _csv(destination / 'storage.csv', storage)
    _csv(destination / 'query-summary.csv', summary)
    _csv(destination / 'comparison.csv', combined)
    _csv(destination / 'query-trials.csv', [{key: row[key] for key in
        ('trial_number', 'variant_id', 'query', 'repetition', 'elapsed_ms', 'result_rows', 'result_sha256')}
        for row in trials])
    _csv(destination / 'pruning.csv', pruning)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    labels = [spec['variant_id'].replace('_', '\n') for spec in variants]
    fig, axes = plt.subplots(1, 2, figsize=(15, 5), constrained_layout=True)
    axes[0].bar(labels, [s['active_data_bytes']/1048576 for s in variants], color='#2563eb')
    axes[0].set_ylabel('Active snapshot data (MiB)')
    axes[1].bar(labels, [s['active_data_files'] for s in variants], color='#0f766e')
    axes[1].set_ylabel('Active data files')
    fig.suptitle(f"E5:{manifest['workload']['rows']:,} real rows / two dates; retained history reported separately")
    fig.savefig(destination / 'storage-comparison.png', dpi=160)
    plt.close(fig)
    fig, axes = plt.subplots(1, 3, figsize=(18, 5), constrained_layout=True)
    for axis, query in zip(axes, QUERIES):
        rows = [next(r for r in summary if r['variant_id'] == s['variant_id'] and r['query'] == query)
                for s in variants]
        axis.bar(labels, [r['mean_ms'] for r in rows], yerr=[r['sd_ms'] for r in rows], capsize=3)
        axis.set_title(query.replace('_', ' '))
        axis.set_ylabel('Wall time (ms), mean +/- sample SD, n=3')
    fig.suptitle('E5:warm process/storage caches; shuffled balanced order; no Spark result cache')
    fig.savefig(destination / 'query-comparison.png', dpi=160)
    plt.close(fig)
    table = ['| Variant | Active MiB | Active files | Physical MiB incl. history | Q1 median ms | Q2 median ms | Q3 median ms |',
             '|---|---:|---:|---:|---:|---:|---:|']
    for row in combined:
        timings = ' | '.join(f"{row[q + '_median_ms']:.1f}" for q in QUERIES)
        table.append(f"| {row['variant_id']} | {row['active_data_bytes']/1048576:.3f} | "
                     f"{row['active_data_files']} | {row['physical_total_bytes']/1048576:.3f} | {timings} |")
    text = f'''# E5 — PASS: format, partitioning and OPTIMIZE ZORDER

Measured on **{manifest['workload']['rows']:,} real rows**, {manifest['workload']['rows_by_day'][DAYS[0]]:,}/date for 2019-11-01/02.
Selection: first N source records per date, original timestamps and multiplicities.
Same nine typed C1 columns plus derived `event_date` in every format; no synthetic metadata.

{chr(10).join(table)}

Q1:daily event types/distinct users. Q2:top product activity/revenue (decimal arithmetic).
Q3:exact user/product lookup. All query results match the same source baseline;
all eight datasets pass exact typed multiset equality. **72 trials**, three per
variant/query, with one untimed warmup for each cell. See [comparison.csv](comparison.csv),
[query-summary.csv](query-summary.csv) and [raw trials](trials/).

![Storage](storage-comparison.png)
![Queries](query-comparison.png)

## Interpretation and limits

- Active snapshot sizes/file counts are logical data footprints. Physical totals
  include retained old Delta files and transaction metadata. The two versions
  of a Delta table share one physical directory: do not sum their physical totals.
- Eight writer tasks; partitioned output can have more files than flat output.
  Delta uses Snappy Parquet; JSON is newline JSON gzip; CSV is uncompressed.
- Delta before/after versions are pinned and queried in the same randomized
  measurement window. OPTIMIZE ZORDER(user_id,product_id) is a combined treatment;
  this small cohort does not establish the independent benefit of ZORDER.
- Wall time includes reader/view creation, planning, execution and result
  collection, excludes validation/metric capture. Spark DataFrame/result caches
  are cleared. JVM, OS and MinIO caches are warm; these are not cold IO timings.
- [pruning.csv](pruning.csv) records runtime selected files and full file sizes,
  not actual IO bytes. [plans/](plans/) contains EXPLAIN FORMATTED for all24 cells.
  Partitioned variants require nonempty event_date filters and fewer selected
  than total active files on every trial. Data skipping is distinct from pruning.
- All old files and time-travel versions are retained with 30-day table retention.
  No VACUUM/deletion is part of E5. First-N rows on two dates are a controlled
  cohort; no extrapolation to full-month or multi-host production throughput.

## Reproduce and provenance

From repo root: `bash benchmarks/e5_storage.sh` or `make bench-e5`.
Each invocation creates new storage/result scope and retains diagnostics.
[manifest.json](manifest.json) binds cohort selection/hashes, queries, lookup IDs,
settings and source URI. [variants.json](variants.json) records exact versions,
OPTIMIZE metrics and snapshot paths; [snapshots/](snapshots/) lists active and
physical files. [workload.json](workload.json) contains the scanned source prefix
hash and actual per-day bounds. Source cohort: [cohort.csv](cohort.csv).

Method references: [Delta OPTIMIZE/ZORDER](https://docs.delta.io/optimizations-oss/),
[Spark 3.5.3 scan metrics](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala).
'''
    relative = os.path.relpath(directory, destination).replace(os.sep, '/')
    if relative != '.':
        for raw_link in ('trials/', 'plans/', 'snapshots/', 'manifest.json', 'variants.json', 'workload.json', 'cohort.csv'):
            text = text.replace('](' + raw_link + ')', '](' + relative + '/' + raw_link + ')')
    with (destination / 'README.md').open('x', encoding='utf-8') as stream:
        stream.write(text)
    result = {'passed': True, 'variants': len(variants), 'trials': len(trials), 'queries': 3,
              'rows': manifest['workload']['rows'], 'all_query_results_matched': True,
              'all_typed_multisets_matched': True, 'partitioned_trials_verified': sum(
                  row['pruning']['verified'] for row in trials),
              'deletion_performed': False}
    save_json(destination / 'verification.json', result)
    emit('E5_PASS', **result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--output', type=Path, help='New report directory; raw measurements remain unchanged')
    args = parser.parse_args()
    build_reports(args.directory, output=args.output)


if __name__ == '__main__':
    main()
