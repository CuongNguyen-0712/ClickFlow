"""E5 controlled real-data format, partition and Delta layout benchmark."""
import argparse
import csv
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import time

from src.common.config import ROOT, load
from src.common.schemas import SOURCE_COLUMNS, ddl

DAYS = ('2019-11-01', '2019-11-02')
COLUMNS = SOURCE_COLUMNS + [('event_date', 'date')]
QUERIES = ('event_type_totals', 'product_activity', 'user_product_lookup')
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss 'UTC'"


def save_json(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, default=str, allow_nan=False)


def emit(event, **values):
    print(json.dumps({'event': event, 'at': datetime.now(timezone.utc).isoformat(), **values}, default=str), flush=True)


def extract_cohort(source, output, *, rows_per_day=50000):
    """First N actual CSV records per date, preserving multiplicity and timestamp."""
    if type(rows_per_day) is not int or rows_per_day < 1:
        raise ValueError('Positive integer quota required')
    source, output = Path(source), Path(output)
    counts, scanned, prefix_sha = dict.fromkeys(DAYS, 0), 0, hashlib.sha256()
    bounds = {}
    with output.open('x', encoding='utf-8', newline='') as target, source.open('rb', buffering=1024*1024) as raw:
        def lines():
            for line in raw:
                prefix_sha.update(line)
                yield line.decode('utf-8')
        reader = csv.DictReader(lines())
        names = [name for name, _ in SOURCE_COLUMNS]
        if reader.fieldnames != names:
            raise ValueError('CSV header must match C1')
        writer = csv.DictWriter(target, fieldnames=names, lineterminator='\n')
        writer.writeheader()
        for row in reader:
            scanned += 1
            stamp = row.get('event_time') or ''
            day = stamp[:10]
            if day not in counts or counts[day] >= rows_per_day:
                continue
            if set(row) != set(names) or any(row[name] is None for name in names):
                raise ValueError('Malformed C1 record')
            datetime.strptime(stamp, '%Y-%m-%d %H:%M:%S UTC')
            if row['event_type'] not in ('view', 'cart', 'purchase'):
                raise ValueError('Invalid event type')
            for name, kind in SOURCE_COLUMNS:
                if kind == 'bigint' and not -(2**63) <= int(row[name]) < 2**63:
                    raise ValueError('C1 ID outside int64')
            price = float(row['price'])
            if not math.isfinite(price) or price < 0:
                raise ValueError('Invalid price')
            writer.writerow(row)
            counts[day] += 1
            bound = bounds.setdefault(day, [stamp, stamp])
            bound[0], bound[1] = min(bound[0], stamp), max(bound[1], stamp)
            if all(value == rows_per_day for value in counts.values()):
                break
    if any(value != rows_per_day for value in counts.values()):
        raise ValueError(f'Source does not meet both date quotas: {counts}')
    return {'source': str(source), 'source_bytes': source.stat().st_size,
            'source_scanned_prefix_sha256': prefix_sha.hexdigest(), 'scanned_rows': scanned,
            'rows': sum(counts.values()), 'rows_by_day': counts, 'time_bounds': bounds,
            'cohort_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
            'selection': 'First N source CSV records per date; timestamps and duplicates preserved'}


def variant_specs():
    variants = []
    for kind in ('csv', 'json_gzip', 'delta'):
        for partitioned in (False, True):
            layout = 'partitioned' if partitioned else 'flat'
            for stage in (('before', 'after') if kind == 'delta' else ('original',)):
                variants.append({'variant_id': f'{kind}_{layout}' + (f'_{stage}' if kind == 'delta' else ''),
                    'format': kind, 'partitioned': partitioned, 'stage': stage, 'dataset': f'{kind}_{layout}'})
    return variants


def trial_schedule(*, seed=42):
    rng, result = random.Random(seed), []
    for repetition in (1, 2, 3):
        cells = [{'variant_id': spec['variant_id'], 'query': query, 'repetition': repetition}
                 for spec in variant_specs() for query in QUERIES]
        rng.shuffle(cells)
        result.extend(cells)
    return result


def query_sql(*, day, user_id, product_id):
    if not isinstance(day, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day):
        raise ValueError('Invalid query date')
    date.fromisoformat(day)
    if any(type(value) is not int or not -(2**63) <= value < 2**63 for value in (user_id, product_id)):
        raise ValueError('Expected int64 lookup IDs')
    where = f"WHERE event_date = DATE '{day}'"
    return {
        'event_type_totals': f'''SELECT event_type, COUNT(*) AS rows, COUNT(DISTINCT user_id) AS users
FROM e5_events {where} GROUP BY event_type ORDER BY event_type''',
        'product_activity': f'''SELECT product_id,
SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
SUM(CASE WHEN event_type = 'purchase' THEN CAST(price AS DECIMAL(20,2)) ELSE CAST(0 AS DECIMAL(20,2)) END) AS revenue
FROM e5_events {where} GROUP BY product_id ORDER BY purchases DESC, product_id LIMIT 20''',
        'user_product_lookup': f'''SELECT * FROM e5_events {where}
AND user_id = {user_id} AND product_id = {product_id}
ORDER BY event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session, event_date''',
    }


def result_digest(rows):
    canonical = sorted(json.dumps(row, sort_keys=True, default=str, allow_nan=False) for row in rows)
    return hashlib.sha256(json.dumps(canonical).encode()).hexdigest()


def pruning_evidence(scans, *, total_files, partitioned):
    selected = sum(scan.get('numFiles', 0) for scan in scans)
    real_filters = bool(scans) and all(scan.get('partition_filters', '') not in ('', '[]')
                                      and 'event_date' in scan['partition_filters'] for scan in scans)
    verified = bool(partitioned and real_filters and 0 < selected < total_files)
    return {'verified': verified, 'total_active_files': total_files, 'selected_files': selected,
            'partition_filters_present': real_filters,
            'selected_file_bytes': sum(scan.get('filesSize', 0) for scan in scans),
            'bytes_definition': 'Full sizes of selected files; not actual IO bytes'}


def read_variant(spark, spec):
    reader = spark.read
    if spec['format'] == 'delta':
        return reader.format('delta').option('versionAsOf', spec['version']).load(spec['path'])
    reader = (reader.schema(ddl(COLUMNS)).option('mode', 'FAILFAST')
              .option('timestampFormat', TIMESTAMP_FORMAT).option('dateFormat', 'yyyy-MM-dd'))
    if spec['format'] == 'csv':
        return reader.option('header', True).option('enforceSchema', False).csv(spec['path'])
    return reader.json(spec['path'])


def delta_files(spark, path):
    from pyspark.sql import DataFrame
    snapshot = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, path).update(
        False, spark._jvm.scala.Option.empty())
    files = [row.asDict(recursive=True) for row in DataFrame(snapshot.allFiles().toDF(), spark)
             .select('path', 'size', 'partitionValues').collect()]
    return int(snapshot.version()), files


def physical_files(spark, path):
    root = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = root.getFileSystem(spark._jsc.hadoopConfiguration())
    iterator, result = filesystem.listFiles(root, True), []
    while iterator.hasNext():
        item = iterator.next()
        result.append({'path': str(item.getPath()).removeprefix(path.rstrip('/') + '/'), 'size': int(item.getLen())})
    return result


def scan_metrics(frame):
    scans = []
    def visit(node):
        if node.getClass().getSimpleName() == 'FileSourceScanExec':
            metrics, metadata = {}, {}
            iterator = node.metrics().iterator()
            while iterator.hasNext():
                pair = iterator.next()
                metrics[str(pair._1())] = int(pair._2().value())
            iterator = node.metadata().iterator()
            while iterator.hasNext():
                pair = iterator.next()
                metadata[str(pair._1())] = str(pair._2())
            scans.append({**metrics, 'partition_filters': metadata.get('PartitionFilters', ''),
                          'data_filters': metadata.get('DataFilters', ''), 'format': metadata.get('Format', '')})
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())
    visit(frame._jdf.queryExecution().executedPlan())
    if not scans:
        raise AssertionError('Expected actual file scans; cached/metadata-only plans are not benchmark evidence')
    return scans


def setup_variants(spark, source, base, directory):
    from delta.tables import DeltaTable
    variants, snapshots = [], {}
    specs = variant_specs()
    for dataset in dict.fromkeys(spec['dataset'] for spec in specs):
        group = [spec for spec in specs if spec['dataset'] == dataset]
        spec, path = group[0], base + '/' + dataset
        emit('e5_write_start', dataset=dataset, path=path)
        started = time.perf_counter()
        if spec['format'] == 'delta':
            builder = (DeltaTable.create(spark).location(path).addColumns(source.schema)
                       .property('delta.logRetentionDuration', 'interval 30 days')
                       .property('delta.deletedFileRetentionDuration', 'interval 30 days'))
            if spec['partitioned']:
                builder = builder.partitionedBy('event_date')
            builder.execute()
            source.write.format('delta').mode('append').save(path)
            version, files = delta_files(spark, path)
        else:
            writer = (source.write.mode('errorifexists').option('timestampFormat', TIMESTAMP_FORMAT)
                      .option('dateFormat', 'yyyy-MM-dd'))
            if spec['partitioned']:
                writer = writer.partitionBy('event_date')
            if spec['format'] == 'csv':
                writer.option('header', True).csv(path)
            else:
                writer.option('compression', 'gzip').json(path)
            suffix = '.csv' if spec['format'] == 'csv' else '.json.gz'
            version, files = None, [f for f in physical_files(spark, path) if f['path'].endswith(suffix)]
        write_ms = (time.perf_counter() - started) * 1000
        before = {**spec, 'path': path, 'version': version, 'active_files': files, 'write_ms': write_ms}
        variants.append(before)
        if spec['format'] == 'delta':
            commands = ([f"OPTIMIZE delta.`{path}` WHERE event_date = DATE '{day}' ZORDER BY (user_id, product_id)"
                         for day in DAYS] if spec['partitioned'] else
                        [f'OPTIMIZE delta.`{path}` ZORDER BY (user_id, product_id)'])
            operations = []
            for command in commands:
                emit('e5_optimize_start', sql=command)
                started = time.perf_counter()
                rows = [row.asDict(recursive=True) for row in spark.sql(command).collect()]
                operations.append({'sql': command, 'elapsed_ms': (time.perf_counter()-started)*1000, 'metrics': rows})
            after_version, after_files = delta_files(spark, path)
            variants.append({**group[1], 'path': path, 'version': after_version,
                             'active_files': after_files, 'write_ms': write_ms, 'optimize': operations})
        physical = physical_files(spark, path)
        snapshots[dataset] = physical
        emit('e5_write_complete', dataset=dataset, physical_files=len(physical))
    for spec in variants:
        active = spec.pop('active_files')
        snapshot = {'version': spec['version'], 'active_files': active, 'physical_files': snapshots[spec['dataset']]}
        save_json(directory / 'snapshots' / (spec['variant_id'] + '.json'), snapshot)
        spec.update(active_data_files=len(active), active_data_bytes=sum(f['size'] for f in active),
                    physical_total_files=len(snapshot['physical_files']),
                    physical_total_bytes=sum(f['size'] for f in snapshot['physical_files']),
                    snapshot_evidence='snapshots/' + spec['variant_id'] + '.json')
        current = read_variant(spark, spec)
        names = [name for name, _ in COLUMNS]
        if ([(field.name, field.dataType.simpleString()) for field in current.schema] != COLUMNS
                or source.select(*names).exceptAll(current.select(*names)).limit(1).count()
                or current.select(*names).exceptAll(source.select(*names)).limit(1).count()):
            raise AssertionError('Typed multiset changed in ' + spec['variant_id'])
        spec['multiset_verified'] = True
        emit('e5_multiset_verified', variant=spec['variant_id'], version=spec['version'])
    return variants


def run(spark, directory, *, source_path, rows_per_day=50000, seed=42):
    from pyspark.sql import functions as F
    from src.common.spark_csv import read_source_csv
    from src.monitoring.e5_report import build_reports

    for name in ('snapshots', 'plans', 'trials', 'query-results'):
        (directory / name).mkdir(exist_ok=False)
    workload = extract_cohort(source_path, directory / 'cohort.csv', rows_per_day=rows_per_day)
    save_json(directory / 'workload.json', workload)
    bucket = os.environ.get('LAKE_BUCKET') or load()['lake']['bucket']
    base = f's3a://{bucket}/_checkpoints/e5/{directory.name}'
    source_uri = base + '/input/cohort.csv'
    PathClass = spark._jvm.org.apache.hadoop.fs.Path
    fs = PathClass(source_uri).getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(PathClass(source_uri)):
        raise ValueError('E5 source scope already exists')
    # S3A enumerates local FileStatus with file: URIs; the supplied source must
    # use the same scheme for Hadoop 3.3.4 URI.relativize to accept it.
    fs.copyFromLocalFile(False, False, PathClass((directory / 'cohort.csv').as_uri()), PathClass(source_uri))
    settings = {'spark.sql.adaptive.enabled': 'false', 'spark.sql.parquet.compression.codec': 'snappy',
                'spark.databricks.delta.optimize.maxFileSize': str(128*1024*1024),
                'spark.databricks.delta.autoCompact.enabled': 'false',
                'spark.databricks.delta.optimizeWrite.enabled': 'false'}
    for key, value in settings.items():
        spark.conf.set(key, value)
    source = (read_source_csv(spark, source_uri).withColumn('event_date', F.to_date('event_time'))
              .repartition(8, 'user_id', 'product_id').cache())
    counts = {str(row.event_date): row['count'] for row in source.groupBy('event_date').count().collect()}
    if counts != workload['rows_by_day']:
        raise AssertionError('Spark cohort counts disagree with extraction')
    pair = (source.where(F.col('event_date') == F.lit(DAYS[0]).cast('date'))
            .groupBy('user_id', 'product_id').count().orderBy(F.desc('count'), 'user_id', 'product_id').first())
    queries = query_sql(day=DAYS[0], user_id=pair.user_id, product_id=pair.product_id)
    source.createOrReplaceTempView('e5_events')
    expected = {name: [row.asDict(recursive=True) for row in spark.sql(sql).collect()] for name, sql in queries.items()}
    for name, rows in expected.items():
        save_json(directory / 'query-results' / (name + '.json'), rows)
    hashes = {name: result_digest(rows) for name, rows in expected.items()}
    manifest = {'case_id': directory.name, 'workload': workload, 'source_uri': source_uri,
                'queries': queries, 'query_day': DAYS[0], 'lookup_pair': pair.asDict(), 'seed': seed,
                'repetitions': 3, 'write_partitions': 8, 'settings': settings,
                'cache_policy': 'Warm JVM/OS/MinIO; no Spark DataFrame/result cache; per-cell untimed warmup',
                'schema': COLUMNS, 'delta_retention_days': 30,
                'spark_version': spark.version, 'master': spark.sparkContext.master,
                'started_at_utc': datetime.now(timezone.utc).isoformat(),
                'expected_result_hashes': hashes,
                'implementation_sha256': {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
                    for path in ('src/monitoring/e5_storage.py', 'src/monitoring/e5_report.py',
                                 'src/common/schemas.py', 'src/common/spark_csv.py',
                                 'configs/spark-defaults.conf', 'contracts/source_csv.yaml')}}
    save_json(directory / 'manifest.json', manifest)
    emit('e5_case', case_id=directory.name, workload=workload, query_pair=pair.asDict())
    variants = setup_variants(spark, source, base, directory)
    save_json(directory / 'variants.json', variants)
    source.unpersist(blocking=True)
    spark.catalog.clearCache()
    lookup = {spec['variant_id']: spec for spec in variants}
    for spec in variants:
        for name, sql in queries.items():
            spark.catalog.clearCache()
            read_variant(spark, spec).createOrReplaceTempView('e5_events')
            frame = spark.sql(sql)
            rows = [row.asDict(recursive=True) for row in frame.collect()]
            if result_digest(rows) != hashes[name]:
                raise AssertionError('Warmup result mismatch')
            plan = spark._jvm.PythonSQLUtils.explainString(frame._jdf.queryExecution(), 'formatted')
            (directory / 'plans' / f"{spec['variant_id']}--{name}.txt").write_text(str(plan), encoding='utf-8')
            emit('e5_warmup', variant=spec['variant_id'], query=name)
    for number, cell in enumerate(trial_schedule(seed=seed), 1):
        spec = lookup[cell['variant_id']]
        spark.catalog.clearCache()
        started = time.perf_counter()
        read_variant(spark, spec).createOrReplaceTempView('e5_events')
        frame = spark.sql(queries[cell['query']])
        rows = [row.asDict(recursive=True) for row in frame.collect()]
        elapsed = (time.perf_counter()-started)*1000
        scans = scan_metrics(frame)
        digest = result_digest(rows)
        if digest != hashes[cell['query']]:
            raise AssertionError('Measured result mismatch')
        evidence = pruning_evidence(scans, total_files=spec['active_data_files'], partitioned=spec['partitioned'])
        if spec['partitioned'] and not evidence['verified']:
            raise AssertionError('Partition pruning not demonstrated')
        trial = {**cell, 'trial_number': number, 'elapsed_ms': elapsed, 'result_sha256': digest,
                 'expected_sha256': hashes[cell['query']], 'result_rows': len(rows),
                 'scans': scans, 'pruning': evidence, 'verified': True,
                 'timing': 'Driver wall clock: build reader/view, plan, execute and collect; excludes checks/metrics'}
        save_json(directory / 'trials' / f'{number:02d}.json', trial)
        emit('e5_trial', **cell, elapsed_ms=round(elapsed, 2), selected_files=evidence['selected_files'])
    return build_reports(directory)


def main():
    from src.common.spark_session import get_spark
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/raw/2019-Nov.csv')
    parser.add_argument('--rows-per-day', type=int, default=50000)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    spark = get_spark('e5-storage-benchmark')
    spark.sparkContext.setLogLevel('WARN')
    try:
        run(spark, args.directory, source_path=args.source, rows_per_day=args.rows_per_day, seed=args.seed)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
