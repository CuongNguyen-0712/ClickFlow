"""Insert-only C4/C5 sinks; retries converge even between the two commits."""
import hashlib
import json
from functools import reduce
from operator import or_

from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F

from src.common.config import ROOT
from src.common.schemas import BRONZE_TABLE_COLUMNS, QUARANTINE_TABLE_COLUMNS, SOURCE_COLUMNS
from src.storage.init_lake import initialize_lake, table_contract


def _binding(spark, uri, expected):
    """Create once or check exactly. Never replace an existing identity file."""
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path):
        stream = fs.open(path)
        try:
            actual = json.loads(bytes(stream.readAllBytes()).decode('utf-8'))
        finally:
            stream.close()
        if actual != expected:
            raise ValueError(f'Stream identity mismatch at {uri}; use compatible config or new targets/checkpoint')
    else:
        fs.mkdirs(path.getParent())
        stream = fs.create(path, False)
        try:
            stream.write(bytearray(json.dumps(expected, sort_keys=True).encode('utf-8')))
        finally:
            stream.close()


def prepare_stream_storage(spark, settings):
    paths = {key: settings[key] for key in ('bronze', 'quarantine')}
    c3 = json.loads((ROOT / 'contracts/event.schema.json').read_text(encoding='utf-8'))
    c3.pop('title', None)
    fingerprint = hashlib.sha256(json.dumps(c3, sort_keys=True).encode()).hexdigest()
    expected = {'pipeline_format': 1, 'topic': settings['topic'],
                'bootstrap_servers': sorted(value.strip() for value in settings['bootstrap_servers'].split(',')),
                **paths, 'c3_fingerprint': fingerprint,
                'contracts': {name: table_contract(name)['version'] for name in paths}}
    # Validate BEFORE creating tables or adopting a checkpoint. An old Kafka
    # checkpoint with different/unknown sinks could otherwise skip its history.
    root = spark._jvm.org.apache.hadoop.fs.Path(settings['checkpoint'])
    marker = spark._jvm.org.apache.hadoop.fs.Path(settings['checkpoint'] + '/_clickflow_stream_identity.json')
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(marker):
        _binding(spark, str(marker), expected)
    elif fs.exists(root) and len(fs.listStatus(root)):
        raise ValueError('Refusing an existing unbound checkpoint; use a fresh checkpoint or an explicit migration')
    initialize_lake(spark, {**paths, 'checkpoints': settings['checkpoint']})
    # A C5 key is scoped to one topic. A second checkpoint may replay the same
    # topic safely, but an unrelated topic may not share these tables.
    for name, uri in paths.items():
        marker = uri + '/_clickflow_stream_identity.json'
        path = spark._jvm.org.apache.hadoop.fs.Path(marker)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(path) and DeltaTable.forPath(spark, uri).toDF().limit(1).count():
            raise ValueError(f'Nonempty {name} table has no source identity; choose new stream targets')
        _binding(spark, marker, expected)
    _binding(spark, settings['checkpoint'] + '/_clickflow_stream_identity.json', expected)


def merge_insert_only(spark, source, target, keys):
    """Return actual inserted rows; an unchanged Delta version means zero."""
    if source.isEmpty():
        return 0
    table = DeltaTable.forPath(spark, target)
    before = table.history(1).first().version
    condition = ' AND '.join(f't.`{key}` = s.`{key}`' for key in keys)
    (table.alias('t').merge(source.alias('s'), condition)
     .whenNotMatchedInsertAll().execute())
    history = table.history(1).first()
    if history.version == before:
        return 0
    return int(history.operationMetrics.get('numTargetRowsInserted', 0))


def _conflicts(valid, chosen, existing):
    columns = [name for name, _ in SOURCE_COLUMNS]
    # Null-safe typed comparison excludes Kafka/producer/ingestion metadata.
    def differing(left, right):
        return reduce(or_, [~F.col(f'{left}.{name}').eqNullSafe(F.col(f'{right}.{name}')) for name in columns])

    within = (valid.alias('s').join(chosen.alias('first'), 'event_id')
              .where(differing('s', 'first')).select('event_id'))
    persisted = (valid.alias('s').join(existing.alias('first'), 'event_id')
                 .where(differing('s', 'first')).select('event_id'))
    return within.union(persisted).distinct()


def process_batch(batch, batch_id, settings, metrics):
    """A failed callback is retried from the same offsets, not skipped."""
    batch = batch.persist()
    try:
        counts = {row.error_reason: row['count'] for row in batch.groupBy('error_reason').count().collect()}
        total = sum(counts.values())
        if not total:
            return
        bronze_columns = [name for name, _ in BRONZE_TABLE_COLUMNS]
        quarantine_columns = [name for name, _ in QUARANTINE_TABLE_COLUMNS]
        valid = batch.filter(F.col('error_reason').isNull()).select(*bronze_columns)
        bad = batch.filter(F.col('error_reason').isNotNull()).select(*quarantine_columns)
        window = Window.partitionBy('event_id').orderBy('kafka_partition', 'kafka_offset')
        chosen = valid.withColumn('_rank', F.row_number().over(window)).filter('_rank = 1').drop('_rank')
        existing = DeltaTable.forPath(batch.sparkSession, settings['bronze']).toDF()
        conflict_ids = _conflicts(valid, chosen, existing).persist()
        try:
            conflicts = conflict_ids.count()
            if conflicts:
                print(json.dumps({'event': 'event_id_conflict', 'batch_id': batch_id,
                                  'count': conflicts, 'event_ids': [r.event_id for r in conflict_ids.limit(20).collect()]}))
        finally:
            conflict_ids.unpersist()
        bronze_inserted = merge_insert_only(batch.sparkSession, chosen, settings['bronze'], ['event_id'])
        metrics.record_sink('bronze', bronze_inserted)
        old_bad = DeltaTable.forPath(batch.sparkSession, settings['quarantine']).toDF()
        new_bad = bad.join(old_bad.select('kafka_partition', 'kafka_offset'),
                           ['kafka_partition', 'kafka_offset'], 'left_anti')
        error_counts = {row.error_reason: row['count'] for row in new_bad.groupBy('error_reason').count().collect()}
        quarantine_inserted = merge_insert_only(batch.sparkSession, bad, settings['quarantine'],
                                                ['kafka_partition', 'kafka_offset'])
        metrics.record_sink('quarantine', quarantine_inserted, errors=error_counts)
        ignored = counts.get(None, 0) - bronze_inserted
        metrics.record_ignored(ignored, conflicts)
        print(json.dumps({'event': 'stream_batch_committed', 'batch_id': batch_id, 'input_rows': total,
                          'bronze_inserted': bronze_inserted, 'quarantine_inserted': quarantine_inserted,
                          'duplicates_ignored': ignored, 'conflicting_event_ids': conflicts,
                          'errors': error_counts}), flush=True)
    finally:
        batch.unpersist()
