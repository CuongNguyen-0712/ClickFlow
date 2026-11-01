"""Prove valid-only dedup plus error bypass in one Spark query; retain state."""
from datetime import datetime
import json
from pathlib import Path
from uuid import uuid4

from src.common.spark_session import get_spark
from src.storage.quality_rules import classify_records
from src.storage.deduplication import deduplicate_valid_records


spark = get_spark('watermark-union-check')
spark.sparkContext.setLogLevel('WARN')
try:
    run_id = uuid4().hex
    folder = Path('/opt/app/data/raw') / ('watermark-union-' + run_id)
    folder.mkdir()
    base = {'event_id': 'same-id', 'event_time': '2019-11-01T00:00:00Z',
            'event_type': 'view', 'product_id': 100, 'category_id': 2053013555631882655,
            'price': 12.5, 'user_id': 200, 'source_file': '2019-Nov.csv',
            'produced_at': '2026-11-01T00:00:00Z'}
    events = [base, base, base, {**base, 'event_time': None},
              {**base, 'event_time': '2030-01-01T00:00:00Z'}]
    with (folder / 'input.json').open('x') as output:
        for offset, event in enumerate(events):
            output.write(json.dumps({'payload': json.dumps(event), 'kafka_partition': 0,
                                     'kafka_offset': offset, 'ingested_at': '2026-11-01T00:00:00Z'}) + '\n')
    raw = spark.readStream.schema('payload string, kafka_partition int, kafka_offset bigint, ingested_at timestamp').json(str(folder))
    result = deduplicate_valid_records(classify_records(raw), '1 hour')
    rows = []

    def consume(frame, batch_id):
        rows.extend(frame.collect())

    query = (result.writeStream.foreachBatch(consume).option('checkpointLocation',
             's3a://lake/_checkpoints/watermark-union-' + run_id).trigger(availableNow=True).start())
    assert query.awaitTermination(180)
    assert len(rows) == 3, [(r.event_id, r.error_reason) for r in rows]
    assert len([row for row in rows if row.error_reason is None]) == 1
    assert len([row for row in rows if row.error_reason == 'bad_event_time']) == 2
    progresses = query.recentProgress
    dropped = sum(int(op.get('customMetrics', {}).get('numDroppedDuplicateRows', 0))
                  for progress in progresses for op in progress.get('stateOperators', []))
    assert dropped == 2, progresses
    assert all(progress.get('eventTime', {}).get('max', '2019') < '2020'
               for progress in progresses)
    print(json.dumps({'event': 'watermark_union_verified', 'rows': len(rows),
                      'duplicates_dropped': dropped, 'progress': progresses}), flush=True)
finally:
    spark.stop()
