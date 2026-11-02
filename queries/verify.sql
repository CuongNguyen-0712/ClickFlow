-- Read-only C4/C5 handoff, UTC. Run with src/monitoring/reconcile.py.
-- ${day}, ${bronze_path}, ${bronze_version} are validated/rendered by the runner.
-- Inputs: verify_source (C1), verify_quarantine (C5 + source_event_date),
-- verify_audit (event_date, duplicates_dropped, late_records_dropped,
-- durable_duplicates_ignored, evidence_complete). Unknown evidence is NOT zero.
-- C5 ingest_date is NEVER treated as source event_date.

-- name: reconciliation
WITH source AS (
  SELECT COUNT(*) AS rows_source FROM verify_source
  WHERE TO_DATE(event_time) = DATE '${day}'
), bronze AS (
  SELECT COUNT(*) AS rows_bronze, COUNT(DISTINCT event_id) AS unique_ids
  FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
  WHERE event_date = DATE '${day}'
), quarantine AS (
  SELECT COUNT(*) AS rows_quarantine,
         COUNT(DISTINCT STRUCT(kafka_partition, kafka_offset)) AS unique_offsets
  FROM verify_quarantine WHERE source_event_date = DATE '${day}'
), unknown_source AS (
  SELECT COUNT(*) AS rows_unassigned FROM verify_source WHERE event_time IS NULL
), unknown_quarantine AS (
  SELECT COUNT(*) AS rows_unassigned FROM verify_quarantine WHERE source_event_date IS NULL
)
SELECT DATE '${day}' AS event_date, s.rows_source, b.rows_bronze, q.rows_quarantine,
       a.duplicates_dropped,
       COALESCE(a.evidence_complete
         AND s.rows_source = b.rows_bronze + q.rows_quarantine + a.duplicates_dropped
                            + a.late_records_dropped + a.durable_duplicates_ignored
         AND b.rows_bronze = b.unique_ids AND q.rows_quarantine = q.unique_offsets
         AND us.rows_unassigned = 0 AND uq.rows_unassigned = 0, FALSE) AS match
FROM source s CROSS JOIN bronze b CROSS JOIN quarantine q
CROSS JOIN unknown_source us CROSS JOIN unknown_quarantine uq
LEFT JOIN verify_audit a ON a.event_date = DATE '${day}';

-- name: accounting_diagnostics
SELECT DATE '${day}' AS event_date, a.evidence_complete,
       a.duplicates_dropped, a.late_records_dropped, a.durable_duplicates_ignored,
       (SELECT COUNT(*) FROM verify_source WHERE event_time IS NULL) AS rows_source_unassigned,
       (SELECT COUNT(*) FROM verify_quarantine WHERE source_event_date IS NULL) AS rows_quarantine_unassigned
FROM verify_audit a WHERE a.event_date = DATE '${day}';

-- name: null_rates
WITH profiles AS (
  SELECT 'source' AS dataset, COUNT(*) AS rows_total,
    ARRAY(
      NAMED_STRUCT('column_name', 'event_time', 'null_count', COUNT(*) - COUNT(event_time)),
      NAMED_STRUCT('column_name', 'event_type', 'null_count', COUNT(*) - COUNT(event_type)),
      NAMED_STRUCT('column_name', 'product_id', 'null_count', COUNT(*) - COUNT(product_id)),
      NAMED_STRUCT('column_name', 'category_id', 'null_count', COUNT(*) - COUNT(category_id)),
      NAMED_STRUCT('column_name', 'category_code', 'null_count', COUNT(*) - COUNT(category_code)),
      NAMED_STRUCT('column_name', 'brand', 'null_count', COUNT(*) - COUNT(brand)),
      NAMED_STRUCT('column_name', 'price', 'null_count', COUNT(*) - COUNT(price)),
      NAMED_STRUCT('column_name', 'user_id', 'null_count', COUNT(*) - COUNT(user_id)),
      NAMED_STRUCT('column_name', 'user_session', 'null_count', COUNT(*) - COUNT(user_session))
    ) AS column_nulls
  FROM verify_source WHERE TO_DATE(event_time) = DATE '${day}'
  UNION ALL
  SELECT 'bronze', COUNT(*), ARRAY(
      NAMED_STRUCT('column_name', 'event_time', 'null_count', COUNT(*) - COUNT(event_time)),
      NAMED_STRUCT('column_name', 'event_type', 'null_count', COUNT(*) - COUNT(event_type)),
      NAMED_STRUCT('column_name', 'product_id', 'null_count', COUNT(*) - COUNT(product_id)),
      NAMED_STRUCT('column_name', 'category_id', 'null_count', COUNT(*) - COUNT(category_id)),
      NAMED_STRUCT('column_name', 'category_code', 'null_count', COUNT(*) - COUNT(category_code)),
      NAMED_STRUCT('column_name', 'brand', 'null_count', COUNT(*) - COUNT(brand)),
      NAMED_STRUCT('column_name', 'price', 'null_count', COUNT(*) - COUNT(price)),
      NAMED_STRUCT('column_name', 'user_id', 'null_count', COUNT(*) - COUNT(user_id)),
      NAMED_STRUCT('column_name', 'user_session', 'null_count', COUNT(*) - COUNT(user_session)),
      NAMED_STRUCT('column_name', 'event_id', 'null_count', COUNT(*) - COUNT(event_id)),
      NAMED_STRUCT('column_name', 'kafka_partition', 'null_count', COUNT(*) - COUNT(kafka_partition)),
      NAMED_STRUCT('column_name', 'kafka_offset', 'null_count', COUNT(*) - COUNT(kafka_offset)),
      NAMED_STRUCT('column_name', 'produced_at', 'null_count', COUNT(*) - COUNT(produced_at)),
      NAMED_STRUCT('column_name', 'ingested_at', 'null_count', COUNT(*) - COUNT(ingested_at)),
      NAMED_STRUCT('column_name', 'event_date', 'null_count', COUNT(*) - COUNT(event_date))
    )
  FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
  WHERE event_date = DATE '${day}'
  UNION ALL
  SELECT 'quarantine', COUNT(*), ARRAY(
      NAMED_STRUCT('column_name', 'payload', 'null_count', COUNT(*) - COUNT(payload)),
      NAMED_STRUCT('column_name', 'error_reason', 'null_count', COUNT(*) - COUNT(error_reason)),
      NAMED_STRUCT('column_name', 'kafka_partition', 'null_count', COUNT(*) - COUNT(kafka_partition)),
      NAMED_STRUCT('column_name', 'kafka_offset', 'null_count', COUNT(*) - COUNT(kafka_offset)),
      NAMED_STRUCT('column_name', 'ingested_at', 'null_count', COUNT(*) - COUNT(ingested_at)),
      NAMED_STRUCT('column_name', 'ingest_date', 'null_count', COUNT(*) - COUNT(ingest_date))
    )
  FROM verify_quarantine WHERE source_event_date = DATE '${day}'
)
SELECT DATE '${day}' AS event_date, dataset, item.column_name, rows_total, item.null_count,
       CASE WHEN rows_total = 0 THEN CAST(NULL AS DOUBLE)
            ELSE item.null_count / CAST(rows_total AS DOUBLE) END AS null_ratio
FROM profiles LATERAL VIEW EXPLODE(column_nulls) n AS item
ORDER BY dataset, column_name;

-- name: duplicate_event_ids
SELECT event_id, COUNT(*) AS occurrences
FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
WHERE event_date = DATE '${day}'
GROUP BY event_id HAVING COUNT(*) > 1
ORDER BY occurrences DESC, event_id LIMIT 100;
