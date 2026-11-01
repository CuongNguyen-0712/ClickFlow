-- C4 handoff: UTC, read-only, snapshot-pinned and partition-pruned examples.
-- Default target: s3a://lake/bronze/events/; sample: bronze/sample_events/.
-- Runner fills ${day}, ${bronze_path}, ${bronze_version}.
-- In spark-sql, set these three SQL substitution variables before each statement.
-- Filter the partition itself; TO_DATE(event_time) is not a substitute for pruning.

-- name: events_for_day
SELECT event_date, event_time, event_type, product_id, category_id, price,
       user_id, event_id, produced_at, ingested_at
FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
WHERE event_date = DATE '${day}'
ORDER BY event_time, event_id LIMIT 20;

-- name: event_type_totals
SELECT event_date, event_type, COUNT(*) AS rows, COUNT(DISTINCT user_id) AS users
FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
WHERE event_date = DATE '${day}'
GROUP BY event_date, event_type ORDER BY event_type;

-- name: product_activity
SELECT event_date, product_id,
       SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
       SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
       SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
       ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS revenue
FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
WHERE event_date = DATE '${day}'
GROUP BY event_date, product_id ORDER BY purchases DESC, product_id LIMIT 20;

-- name: ingestion_latency
-- produced_at can be NULL; no latency claim is made for the finite sample batch.
SELECT event_date, COUNT(*) AS rows_with_produced_at,
       PERCENTILE_APPROX((UNIX_MILLIS(ingested_at) - UNIX_MILLIS(produced_at)) / 1000.0,
                         ARRAY(0.5, 0.95)) AS latency_seconds_p50_p95
FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
WHERE event_date = DATE '${day}' AND produced_at IS NOT NULL
GROUP BY event_date;

-- name: partition_pruning
EXPLAIN FORMATTED
SELECT event_id, user_id, product_id
FROM delta.`${bronze_path}` VERSION AS OF ${bronze_version}
WHERE event_date = DATE '${day}';
