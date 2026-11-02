-- C4/C5 handoff: read-only Spark SQL; session timezone UTC.
-- Dùng bronze/sample_events thay bronze/events khi xem bảng thử 100k.

SELECT event_time, event_type, product_id, category_id, price,
       user_id, event_id, produced_at, ingested_at, event_date
FROM delta.`s3a://lake/bronze/events/`
WHERE event_date = DATE '2019-11-01'
LIMIT 20;

SELECT event_date, COUNT(*) AS rows, COUNT(DISTINCT event_id) AS unique_events
FROM delta.`s3a://lake/bronze/events/`
GROUP BY event_date
ORDER BY event_date;

-- Invariant C4: kết quả phải rỗng; kiểm tra trên toàn bảng.
SELECT event_id, COUNT(*) AS occurrences
FROM delta.`s3a://lake/bronze/events/`
GROUP BY event_id
HAVING COUNT(*) > 1;

SELECT ingest_date, error_reason, COUNT(*) AS rejected_messages
FROM delta.`s3a://lake/quarantine/events/`
GROUP BY ingest_date, error_reason
ORDER BY ingest_date, error_reason;
