-- E5 read-only demonstration: exact measured optimized snapshot, no writes.
EXPLAIN FORMATTED
SELECT *
FROM delta.`s3a://lake/_checkpoints/e5/e5-e385de717a4a4bfd8994c96ad3906ce6/delta_partitioned` VERSION AS OF 3
WHERE event_date = DATE '2019-11-01'
  AND user_id = 557746614 AND product_id = 1801881
ORDER BY event_time, event_type, product_id, category_id, category_code,
         brand, price, user_id, user_session, event_date;

SELECT *
FROM delta.`s3a://lake/_checkpoints/e5/e5-e385de717a4a4bfd8994c96ad3906ce6/delta_partitioned` VERSION AS OF 3
WHERE event_date = DATE '2019-11-01'
  AND user_id = 557746614 AND product_id = 1801881
ORDER BY event_time, event_type, product_id, category_id, category_code,
         brand, price, user_id, user_session, event_date;
