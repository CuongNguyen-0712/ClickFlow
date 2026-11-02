# Tài liệu tham khảo và nguồn minh chứng phần Storage

Đối chiếu ngày **02/11/2026**. Dự án dùng **Spark 3.5.3, Delta Lake 3.2.1,
Hadoop S3A 3.3.4**; các trang Delta hiện hành là nguồn giải thích khái niệm,
không làm thay đổi phiên bản triển khai. Các tính năng nêu trong hai chương
được đối chiếu thêm với code và các lần chạy thật trong repo.

## DL1

Amazon Web Services. [What is a Data Lake?](https://aws.amazon.com/what-is/data-lake/).
Nguồn định nghĩa kho lưu dữ liệu có cấu trúc/phi cấu trúc và lưu bản gốc cho
nhiều cách khai thác. ClickFlow dùng MinIO local, không triển khai AWS.

## DL2

Delta Lake. [Concurrency control](https://docs.delta.io/concurrency-control/).
Nguồn cho transaction trên một bảng, snapshot nhất quán và optimistic concurrency
control; không phải bảo đảm transaction chung cho nhiều bảng của ClickFlow.

## DL3

Delta Lake. [Table batch reads and writes](https://docs.delta.io/delta-batch/),
các mục Schema validation, Query an older snapshot và Data retention.
Nguồn cho schema enforcement, đọc theo version và điều kiện giữ log/data files.

## DL4

Delta Lake. [Optimizations](https://docs.delta.io/optimizations-oss/).
Nguồn phân biệt compaction, data skipping và ZORDER. Các số đo cải thiện trong
báo cáo lấy từ E5, không lấy từ ví dụ hiệu năng của tài liệu.

## DL5

Delta Lake. [Table utility commands](https://docs.delta.io/delta-utility/), mục VACUUM.
Nguồn cho thu hồi file dữ liệu hết retention và ảnh hưởng tới đọc lịch sử.

## SP1

Apache Spark, tag v3.5.3.
[Structured Streaming Programming Guide](https://github.com/apache/spark/blob/v3.5.3/docs/structured-streaming-programming-guide.md).
Nguồn cho micro-batch, checkpoint/recovery và `foreachBatch` mặc định at-least-once.

## SP2

Apache Spark, tag v3.5.3.
[PySpark DataFrame API source](https://github.com/apache/spark/blob/v3.5.3/python/pyspark/sql/dataframe.py),
docstrings `withWatermark` và `dropDuplicatesWithinWatermark`.
Nguồn cho watermark trên event-time, state và phạm vi bảo đảm khử trùng.

## SP3

Apache Spark, tag v3.5.3.
[DataSourceScanExec](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala).
Nguồn giải thích `PartitionFilters`, metric số file và tổng kích thước file được chọn.

## IMPL

Nguồn hiện thực: [pipeline.yaml](../configs/pipeline.yaml),
[stream_to_bronze.py](../src/storage/stream_to_bronze.py),
[quality_rules.py](../src/storage/quality_rules.py),
[deduplication.py](../src/storage/deduplication.py),
[stream_sink.py](../src/storage/stream_sink.py),
[maintenance.py](../src/storage/maintenance.py),
[reconcile.py](../src/monitoring/reconcile.py) và [verify.sql](../queries/verify.sql).
Contract: [C1](../contracts/source_csv.yaml), [C3](../contracts/event.schema.json),
[C4](../contracts/bronze_events.yaml), [C5](../contracts/quarantine_events.yaml).

## E3

Lần chạy `e3-3eb2dde82e154bf09283424977d28cf6`, ngày 01/11/2026:
[summary.csv](../benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/summary.csv),
[diễn giải và giới hạn](../benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/ANALYSIS.md),
[audit độc lập](../benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/independent-audit.json).
18 trial, 100.000 dòng/trial, 1,8 triệu dòng đo; warmup riêng; hai pacing miss vẫn giữ nguyên.

## E4b

Lần chạy `e4b-1016f257a6024eb085ee425572dbdaea`, ngày 01/11/2026 (UTC+7):
[kết quả](../benchmarks/results/e4b-1016f257a6024eb085ee425572dbdaea/verification.json),
[kill](../benchmarks/results/e4b-1016f257a6024eb085ee425572dbdaea/kill.json),
[recovery](../benchmarks/results/e4b-1016f257a6024eb085ee425572dbdaea/recovery.json),
[reconcile.csv](../benchmarks/results/e4b-1016f257a6024eb085ee425572dbdaea/reconcile.csv),
[log sau phục hồi](../benchmarks/results/e4b-1016f257a6024eb085ee425572dbdaea/after-driver.log).
Cohort20.000 dòng từ mẫu 100.000 dòng, timestamp dịch qua hai ngày; toàn bộ bản ghi hợp lệ.

## E5

Lần chạy `e5-e385de717a4a4bfd8994c96ad3906ce6`, ngày 02/11/2026:
[comparison.csv](../benchmarks/results/e5-e385de717a4a4bfd8994c96ad3906ce6/comparison.csv),
[query-summary.csv](../benchmarks/results/e5-e385de717a4a4bfd8994c96ad3906ce6/query-summary.csv),
[pruning.csv](../benchmarks/results/e5-e385de717a4a4bfd8994c96ad3906ce6/pruning.csv),
[verification.json](../benchmarks/results/e5-e385de717a4a4bfd8994c96ad3906ce6/verification.json),
[phân tích](../benchmarks/results/e5-e385de717a4a4bfd8994c96ad3906ce6/ANALYSIS.md).
100.000 dòng timestamp gốc, 50.000 dòng/ngày 01–02/11/2019; tám cấu hình, 72 trial.

## Quy ước đọc số liệu

E3 báo mean và sample SD của percentile từng trial, không phải percentile gộp
hay confidence interval. E5 bảng chương 4 dùng median ba lần/cell và MiB=2^20bytes;
biểu đồ E5 dùng mean±sample SD. E4b là một case fault injection, không phải
thống kê xác suất lỗi. Ba cohort phục vụ ba câu hỏi khác nhau, không ghép thành
một tập đo thống nhất và không suy rộng thành kết quả full-month.
