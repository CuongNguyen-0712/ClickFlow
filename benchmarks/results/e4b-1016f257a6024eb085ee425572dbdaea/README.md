# E4b — PASS: Spark SIGKILL và phục hồi từ checkpoint

Đã chạy trên Kafka, Spark 3.5.3, Delta 3.2.1 và MinIO thật. Case sử dụng
**20.000 dòng phân biệt lấy từ mẫu thật 100k**, dịch `event_time` qua hai ngày
01–02/11/2019 trong khoảng 710 giây. Thời điểm đo: **23:49:45–23:52:40 ngày
01/11/2026 (UTC+7)**. Topic, bảng Delta và checkpoint thuộc scope riêng
`e4b-1016f257a6024eb085ee425572dbdaea`.

| event_date | rows_source | rows_bronze | rows_quarantine | duplicates_dropped | match |
|---|---:|---:|---:|---:|---|
| 2019-11-01 | 10027 | 10027 | 0 | 0 | true |
| 2019-11-02 | 9973 | 9973 | 0 | 0 | true |

[CSV đối soát](reconcile.csv) · [Kết quả kiểm tra dữ liệu](verification.json) ·
[Video terminal 25 giây](failover.mp4) · [Bản ghi terminal gốc](session.cast)

## Tình huống lỗi và kết quả

1. Job đầu đã checkpoint 1.000 dòng. Batch **3** tiếp tục ghi 5.000 dòng vào
   C4, nâng tổng Delta lên **6.000 dòng**, version **2**. Callback dừng tại điểm
   ngay sau Delta commit và trước khi Spark commit checkpoint batch đó.
2. Producer tiếp tục phát ở 250 dòng/giây. Khi Kafka đã xác nhận **7.123 dòng**,
   controller kiểm tra JVM/Python driver thuộc process group **42**, gửi
   **SIGKILL** và ghi nhận exit **-9**. Producer vẫn chạy; số ACK tăng lên
   **7.645** trong hai giây gián đoạn tiếp theo.
3. Job phục hồi dùng entrypoint production và nguyên `pipeline.yaml`/checkpoint,
   không cài fault callback. `query_id` vẫn là
   `381e0292-9b53-4008-9e1b-0278b7e06e14`, `run_id` đổi từ
   `ed88ecb2-b556-4cdd-94a5-ed7ed1fc8265` sang
   `3bbcac0a-634c-4ed2-915f-ab3cf2bdb838`; fingerprint checkpoint giữ nguyên.
4. Batch **3** được xử lý lại: **5.000 dòng được MERGE bỏ qua, 0 dòng chèn thêm**.
   Sau đó job ghi thêm 14.000 dòng và checkpoint theo kịp đủ **20.000 offset**.
   Driver phục hồi dừng graceful, exit **0**.
5. Job kiểm tra riêng so sánh đầy đủ trường C1, `event_id`, `produced_at`, Kafka
   partition/offset và `event_date`: **0 mất, 0 thừa, 0 sai giá trị, 0 trùng ID**.
   Toàn bộ **15 trường của 6.000 dòng trước kill**, kể cả `ingested_at`, giữ nguyên.
   `reconcile.py` chạy cả chín truy vấn Processing trên từng ngày và trả về
   **`match=true` cho cả hai ngày**. Container benchmark kết thúc exit **0**.

5.000 lượt bỏ qua khi replay là **lượt xử lý lại cùng Kafka offset**, không phải
5.000 thông điệp nguồn bổ sung. Vì thế chúng được lưu trong metric replay riêng,
không trừ lại vào cân bằng nguồn theo ngày. Stateful duplicate/late drops đều 0.
Checkpoint được lưu tại
`s3a://lake/_checkpoints/e4b/e4b-1016f257a6024eb085ee425572dbdaea/checkpoint`.

## Minh chứng

- [Log trước kill](before-driver.log), [log sau phục hồi](after-driver.log),
  [kill và trạng thái producer](kill.json), [điểm fault](gate.json),
  [query/checkpoint phục hồi](recovery.json), [bản sao checkpoint](checkpoint-evidence.json).
- [Nguồn C1](source.csv), [cohort có event ID](cohort.json),
  [20.000 delivery receipts](deliveries.jsonl), [phạm vi và SHA nguồn](case.json).
- [Đối soát ngày 01/11](reconcile-2019-11-01.json),
  [ngày 02/11](reconcile-2019-11-02.json), [audit manifest](audit-manifest.json),
  [log job kiểm tra](verify-driver.log), [metric recovery](recovery-metrics.prom).
- [Kiểm tra chéo minh chứng và video](evidence-validation.json): source hash,
  fingerprint code, unique receipts, checkpoint, replay, lịch sử C4 và
  nội dung `.cast` khớp `terminal.log`. [FFmpeg](video-decode.log) giải mã đủ
  **100 frame, 25 giây, H.264/yuv420p, 1280×720**. Video là bản phát lại **8x**
  của stdout thực; [metadata](video-metadata.json) lưu thời gian nguồn.
- [399 unit test pass](checks/unit-tests.log),
  [Compose/Bash/git diff checks](checks/static-checks.json),
  [dependency và fingerprint code](environment.json),
  [SHA-256 artifact](artifact-sha256.json).

## Chạy lại và phạm vi

Từ root repo, dùng `bash benchmarks/e4_failover.sh` hoặc `make bench-e4b`.
Mỗi lần chạy tạo scope mới; xem [runbook](../../../docs/runbook.md#e4b-spark-driver-checkpoint-failover).
Recovery trong harness gọi trực tiếp cùng `spark-submit` entrypoint của
`make stream`, với YAML giữ nguyên; không cần GNU Make bên trong container.

Đây là minh chứng crash/recovery trên cohort hợp lệ 20k từ mẫu 100k, với timestamp
đã dịch qua hai ngày. Không suy rộng thành benchmark toàn bộ dữ liệu ngày hoặc
kiểm thử lỗi broker/bản ghi C5 hỏng. Dữ liệu, checkpoint, topic, container và hai
lần thử lỗi trước đó được giữ lại. Hai thư mục thử trước có README ghi rõ chưa
đạt; lần thử này là kết quả PASS dùng bàn giao E4b.
