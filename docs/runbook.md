# Runbook

## MinIO + Spark khi chưa có dataset

Yêu cầu: Docker Desktop đang chạy Linux containers. Luồng này chỉ chạy
MinIO, `minio-init`, `spark-master`, `spark-worker-1` và `spark-worker-2`.
Không cần Python/Java/Make cài trên host nếu dùng lệnh PowerShell bên dưới.

```powershell
# Chạy từ thư mục repo; giữ nguyên .env nếu đã có.
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d --build --wait minio minio-init spark-master spark-worker-1 spark-worker-2
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/init_lake.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/smoke_lake.py --sample
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/tests/integration/storage_checks.py
```

Tương đương với Make: `make up-storage`, `make init-lake`,
`make smoke-storage`, `make test-storage`.

`minio-init` tạo bucket theo `LAKE_BUCKET`. `init_lake.py` tạo các prefix
`raw/csv/`, `raw/ecom.events.raw/`, `bronze/`, `quarantine/`, `_checkpoints/`
và bảng `bronze/events`, `quarantine/events` theo contract C4/C5, bao gồm
kiểu dữ liệu, nullable và cột phân vùng date `event_date` / `ingest_date`.
Chạy lại không xóa dữ liệu; bảng có schema/partition khác contract sẽ báo lỗi.
Credentials lấy từ environment của container, không ghi trong Spark config.

Smoke test upload `data/sample/storage_smoke.csv` vào
`raw/csv/_smoke/<run-id>.csv`, đọc bằng S3A với schema C1, kiểm tra timestamp
UTC, bigint 19 chữ số và null, rồi ghi/đọc Delta vào
`_checkpoints/smoke/<run-id>`. Các object thử được dọn sau mỗi lần chạy.
Không ghi dữ liệu thử vào bronze/quarantine. Kết quả thành công có log JSON
`"event": "storage_smoke_passed"`, `"synthetic": true`, `"rows": 3` và
`"master": "spark://spark-master:7077"`.

Integration checks dùng prefix UUID riêng, kiểm tra khởi tạo bảng rỗng,
partition/nullability, chạy lại vẫn giữ bản ghi đã ghi, từ chối bảng sai
contract và CSV sai header/timestamp. Các lỗi CSV trong log của test âm là
dự kiến; kết quả cuối phải là `Ran 4 tests` và `OK`.

## Khi có dataset thật

Upload `2019-Nov.csv` như README (`make upload` hoặc MinIO Console), rồi chạy:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/smoke_lake.py
```

Mặc định đọc tối đa 100 dòng từ `s3a://<LAKE_BUCKET>/raw/csv/2019-Nov.csv`.
Dùng `--limit 1000` để tăng mẫu hoặc `--source s3a://lake/raw/csv/2019-Oct.csv`
để chọn file khác. File chưa upload sẽ báo lỗi rõ ràng, không tự thay bằng
dữ liệu mẫu. Kiểm thử mẫu xác nhận hạ tầng và schema; chưa thay thế kiểm định
toàn bộ dữ liệu Kaggle, EDA hoặc benchmark tải lớn.

## Kiểm tra và xử lý lỗi

- `docker compose ps -a`: MinIO và ba service Spark phải healthy;
  `minio-init` phải `Exited (0)`.
- Spark UI: http://localhost:8090 phải có 2 worker `ALIVE` (tổng 4 cores).
- MinIO Console: http://localhost:9003; S3 API: http://localhost:9002.
- `docker compose logs --tail 100 spark-master spark-worker-1 spark-worker-2`
  để xem lỗi cluster; driver job chạy trực tiếp trong terminal `spark-submit`.
- Nếu `AccessDenied` với S3, kiểm tra `.env` và recreate các container để
  cập nhật environment; nếu schema cũ không khớp, kiểm tra contract và lập
  kế hoạch migration, không xóa bảng để ép init chạy qua.
- Delta trên S3 trong cấu hình này dùng một Spark driver ghi cùng một bảng
  tại một thời điểm. Không chạy các driver ghi đồng thời cùng bảng.
- Dừng riêng storage (giữ volume):
  `docker compose stop spark-worker-1 spark-worker-2 spark-master minio`.

Tài liệu tham chiếu:
[Delta compatibility](https://docs.delta.io/releases/),
[Spark CSV options](https://spark.apache.org/docs/3.5.6/sql-data-sources-csv.html).

## Kết quả kiểm chứng ngày 29/10/2026

Docker build và Compose config thành công. MinIO + master + hai worker
healthy; master ghi nhận 2 worker ALIVE, tổng 4 cores. Smoke test từ
`spark-master` trả `storage_smoke_passed`, `synthetic: true`, `rows: 3`.
Pytest: **5 passed**; integration: **4 tests, OK**. Có cảnh báo runtime
Spark/Python về metrics config và socket khi collect; không làm job thất bại.
Dataset thật chưa có, nên chưa xác nhận nội dung `2019-Nov.csv`.
