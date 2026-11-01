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

Nếu muốn giữ lại kết quả Delta để xem trên MinIO và không chạy bước xóa
object thử, thêm `--keep-artifacts`:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/smoke_lake.py --limit 1000 --keep-artifacts
```

Log trả `artifacts_retained: true` và `delta_path` chứa kết quả được giữ lại.

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

## Kết quả kiểm chứng với dataset thật ngày 01/11/2026

Đã tải file ưu tiên `2019-Nov.csv` từ dataset Kaggle được dẫn trong README:

- CSV local: `D:\ClickFlow\data\raw\2019-Nov.csv`, **9.006.762.395 byte**.
- ZIP gốc được giữ tại `data/raw/2019-Nov.csv.zip`, **2.874.125.107 byte**.
- Đã kiểm tra CRC khi giải nén; đếm **67.501.979 dòng dữ liệu** (không tính header).
- SHA-256 CSV: `addd9a27ed99abdece368019ebfba19568972c7fcbe949db3484aab8a3bcffec`.
- Manifest: `data/raw/2019-Nov.download-manifest.json`.
- Đã upload toàn bộ CSV lên `s3a://lake/raw/csv/2019-Nov.csv`;
  kích thước object bằng kích thước local.

Lệnh kiểm thử chạy từ `spark-master`:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/smoke_lake.py --limit 1000 --keep-artifacts
```

Kết quả: **`storage_smoke_passed`, `synthetic: false`, `rows: 1000`**.
Schema C1 và giá trị được giữ nguyên sau khi ghi/đọc Delta trên MinIO.
Hai worker đều ALIVE, mỗi worker 2 cores / 2048 MB. `init_lake.py` chạy lại
thành công; cả 5 prefix tồn tại. Bronze vẫn rỗng, 15 cột và partition
`event_date`; quarantine vẫn rỗng, 6 cột và partition `ingest_date`.

Kết quả Delta được giữ tại
`s3a://lake/_checkpoints/smoke/9dab97a7bfa246e490317d896b0d04c2`.
Một Spark job riêng đã đọc lại đường dẫn này và xác nhận đủ 1.000 dòng sau
khi smoke job kết thúc. Không chạy lệnh xóa hoặc bước dọn object thử.

Log được giữ trong `data/raw/verification-2026-11-01-*.log`:
`extract`, `upload`, `init`, `real-smoke`, `state`.
Task MinIO + Spark + khởi tạo lake + đọc CSV/ghi-đọc Delta đã được kiểm chứng
với dữ liệu thật. Đây là kiểm thử storage trên 1.000 dòng; chưa chạy toàn bộ
67,5 triệu dòng qua pipeline streaming, EDA hoặc benchmark.

## Đọc mẫu 100k và ghi bronze thử

`configs/pipeline.yaml` mặc định `stream.source: sample` và chứa:

```yaml
sample_path: /opt/app/data/sample/events_sample_100k.csv
sample_rows: 100000
sample_day: '2019-11-01'
sample_bronze_path: s3a://lake/bronze/sample_events/
```

Chạy từ repo khi MinIO/Spark sẵn sàng và đã upload `2019-Nov.csv`:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/sources/make_sample.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/stream_to_bronze.py
```

Tương đương `make sample` rồi `make stream`; `make stream-sample` ép nguồn
sample cho riêng lần chạy. Có thể đặt `stream.sample_path` trực tiếp thành
`s3a://lake/raw/csv/2019-Nov.csv` để bỏ bước tạo CSV local.

Reader dùng `ddl(SOURCE_COLUMNS)` theo C1, FAILFAST, kiểm tra header và
timestamp UTC. `category_id` luôn là Spark bigint/long. Việc chọn mẫu đọc
một tập ứng viên có giới hạn (110.000 dòng thuộc ngày đã chọn cho mẫu 100k), loại
dòng trùng hoàn toàn, sắp xếp theo các cột C1 rồi lấy đúng 100.000 dòng. Nếu
không đủ sự kiện khác nhau thì job báo lỗi, không ghi mẫu thiếu. Lý do:
100.000 dòng đầu của dataset thật chứa 27 dòng trùng hoàn toàn, trong khi C4
yêu cầu `event_id` duy nhất. File nguồn không bị thay đổi.

Thứ tự ứng viên phụ thuộc cách Spark chia file nguồn. CSV đã tạo là mẫu
cố định dùng cho batch thử; thay đổi cấu hình chia partition có thể làm
việc tạo lại từ file tháng chọn tập khác và bị từ chối nếu đích cũ đã tồn tại.

Bronze thử dùng đủ 14 cột C4 cộng `event_date`, đúng kiểu, nullable và
partition. `event_id` là SHA-256 của JSON chuẩn hóa từ các giá trị C1;
`kafka_partition = -1`, `kafka_offset = -1`, `produced_at = null`,
`ingested_at` là thời điểm ghi. Các trường Kafka này là giá trị giả lập,
không dùng để đo consumer offset hoặc độ trễ Kafka. `bronze/events` chính
không bị trộn dữ liệu thử.

Job đọc lại Delta và so sánh toàn bộ giá trị, số lần xuất hiện, schema và
số dòng. Chạy lại cùng mẫu không append thêm hoặc thay `ingested_at`;
nếu file CSV/bảng đích đã chứa nội dung khác thì báo lỗi và yêu cầu chọn
đích mới. Không overwrite, không xóa. Chỉ chạy một driver cho một bảng thử.

`make test-sample-bronze` chạy integration checks với fixture/đích UUID
riêng và giữ lại mọi artifact. Log thành công của job chính là
`sample_bronze_verified`, `rows: 100000`, `partition_by: ["event_date"]`.

Sample mode là một batch hữu hạn, không cần Kafka/checkpoint streaming.
Đổi cấu hình sang Kafka sẽ báo rõ chưa triển khai cho đến khi task
Structured Streaming + quality/quarantine được hoàn thành.

### Kết quả kiểm chứng ngày 01/11/2026

- `data/sample/events_sample_100k.csv`: **100.000 dòng khác nhau**, 13.343.184
  byte, thuộc ngày 2019-11-01; đọc lại CSV theo C1 giữ đúng bigint và null.
- `s3a://lake/bronze/sample_events`: **100.000 dòng**, **100.000 event_id
  duy nhất**, **15 cột** đúng C4 kể cả `event_date`.
- Partition: `event_date=2019-11-01`; xác nhận chính xác `category_id`
  bigint `2053013555631882655`.
- Chạy lại cả tạo mẫu và ghi bronze đều báo `reused: true`. Delta version
  vẫn là **1**, không thêm transaction ghi hoặc tăng số dòng.
- Một Spark job riêng đọc lại sau khi Docker khởi động lại vẫn xác nhận
  đủ 100.000 dòng. Bảng chính `bronze/events` vẫn rỗng.
- **4 integration test đạt** (60,6 giây), **5 pytest đạt**. Test và job
  đều giữ lại dữ liệu thử, không chạy lệnh xóa.

Báo cáo máy đọc được: `data/raw/sample-bronze-verification.json`.
Log: `data/raw/sample-100k-generation.log`,
`sample-100k-generation-rerun.log`, `sample-bronze-first.log`,
`sample-bronze-rerun.log`, `sample-bronze-tests.log`,
`sample-bronze-final-state-after-restart.log` (đều trong `data/raw/`).
