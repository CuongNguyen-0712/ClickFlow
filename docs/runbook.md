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
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/smoke_lake.py --sample --keep-artifacts
```

Hai bước đầu tương đương `make up-storage`, `make init-lake`. Smoke dùng
lệnh tường minh có `--keep-artifacts` để giữ kết quả. Bộ integration storage
cũ có teardown dọn prefix thử nên không nằm trong các bước giữ artifact ở đây.

`minio-init` tạo bucket theo `LAKE_BUCKET`. `init_lake.py` tạo các prefix
`raw/csv/`, `raw/ecom.events.raw/`, `bronze/`, `quarantine/`, `_checkpoints/`
và bảng `bronze/events`, `quarantine/events` theo contract C4/C5, bao gồm
kiểu dữ liệu, nullable và cột phân vùng date `event_date` / `ingest_date`.
Chạy lại không xóa dữ liệu; bảng có schema/partition khác contract sẽ báo lỗi.
Credentials lấy từ environment của container, không ghi trong Spark config.

Smoke test upload `data/sample/storage_smoke.csv` vào
`raw/csv/_smoke/<run-id>.csv`, đọc bằng S3A với schema C1, kiểm tra timestamp
UTC, bigint 19 chữ số và null, rồi ghi/đọc Delta vào
`_checkpoints/smoke/<run-id>`. Lệnh trên dùng `--keep-artifacts` để giữ object thử.
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
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/smoke_lake.py --keep-artifacts
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

`configs/pipeline.yaml` mặc định `stream.source: kafka`. Nhánh sample được
giữ nguyên và dùng các thiết lập:

```yaml
sample_path: /opt/app/data/sample/events_sample_100k.csv
sample_rows: 100000
sample_day: '2019-11-01'
sample_bronze_path: s3a://lake/bronze/sample_events/
```

Chạy từ repo khi MinIO/Spark sẵn sàng và đã upload `2019-Nov.csv`:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/sources/make_sample.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/stream_to_bronze.py --source sample
```

Tương đương `make sample` rồi `make stream-sample`; `--source sample` ép nguồn
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
Nguồn Kafka mặc định chạy Structured Streaming và dùng bảng C4/C5 chính;
cấu hình/lệnh vận hành nằm trong phần tiếp theo.

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

## Contract C4/C5 và bàn giao Processing

Bản 1.0.0 của C4/C5 giữ schema vật lý hiện có: C4 **14+1=15 cột**, C5
**5+1=6 cột**. [Bản bàn giao](contracts-c4-c5-handoff.md) mô tả ý nghĩa,
nullability, UTC, unique key, error precedence, truy vấn đọc và phiếu duyệt.
Trạng thái hiện tại là **review_ready, approval pending**. Schema ba bảng
Delta đã được đối chiếu; kiểm thử classifier/replay Kafka được ghi riêng
bên dưới. Các kết quả kỹ thuật không thay cho xác nhận duyệt của nhóm.

## Kafka → C4/C5 streaming

Luồng hiện tại dùng một query Kafka và một callback `foreachBatch` cho cả
hai đích. JSON/C3 được kiểm tra nghiêm ngặt trước Spark cast để tránh chấp
nhận sai kiểu do ép kiểu. Classifier chọn một lỗi chính theo thứ tự C5.
C4 dùng insert-only MERGE theo `event_id` toàn bảng; C5 theo
`(kafka_partition, kafka_offset)` trong một topic cố định. Payload lỗi được
giữ nguyên, kể cả khoảng trắng; tombstone giữ `payload=null`.

Sau classifier, chỉ nhánh `error_reason IS NULL` đi qua
`withWatermark("event_time", "1 hour").dropDuplicatesWithinWatermark(["event_id"])`.
Nhánh lỗi luật 1–4 bỏ qua state rồi `unionByName` với nhánh hợp lệ để giữ
một query và một callback. Timestamp null/ngoài tháng/tương lai của dòng
lỗi không tham gia watermark và vẫn tới C5.

Spark bỏ riêng cả dòng hợp lệ quá watermark, kể cả ID mới. State có thể
hết hạn theo watermark; vì vậy MERGE vẫn bảo vệ tính duy nhất toàn bảng C4
cho các dòng còn lại. Với cùng ID, dòng C4 đã tồn tại luôn được giữ nguyên.
Chỉ dòng sống sót qua stateful dedup mới tới bước cảnh báo xung đột: so sánh
đúng kiểu, null-safe trên 9 giá trị C1; Kafka/producer/ingestion metadata
thay đổi khi replay không tạo xung đột. Trùng trong cửa sổ chỉ bị bỏ và
đếm, không tới bước so sánh C1. Writer chọn ứng viên còn lại trong cùng
batch theo partition rồi offset; stateful dedup không cam kết chọn bản sao
theo thứ tự toàn cục giữa các partition.

### Khởi động

Nếu chưa có `.env`, copy `.env.example` rồi chỉnh mật khẩu. Không cần Kaggle
hay producer trên host để khởi động job; topic có thể rỗng và chờ message.

```bash
make up-stream
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic ecom.events.raw --partitions 6 --replication-factor 3 --config min.insync.replicas=2
make stream
```

`make topics` vẫn là khung TV2; Kafka CLI trên là cách tạo topic hiện có.
Nếu topic đã tồn tại, kiểm tra số partition/replica bằng `--describe` trước
khi dùng; `--if-not-exists` không sửa cấu hình topic cũ.

PowerShell tương đương:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d --build --wait kafka-1 kafka-2 kafka-3 minio minio-init spark-master spark-worker-1 spark-worker-2
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic ecom.events.raw --partitions 6 --replication-factor 3 --config min.insync.replicas=2
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/src/storage/stream_to_bronze.py
```

Job tự kiểm tra binding và khởi tạo bảng đúng contract nếu chưa có. Không
cần chạy init riêng. Cấu hình mặc định trong `configs/pipeline.yaml`:

| Thiết lập | Giá trị / hành vi |
|---|---|
| `stream.source` | `kafka`; override bằng `--source sample` hoặc `make stream-sample` |
| `kafka.topic` | `ecom.events.raw`, đúng một topic |
| `kafka.bootstrap_servers_internal` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| `stream.checkpoint_path` | `s3a://lake/_checkpoints/kafka_to_delta_watermark_v1/` |
| `stream.trigger_seconds` | `10` |
| `stream.max_offsets_per_trigger` | `500000`, giới hạn Kafka offsets mỗi trigger |
| `stream.starting_offsets` | `earliest`, chỉ khi chưa có offsets checkpoint |
| `failOnDataLoss` | Luôn `true`; không tự bỏ qua offsets đã mất do retention |
| `stream.watermark` | `1 hour`, stateful dedup và xử lý dòng hợp lệ quá watermark |
| `stream.query_name` | `clickflow-kafka-to-delta` |
| `stream.metrics_port` | `8002`, bind `0.0.0.0` trong driver container |

Có thể chọn YAML riêng bằng `--config /opt/app/data/raw/<case>/pipeline.yaml`.
`LAKE_BUCKET` của container quyết định bucket thực dùng. Chỉ chạy một
driver cho cùng cặp bảng Delta; không chạy continuous và drain đồng thời.

### Dừng, drain và resume

Chạy `make stream-drain` khi driver trước đã dừng để xử lý offsets hiện có
rồi thoát (`availableNow=True`). PowerShell tương đương:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/src/storage/stream_to_bronze.py --source kafka --available-now
```

Chạy lại `make stream` hoặc `make stream-drain` với cùng cấu hình để resume.
Sau lần đầu, checkpoint quyết định offsets; đổi `starting_offsets` không
làm đọc lại lịch sử đã commit.

Main bắt SIGINT/SIGTERM bằng cờ dừng, kiểm tra qua `awaitTermination(1)`,
rồi gọi `query.stop()`, gỡ listener và đóng endpoint trước khi dừng Spark.
Thời gian stop còn phụ thuộc Spark hủy công việc đang chạy. Log JSON
`kafka_stream_started` có `driver_pid`, `query_id`, `run_id`, checkpoint và
cổng metrics. Nếu Ctrl+C chỉ đóng Docker client nhưng driver vẫn chạy,
gửi TERM đúng PID lấy từ log của lần chạy đó:

```powershell
docker compose exec -T spark-master /bin/kill -TERM <driver_pid>
```

Thay `<driver_pid>` bằng số thực đã in, xác nhận driver kết thúc trước khi
khởi động driver mới. Giữ nguyên checkpoint và các bảng khi dừng/resume.

### Binding và tính nhất quán

`_clickflow_stream_identity.json` trong checkpoint và từng đích ràng buộc
topic, danh sách broker, đường dẫn bronze/quarantine, fingerprint schema C3
và phiên bản contract C4/C5. Riêng marker checkpoint có `checkpoint_layout`
ghi operator `dropDuplicatesWithinWatermark`, keys `[event_id]`, cột
`event_time`, watermark, routing `valid_union_invalid_v1` và shuffle partitions.
Identity nguồn/đích cũ tương thích vẫn dùng được; checkpoint stateless cũ
thiếu layout hoặc checkpoint có state plan thay đổi bị từ chối trước init.
Default mới dùng prefix `kafka_to_delta_watermark_v1/`; giữ nguyên prefix
stateless `kafka_to_delta/` cùng mọi bằng chứng cũ. Không tái dùng checkpoint
cũ cho state plan mới. Thay operator/keys/routing/event-time/watermark hoặc
shuffle partitions cần checkpoint mới hoặc migration rõ ràng.

Binding không khớp bị từ chối. Checkpoint đã có
nội dung nhưng chưa có binding bị từ chối **trước khi khởi tạo bảng hoặc
nhận checkpoint**. Bảng không rỗng chưa có binding cũng không được tự nhận.
Đổi topic/đích/schema cần checkpoint và đích mới phù hợp hoặc kế hoạch
migration rõ ràng; không xóa checkpoint/binding để vượt kiểm tra.

Bronze và quarantine là **hai commit Delta riêng**, không có transaction
chung hai bảng. Nếu lỗi sau commit bronze, reader có thể thấy bronze mới
trong khi quarantine chưa có. Callback thất bại không hoàn tất checkpoint
batch; driver mới retry cùng offsets, MERGE bỏ qua khóa đã ghi và bổ sung
phần còn thiếu. Chỉ đánh giá kết quả hai bảng sau khi batch đã thành công;
không coi quan sát giữa hai commit là snapshot chung.

Nếu Kafka offsets đã hết retention, `failOnDataLoss=true` làm job báo lỗi.
Giữ checkpoint và dữ liệu, xác định phần lịch sử thiếu và lập kế hoạch
khôi phục/migration trước khi tiếp tục.

### Metrics

Prometheus đã có job `spark-stream` với target `spark-master:8002` trong
`configs/prometheus.yml`. Chạy profile monitoring khi cần Prometheus/Grafana;
không cần publish cổng metrics ra host. Endpoint chỉ sống khi driver sống.

- `clickflow_stream_sink_rows_total{destination}`: số dòng thực chèn sau
  commit, lấy từ Delta operation metrics; replay không cộng lại dòng cũ.
- `clickflow_stream_quarantine_errors_total{reason}`: số dòng mới theo lỗi C5.
- `clickflow_stream_duplicates_ignored_total` và
  `clickflow_stream_conflicting_event_ids_total`: số attempted rows bị MERGE
  bền bỏ qua và conflict IDs quan sát trong các dòng đã qua stateful dedup,
  ở các batch sink xử lý thành công.
- `clickflow_stream_duplicates_dropped_total`: tổng
  `stateOperators[].customMetrics.numDroppedDuplicateRows` từ Spark.
- `clickflow_stream_late_records_dropped_total`: tổng riêng
  `stateOperators[].numRowsDroppedByWatermark`; dòng hợp lệ quá watermark
  không được cộng vào duplicate drops hoặc C5.
- Progress gauges: `input_rows_per_second`, `processed_rows_per_second`,
  `input_rows`, `batch_id`, `batch_duration_milliseconds`,
  `event_watermark_seconds`, cùng prefix `clickflow_stream_`; có thêm
  `batches_total`, `running`, `failures_total`.

Spark progress có thể đếm lại input khi `foreachBatch` thực hiện nhiều
action; không dùng nó để đối soát dòng đã commit. Counter trong memory reset
khi driver restart và không thay thế audit lâu dài trong Delta. Log
`stream_batch_committed` tóm tắt insert/ignored/conflict của mỗi batch;
`event_id_conflict` ghi cảnh báo kèm một số ID để điều tra.
Log JSON `dedup_progress` chỉ ghi query/run/batch và hai số drop mới, kể cả
batch có 0 drop. Cả hai counter stateful có giá trị 0 ngay khi driver khởi
động. Progress lặp/cũ trong cùng query/run không cộng lại; tracker chỉ giữ
batch cuối của run hiện tại. Nếu payload thiếu identity hợp lệ, mỗi lần gọi
là một observation mới; Spark thực tế cung cấp query ID, run ID và batch ID.
Field count hỏng được cảnh báo và bỏ qua, không làm dừng query. Đối chiếu
manifest nhiều driver phải cộng số của từng stage, tránh cộng lại scrape
của cùng driver. `duplicates_ignored_total` không thay cho counter stateful.

### Bằng chứng Kafka trước stateful Rule 5 — 01/11/2026

Các kết quả và recipe bên dưới đã chạy với plan stateless trước Rule 5;
giữ lại để đối chiếu lịch sử. Replay/conflict trong state window hiện có
thể bị Spark bỏ trước MERGE, nên không dùng các kỳ vọng cũ để đánh giá plan
mới. Không dùng lại checkpoint stateless. Recipe kiểm chứng plan hiện tại
ở mục Luật 5 bên dưới.

Lệnh dưới đây lưu cách chạy tại mốc lịch sử; không chạy fixture stateless
này với plan mới. Dùng recipe Luật 5 để kiểm chứng triển khai hiện tại.

Harness dùng topic/đích/checkpoint riêng theo UUID, giữ toàn bộ artifact.
Mỗi stage sau setup phải chạy bằng **một spark-submit mới**, không dùng lại
SparkSession cũ. Không cần simulator TV1/producer TV2.

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/tests/integration/kafka_stream_checks.py --stage setup
# Lấy đúng hai giá trị từ JSON setup vừa in:
$case = '/opt/app/data/raw/kafka-stream-test-<run_id>/case.json'
$topic = 'clickflow-stream-test-<run_id>'
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic $topic --partitions 2 --replication-factor 3
if ($LASTEXITCODE -ne 0) { throw 'Topic creation failed' }
foreach ($stage in @('fault', 'resume', 'append', 'verify')) {
    docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/tests/integration/kafka_stream_checks.py --case $case --stage $stage
    if ($LASTEXITCODE -ne 0) { throw "Integration stage failed: $stage" }
}
```

Không chạy lại stage đã có báo cáo với cùng case: harness dùng exclusive
create để giữ bằng chứng. Tạo case mới cho lần kiểm chứng tiếp theo.

Kết quả đã xác nhận ngày 01/11/2026, case
`data/raw/kafka-stream-test-767d9cd4c267`:

| Stage / kiểm tra | Bằng chứng |
|---|---|
| `fault` | Lỗi chủ động `INJECTED_AFTER_BRONZE_COMMIT`; 1 bronze, 0 quarantine |
| `resume`, driver mới | Cùng query ID, run ID mới; 1 bronze, 2 quarantine; giữ nguyên dòng bronze đầu |
| `append`, driver mới | 2 bronze, 7 quarantine; đủ 5 `error_reason`, 7 tọa độ Kafka khác nhau |
| Replay / conflict | Replay chỉ đổi metadata bị bỏ qua; một conflict giữ nguyên 9 giá trị C1 và metadata dòng đầu |
| `verify`, driver mới | Resume checkpoint đã commit không ghi thêm; Delta version cả hai bảng vẫn **2**; payload raw và null tombstone khớp chính xác |
| Binding | Đổi topic bị từ chối; regression checkpoint không rỗng/chưa binding đã có RED trước guard, GREEN từ chối nhận checkpoint |
| Metrics / unit | HTTP scrape được trong các stage; **187 unit test đạt** |

Case giữ `case.json`, YAML và báo cáo JSON từng stage cùng log kiểm chứng
trong `data/raw/`; topic và object S3 vẫn được giữ. Có thể kiểm tra guard
checkpoint độc lập bằng `make test-checkpoint-binding` (tạo artifact riêng).
Các kết quả trên kiểm tra chức năng fixture; chưa phải benchmark throughput,
chạy toàn bộ Kaggle hoặc phê duyệt contract của nhóm. CLI continuous mặc
định đã khởi động với endpoint HTTP `8002` hoạt động; gửi SIGTERM đúng
`driver_pid` kết thúc với exit code **0**. Log giữ tại
`data/raw/kafka-stream-continuous.log`, nội dung scrape tại
`data/raw/kafka-default-continuous-metrics.txt`.

Sau SIGTERM, entrypoint mặc định đã được chạy lại bằng `--available-now`
và thoát với exit code **0**, giữ query ID
`bead834e-3760-4927-90c2-9f8407876d5e` và có run ID mới. Cổng metrics 8002
được dùng lại thành công. Log: `data/raw/kafka-stream-after-sigterm.log`.
Hồi quy `--source sample` cũng đạt: bảng thử vẫn **100.000 dòng**, báo
`reused: true`; log `data/raw/kafka-stream-sample-regression.log`.

Báo cáo tổng hợp: `data/raw/kafka-stream-verification.json`. Review độc lập
không còn phát hiện quan trọng sau khi sửa guard checkpoint. Toàn bộ kiểm
chứng giữ topic, checkpoint, Delta và log; không chạy lệnh xóa.

## Luật 5: stateful dedup và watermark

Rule 5 dùng Spark 3.5 `dropDuplicatesWithinWatermark(["event_id"])` trên
nhánh hợp lệ, sau watermark event-time `1 hour`. Một query union hai nhánh
hợp lệ/lỗi rồi gọi `foreachBatch` hiện có. Rules 1–4, schema C4/C5 và sample
override không đổi; nguồn mặc định vẫn là Kafka.

Prototype Spark 3.5.3 đã xác nhận operator `dedupeWithinWatermark`, bỏ
**2 duplicates** và không để timestamp null/tương lai của dòng lỗi làm tăng
watermark. Guard checkpoint thật đã xác nhận từ chối plan stateless cũ và
layout thay đổi. Suite sau triển khai có **240 unit test đạt**. Bảng acceptance
bên dưới là kết quả chạy Kafka/Spark/MinIO thật ngày 01/11/2026, case
`data/raw/watermark-dedup-test-386af898f66d`.

`src/sources/fault_injector.py` cung cấp
`inject_duplicates(events, duplicate_ratio=1.0, seed=42)`: trả về records
đã shuffle bằng RNG riêng và manifest. Số bản sao thêm là
`floor(original_records * duplicate_ratio)`, lấy mẫu có hoàn lại. Mỗi record
được deep-copy; giữ nguyên C3, `event_id` và timestamp, không sửa input hay
random state toàn cục. Manifest gồm `original_records`,
`input_duplicate_records`, `injected_duplicates`, `emitted_records`,
`unique_event_ids`, `expected_duplicates_dropped`, `seed`, `duplicate_ratio`.
Expected drops bằng emitted trừ unique IDs, với điều kiện các dòng hợp lệ
và trong watermark. Chưa có tính năng chèn record hỏng; harness tự tạo
fixture lỗi riêng cho C5.

Harness mới giữ topic, checkpoint, Delta, manifest và báo cáo từng phase.
Chạy `setup`, tạo topic **2 partition / RF3**, rồi `seed`, `recover` và
`within-hour` bằng **ba spark-submit mới**. `recover` tự thực hiện replay,
advance watermark và boundary check; không cần gọi các phase đó bằng CLI riêng.

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/tests/integration/watermark_dedup_checks.py --stage setup
# Điền đúng case và topic từ JSON setup vừa in:
$case = '/opt/app/data/raw/watermark-dedup-test-<run_id>/case.json'
$topic = 'watermark-dedup-test-<run_id>'
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic $topic --partitions 2 --replication-factor 3
if ($LASTEXITCODE -ne 0) { throw 'Topic creation failed' }
foreach ($stage in @('seed', 'recover', 'within-hour')) {
    docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/tests/integration/watermark_dedup_checks.py --case $case --stage $stage
    if ($LASTEXITCODE -ne 0) { throw "Integration stage failed: $stage" }
}
```

Không chạy lại stage đã tạo manifest/report trên cùng case; file được tạo
exclusive để giữ bằng chứng. Tạo case mới cho lần kiểm chứng mới, giữ
nguyên artifact cũ. Harness so sánh counter với tổng custom metric Spark và
manifest của từng phase, đồng thời đọc lại C4/C5 và scrape HTTP.

| Phase | Kết quả đã kiểm chứng |
|---|---|
| `seed` | 5 ID hợp lệ + 10 bản sao được chèn + 2 lỗi timestamp → C4 **5**, C5 **2**, duplicate drops **10**, late drops **0** |
| `recover`: replay | Driver mới cùng query ID, run ID mới; state phục hồi bỏ **15** record replay; C4/C5 vẫn **5/2**, giữ nguyên metadata dòng đầu |
| Advance watermark | Event ở **03:00 UTC** đẩy max event-time thêm 3 giờ; loại ít nhất **5** state keys cũ; C4 tăng lên **6** |
| Boundary | 2 dòng hợp lệ quá watermark bị bỏ riêng; cùng ID cũ với thời gian mới **03:01 UTC** qua state rồi bị MERGE bỏ **1**; 2 lỗi timestamp vẫn tới C5 |
| `within-hour` | Gửi lại ID ở **03:01** với timestamp **03:30 UTC**: state bỏ **1**, durable MERGE bỏ **0**; C4/C5 giữ **6/4** |
| Cuối case | C4 **6 dòng / 6 ID duy nhất**, C5 **4**; manifest khớp **26 duplicate drops**, **2 late drops** riêng và **1 durable duplicate ignored** |

Các dòng quá watermark không vào C5; duplicate trong state không tạo
`event_id_conflict`. Chỉ trường hợp còn tới MERGE sau expiry mới được kiểm
tra xung đột và giữ dòng C4 đầu. Counter reset mỗi driver; cộng báo cáo
seed/replay/within-hour để so với manifest **26**, không lấy riêng counter driver cuối.

Báo cáo cuối: `data/raw/watermark-dedup-test-386af898f66d/acceptance-report.json`.
Tổng Kafka end offsets xác nhận **39 message = 6 bronze + 4 quarantine +
26 state duplicate drops + 2 late drops + 1 durable duplicate ignored**.
Không dùng `numInputRows` để đối soát vì hai nhánh union có thể đọc lại nguồn.
Quarantine chỉ chứa bốn lỗi timestamp, giữ payload và tọa độ Kafka gốc.
Các log giữ tại `data/raw/watermark-dedup-seed.log`,
`data/raw/watermark-dedup-recover.log`, `data/raw/watermark-dedup-within-hour.log`.

Kiểm tra tương đương workflow CI trong Python 3.11 Linux đạt **238 passed,
2 skipped** (hai callback cần PySpark); log
`data/raw/watermark-ci-python311.log`. Đây là kiểm chứng local, chưa tạo run
GitHub Actions. Trong image Spark có PySpark, đủ **240 passed**.

Hồi quy lỗi giữa hai sink cũng đã chạy lại với plan stateful mới, case
`data/raw/kafka-stream-test-d6f3f3347503`: cố tình lỗi sau bronze commit cho
**1 C4 / 0 C5**, driver mới resume cho **1/2**; append cho **2/7** và đủ năm
lý do C5. Driver verify tiếp theo giữ nguyên số dòng và Delta version **2/2**,
đồng thời từ chối topic khác trên checkpoint cũ. Báo cáo từng stage nằm trong
case; log giữ ở `data/raw/watermark-fault-{fault,resume,append,verify}.log`.
Review độc lập không còn phát hiện quan trọng. Toàn bộ topic, checkpoint,
Delta và artifact kiểm thử được giữ lại.

## Bảo trì Delta: OPTIMIZE ZORDER và VACUUM

`src/storage/maintenance.py` dùng Spark 3.5.3 / Delta 3.2.1 để OPTIMIZE
**một ngày đã đóng**, `ZORDER BY (user_id, product_id)`, rồi VACUUM preview.
`--day` và `--closed-before` là bắt buộc. Ngày cần bảo trì phải nhỏ hơn mốc
đóng độc quyền; mốc đóng không được sau ngày hiện tại UTC. Operator chỉ công bố
mốc này sau khi writer, simulator replay và backfill không còn ghi các ngày
trước đó. Watermark hoặc ngày lịch đã qua không tự chứng minh partition đóng.

Mặc định dùng `lake.bronze`, tôn trọng `LAKE_BUCKET`; `--table-path` cho phép
chọn bảng thử/sample riêng. Lệnh dưới đây chạy OPTIMIZE và **chỉ DRY RUN**:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/maintenance.py --day 2019-11-01 --closed-before 2019-11-02
# Tương đương nếu có GNU Make:
make maintenance DAY=2019-11-01 CLOSED_BEFORE=2019-11-02
```

Scheduler hằng ngày gọi lệnh này sau khi bước đóng ngày hoàn tất, truyền ngày
và mốc đóng do pipeline xác nhận. Ví dụ đóng ngày D xong thì bảo trì `DAY=D`,
`CLOSED_BEFORE=D+1`; không lấy ngày hôm qua một cách tự động khi replay còn chạy.
Chưa cài lịch chạy trên host. Target Make không có cờ xóa tự động.

Chính sách người dùng đã chọn trong `configs/pipeline.yaml → maintenance`:

| Cấu hình | Giá trị / ý nghĩa |
|---|---|
| `retention_hours` | **720 giờ / 30 ngày**, mức giữ file khi VACUUM |
| `time_travel_hours` | **720 giờ / 30 ngày** cần bảo vệ |
| `max_stream_lag_hours` | **168 giờ / 7 ngày** gián đoạn/lag tối đa của stream đọc Delta |
| `max_transaction_hours` | **24 giờ**, tăng nếu writer/reader transaction có thể lâu hơn |
| `target_file_size_bytes` | **128 MiB**, mục tiêu file OPTIMIZE, không phải cam kết kích thước chính xác |
| `small_file_size_bytes` | File **<16 MiB** được đếm là nhỏ |

Retention phải ít nhất 7 ngày, đủ cửa sổ time travel và lớn hơn thời gian
transaction/stream lag. Job không giảm `delta.deletedFileRetentionDuration`
đã có; bảng đặt lớn hơn 30 ngày thì tăng retention của job tương ứng. Job
từ chối nếu `delta.logRetentionDuration` ngắn hơn 30 ngày hoặc nếu
`spark.databricks.delta.retentionDurationCheck.enabled=false`. Không tắt
safety check, không sửa table properties để ép chạy. Log retention mặc định
Delta là 30 ngày; nếu tăng time travel thì phải tăng cả log retention của bảng.
Chính sách này giữ các phiên bản còn tồn tại, không phục hồi file đã bị xóa
trong quá khứ. Thời gian retention dựa trên tuổi file/tombstone, **không phải**
`event_time=2019` của dataset. Tham khảo [Delta retention và VACUUM](https://docs.delta.io/delta-utility/#remove-files-no-longer-referenced-by-a-delta-table),
[table properties](https://docs.delta.io/table-properties/).

`OPTIMIZE WHERE event_date = ...` chỉ viết lại partition đích;
**VACUUM áp dụng toàn bảng**, có thể chọn file cũ thuộc các ngày khác.
DRY RUN liệt kê candidate paths, không xóa và không giải phóng dung lượng.
Chỉ thêm `--execute-vacuum` khi đã được duyệt xóa thật cho đúng bảng và retention;
job vẫn preview trước khi execute. Phải tính cả reader/stream/time travel cần
giữ của **toàn bảng**, không chỉ partition vừa compact. Tài liệu [OPTIMIZE và
ZORDER](https://docs.delta.io/optimizations-oss/#z-ordering-multi-dimensional-clustering)
mô tả partition predicate và data skipping.

Job xuất JSON log `before`, `after_optimize`, kết quả cuối và mặc định tạo
`data/raw/maintenance-<day>-<uuid>.json`; có thể chỉ định `--report /opt/app/data/raw/<file-moi>.json`.
Report tồn tại bị từ chối trước khi thay đổi bảng. Ngày không có file sẽ báo
`skipped_empty_partition`, không chạy OPTIMIZE/VACUUM. Bảng sai partition/type,
ngày mở hoặc retention không an toàn bị từ chối trước OPTIMIZE.

`before/after` ghi Delta version, số active files, tổng bytes, small files,
min/max/avg bytes cho cả bảng và partition. Số liệu lấy từ AddFile trong snapshot
Delta 3.2.1, không quét toàn bộ dòng Parquet. File cũ đã bỏ khỏi snapshot vẫn
còn vật lý để giữ time travel: không diễn giải `files_reduced` thành số object
đã xóa hoặc dung lượng đĩa đã thu hồi. `optimize_metrics`, `elapsed_seconds`,
SQL, plan và `vacuum.candidate_paths` dùng làm bằng chứng E5.

Kiểm chứng fixture riêng (không chạy xóa):

```powershell
make test-maintenance
# PowerShell không có make:
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/tests/integration/maintenance_checks.py
```

Fixture tạo 4.800 dòng/12 file nhỏ ở ngày đích, 1.200 dòng/3 file ở ngày khác.
Harness đối chiếu dữ liệu chính xác bằng `exceptAll` hai chiều, schema,
time travel trước OPTIMIZE, file paths của partition khác, empty-day no-op,
retention guard và safety flag. Mỗi lần chạy dùng prefix UUID mới, giữ toàn bộ
fixture/report; đây là kiểm chứng chức năng cho E5, chưa phải benchmark toàn bộ Kaggle.

Kết quả đã chạy ngày 01/11/2026:

| Kiểm tra | Kết quả |
|---|---|
| Fixture ngày đích | **12 → 1 active file**, small files **12 → 1** (giảm **91,7%**); bytes **137.099 → 66.895** |
| Toàn bảng fixture | **15 → 4 active files**; **6.000 dòng** giữ nguyên chính xác, schema khớp |
| Partition khác / time travel | Ba file ngày khác giữ nguyên đường dẫn; đọc lại version trước OPTIMIZE thành công |
| Guards | Empty date no-op; safety flag false và retention bảng 60 ngày bị từ chối trước OPTIMIZE |
| CLI trên sample 100k | OPTIMIZE thành công, Delta **v1 → v2**; **100.000 dòng / 100.000 ID** và schema giữ nguyên chính xác, đọc lại v1 thành công; sample vốn chỉ có một file nên vẫn **1 → 1**, không phải case đo giảm file |
| VACUUM | Cả fixture và sample đều **DRY RUN 720 giờ**, **0 candidate paths**, chưa chạy xóa thật |
| Unit / cấu hình | **283 passed, 0 skipped** trong Python 3.11 + PySpark 3.5.3; Compose config và diff whitespace đạt |

Bằng chứng fixture:
`data/raw/maintenance-verification-0f53e3542bfd432480e866918b7198d3.json`;
log `data/raw/maintenance-integration-verified.log`. Bằng chứng CLI sample:
`data/raw/maintenance-sample-100k-d08972cae2584e75a4a9764b2d11c211.json`.
Đối chiếu 100k bằng `exceptAll` hai chiều:
`data/raw/maintenance-sample-100k-d08972cae2584e75a4a9764b2d11c211-readback.json`;
log `data/raw/maintenance-sample-readback.log`.
Unit log: `data/raw/maintenance-unit-final.log`. Các fixture RED và runtime
trước bản sửa cũng được giữ nguyên, không xóa để làm sạch kết quả.

Review độc lập không có lỗi Critical/Important trong luồng preview. Hạn chế
nhỏ còn lại: nếu Spark không khởi động được, file report đã dành chỗ có thể
rỗng; xem stderr/log của `spark-submit`. Kiểm chứng time travel là đọc ngay
phiên bản trước trên dữ liệu còn giữ, chưa mô phỏng 30 ngày trôi qua hoặc
stream ngừng đủ 7 ngày. Các kết quả trên là mốc preview ban đầu.

Sau khi người dùng cho phép, đã chạy **một lần VACUUM thật** trên
`s3a://lake/bronze/sample_events` với `RETAIN 720 HOURS` ngày 01/11/2026.
CLI hoàn tất với exit code **0**, safety check vẫn bật. Preview mới có **0**
candidate paths; Delta báo **Deleted 0 files and directories**. Lần CLI này
chạy OPTIMIZE trước VACUUM, đưa sample từ version **2 → 3**, vẫn một active file.
Không chạy VACUUM trên bảng khác hoặc tự động cấp quyền cho các lần xóa tiếp theo.

Báo cáo: `data/raw/maintenance-vacuum-approved-dd384b6b5a8341d1b961340404243e6f.json`;
log cùng tên `.log`. Đây là kiểm chứng nhánh execute với 0 đối tượng đủ tuổi,
không phải bằng chứng đã thu hồi dung lượng hoặc đã xóa file cũ hơn 30 ngày.
Đọc lại sau VACUUM xác nhận **100.000 dòng / 100.000 ID duy nhất**, schema giữ
nguyên, `exceptAll` hai chiều với **version 1** đều rỗng và version 1 vẫn đọc
được. Bằng chứng: báo cáo cùng tên `-readback.json`, log
`data/raw/maintenance-vacuum-approved-readback.log`. Task bảo trì đã được
kiểm chứng cả nhánh preview và execute trong phạm vi được duyệt.

## Đối soát SQL và bàn giao Processing

`queries/verify.sql` cung cấp đối soát ngày, chẩn đoán accounting, null profile
và kiểm tra ID trùng trong ngày. `queries/sample_queries.sql` có năm query:
20 event mẫu, tổng theo event type, activity/revenue theo product, latency
p50/p95 (bỏ `produced_at=null`), và EXPLAIN FORMATTED. Mỗi query C4 có điều kiện
trực tiếp `event_date = DATE ...` để partition pruning. Không thay bộ lọc
partition bằng `TO_DATE(event_time)`.

Chạy toàn bộ chín query bằng Spark, giữ timezone UTC và **không ghi Delta**:

```powershell
# Cohort mẫu hữu hạn 100k, bảng bronze/sample_events, không có giai đoạn C5/Kafka:
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/monitoring/reconcile.py --sample --day 2019-11-01
# CSV tháng thật -> kiểm tra C4/C5 chính; mặc định đọc toàn file CSV trên MinIO:
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/monitoring/reconcile.py --day 2019-11-01
# Nếu có Make:
make verify-sample DAY=2019-11-01
make verify DAY=2019-11-01
make test-processing-queries
```

CLI mặc định lấy nguồn từ `lake.raw_csv` + file đầu `simulator.files` (hiện
`2019-Nov.csv`); với tháng khác truyền `--source-path` đúng file. `--sample`
dùng `stream.sample_path` và `stream.sample_bronze_path`, C5 là view rỗng có
đúng kiểu vì job sample không ghi quarantine. Có thể truyền `--bronze-path`,
`--quarantine-path`, `--source-path` để chọn cohort thử riêng. Đường dẫn bên
trong container dùng `/opt/app/...` hoặc `s3a://...`.

Kết quả CSV có đúng thứ tự cột:

```csv
event_date,rows_source,rows_bronze,rows_quarantine,duplicates_dropped,match
```

`rows_source` đếm dòng **thực có** trong cohort CSV của ngày UTC được chọn;
`rows_bronze` từ partition C4, `rows_quarantine` theo ngày nguồn đã xác định.
`duplicates_dropped` là **stateful** drops từ bằng chứng, không gộp late hoặc
durable MERGE. Điều kiện số học:

```text
rows_source = rows_bronze + rows_quarantine + duplicates_dropped
              + late_records_dropped + durable_duplicates_ignored
```

Hai số cuối nằm trong kết quả `accounting_diagnostics` của JSON, giữ CSV đúng
sáu cột. `match` còn yêu cầu evidence đầy đủ, không có ngày nguồn chưa xác định,
không trùng ID C4 trong ngày và không trùng tọa độ C5 được đếm. Không lấy
`source - bronze - quarantine` làm số duplicate và không dùng `numInputRows`
của Spark (có thể đọc lặp nguồn). Counter driver reset qua restart: manifest
phải tổng hợp đúng các driver/batch/nguồn của cohort, đối chiếu từ bằng chứng
đã giữ. Bản CSV Kaggle ban đầu không tự đại diện cho các message replay/injected
thêm; khi đối soát Kafka cần cohort CSV tương ứng các dòng thực đã phát.

Thiếu manifest cho ngày thì các drop counts là **NULL**, `match=false`; CSV
ghi field NULL rỗng. Sample mode biết chắc không có state/late/MERGE drops
nên dùng 0. Mismatch xuất đủ báo cáo rồi thoát **code 2**; lỗi đọc/schema/
manifest có exception, không được coi là mismatch bình thường.

Manifest JSON, truyền bằng `--audit-manifest /opt/app/data/raw/<audit>.json`:

```json
{
  "source_path": "s3a://lake/raw/csv/2019-Nov.csv",
  "bronze_path": "s3a://lake/bronze/events",
  "quarantine_path": "s3a://lake/quarantine/events",
  "days": [{"event_date": "2019-11-01", "duplicates_dropped": 0,
            "late_records_dropped": 0, "durable_duplicates_ignored": 0}],
  "quarantine_dates": [{"kafka_partition": 0, "kafka_offset": 42,
                         "event_date": "2019-11-01"}]
}
```

Đây là mẫu cấu trúc, **không phải bằng chứng 0 drops của bảng chính**; thay số
và mapping bằng giá trị đo thực tế hoặc bỏ `quarantine_dates` nếu không cần.
Ba đường dẫn phải khớp cohort hiện chạy (không slash cuối cho bảng); số phải là
integer >=0 trong miền bigint, ngày hợp lệ/không trùng, mapping partition/offset
không trùng. Không dùng lại manifest cho topic/nguồn/đích khác.

C5 phân vùng theo **ingest_date**, không phải event_date. `parse_error` hoặc
`bad_event_time` cần mapping ngày nguồn từ manifest producer/offset; không suy
ngày từ `source_file` chỉ có tháng, ngày nạp, hoặc timestamp lỗi. Lỗi khác đã
qua luật timestamp lấy ngày UTC từ payload; mapping mâu thuẫn với ngày hợp lệ
bị từ chối, không thể chuyển dòng C5 sang ngày khác để cân số. Dòng chưa xác định được ngày
được báo riêng và ngăn `match=true`, kể cả khi thay drop count làm phép cộng
tình cờ cân. Có thể giới hạn cohort C5 bằng `--ingest-from YYYY-MM-DD` và
`--ingest-to YYYY-MM-DD` (hai đầu bao gồm); chọn cửa sổ nạp thực của cohort,
không tự truyền ngày 2019 khi dữ liệu được nạp năm 2026.

Runner pin version C4 và C5 trong report trước khi chạy SQL. Đây là cặp snapshot
tái lập được, không phải một transaction chung hai bảng: chỉ đối soát sau batch
hoàn tất trên cohort đã đóng. CSV nguồn cần bất biến trong lần chạy. ID trùng
được kiểm tra trong ngày được chọn; kiểm chứng global uniqueness C4 là bước
toàn bảng riêng theo contract, không suy ra từ một báo cáo ngày.

Null profile trả **30 dòng**: C1=9, C4=15 (cả `event_date`), C5=6 (cả
`ingest_date`). `null_ratio` trong [0,1], tính null thật, không coi chuỗi rỗng
hay NaN là SQL NULL. Nhóm 0 dòng có `null_count=0`, ratio **NULL**, không giả
vờ có dữ liệu sạch. Timestamp nguồn/C5 chưa gán được ngày nằm trong diagnostics,
không bị bỏ qua để nhận PASS. Optional null của contract không mặc nhiên là lỗi.

Mỗi query có marker `-- name: ...`; runner đọc cả khối, không tách theo dấu
chấm phẩy trong string. Tham số `${day}`, `${bronze_path}`, `${bronze_version}`
được kiểm tra ngày/URI/version trước khi render. Khi dùng `spark-sql` thủ công,
đặt ba biến substitution này rồi chạy từng statement; `verify.sql` còn cần
ba temp view `verify_source`, `verify_quarantine` (có `source_event_date`),
`verify_audit` như header. Cách tái lập đầy đủ là dùng CLI ở trên.

Mặc định ghi `data/raw/processing-verify-<day>-<uuid>.json` và CSV cùng tên;
`--report /opt/app/data/raw/<file-moi>.json` chọn vị trí khác. Không ghi đè report
đã có. JSON giữ version, scope, manifest provenance, đủ kết quả chín query và
EXPLAIN. Chỉ tạo view/session và artifact local, không sửa bảng/checkpoint.

Kết quả đã chạy ngày 01/11/2026:

- Mẫu100k: CSV `2019-11-01,100000,100000,0,0,true`, sample C4 version **3**;
  đủ chín query. Báo cáo `data/raw/processing-sample-59ff2843b8cd46558002a589c422fedd.{json,csv}`.
- CSV MinIO tháng thật: ngày 01/11/2019 có **1.445.360 dòng**; main C4/C5
  **0/0**, thiếu drop manifest, CSV `2019-11-01,1445360,0,0,,false`, exit **2**.
  Báo cáo `data/raw/processing-production-59ff2843b8cd46558002a589c422fedd.{json,csv}`.
- Fixture hai ngày: **9 = 2 bronze + 3 quarantine + 2 state drops + 1 late +
  1 durable**; 30 null-profile rows đúng, chỉ đọc **1 file** ngày đích, không
  đọc file ngày kia. Evidence `data/raw/processing-query-verification-b62428e85048490c975b1e46b6c23c16.json`.
  Runner không làm tăng Delta version; append duplicate dùng test pinning chỉ
  tác động fixture riêng. Log `data/raw/processing-queries-final-integration.log`.
- Review đã sửa lỗi mapping ghi đè timestamp C5 hợp lệ bằng regression
  RED→GREEN; mapping mâu thuẫn và drop count bù không còn nhận PASS.
  Suite cuối **332 passed, 0 skipped**, log `data/raw/processing-queries-unit-final.log`;
  Compose validation và diff whitespace đạt. Đây là kiểm chứng local, chưa push/PR.

`make verify` mặc định chưa truyền manifest; dùng CLI có `--audit-manifest`
khi đã có evidence đo được. Hạn chế báo cáo lỗi: nếu ghi CSV thất bại sau khi
JSON đã serialize, error object có thể được nối thêm làm JSON không parse
được; process vẫn lỗi, cần xem stderr/log. Các report và fixture đều được giữ.

## E3: latency và file nhỏ

`benchmarks/e3_latency.sh` chạy riêng E3; `src/monitoring/e3_benchmark.py`
là replayer phục vụ thí nghiệm, dùng producer native `confluent-kafka` và
**pipeline production hiện có** trong `start_kafka_stream`. Các module
simulator/producer tổng quát vẫn là task riêng, không được coi đã hoàn tất
nhờ benchmark này.

Ma trận mặc định gồm **6 cấu hình × 3 lần = 18 trial**: processing-time trigger
5/10/30 giây, replay100/1000x. Trong mỗi block repetition, thứ tự sáu cấu hình
được shuffle với seed42. Mỗi trial dùng cùng **100.000 dòng C1 thật** từ
`data/sample/events_sample_100k.csv`, giữ giá trị, thứ tự event-time và timestamp
nguồn. SHA256, số dòng và biên thời gian nằm trong manifest. Mẫu hiện có span
**3.452 giây** (15:35:08–16:32:40 UTC ngày 01/11/2019), nên thời gian phát
mục tiêu là **34,52s ở100x**, **3,452s ở1000x**. Đây là thí nghiệm cohort cố định,
bao gồm burst ngắn; không suy ra steady-state throughput hay toàn bộ Kaggle.

### Chạy và artifact

Cluster Kafka3/MinIO/Spark master+hai workers phải đang healthy. Từ Git Bash
(Windows) hoặc Bash/WSL/Linux ở repo root:

```bash
bash benchmarks/e3_latency.sh
# Hoặc:
make bench-e3
# Pilot1000 dòng, chỉ kiểm tra đường chạy; KHÔNG đủ điều kiện Done E3:
bash benchmarks/e3_latency.sh --pilot --rows 1000
```

Script cài `benchmarks/requirements.txt` (confluent-kafka2.6.0,
matplotlib3.9.2) vào `/tmp/clickflow-e3-deps` trong Spark master rồi thiết lập
PYTHONPATH riêng cho driver. Không recreate container và không thay package của
worker. Trên Git Bash, script tắt chuyển đổi path MSYS cho Docker; dùng Git Bash
thực thay cho `C:\Windows\System32\bash.exe` nếu máy chưa có WSL.

Output được stage trong `data/raw/e3-<id>/`, rồi copy vào
`benchmarks/results/e3-<id>/`, giữ driver log và artifact kể cả khi có lỗi:

- `manifest.json`, `environment.json`: source SHA, thứ tự trial, cấu hình cố định,
  source/contract/code hashes, phiên bản Python/Spark/Delta/producer và Spark resources.
- `trial-01.json`…: số đo từng lần, source/query/Kafka identities, offsets,
  warm-up baseline, progress và scrape metrics. JSON trial đã xác minh không ghi đè.
- `trials.csv`, `summary.csv`, `README.md`: bảng raw và tổng hợp mean/sample SD.
- `latency.png/.svg`, `tradeoff.png/.svg`: biểu đồ riêng để xuất/chia sẻ.
- `completed.json`: `full_e3_complete=true` chỉ khi đủ matrix và >=3 trial/cell.
  Pilot luôn `false`, không trộn vào bảng E3 chính.

Bảng/biểu đồ là artifact dẫn xuất có thể tái tạo từ JSON gốc khi export bị ngắt;
không thay thế JSON bằng chứng. Mỗi lần gọi wrapper tạo session mới. **Không
truyền `--resume` hay `--output` tùy ý qua wrapper** vì copy path của wrapper
thuộc session mới. Resume trực tiếp đúng stage cũ:

```powershell
docker compose exec -T -e PYTHONPATH=/tmp/clickflow-e3-deps:/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip -e MPLCONFIGDIR=/tmp/clickflow-e3-mpl spark-master /opt/spark/bin/spark-submit /opt/app/src/monitoring/e3_benchmark.py --output /opt/app/data/raw/e3-<id> --resume
docker compose cp spark-master:/opt/app/data/raw/e3-<id>/. benchmarks/results/e3-<id>/
```

Resume từ chối thay đổi source, row count, matrix, producer/pipeline cấu hình,
Spark/dependency môi trường hoặc code/contract đã bind. Cached trial còn phải
khớp cell/repetition, session, workload và fingerprint hiện tại. Nếu chủ động
đổi implementation thì bắt đầu session mới; không trộn kết quả trước/sau.
Failure attempt được giữ riêng, không tự xóa topic hay checkpoint để chạy lại.

### Định nghĩa phép đo

Warm-up200 dòng có prefix riêng, đợi **Delta đã commit + Kafka offsets đã drain**
trước khi lấy baseline Delta version. Phát dữ liệu đo ở pha1/6,1/2,5/6 của
trigger theo repetition, căn theo **epoch trigger grid của Spark**, không theo
timestamp startup/warm-up. Requested và observed phase đều được lưu. Producer
giữ key=user_id, `acks=all`, idempotence, LZ4, linger20ms, batch262144 như cấu hình.
Sáu partition/RF3/minISR2 cố định; chờ metadata có leader và full ISR trước warm-up.

`produced_at` là UTC trước lần gọi enqueue đầu tiên. `ingested_at` hiện được
Spark thay bằng timestamp của micro-batch (`CurrentBatchTimestamp`), xem
[mã Spark3.5.3](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/MicroBatchExecution.scala#L631).
Metric chính là **hiệu có dấu** `(ingested_at-produced_at)` theo microsecond,
trả ms với exact `percentile` p50/p95; **chưa bao gồm thời gian Delta commit**.
Report thêm thời điểm toàn cohort visible/drain và số batch overrun; không đổi
định nghĩa `ingested_at` của C4 để làm đẹp số.

NULL được đếm riêng, nhóm không có timestamp trả percentileNULL. Độ trễ âm
không bị clip hoặc bỏ khỏi percentile; `negative_latency_rows` cho biết có bao
nhiêu cặp như vậy (chúng có thể xuất hiện gần ranh giới snapshot offsets/batch
timestamp). Trial E3 phải có đủ timestamp cho cả100k dòng. Nếu BufferError xảy
ra, timestamp trước enqueue đầu tiên vẫn được giữ, nên phép đo bao gồm thời
gian retry; `buffer_full_retries` và pacing lag ghi rõ trường hợp này. Đây là
hạn chế boundary cho tải vượt queue, không được gọi là successful-enqueue time.

Không ép `processAllAvailable`, AvailableNow, OPTIMIZE hay VACUUM trong thí
nghiệm. `files_created` đếm **AddFile actions sau baseline warm-up** qua Delta
log; `small_files_created` là các file đó <16MiB. Có thêm số active files/bytes
cuối trial. Không nhầm số active files gồm warm-up với số file sinh ra khi đo,
hoặc số object vật lý còn giữ để time travel.

Trước khi chấp nhận trial, phải khớp: số producer delivery thành công, Kafka
end offsets, sink rows đã commit và100k dòng/100k ID đo trong Delta; C5 và ba
nhóm state/late/durable drops phải0. Không dùng `numInputRows` của union làm số
message thật. Producer pace đo độc lập bằng monotonic clock: chỉ đánh dấu đạt
khi actual speed trong ±10% của target. Miss vẫn được ghi và đếm, không đổi tên
tốc độ hay tự loại lần chạy chậm để chọn kết quả đẹp.

Biểu đồ/table tổng hợp **mean của percentile từng trial**, error bar là sample
SD của >=3 lần, chấm là raw trial. Đây không phải percentile pooled toàn bộ
event, CI95 hay dự đoán production. Cùng100k dòng giúp so sánh file count;
cohort1000x ngắn có thể vào1–2 batch nên đánh đổi file khác100x.

Profile một bảng đã có bằng script chỉ đọc:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/monitoring/latency.py --table-path s3a://lake/bronze/events
```

`--event-id-prefix` chọn cohort đo, `--after-version` chọn baseline đếm AddFile.
Không có produced_at thật (như finite sample C4) thì percentile làNULL, không0.
Kiểm chứng metric độc lập: `tests/integration/latency_metric_checks.py` đã xác
nhận p50=1,5ms/p95=3,7ms trên signed[-1,1,2,4]ms, đúng NULL coverage và chỉ1file
sinh sau warm-up; fixture/report đều được giữ.

### Kết quả E3 đã kiểm chứng — 01/11/2026

Run `benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/` hoàn tất18/18
trial, **3 lần ở mỗi cell**, từ22:18:15 đến22:38:42 Asia/Saigon. Có1,8 triệu
dòng đo,3.600 warm-up; **48 AddFile sau baseline**, tất cả <16MiB. Mọi trial
đủ100.000 dòng/ID/delivery và Kafka offsets; không có NULL timestamp, signed
negative latency, quarantine hoặc state/late/durable drop.

[Bảng/biểu đồ](../benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/README.md)
và [ANALYSIS](../benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/ANALYSIS.md)
giữ đủ số liệu, raw trial dots và SD. Hai file PNG và hai SVG đã được xem kiểm
tra bố cục. Ví dụ ở100x:5s trigger trung bình7file/p95=6,911s;30s trigger2file/
p95=28,315s. Đây là quan sát trên cohort/máy này, không phải khuyến nghị SLA.

**16/18 trial đạt pace trong ±10%.** Hai outlier10s/100x đạt75,533x và67,386x,
p95 lần lượt48,678s và110,311s; không loại khỏi mean/SD. Cell này vì vậy không
đủ để tách tác động riêng của trigger khi actual input rate cố định100x.
Snapshot tải máy và Docker VM12CPU/7,558GiB nằm cùng kết quả; được lấy sau
outlier, chưa chứng minh nguyên nhân. Không sửa nguồn/sink production để làm
thay đổi kết quả.

[Audit độc lập](../benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/independent-audit.json)
đọc lại toàn bộ1,8M timestamp pairs, dùng NumPy linear percentiles và parse
AddFile JSON trực tiếp:18/18 khớp, code hashes không đổi. Script/log audit được
đóng gói cùng kết quả. Suite **378 passed, 0 skipped**;14 warning deprecation
từ Matplotlib/pyparsing, không có test fail. Log unit được giữ tại
`benchmarks/results/e3-3eb2dde82e154bf09283424977d28cf6/verification/unit-final.log`;
proof metric và log integration cùng thư mục `verification/`.
Compose validation, Bash syntax và diff whitespace đạt; chưa commit/push/remoteCI.

Review độc lập đã sửa hai phát hiện quan trọng bằng test RED→GREEN: epoch-grid
phase và code/contract-bound resume. Hai hạn chế nhỏ còn lại (wrapper resume và
timestamp BufferError) đã ghi ở trên; lượt chính có0 BufferError retry nên không
bị trường hợp timestamp stale này. Không chạy lệnh xóa hay bảo trì Delta.

Sau khi E3 hoàn tất, người dùng yêu cầu dọn file không cần thiết: đã xóa bản
staging local, hai pilot, log debug/TDD trung gian và execution ledger nội bộ.
Toàn bộ18 trial chính, bảng/biểu đồ, manifest/environment, driver log, audit và
bằng chứng kiểm chứng cuối được giữ trong thư mục kết quả trên.35 artifact
cuối ban đầu đã được kiểm tra hash không đổi sau khi dọn. Danh sách60 file đã
dọn và hash trước khi xóa nằm trong `cleanup-manifest.json` cùng kết quả.

Stage cũ `data/raw/e3-3eb2dde82e154bf09283424977d28cf6/` không còn cần cho run
đã hoàn tất và đã được dọn; lệnh resume ở trên dành cho run còn stage chưa
dọn. Nếu cần audit lại bản lưu, copy `manifest.json`, `completed.json`,
`trial-*.json` và `independent-audit.py` vào một thư mục mới trong container rồi
chạy audit trên thư mục đó; không copy `independent-audit.json` cũ vì script
tạo report mới bằng chế độ exclusive. Dataset CSV, Kafka topic, Delta và
checkpoint MinIO không bị tác động bởi việc dọn local này.

## Luật kiểm tra 1–4 và bộ đếm

`src/storage/quality_rules.py` kiểm tra C3 trước khi Spark ép kiểu, giữ chính
xác bigint và chọn một lỗi chính theo thứ tự C5:

| Luật | error_reason | Điều kiện chính |
|---|---|---|
| 1 | parse_error | JSON không parse được, sai cấu trúc/kiểu/schema |
| 2 | bad_event_time | Thiếu/null/hỏng timestamp hoặc ngoài tháng UTC của source_file |
| 3 | bad_event_type | Thiếu/null hoặc string ngoài view/cart/purchase |
| 4 | missing_id, negative_price | product_id/user_id thiếu/null; hoặc price < 0 |

`category_code`, `brand`, `user_session` thiếu hoặc null hợp lệ; các giá trị
khác null vẫn phải đúng kiểu C3 (session có format UUID). Luật 1 nhường các
trường hợp giá trị dành riêng cho luật 2–4 như quy định trong contract C5.
Khi thiếu ID đồng thời price âm, lý do chính là `missing_id`.

Hai nhóm counter có cùng cơ sở là **số dòng quarantine mới chèn thành công**:

- `clickflow_stream_quarantine_errors_total{reason}`: 5 lý do C5.
- `clickflow_stream_quality_rule_violations_total{rule}`: 4 luật, nhãn
  `"1"`, `"2"`, `"3"`, `"4"`; cả bốn được tạo với giá trị 0.

`quality_rule_counts()` gộp `missing_id + negative_price` vào luật 4 và
kiểm tra đầu vào trước khi cập nhật metrics. Mỗi dòng chỉ tính một luật
theo lý do ưu tiên; tổng bốn bộ đếm bằng tổng số dòng quarantine mới ghi
trong vòng đời driver. Không tăng counter trong Python UDF, vì Spark có
thể thực thi lại task. Retry cùng tọa độ Kafka bị sink bỏ qua nên không
cộng lại. Counter reset khi driver restart; dùng Delta để đối soát lâu dài.

Chạy các test pure (không cần PySpark):

```bash
python -m pytest -q tests/test_quality_rules.py tests/test_stream_metrics.py
python -m pytest -q
docker compose --env-file .env.example config -q
```

Workflow `.github/workflows/ci.yml` chạy toàn bộ pytest và Compose validation.
CI cài `requirements-dev.txt`, bao gồm PySpark **3.5.3** khớp Spark container,
để chạy cả hai test callback listener. Khi tự tạo môi trường phát triển, dùng
`python -m pip install -r requirements-dev.txt` trước khi chạy pytest.

Sau khi cài PySpark 3.5.3 ngày 01/11/2026, môi trường Python 3.11 Linux
đã chạy **240 passed, 0 skipped**; `pip check` không có dependency lỗi.
Container `clickflow-pyspark-ci-20261101` và log
`data/raw/pyspark-install-ci-python311.log` được giữ lại. Đây là kết quả local
theo cấu hình CI mới; chưa tạo run GitHub Actions.

Kiểm chứng Spark/Delta thật khi storage cluster đã sẵn sàng:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/tests/integration/quality_rule_checks.py
```

Fixture có 9 message: 1 hợp lệ với cả ba optional field null, 8 lỗi bao phủ
bốn luật, gồm JSON hỏng, sai schema, null/out-of-month timestamp, sai
event_type, hai ID null và giá âm. Kết quả kiểm chứng ngày 01/11/2026:
bronze **1**, quarantine **8**, counter theo luật **2/2/1/3**. Retry cùng
batch giữ nguyên dữ liệu và bộ đếm; payload nguyên bản và int64 đã được
đối chiếu, metric đã đọc qua HTTP.

Báo cáo được giữ tại
`data/raw/quality-rules-verification-70767c47e8a245d784237b2d4331b3cf.json`;
log: `data/raw/quality-rules-integration.log`. Mỗi lần test dùng prefix UUID
riêng, không xóa hoặc ghi đè các artifact cũ. Unit test trong image Spark:
**196 passed**, trong đó **168 test luật/aggregation**.

Kiểm tra tương đương CI đã chạy local trong Python 3.11 Linux: cài toàn bộ
`requirements.txt` thành công, pytest **194 passed, 2 skipped** (hai callback
listener cần PySpark), và `docker compose --env-file .env.example config -q`
đạt. Container `clickflow-quality-ci-20261101` đã thoát với code **0** và
được giữ lại. Log: `data/raw/quality-rules-ci-python311.log`.
Đây là kết quả kiểm tra local theo workflow; chưa tạo run GitHub Actions từ
push/PR. Review độc lập không có phát hiện cần sửa.
## E4b Spark driver checkpoint failover

Start the existing Kafka/MinIO/Spark stack (`make up-stream`), ensure the real
`data/sample/events_sample_100k.csv` exists, and run from Git Bash/Linux:

```bash
bash benchmarks/e4_failover.sh
# Equivalent Make target:
make bench-e4b
```

PowerShell with this machine's Git Bash installation:

```powershell
& 'D:\Git\Git\bin\bash.exe' benchmarks/e4_failover.sh
```

The default cohort is 20,000 distinct real sample rows. Timestamps are translated
across 2019-11-01/02, remain within the source month and span less than one hour.
The retained source CSV describes exactly those shifted records; this is a
controlled failover experiment, not an unchanged full-day dataset replay.

Each invocation creates a new topic (six partitions, RF3, min ISR2), separate
Delta tables, checkpoint and local result directory. No existing topic, table,
checkpoint, E3 result or dataset is removed. A retained controller container
shares the Spark master's network namespace so executors can reach the driver;
its process namespace is separate. Docker and pip/network access are required.
Do not run competing Spark applications during the experiment.

The controller first waits for 1,000 rows to be checkpointed. A benchmark-only
callback then runs the real production sink and pauses after its Delta commit,
before returning to Spark. Another 5,000 rows exercise this fault window; the
remaining records continue publishing at 250/s. The controller validates its
owned SparkSubmit JVM's PID, process group, script and config, sends SIGKILL to
that group while the producer is active, and records exit status -9. It neither
kills the Spark master nor stops Kafka, workers or MinIO.

Recovery starts the production streaming entrypoint with the identical YAML and
checkpoint; the fault callback is absent. The harness executes the same submit
entrypoint as `make stream`, directly inside the retained controller:

```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/app/src/storage/stream_to_bronze.py \
  --config /opt/app/benchmarks/results/e4b-<id>/pipeline.yaml
```

`make stream STREAM_CONFIG=/opt/app/configs/<config>.yaml` also supports a custom
stream configuration on the standard Compose stack. That path must exist inside
`spark-master`; the default stack does not mount `benchmarks/results`, whereas
the isolated E4b controller does. The benchmark does not invoke GNU Make.

The controller verifies the same query ID, a different run ID, unchanged
checkpoint binding, the replayed batch commit and all acknowledged Kafka offsets.
The valid/invalid union writes two mirrored Kafka source entries in its offset
log: these must agree and are counted once, rather than summed twice.
The recovered production driver then stops gracefully before a separate Spark
verification application starts. Verification compares all C1 fields, event ID,
produced_at, Kafka partition/offset and event_date, checks global uniqueness and
compares all 15 fields of historical pre-kill C4 rows against the final table.
Finally it runs the existing nine Processing queries via `reconcile.py` on both
affected dates. Failure of any check produces a nonzero exit; zero source-level
drop counters are accepted only after exact receipt/source/C4 equality succeeds.

`foreachBatch` can execute a batch again after failure. The sink's insert-only
MERGE makes those attempts idempotent. Retry-attempt metrics in
`recovery-metrics.prom` are not additional distinct Kafka records and must not be
subtracted again in the daily source balance. This experiment tests recovery of
valid C4 records; it does not inject malformed C5 records or broker failures.

Primary evidence in `benchmarks/results/e4b-<id>/`:

- `case.json`, `pipeline.yaml`, `source.csv`, `cohort.json`, `deliveries.jsonl`:
  workload identity, isolated resource paths and actual delivery acknowledgments.
- `before-driver.log`, `after-driver.log`, `kill.json`, `gate.json`,
  `seed-checkpoint.json`, `recovery.json`: actual crash and recovery evidence.
- `verification.json`, `reconcile.csv`, `reconcile-2019-11-0*.json`,
  `audit-manifest.json`, `verify-driver.log`: exact comparisons and daily SQL results.
- `terminal.log`, `session.cast`, `failover.mp4`, `poster.png`,
  `video-metadata.json`: actual timed stdout capture and accelerated terminal replay.

Inspect the two CSV rows: each must have `match=True`. A successful terminal ends
with `E4B_PASS`. The stopped controller, test topic and all MinIO objects are
retained intentionally; removing them requires the user's deletion permission.
