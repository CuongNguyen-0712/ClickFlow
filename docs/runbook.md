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

Watermark `1 hour` chỉ làm marker event-time trong progress. Job không dùng
stateful dedup/drop: một event hợp lệ, ID mới vẫn được ghi dù đến muộn.
Với cùng ID, dòng C4 đã tồn tại luôn được giữ nguyên. Xung đột so sánh đúng
kiểu, null-safe trên 9 giá trị C1; Kafka/producer/ingestion metadata thay đổi
khi replay không tạo xung đột. Trong cùng batch, ứng viên đầu được chọn theo
partition rồi offset, không suy diễn thứ tự toàn cục giữa các partition.

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
| `stream.checkpoint_path` | `s3a://lake/_checkpoints/kafka_to_delta/` |
| `stream.trigger_seconds` | `10` |
| `stream.max_offsets_per_trigger` | `500000`, giới hạn Kafka offsets mỗi trigger |
| `stream.starting_offsets` | `earliest`, chỉ khi chưa có offsets checkpoint |
| `failOnDataLoss` | Luôn `true`; không tự bỏ qua offsets đã mất do retention |
| `stream.watermark` | `1 hour`, marker progress |
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
và phiên bản contract C4/C5. Binding không khớp bị từ chối. Checkpoint đã có
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
  `clickflow_stream_conflicting_event_ids_total`: số attempted rows bỏ qua
  và conflict IDs quan sát ở các batch xử lý thành công.
- Progress gauges: `input_rows_per_second`, `processed_rows_per_second`,
  `input_rows`, `batch_id`, `batch_duration_milliseconds`,
  `event_watermark_seconds`, cùng prefix `clickflow_stream_`; có thêm
  `batches_total`, `running`, `failures_total`.

Spark progress có thể đếm lại input khi `foreachBatch` thực hiện nhiều
action; không dùng nó để đối soát dòng đã commit. Counter trong memory reset
khi driver restart và không thay thế audit lâu dài trong Delta. Log
`stream_batch_committed` tóm tắt insert/ignored/conflict của mỗi batch;
`event_id_conflict` ghi cảnh báo kèm một số ID để điều tra.

### Integration Kafka: lỗi giữa commit và restart driver

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

Workflow `.github/workflows/ci.yml` đã chạy toàn bộ pytest và Compose
validation, vì vậy tự bao gồm test luật và counter mới. Hai test callback
listener sẽ skip khi môi trường CI không cài PySpark; test luật/counter vẫn
chạy đầy đủ.

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
