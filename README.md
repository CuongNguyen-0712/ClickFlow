# ClickFlow

Đồ án Big Data – E-Commerce Behavior, phạm vi **Sources → Ingestion → Storage**.

Dữ liệu hành vi sàn TMĐT (Kaggle [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store))
được simulator phát lại như luồng sự kiện thời gian thực → Kafka 3 broker → Spark Structured Streaming → Delta Lake trên MinIO.
Toàn bộ chạy bằng Docker Compose trên một máy.

Dữ liệu nguồn tải tay từ Kaggle rồi đẩy lên `lake/raw/csv/` trên MinIO; mọi thành phần đọc từ đó, không đọc file local. Chi tiết ở mục [Dữ liệu nguồn](#dữ-liệu-nguồn).

```
Kaggle ──(tải tay, make upload)──► lake/raw/csv/2019-Nov.csv
                                          │
                                          ▼
                                     Simulator ──┬──────────────────────────► Kafka ecom.events.raw ──┬──► Spark Streaming ──► lake/bronze/events (Delta)
                                                 │                            (6 partition, RF 3)     │                     └─► lake/quarantine/events (Delta)
                                                 └──► Collector API (HTTP) ──►┘                       └──► Kafka Connect S3 Sink ──► lake/raw/ecom.events.raw/ (JSON gzip)

Prometheus + Grafana thu metric từ mọi thành phần.
```

## Dữ liệu nguồn

Dataset Kaggle [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
(REES46 / Open CDP), mỗi dòng là một sự kiện `view` / `cart` / `purchase` của một người dùng trên một sản phẩm, 9 cột theo contract C1.

| Ưu tiên | File | Kích thước | Số dòng (ước tính) | Dùng cho |
|---|---|---|---|---|
| **1 — làm trước** | `2019-Nov.csv` | ~9 GB | ~67 triệu | Dev (1 ngày `2019-11-01`), Demo (01–07/11), Full (cả tháng) |
| 2 — nếu còn thời gian | `2019-Oct.csv` | ~5,7 GB | ~42 triệu | Mở rộng Full lên 2 tháng |
| 3 — tuỳ chọn | 12/2019–4/2020 (REES46) | lớn | ~175 triệu | Chỉ khi còn đĩa và thời gian |

Số dòng chính xác lấy từ notebook EDA (BIG-15).

**Cách lấy dữ liệu** (làm một lần, mỗi máy cần dữ liệu):

1. Đăng nhập Kaggle, tải `2019-Nov.csv` từ trang dataset bằng trình duyệt (file zip ~2 GB, giải nén ra ~9 GB).
2. Đặt file vào `data/raw/` (thư mục này gitignore, không commit).
3. `make up` để MinIO chạy, rồi `make upload` để đẩy vào `s3a://lake/raw/csv/2019-Nov.csv`. Thay thế: kéo thả file trên MinIO Console `localhost:9003` vào `lake/raw/csv/`.
4. Kiểm tra: `docker compose exec minio mc ls local/lake/raw/csv/` thấy file đúng kích thước (alias `local` do `make upload` tạo; nếu đã recreate container thì chạy lại `make upload`).

Khi thêm file sau này chỉ cần lặp lại bước 1–3 và thêm tên file vào `configs/pipeline.yaml → simulator.files`; không sửa mã.

**Vì sao không dùng Kaggle CLI:** mỗi máy chỉ tải 1–2 lần, tải tay bằng trình duyệt nhanh hơn và không cần lưu API token trong `.env`.

## Luồng chạy

Một sự kiện đi qua pipeline theo thứ tự sau. Cột "Mã" chỉ chỗ xử lý trong `src/`.

| # | Chặng | Việc xảy ra | Giao thức / định dạng | Mã |
|---|---|---|---|---|
| 0 | Kaggle → raw/csv | Tải tay `2019-Nov.csv` (ưu tiên 1), đẩy lên `lake/raw/csv/` bằng `make upload`. Làm một lần, xem mục Dữ liệu nguồn. | S3 API | `Makefile → upload` |
| 1 | raw/csv → Simulator | Đọc `2019-Nov.csv` từ MinIO qua s3fs theo chunk, lọc theo ngày, giữ thứ tự `event_time`, phát với hệ số tăng tốc `SPEED` (×100 demo, ×1000 benchmark). Gắn `event_id` (hash dòng gốc), `source_file`, `produced_at`. Tuỳ chọn chèn bản ghi hỏng/trùng để kiểm chứng. | S3 API, pyarrow, chunk 100k dòng | `sources/read_csv.py`, `sources/simulator.py`, `sources/fault_injector.py` |
| 2a | Simulator → Kafka | Đường nạp khối lớn: producer gửi thẳng vào topic. `enable.idempotence=true`, `acks=all`, nén lz4, key = `user_id` để mọi sự kiện của một người dùng vào cùng partition. | Kafka protocol, JSON UTF-8 | `ingestion/producer.py` |
| 2b | Simulator → Collector API → Kafka | Đường nạp mô phỏng web/app thật: `POST /events` lô ≤ 500 sự kiện, Pydantic kiểm tra schema C3, 202 nếu hợp lệ / 400 kèm lỗi, rồi đẩy vào cùng topic. | HTTP/JSON | `ingestion/collector_api/` |
| 3 | Kafka `ecom.events.raw` | Bộ đệm bền: 6 partition, replication factor 3, `min.insync.replicas=2`, retention theo dung lượng. Tắt 1 broker vẫn ghi được. | — | `ingestion/create_topics.py`, `configs/kafka_topics.yaml` |
| 4a | Kafka → Spark → bronze / quarantine | Một Structured Streaming query, trigger 10 s, kiểm tra C3 nghiêm ngặt trước khi Spark cast. Một `foreachBatch` phân tuyến luật 1–4 rồi insert-only MERGE: C4 theo `event_id` toàn bảng, C5 theo partition/offset của topic cố định. Checkpoint trên MinIO để resume. | Delta trên S3A | `storage/stream_to_bronze.py`, `storage/quality_rules.py`, `storage/stream_sink.py` |
| 4b | Kafka → Kafka Connect → raw/ecom.events.raw | S3 Sink ghi message Kafka theo giờ nạp, không qua code, để nạp lại khi job Spark lỗi. Nằm cạnh `raw/csv/` nhưng là prefix riêng. | JSON gzip, S3 API | `configs/connect/s3-sink.json` |
| 5 | Bảo trì bronze | `OPTIMIZE ... ZORDER BY (user_id, product_id)` hằng ngày cho partition đã đóng, rồi `VACUUM`. | Delta | `storage/maintenance.py` |
| 6 | Kiểm chứng | `make verify` đối soát số dòng nguồn vs bronze vs quarantine theo ngày; `latency.py` tính p50/p95 từ `produced_at` đến `ingested_at`. | Spark SQL | `monitoring/reconcile.py`, `queries/verify.sql` |
| 7 | Giám sát | Mọi thành phần mở `/metrics`; Prometheus scrape 5 s; Grafana "Ingestion health": sự kiện/s vào Kafka và bronze, consumer lag theo partition, độ trễ p50/p95, quarantine theo luật, trạng thái 3 broker. | HTTP scrape | `common/metrics.py`, `monitoring/grafana/` |

Luật kiểm tra khi ghi bronze (vi phạm → `quarantine/`, mỗi luật có bộ đếm):

1. JSON không parse được hoặc sai schema C3.
2. `event_time` null hoặc ngoài khoảng thời gian của file nguồn.
3. `event_type` ngoài `{view, cart, purchase}`.
4. `product_id` hoặc `user_id` null; `price` âm.
5. Trùng `event_id` trên toàn bảng C4 → giữ dòng đầu, đếm số bỏ qua; cùng ID khác một trong 9 giá trị C1 → giữ dòng đầu và cảnh báo xung đột.

`category_code`, `brand`, `user_session` null là hợp lệ và vẫn vào bronze.
Watermark 1 giờ là marker trong progress, không có stateful drop; sự kiện
hợp lệ, ID mới vẫn được giữ dù đến muộn. Hai MERGE là hai commit Delta riêng:
reader có thể thấy bronze trước quarantine; retry cùng checkpoint hội tụ
về kết quả đầy đủ, không tạo thêm dòng đã ghi.

## Thành phần

| Service (compose) | Vai trò | Cấu hình chính | Cổng host |
|---|---|---|---|
| `kafka-1..3` | Cụm Kafka KRaft 3 broker, broker kiêm controller | heap 512 MB/broker, RF mặc định 3, `auto.create.topics=false` | 29092–29094 |
| `kafka-ui` | Xem topic, message, consumer lag, connector | — | 8080 |
| `kafka-connect` | Worker Connect + plugin S3 Sink, ghi `raw/` | `configs/connect/s3-sink.json`, `tasks.max=2` | 8083 |
| `collector-api` | FastAPI + aiokafka, đường nạp HTTP | `docker/collector/Dockerfile` | 8000 |
| `minio` | Object storage S3 API, bucket `lake`; nhận CSV tải tay vào `raw/csv/` | Chainguard image ghim digest; `minio-init` tạo bucket; mount `./data/raw` → `/upload` cho `make upload` | 9002 (S3), 9003 (console) |
| `spark-master`, `spark-worker-1..2` | Spark 3.5.3 Standalone; mỗi worker 2 core, 2 GB | `docker/spark/Dockerfile` (Delta 3.2.1, hadoop-aws, spark-sql-kafka), `configs/spark-defaults.conf` | 8090 (UI), 7077, 4040 |
| `prometheus` | Thu metric từ kafka-exporter, collector-api, simulator (host:8001), Spark driver (8002), MinIO | `configs/prometheus.yml`, profile `monitoring` | 9090 |
| `kafka-exporter` | Xuất consumer lag, offset theo partition cho Prometheus | profile `monitoring` | — |
| `grafana` | Dashboard "Ingestion health" tự provision | `configs/grafana/provisioning/`, profile `monitoring` | 3000 |
| Simulator (host) | Chạy bằng `make replay` ngoài Docker, đọc CSV từ MinIO qua `localhost:9002`, nhiều tiến trình theo ngày | `configs/pipeline.yaml → simulator`, `lake.endpoint_host` | 8001 (metrics) |

Vùng lưu trữ trong bucket `lake`:

| Vùng | Đường dẫn | Nội dung | Partition | Ghi bởi |
|---|---|---|---|---|
| raw/csv | `s3a://lake/raw/csv/` | CSV gốc Kaggle, tải tay | — (1 file/tháng) | Người dùng (`make upload`) |
| raw/ecom.events.raw | `s3a://lake/raw/ecom.events.raw/` | JSON gzip nguyên bản từ Kafka | `dt`, `hour` (giờ nạp) | Kafka Connect |
| bronze | `s3a://lake/bronze/events/` | Delta, 14 cột dữ liệu + 1 partition = 15; khóa `event_id` | `event_date` | Spark Streaming |
| quarantine | `s3a://lake/quarantine/events/` | Delta, 5 cột dữ liệu + 1 partition = 6; payload gốc + `error_reason` | `ingest_date` | Spark Streaming |
| checkpoints | `s3a://lake/_checkpoints/` | Offset và trạng thái stream | — | Spark Streaming |

## Yêu cầu máy

- RAM ≥ 16 GB, đĩa trống ≥ 40 GB (CSV gốc ~9 GB trên host + bản sao ~9 GB trong MinIO, Kafka log tối đa ~18 GB, bronze)
- Docker + Docker Compose v2, GNU Make, Python 3.11
- Windows: dùng Git Bash hoặc WSL; cài `make` qua `winget install GnuWin32.Make` hoặc `scoop install make`. Không có `make` thì chạy thẳng lệnh trong `Makefile`.
- File `2019-Nov.csv` tải tay từ Kaggle, đặt vào `data/raw/` (gitignore)

## Chạy nhanh

**Chưa tải dataset:** vẫn khởi tạo và kiểm chứng được MinIO + Spark bằng
fixture tổng hợp 3 dòng, xem [runbook storage](docs/runbook.md). Không cần
Kafka hay Python trên host cho luồng này:

```bash
cp .env.example .env          # chỉ khi chưa có .env
make up-storage
make init-lake
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/app/src/storage/smoke_lake.py --sample --keep-artifacts
```

`smoke-storage` đọc CSV theo C1 từ MinIO rồi ghi/đọc Delta qua hai worker.
Fixture có tên riêng, không tạo file giả `2019-Nov.csv`; bảng C4/C5 vẫn rỗng.
Khi tải và upload dataset thật, chạy cùng script với `--keep-artifacts`
và bỏ `--sample` để kiểm tra tối đa 100 dòng từ `raw/csv/2019-Nov.csv`.

**Mẫu 100k → bronze thử:** sau khi upload dataset thật, chạy `make sample`
rồi `make stream-sample`. Spark tạo
`data/sample/events_sample_100k.csv` với 100.000 sự kiện khác nhau trong ngày
2019-11-01, rồi ghi C4 Delta tại `lake/bronze/sample_events/`, partition
`event_date`. Chạy lại cùng mẫu giữ nguyên số dòng; dữ liệu khác ở cùng đích
sẽ bị từ chối, không ghi đè. Xem [runbook](docs/runbook.md) để chạy PowerShell.
Đây là batch mẫu hữu hạn, dùng `--source sample` để tách khỏi nguồn Kafka
mặc định và không ghi vào bảng bronze chính.

```bash
make up-stream                # Kafka x3 + MinIO + Spark, không cần dataset
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic ecom.events.raw --partitions 6 --replication-factor 3 --config min.insync.replicas=2
make stream                   # mặc định source=kafka, chạy liên tục
# Hoặc khi stream đã dừng: xử lý offsets hiện có rồi thoát
make stream-drain
```

Tạo `.env` từ `.env.example` nếu chưa có, rồi sửa mật khẩu trước khi khởi động.
`make topics` vẫn là khung TV2; dùng Kafka CLI ở trên để tạo topic. Chỉ chạy
một driver ghi cùng cặp bảng. Cấu hình mặc định: trigger 10 giây,
`maxOffsetsPerTrigger=500000`, `startingOffsets=earliest` chỉ ở lần đầu,
`failOnDataLoss=true`, checkpoint `s3a://lake/_checkpoints/kafka_to_delta/`.
Các lần sau resume từ checkpoint. Xem [runbook](docs/runbook.md#kafka--c4c5-streaming)
cho PowerShell, dừng/resume và kiểm thử từng driver.

Kafka → C4/C5, quality routing, insert-only MERGE, checkpoint binding và
Prometheus driver đã được triển khai. Endpoint `0.0.0.0:8002` bên trong
`spark-master` được scrape bởi target có sẵn `spark-master:8002`; không cần
publish cổng host. Counter sink đếm dòng thực chèn sau commit; progress Spark
có thể đếm lại source do nhiều action trong `foreachBatch`. Counter reset khi
driver khởi động lại. Simulator/producer/Collector, `make topics` và một số
job đối soát/benchmark vẫn là khung cho các task liên quan.

Kiểm chứng ngày 01/11/2026: **187 unit test đạt**; integration giữ lại case
`data/raw/kafka-stream-test-767d9cd4c267`, xác nhận lỗi giữa hai commit,
resume, replay/conflict, đủ 5 lý do lỗi và restart không tăng Delta version.
Đây là kiểm thử chức năng có fixture, chưa phải benchmark toàn bộ dataset.

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8090 |
| MinIO Console | http://localhost:9003 (S3 API: localhost:9002) |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Collector API | http://localhost:8000/docs |

## Cấu trúc repo

```
docker-compose.yml       kafka x3 (KRaft), kafka-ui, kafka-connect, minio + minio-init, spark-master, spark-worker x2,
                         collector-api; profile monitoring: prometheus, kafka-exporter, grafana
Makefile                 up | up-stream | up-core | down | topics | upload | sample | stream | stream-sample | stream-drain | test
.github/workflows/ci.yml pytest + docker compose config trên mỗi PR
.env.example             KAFKA_CLUSTER_ID, MINIO_ROOT_*, LAKE_BUCKET, GRAFANA_ADMIN_PASSWORD — copy thành .env
.gitattributes           ép LF cho script mount vào container Linux
docker/                  Dockerfile cho spark (Delta + S3A + Kafka), connect (S3 Sink), collector (FastAPI)
configs/                 pipeline.yaml, kafka_topics.yaml, connect/s3-sink.json, spark-defaults.conf, prometheus.yml, grafana/
contracts/               data contract C1–C5: source_csv.yaml, collector_openapi.yaml, event.schema.json,
                         bronze_events.yaml, quarantine_events.yaml
src/common/              config.py, schemas.py, spark_session.py, metrics.py
src/sources/             read_csv.py, make_sample.py, simulator.py, fault_injector.py
src/ingestion/           create_topics.py, producer.py, collector_api/
src/storage/             init_lake.py, stream_to_bronze.py, stream_config.py, stream_sink.py, quality_rules.py, maintenance.py
src/monitoring/          stream_listener.py, reconcile.py, latency.py, grafana/ingestion_health.json
queries/                 verify.sql, sample_queries.sql
benchmarks/              e1_producer.sh … e5_storage.sh, results/
tests/                   test_schemas.py, test_quality_rules.py, test_collector_api.py
data/raw/ (gitignore)    2019-Nov.csv tải tay, nguồn cho make upload      data/sample/   events_sample_100k.csv (commit kèm)
notebooks/               01_eda.ipynb      docs/          runbook.md, contribution.md, ai_usage.md
```

## Data contract

Năm hợp đồng trong `contracts/` là giao diện giữa các tầng; đổi phải báo cả nhóm. `tests/test_schemas.py` giữ `src/common/schemas.py` khớp với contract.

| # | Giao diện | Bên ghi → bên đọc | Kiểm tra |
|---|---|---|---|
| C1 | File CSV nguồn trên `lake/raw/csv/` | Kaggle (tải tay) → Simulator | Test schema trên mẫu 100k dòng |
| C2 | Collector API `POST /events` | Simulator → API | pytest + httpx |
| C3 | Topic `ecom.events.raw` (12 trường, key = `user_id`) | Simulator, API → Spark, Connect | JSON Schema |
| C4 | Bảng `bronze/events` | Spark → nhóm Processing | Đối soát số dòng theo ngày |
| C5 | Bảng `quarantine/events` | Spark → nhóm dữ liệu | Mỗi bản ghi hỏng cố ý đều xuất hiện |

C4/C5 bản **1.0.0 sẵn sàng review, đang chờ nhóm duyệt**. C4 có 14 cột dữ
liệu (9 C1 + 5 metadata) và `event_date` là cột thứ 15; C5 có 5 cột dữ liệu
và `ingest_date` là cột thứ 6. Xem [bản bàn giao và phiếu duyệt nhóm](docs/contracts-c4-c5-handoff.md)
để xác nhận nullability, UTC, dedup toàn bảng, payload gốc và thứ tự lỗi.
Sink Kafka dùng MERGE bền theo khóa C4/C5, không dựa vào watermark để khử
trùng. Metadata ràng buộc checkpoint với topic/broker, hai đích, fingerprint
C3 và phiên bản C4/C5. Checkpoint không rỗng nhưng chưa có binding bị từ chối
trước khi init; đổi nguồn/đích phải chọn checkpoint và đích phù hợp hoặc lập
kế hoạch migration, không xóa checkpoint để ép chạy lại.

## Quản lý công việc

- Backlog và sprint: [Jira BIG](https://nguyenquoccuong07122004.atlassian.net/jira/software/projects/BIG/boards/3/backlog), 6 sprint theo tuần từ 29/10 đến 09/12/2026.
- Epic theo tầng: Sources, Ingestion, Storage, Infra, Report. Label `week-N` là tuần, `tvN` là người phụ trách:

  | Label | Thành viên | Phụ trách |
  |---|---|---|
  | `tv1` | Thơ Trần | Sources: dữ liệu, EDA, simulator, fault injector, chương 3, video |
  | `tv2` | Tiến An | Ingestion: Kafka, producer, Collector API, Kafka Connect, chương 2/4 (Kafka) |
  | `tv3` | Thanh Trúc | Storage: Spark Streaming, Delta, quality rules, chương 2/4 (Data Lake) |
  | `tv4` | Nguyễn Quốc Cường (nhóm trưởng) | Hạ tầng, giám sát, benchmark, nạp Full, chương 1/5/6/7, ghép báo cáo |

- Dàn ý báo cáo và người viết từng chương: `report/outline.md`.
- Tên nhánh và commit mang key issue để Jira liên kết, ví dụ `feat/sources-simulator-BIG-18`.

## Quy ước

- Luồng storage hiện tại: `make up-stream`, tạo topic bằng Kafka CLI rồi `make stream`; kiểm thử chức năng và các bước resume nằm trong runbook.
- Làm trên nhánh `feat/<tầng>-<việc>-<BIG-key>`, merge qua Pull Request có 1 người review, CI xanh.
- Không commit dữ liệu lớn, mật khẩu hay file `.env`.
- Mọi thành phần in log có cấu trúc và xuất metric cho Prometheus.
- Ghim phiên bản trong `docker/`: Spark 3.5.3, Delta 3.2.1, Kafka 3.8.1, Connect 7.7.1, MinIO (Chainguard, ghim digest), Python 3.11. Không đổi giữa chừng.
