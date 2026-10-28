# ClickFlow

Đồ án Big Data – E-Commerce Behavior, phạm vi **Sources → Ingestion → Storage**.

Dữ liệu hành vi sàn TMĐT (Kaggle [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store))
được simulator phát lại như luồng sự kiện thời gian thực → Kafka 3 broker → Spark Structured Streaming → Delta Lake trên MinIO.
Toàn bộ chạy bằng Docker Compose trên một máy.

Phạm vi dữ liệu hiện tại: **`2019-Nov.csv`** (khai báo ở `configs/pipeline.yaml → simulator.files`). Ngày mặc định cho dev/demo là `2019-11-01`.

```
CSV ──► Simulator ──┬──► Kafka (ecom.events.raw, 6 partition, RF 3) ──┬──► Spark Streaming ──► lake/bronze/events (Delta)
                    │                                                  │                    └─► lake/quarantine/events (Delta)
                    └──► Collector API (HTTP) ──►┘                     └──► Kafka Connect S3 Sink ──► lake/raw/ (JSON gzip)
Prometheus + Grafana thu metric từ mọi thành phần.
```

## Luồng chạy

Một sự kiện đi qua pipeline theo thứ tự sau. Cột "Mã" chỉ chỗ xử lý trong `src/`.

| # | Chặng | Việc xảy ra | Giao thức / định dạng | Mã |
|---|---|---|---|---|
| 1 | CSV → Simulator | Đọc `2019-Nov.csv` theo chunk, lọc theo ngày, giữ thứ tự `event_time`, phát với hệ số tăng tốc `SPEED` (×100 demo, ×1000 benchmark). Gắn `event_id` (hash dòng gốc), `source_file`, `produced_at`. Tuỳ chọn chèn bản ghi hỏng/trùng để kiểm chứng. | pyarrow, chunk 100k dòng | `sources/simulator.py`, `sources/fault_injector.py` |
| 2a | Simulator → Kafka | Đường nạp khối lớn: producer gửi thẳng vào topic. `enable.idempotence=true`, `acks=all`, nén lz4, key = `user_id` để mọi sự kiện của một người dùng vào cùng partition. | Kafka protocol, JSON UTF-8 | `ingestion/producer.py` |
| 2b | Simulator → Collector API → Kafka | Đường nạp mô phỏng web/app thật: `POST /events` lô ≤ 500 sự kiện, Pydantic kiểm tra schema C3, 202 nếu hợp lệ / 400 kèm lỗi, rồi đẩy vào cùng topic. | HTTP/JSON | `ingestion/collector_api/` |
| 3 | Kafka `ecom.events.raw` | Bộ đệm bền: 6 partition, replication factor 3, `min.insync.replicas=2`, retention theo dung lượng. Tắt 1 broker vẫn ghi được. | — | `ingestion/create_topics.py`, `configs/kafka_topics.yaml` |
| 4a | Kafka → Spark → bronze / quarantine | Structured Streaming trigger 10 s: parse JSON theo C3, ép kiểu, chạy luật 1–4 (sai → `quarantine/` kèm `error_reason`), luật 5 khử trùng `event_id` trong watermark 1 giờ, ghi Delta partition `event_date`. Checkpoint trên MinIO để chạy lại không mất/trùng. | Delta trên S3A | `storage/stream_to_bronze.py`, `storage/quality_rules.py` |
| 4b | Kafka → Kafka Connect → raw | S3 Sink ghi bản gốc theo giờ nạp, không qua code, để nạp lại khi job Spark lỗi. | JSON gzip, S3 API | `configs/connect/s3-sink.json` |
| 5 | Bảo trì bronze | `OPTIMIZE ... ZORDER BY (user_id, product_id)` hằng ngày cho partition đã đóng, rồi `VACUUM`. | Delta | `storage/maintenance.py` |
| 6 | Kiểm chứng | `make verify` đối soát số dòng nguồn vs bronze vs quarantine theo ngày; `latency.py` tính p50/p95 từ `produced_at` đến `ingested_at`. | Spark SQL | `monitoring/reconcile.py`, `queries/verify.sql` |
| 7 | Giám sát | Mọi thành phần mở `/metrics`; Prometheus scrape 5 s; Grafana "Ingestion health": sự kiện/s vào Kafka và bronze, consumer lag theo partition, độ trễ p50/p95, quarantine theo luật, trạng thái 3 broker. | HTTP scrape | `common/metrics.py`, `monitoring/grafana/` |

Luật kiểm tra khi ghi bronze (vi phạm → `quarantine/`, mỗi luật có bộ đếm):

1. JSON không parse được hoặc sai schema C3.
2. `event_time` null hoặc ngoài khoảng thời gian của file nguồn.
3. `event_type` ngoài `{view, cart, purchase}`.
4. `product_id` hoặc `user_id` null; `price` âm.
5. Trùng `event_id` trong watermark 1 giờ → bỏ, chỉ đếm (`dropDuplicatesWithinWatermark`).

`category_code`, `brand`, `user_session` null là hợp lệ và vẫn vào bronze.

## Thành phần

| Service (compose) | Vai trò | Cấu hình chính | Cổng host |
|---|---|---|---|
| `kafka-1..3` | Cụm Kafka KRaft 3 broker, broker kiêm controller | heap 512 MB/broker, RF mặc định 3, `auto.create.topics=false` | 29092–29094 |
| `kafka-ui` | Xem topic, message, consumer lag, connector | — | 8080 |
| `kafka-connect` | Worker Connect + plugin S3 Sink, ghi `raw/` | `configs/connect/s3-sink.json`, `tasks.max=2` | 8083 |
| `collector-api` | FastAPI + aiokafka, đường nạp HTTP | `docker/collector/Dockerfile` | 8000 |
| `minio` | Object storage S3 API, bucket `lake` | Chainguard image ghim digest; `minio-init` tạo bucket | 9002 (S3), 9003 (console) |
| `spark-master`, `spark-worker-1..2` | Spark 3.5.3 Standalone; mỗi worker 2 core, 2 GB | `docker/spark/Dockerfile` (Delta 3.2.1, hadoop-aws, spark-sql-kafka), `configs/spark-defaults.conf` | 8090 (UI), 7077, 4040 |
| `prometheus` | Thu metric từ kafka-exporter, collector-api, simulator (host:8001), Spark driver (8002), MinIO | `configs/prometheus.yml`, profile `monitoring` | 9090 |
| `kafka-exporter` | Xuất consumer lag, offset theo partition cho Prometheus | profile `monitoring` | — |
| `grafana` | Dashboard "Ingestion health" tự provision | `configs/grafana/provisioning/`, profile `monitoring` | 3000 |
| Simulator (host) | Chạy bằng `make replay` ngoài Docker, nhiều tiến trình theo ngày | `configs/pipeline.yaml → simulator` | 8001 (metrics) |

Vùng lưu trữ trong bucket `lake`:

| Vùng | Đường dẫn | Nội dung | Partition | Ghi bởi |
|---|---|---|---|---|
| raw | `s3a://lake/raw/ecom.events.raw/` | JSON gzip nguyên bản từ Kafka | `dt`, `hour` (giờ nạp) | Kafka Connect |
| bronze | `s3a://lake/bronze/events/` | Delta, 14 cột đúng kiểu, không trùng `event_id` | `event_date` | Spark Streaming |
| quarantine | `s3a://lake/quarantine/events/` | Delta, payload gốc + `error_reason` | `ingest_date` | Spark Streaming |
| checkpoints | `s3a://lake/_checkpoints/` | Offset và trạng thái stream | — | Spark Streaming |

## Yêu cầu máy

- RAM ≥ 16 GB, đĩa trống ≥ 30 GB (CSV gốc ~9 GB, Kafka log, raw và bronze)
- Docker + Docker Compose v2, GNU Make, Python 3.11
- Tài khoản Kaggle + API token (`~/.kaggle/kaggle.json` hoặc `KAGGLE_USERNAME`/`KAGGLE_KEY` trong `.env`)

## Chạy nhanh

```bash
cp .env.example .env          # sửa mật khẩu
pip install -r requirements.txt
make up                       # kafka x3, kafka-ui, connect, minio, spark x3, collector, prometheus, grafana
make topics                   # tạo topic theo configs/kafka_topics.yaml
make download && make sample  # tải 2019-Nov.csv, cắt mẫu 100k dòng vào data/sample/
make stream                   # Spark Streaming Kafka → bronze/quarantine
make replay DAY=2019-11-01 SPEED=100
make sink                     # đăng ký Kafka Connect S3 Sink → raw/
make verify                   # đối soát số dòng theo ngày
make down
```

`make up-core` bỏ qua Prometheus/Grafana cho máy yếu. `make test` chạy pytest.

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
docker-compose.yml       kafka x3 (KRaft), kafka-ui, kafka-connect, minio, spark-master, spark-worker x2,
                         collector-api, prometheus, grafana (profile monitoring)
Makefile                 up | up-core | down | topics | download | sample | replay | stream | sink | verify | bench | test
docker/                  Dockerfile cho spark (Delta + S3A + Kafka), connect (S3 Sink), collector (FastAPI)
configs/                 pipeline.yaml, kafka_topics.yaml, connect/s3-sink.json, spark-defaults.conf, prometheus.yml, grafana/
contracts/               data contract C1–C5: source_csv.yaml, collector_openapi.yaml, event.schema.json,
                         bronze_events.yaml, quarantine_events.yaml
src/common/              config.py, schemas.py, spark_session.py, metrics.py
src/sources/             download.py, make_sample.py, simulator.py, fault_injector.py
src/ingestion/           create_topics.py, producer.py, collector_api/
src/storage/             init_lake.py, stream_to_bronze.py, quality_rules.py, maintenance.py
src/monitoring/          reconcile.py, latency.py, grafana/ingestion_health.json
queries/                 verify.sql, sample_queries.sql
benchmarks/              e1_producer.sh … e5_storage.sh, results/
tests/                   test_schemas.py, test_quality_rules.py, test_collector_api.py
data/raw/ (gitignore)    2019-Nov.csv      data/sample/   events_sample_100k.csv (commit kèm)
notebooks/               01_eda.ipynb      docs/          runbook.md, contribution.md, ai_usage.md
```

## Data contract

Năm hợp đồng trong `contracts/` là giao diện giữa các tầng; đổi phải báo cả nhóm. `tests/test_schemas.py` giữ `src/common/schemas.py` khớp với contract.

| # | Giao diện | Bên ghi → bên đọc | Kiểm tra |
|---|---|---|---|
| C1 | File CSV nguồn | Kaggle → Simulator | Test schema trên mẫu 100k dòng |
| C2 | Collector API `POST /events` | Simulator → API | pytest + httpx |
| C3 | Topic `ecom.events.raw` (12 trường, key = `user_id`) | Simulator, API → Spark, Connect | JSON Schema |
| C4 | Bảng `bronze/events` | Spark → nhóm Processing | Đối soát số dòng theo ngày |
| C5 | Bảng `quarantine/events` | Spark → nhóm dữ liệu | Mỗi bản ghi hỏng cố ý đều xuất hiện |

## Quản lý công việc

- Backlog và sprint: [Jira BIG](https://nguyenquoccuong07122004.atlassian.net/jira/software/projects/BIG/boards/3/backlog), 6 sprint theo tuần từ 29/10 đến 09/12/2026.
- Epic theo tầng: Sources, Ingestion, Storage, Infra, Report. Label `week-N` là tuần, `tvN` là người phụ trách.
- Tên nhánh và commit mang key issue để Jira liên kết, ví dụ `feat/sources-simulator-BIG-18`.

## Quy ước

- `main` luôn chạy được `make up && make replay DAY=2019-11-01 && make verify`.
- Làm trên nhánh `feat/<tầng>-<việc>-<BIG-key>`, merge qua Pull Request có 1 người review, CI xanh.
- Không commit dữ liệu lớn, mật khẩu hay file `.env`.
- Mọi thành phần in log có cấu trúc và xuất metric cho Prometheus.
- Ghim phiên bản trong `docker/`: Spark 3.5.3, Delta 3.2.1, Kafka 3.8.1, Connect 7.7.1, MinIO (Chainguard, ghim digest), Python 3.11. Không đổi giữa chừng.
