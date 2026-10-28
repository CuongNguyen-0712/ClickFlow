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

| Vùng | Đường dẫn | Nội dung | Partition |
|---|---|---|---|
| raw | `s3a://lake/raw/ecom.events.raw/` | JSON gzip nguyên bản từ Kafka | `dt`, `hour` (giờ nạp) |
| bronze | `s3a://lake/bronze/events/` | Delta, 14 cột đúng kiểu, không trùng `event_id` | `event_date` |
| quarantine | `s3a://lake/quarantine/events/` | Delta, payload gốc + `error_reason` | `ingest_date` |
| checkpoints | `s3a://lake/_checkpoints/` | Offset và trạng thái stream | — |

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
| MinIO Console | http://localhost:9001 |
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
- Ghim phiên bản trong `docker/`: Spark 3.5.3, Delta 3.2.1, Kafka 3.8.1, Connect 7.7.1, Python 3.11. Không đổi giữa chừng.
