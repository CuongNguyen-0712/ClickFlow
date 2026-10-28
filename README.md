# ClickFlow

Đồ án Big Data – E-Commerce Behavior, phạm vi **Sources → Ingestion → Storage**.

Dữ liệu hành vi sàn TMĐT (Kaggle *eCommerce behavior data from multi category store*, ~110 triệu sự kiện 10–11/2019)
được simulator phát lại như luồng sự kiện thời gian thực → Kafka 3 broker → Spark Structured Streaming → Delta Lake trên MinIO.

```
CSV ──► Simulator ──┬──► Kafka (ecom.events.raw, 6 partition, RF 3) ──┬──► Spark Streaming ──► lake/bronze/events (Delta)
                    │                                                  │                    └─► lake/quarantine/events (Delta)
                    └──► Collector API (HTTP) ──►┘                     └──► Kafka Connect S3 Sink ──► lake/raw/ (JSON gzip)
Prometheus + Grafana thu metric từ mọi thành phần.
```

## Yêu cầu máy

- RAM ≥ 16 GB, đĩa trống ≥ 60 GB (mức Full cần ~50 GB)
- Docker + Docker Compose v2, GNU Make, Python 3.11
- Tài khoản Kaggle + API token (`~/.kaggle/kaggle.json` hoặc `KAGGLE_USERNAME`/`KAGGLE_KEY` trong `.env`)

| Thành viên | RAM | Đĩa trống | Chạy mức Full? |
|---|---|---|---|
| TV1 | | | |
| TV2 | | | |
| TV3 | | | |
| TV4 | | | |

## Chạy nhanh

```bash
cp .env.example .env          # sửa mật khẩu
pip install -r requirements.txt
make up                       # kafka x3, kafka-ui, connect, minio, spark x3, collector, prometheus, grafana
make topics                   # tạo topic theo configs/kafka_topics.yaml
make download && make sample  # tải CSV gốc, cắt mẫu 100k dòng
make stream                   # Spark Streaming Kafka → bronze/quarantine
make replay DAY=2019-10-01 SPEED=100
make sink                     # đăng ký Kafka Connect S3 Sink → raw/
make verify                   # đối soát số dòng theo ngày
make down
```

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8090 |
| MinIO Console | http://localhost:9001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Collector API | http://localhost:8000/docs |

## Cấu trúc

| Thư mục | Nội dung | Chủ sở hữu |
|---|---|---|
| `src/sources/`, `data/`, `notebooks/` | tải dữ liệu, EDA, mẫu 100k, simulator, fault injector | TV1 |
| `src/ingestion/`, `docker/connect/`, `configs/kafka_topics.yaml` | topic, producer, Collector API, Kafka Connect | TV2 |
| `src/storage/`, `contracts/bronze_*.yaml` | Spark Streaming → Delta, quality rules, OPTIMIZE/VACUUM | TV3 |
| `docker-compose.yml`, `src/monitoring/`, `benchmarks/`, `queries/` | hạ tầng, giám sát, đối soát, benchmark | TV4 |
| `contracts/` | data contract C1–C5 (đổi phải báo cả nhóm) | cả nhóm |

## Quy ước

- `main` luôn chạy được `make up && make replay DAY=2019-10-01 && make verify`.
- Nhánh `feat/<tầng>-<việc>`, merge qua Pull Request có 1 người review.
- Không commit dữ liệu lớn, mật khẩu hay file `.env`.
- Mọi thành phần in log có cấu trúc và xuất metric cho Prometheus.
