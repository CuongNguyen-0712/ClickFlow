# ClickFlow

Đồ án Big Data – E-Commerce Behavior, phạm vi **Sources → Ingestion → Storage**.

Dữ liệu hành vi sàn TMĐT (Kaggle [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store))
được simulator phát lại như luồng sự kiện thời gian thực → Kafka 3 broker → Spark Structured Streaming → Delta Lake trên MinIO.
Toàn bộ chạy bằng Docker Compose trên một máy.

Dữ liệu nguồn tải từ Kaggle bằng `make download` rồi `make upload` lên `lake/raw/csv/` trên MinIO; mọi thành phần đọc từ đó, không đọc file local. Chi tiết ở mục [Dữ liệu nguồn](#dữ-liệu-nguồn).

```
Kaggle ──(make download, make upload)──► lake/raw/csv/2019-Nov.csv
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
| **1 — làm trước** | `2019-Nov.csv` | 9,0 GB (zip 2,87 GB) | ~67 triệu | Dev (1 ngày `2019-11-01`), Demo (01–07/11), Full (cả tháng) |
| 2 — nếu còn thời gian | `2019-Oct.csv` | ~5,7 GB | ~42 triệu | Mở rộng Full lên 2 tháng |
| 3 — tuỳ chọn | 12/2019–4/2020 (REES46) | lớn | ~175 triệu | Chỉ khi còn đĩa và thời gian |

Số dòng chính xác lấy từ notebook EDA (`notebooks/01_eda.ipynb`).

**Cách lấy dữ liệu** (làm một lần, mỗi máy cần dữ liệu):

1. `pip install -r requirements.txt` (có Kaggle CLI 2.2.5; API Kaggle mới chỉ nhận CLI ≥ 1.8). Dataset public nên **không cần token**; chỉ khi Kaggle báo đòi đăng nhập mới tạo token (Settings → API → Create New Token) lưu `~/.kaggle/kaggle.json` hoặc điền `KAGGLE_USERNAME` / `KAGGLE_KEY` vào `.env`.
2. `make download`: tải các file trong `configs/pipeline.yaml → simulator.files` về `data/raw/` (gitignore), tự giải nén (zip 2,9 GB → ~9 GB). Chạy lại thì bỏ qua file đã có; nếu đã có sẵn `<file>.zip` (tải tay) thì chỉ giải nén, không tải lại.
3. `make up` để MinIO chạy, rồi `make upload` để đẩy vào `s3a://lake/raw/csv/2019-Nov.csv`. Thay thế: kéo thả file trên MinIO Console `localhost:9003` vào `lake/raw/csv/`.
4. Kiểm tra: `docker compose exec minio mc ls local/lake/raw/csv/` thấy file đúng kích thước (alias `local` do `make upload` tạo; nếu đã recreate container thì chạy lại `make upload`).

Khi thêm file sau này chỉ cần thêm tên vào `simulator.files` rồi chạy lại bước 2–3; không sửa mã. Dataset khai báo ở `simulator.kaggle_dataset`.

## Luồng chạy

Một sự kiện đi qua pipeline theo thứ tự sau. Cột "Mã" chỉ chỗ xử lý trong `src/`.

| # | Chặng | Việc xảy ra | Giao thức / định dạng | Mã |
|---|---|---|---|---|
| 0 | Kaggle → raw/csv | `make download` tải `2019-Nov.csv` (ưu tiên 1) về `data/raw/` qua Kaggle CLI, `make upload` đẩy lên `lake/raw/csv/`. Làm một lần, xem mục Dữ liệu nguồn. | Kaggle API, S3 API | `sources/download.py`, `Makefile → upload` |
| 1 | raw/csv → Simulator | Đọc `2019-Nov.csv` từ MinIO qua s3fs theo chunk, lọc theo ngày, giữ thứ tự `event_time`, phát với hệ số tăng tốc `SPEED` (×100 demo, ×1000 benchmark). Gắn `event_id` (hash dòng gốc), `source_file`, `produced_at`. Tuỳ chọn chèn bản ghi hỏng/trùng để kiểm chứng. | S3 API, pyarrow, chunk 100k dòng | `sources/read_csv.py`, `sources/simulator.py`, `sources/fault_injector.py` |
| 2a | Simulator → Kafka | Đường nạp khối lớn: producer gửi thẳng vào topic. `enable.idempotence=true`, `acks=all`, nén lz4, key = `user_id` để mọi sự kiện của một người dùng vào cùng partition. | Kafka protocol, JSON UTF-8 | `ingestion/producer.py` |
| 2b | Simulator → Collector API → Kafka | Đường nạp mô phỏng web/app thật: `POST /events` lô ≤ 500 sự kiện, Pydantic kiểm tra schema C3, 202 nếu hợp lệ / 400 kèm lỗi, rồi đẩy vào cùng topic. | HTTP/JSON | `ingestion/collector_api/` |
| 3 | Kafka `ecom.events.raw` | Bộ đệm bền: 6 partition, replication factor 3, `min.insync.replicas=2`, retention theo dung lượng. Tắt 1 broker vẫn ghi được. | — | `ingestion/create_topics.py`, `configs/kafka_topics.yaml` |
| 4a | Kafka → Spark → bronze / quarantine | Structured Streaming trigger 10 s: parse JSON theo C3, ép kiểu, chạy luật 1–4 (sai → `quarantine/` kèm `error_reason`), luật 5 khử trùng `event_id` trong watermark 1 giờ, ghi Delta partition `event_date`. Checkpoint trên MinIO để chạy lại không mất/trùng. | Delta trên S3A | `storage/stream_to_bronze.py`, `storage/quality_rules.py` |
| 4b | Kafka → Kafka Connect → raw/ecom.events.raw | S3 Sink ghi message Kafka theo giờ nạp, không qua code, để nạp lại khi job Spark lỗi. Nằm cạnh `raw/csv/` nhưng là prefix riêng. | JSON gzip, S3 API | `configs/connect/s3-sink.json` |
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

## Thí nghiệm

Sáu thí nghiệm tạo số liệu cho chương 6–7; script trong `benchmarks/`, mỗi cấu hình chạy ≥ 3 lần, kết quả ghi `benchmarks/results/`.

| # | Thí nghiệm | Biến thay đổi | Đo gì |
|---|---|---|---|
| E1 | Thông lượng producer | nén none/lz4/zstd; acks 1/all; `linger.ms`, `batch.size`; producer trực tiếp vs Collector API | sự kiện/s, MB/s, tỷ lệ nén |
| E2 | Mở rộng partition / worker | 1/3/6/12 partition; 1 vs 2 worker Spark | dòng/s ghi bronze, consumer lag |
| E3 | Độ trễ end-to-end | trigger 5/10/30 s; tốc độ ×100/×1000 | p50/p95 `produced_at`→`ingested_at`, số file |
| E4 | Chịu lỗi | (a) tắt 1 broker; (b) kill job Spark rồi chạy lại từ checkpoint | số dòng bronze vs đã gửi: không mất, không trùng |
| E5 | Định dạng và phân vùng | CSV vs JSON gzip vs Delta; có/không partition; trước/sau OPTIMIZE+ZORDER | dung lượng, số file, thời gian 3 truy vấn, partition pruning |
| E6 | Chất lượng + nạp Full | chèn 0,1% hỏng + 0,1% trùng; nạp cả tháng 11/2019 | quarantine theo luật, trùng bị loại, tổng thời gian nạp |

## Thành phần

| Service (compose) | Vai trò | Cấu hình chính | Cổng host |
|---|---|---|---|
| `kafka-1..3` | Cụm Kafka KRaft 3 broker, broker kiêm controller | heap 512 MB/broker, RF mặc định 3, `auto.create.topics=false` | 29092–29094 |
| `kafka-ui` | Xem topic, message, consumer lag, connector | — | 8080 |
| `kafka-connect` | Worker Connect + plugin S3 Sink, ghi `raw/` | `configs/connect/s3-sink.json`, `tasks.max=2` | 8083 |
| `collector-api` | FastAPI + aiokafka, đường nạp HTTP | `docker/collector/Dockerfile` | 8000 |
| `minio` | Object storage S3 API, bucket `lake`; nhận CSV từ `make upload` vào `raw/csv/` | Chainguard image ghim digest; `minio-init` tạo bucket; mount `./data/raw` → `/upload` cho `make upload` | 9002 (S3), 9003 (console) |
| `spark-master`, `spark-worker-1..2` | Spark 3.5.3 Standalone; mỗi worker 2 core, 2 GB | `docker/spark/Dockerfile` (Delta 3.2.1, hadoop-aws, spark-sql-kafka), `configs/spark-defaults.conf` | 8090 (UI), 7077, 4040 |
| `prometheus` | Thu metric từ kafka-exporter, collector-api, simulator (host:8001), Spark driver (8002), MinIO | `configs/prometheus.yml`, profile `monitoring` | 9090 |
| `kafka-exporter` | Xuất consumer lag, offset theo partition cho Prometheus | profile `monitoring` | — |
| `grafana` | Dashboard "Ingestion health" tự provision | `configs/grafana/provisioning/`, profile `monitoring` | 3000 |
| Simulator (host) | Chạy bằng `make replay` ngoài Docker, đọc CSV từ MinIO qua `localhost:9002`, nhiều tiến trình theo ngày | `configs/pipeline.yaml → simulator`, `lake.endpoint_host` | 8001 (metrics) |

Vùng lưu trữ trong bucket `lake`:

| Vùng | Đường dẫn | Nội dung | Partition | Ghi bởi |
|---|---|---|---|---|
| raw/csv | `s3a://lake/raw/csv/` | CSV gốc Kaggle | — (1 file/tháng) | `make download` + `make upload` |
| raw/ecom.events.raw | `s3a://lake/raw/ecom.events.raw/` | JSON gzip nguyên bản từ Kafka | `dt`, `hour` (giờ nạp) | Kafka Connect |
| bronze | `s3a://lake/bronze/events/` | Delta, 14 cột dữ liệu + 1 partition = 15; khóa `event_id` | `event_date` | Spark Streaming |
| quarantine | `s3a://lake/quarantine/events/` | Delta, 5 cột dữ liệu + 1 partition = 6; payload gốc + `error_reason` | `ingest_date` | Spark Streaming |
| checkpoints | `s3a://lake/_checkpoints/` | Offset và trạng thái stream | — | Spark Streaming |

## Yêu cầu máy

- RAM ≥ 16 GB, đĩa trống ≥ 40 GB (CSV gốc ~9 GB trên host + bản sao ~9 GB trong MinIO, Kafka log tối đa ~18 GB, bronze)
- Docker + Docker Compose v2, Python 3.11+ (host dùng để chạy simulator, `run.py`)
- GNU Make là tuỳ chọn: mọi `make <lệnh>` trong tài liệu này tương đương `python run.py <lệnh>` (Windows PowerShell/cmd chạy được, không cần cài make). Makefile chỉ là wrapper gọi `run.py`.
- Kaggle CLI 2.2.5 (trong `requirements.txt`) cho `make download`; dataset public, token Kaggle không bắt buộc

## Chạy nhanh

Mọi lệnh chạy bằng `python run.py <lệnh>` (Windows PowerShell/cmd được, không cần make); có GNU Make thì `make <lệnh>` tương đương. Tham số `DAY`, `SPEED` đặt qua biến môi trường (`$env:DAY="2019-11-02"; python run.py replay`). Các lệnh Spark chạy bằng `spark-submit` trong container `spark-master`, host không cần Java/Spark.

**1. Storage (MinIO + Spark), chưa cần dataset hay Kafka:**

```bash
cp .env.example .env              # chỉ khi chưa có .env; sửa mật khẩu
pip install -r requirements.txt
python run.py up-storage          # minio, minio-init, spark-master, spark-worker x2 (đợi healthy)
python run.py init-lake           # tạo prefix + bảng Delta rỗng C4/C5; chạy lại an toàn
python run.py smoke-storage       # CSV tổng hợp 3 dòng -> MinIO -> Spark -> Delta -> đọc lại
python run.py test-storage        # integration checks storage (4 test)
```

**2. Dữ liệu thật → mẫu 100k → bronze thử:**

```bash
python run.py download            # Kaggle -> data/raw/2019-Nov.csv (public, không cần token)
python run.py upload              # data/raw/*.csv -> lake/raw/csv/
python run.py smoke-storage-real  # đọc tối đa 100 dòng từ raw/csv/2019-Nov.csv
python run.py sample              # Spark cắt data/sample/events_sample_100k.csv (ngày 2019-11-01)
python run.py stream-sample       # 100k sự kiện -> Delta lake/bronze/sample_events/, partition event_date
python run.py test-sample-bronze  # kiểm tra kiểu, partition, chạy lại không ghi đè (4 test)
```

`stream-sample` (và `stream` khi `stream.source: sample`) là batch hữu hạn; chạy lại cùng mẫu giữ nguyên số dòng, dữ liệu khác ở cùng đích bị từ chối. Bảng thử nằm riêng ở `bronze/sample_events/`, không ghi vào `bronze/events`. Chi tiết và lệnh PowerShell thuần: [docs/runbook.md](docs/runbook.md).

**3. Toàn pipeline (Kafka → bronze/quarantine):**

```bash
python run.py up                  # thêm kafka x3, kafka-ui, connect, collector, prometheus, grafana
python run.py topics              # tạo topic theo configs/kafka_topics.yaml
python run.py stream              # Spark Streaming theo stream.source (đổi sang kafka khi sẵn sàng)
python run.py replay              # simulator phát DAY ở tốc độ SPEED
python run.py sink                # đăng ký Kafka Connect S3 Sink -> lake/raw/ecom.events.raw/
python run.py verify              # đối soát số dòng nguồn vs bronze theo ngày
python run.py down
```

`up-core` bỏ qua Prometheus/Grafana/kafka-exporter cho máy yếu. `test` chạy pytest trên host. `bench` chạy E1–E5, kết quả vào `benchmarks/results/`.

### Trạng thái (03/11/2026)

| Luồng | Lệnh | Trạng thái |
|---|---|---|
| Hạ tầng storage | `up-storage`, `init-lake` | ✅ chạy được; bảng C4/C5 khởi tạo đúng contract |
| Smoke MinIO ↔ Spark ↔ Delta | `smoke-storage`, `smoke-storage-real`, `test-storage` | ✅ pass (3 dòng tổng hợp; 100 dòng thật; 4/4 test) |
| Dữ liệu nguồn | `download`, `upload` | ✅ `2019-Nov.csv` 9,0 GB lên `lake/raw/csv/` |
| Mẫu 100k → bronze thử | `sample`, `stream-sample`, `test-sample-bronze` | ✅ 100.000 dòng, 15 cột, partition `event_date`; 4/4 test |
| Test contract trên host | `test` | ✅ 13 test pass; CI GitHub xanh |
| Kafka: topic, producer, simulator | `topics`, `replay` | ⏳ chưa có (script còn là khung) |
| Spark Streaming Kafka → bronze/quarantine, luật 1–5 | `stream` (`stream.source: kafka`) | ⏳ chưa có; hiện `stream` chạy batch mẫu |
| Collector API, Kafka Connect, đối soát, giám sát | `sink`, `verify`, Grafana | ⏳ chưa có (Collector API mới có `/health`) |

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
run.py                   lệnh dự án: up | up-core | up-storage | down | init-lake | smoke-storage | smoke-storage-real |
                         test-storage | topics | download | upload | sample | replay | stream | stream-sample |
                         test-sample-bronze | sink | verify | bench | test
Makefile                 wrapper: make <target> = python run.py <target>; DAY, SPEED export sang run.py
pytest.ini               pythonpath=. , testpaths=tests
.github/workflows/ci.yml pytest + docker compose config trên mỗi PR
.env.example             KAFKA_CLUSTER_ID, MINIO_ROOT_*, LAKE_BUCKET, GRAFANA_ADMIN_PASSWORD, KAGGLE_* — copy thành .env
.gitattributes           ép LF cho script mount vào container Linux
docker/                  Dockerfile cho spark (Delta + S3A + Kafka), connect (S3 Sink), collector (FastAPI)
configs/                 pipeline.yaml, kafka_topics.yaml, connect/s3-sink.json, spark-defaults.conf, prometheus.yml, grafana/
contracts/               data contract C1–C5: source_csv.yaml, collector_openapi.yaml, event.schema.json,
                         bronze_events.yaml, quarantine_events.yaml
src/common/              config.py, schemas.py, spark_session.py, spark_csv.py (đọc C1, chọn mẫu), metrics.py
src/sources/             download.py, make_sample.py, read_csv.py, simulator.py, fault_injector.py
src/ingestion/           create_topics.py, producer.py, collector_api/
src/storage/             init_lake.py, smoke_lake.py, stream_to_bronze.py, quality_rules.py, maintenance.py
src/monitoring/          reconcile.py, latency.py, grafana/ingestion_health.json
queries/                 verify.sql, sample_queries.sql
benchmarks/              e1_producer.sh … e5_storage.sh, results/
tests/                   test_schemas.py, test_lake_config.py, test_quality_rules.py, test_collector_api.py (host, pytest)
tests/integration/       storage_checks.py, sample_bronze_checks.py (chạy bằng spark-submit trong container)
data/raw/ (gitignore)    CSV do download tải về, nguồn cho upload
data/sample/             events_sample_100k.csv, storage_smoke.csv (fixture 3 dòng)
notebooks/               01_eda.ipynb
docs/                    runbook.md, contracts-c4-c5-handoff.md, contribution.md, ai_usage.md
report/                  outline.md (dàn ý + người viết), chNN_*.md (mỗi chương một file), figures/
```

## Data contract

Năm hợp đồng trong `contracts/` là giao diện giữa các tầng; đổi phải báo cả nhóm. `tests/test_schemas.py` giữ `src/common/schemas.py` khớp với contract.

| # | Giao diện | Bên ghi → bên đọc | Kiểm tra |
|---|---|---|---|
| C1 | File CSV nguồn trên `lake/raw/csv/` | Kaggle → Simulator | Test schema trên mẫu 100k dòng |
| C2 | Collector API `POST /events` | Simulator → API | pytest + httpx |
| C3 | Topic `ecom.events.raw` (12 trường, key = `user_id`) | Simulator, API → Spark, Connect | JSON Schema |
| C4 | Bảng `bronze/events` | Spark → nhóm Processing | Đối soát số dòng theo ngày |
| C5 | Bảng `quarantine/events` | Spark → nhóm dữ liệu | Mỗi bản ghi hỏng cố ý đều xuất hiện |

C4/C5 bản **1.0.0 sẵn sàng review, đang chờ nhóm duyệt**. C4 có 14 cột dữ
liệu (9 C1 + 5 metadata) và `event_date` là cột thứ 15; C5 có 5 cột dữ liệu
và `ingest_date` là cột thứ 6. Xem [bản bàn giao và phiếu duyệt nhóm](docs/contracts-c4-c5-handoff.md)
để xác nhận nullability, UTC, dedup toàn bảng, payload gốc và thứ tự lỗi.
Watermark 1 giờ chưa thay cho bảo đảm unique key toàn bảng; sink Kafka ở
bước streaming Kafka phải có chống replay bền và đối soát.

## Quy ước

- `main` là bản ổn định, `develop` là nhánh tích hợp; làm trên nhánh feature, merge vào `develop` qua Pull Request có 1 người review, CI xanh.
- Không commit dữ liệu lớn, mật khẩu hay file `.env`.
- Mọi thành phần in log có cấu trúc và xuất metric cho Prometheus.
- Ghim phiên bản trong `docker/`: Spark 3.5.3, Delta 3.2.1, Kafka 3.8.1, Connect 7.7.1, MinIO (Chainguard, ghim digest), Python 3.11. Không đổi giữa chừng.
