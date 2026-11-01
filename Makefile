COMPOSE      := docker compose
DAY          ?= 2019-11-01
CLOSED_BEFORE ?=
SPEED        ?= 100
SPARK_SUBMIT = $(COMPOSE) exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077

.PHONY: up up-core up-storage up-stream init-lake smoke-storage smoke-storage-real test-storage test-sample-bronze test-checkpoint-binding test-maintenance test-processing-queries maintenance down topics upload sample replay stream stream-sample stream-drain sink verify verify-sample bench bench-e3 test

up-storage: ## MinIO + master + 2 workers; no dataset or Kafka required
	$(COMPOSE) up -d --build --wait minio minio-init spark-master spark-worker-1 spark-worker-2

up-stream: ## Kafka x3 + MinIO + Spark, không cần Connect/Collector
	$(COMPOSE) up -d --build --wait kafka-1 kafka-2 kafka-3 minio minio-init spark-master spark-worker-1 spark-worker-2

init-lake: ## prefixes + empty Delta C4/C5; safe to repeat
	$(SPARK_SUBMIT) /opt/app/src/storage/init_lake.py

smoke-storage: ## synthetic C1 CSV -> MinIO -> Spark -> Delta -> Spark
	$(SPARK_SUBMIT) /opt/app/src/storage/smoke_lake.py --sample --keep-artifacts

smoke-storage-real: ## read up to 100 rows from raw/csv/2019-Nov.csv
	$(SPARK_SUBMIT) /opt/app/src/storage/smoke_lake.py --keep-artifacts

test-storage: ## integration checks, including reinitialization without data loss
	$(SPARK_SUBMIT) /opt/app/tests/integration/storage_checks.py

up:        ## toàn bộ stack (core + monitoring)
	$(COMPOSE) --profile monitoring up -d --build

up-core:   ## chỉ Kafka, Connect, MinIO, Spark, Collector (máy yếu)
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) --profile monitoring down

topics:    ## tạo topic theo configs/kafka_topics.yaml
	python -m src.ingestion.create_topics

upload:    ## đẩy data/raw/*.csv (tải tay từ Kaggle) lên lake/raw/csv/; hoặc kéo thả trên MinIO Console :9003
	$(COMPOSE) exec minio sh -c 'mc alias set -q local http://127.0.0.1:9000 $$MINIO_ROOT_USER $$MINIO_ROOT_PASSWORD && mc cp /upload/*.csv local/$$LAKE_BUCKET/raw/csv/'

sample:    ## cắt data/sample/events_sample_100k.csv
	$(SPARK_SUBMIT) /opt/app/src/sources/make_sample.py

stream-sample: ## ghi/kiểm chứng 100k sự kiện vào bronze/sample_events
	$(SPARK_SUBMIT) /opt/app/src/storage/stream_to_bronze.py --source sample

test-sample-bronze: ## kiểm tra kiểu, partition, chạy lại; giữ artifact
	$(SPARK_SUBMIT) /opt/app/tests/integration/sample_bronze_checks.py

replay:    ## make replay DAY=2019-11-01 SPEED=100
	python -m src.sources.simulator --day $(DAY) --speed $(SPEED)

stream:    ## chạy nguồn được cấu hình trong stream.source
	$(SPARK_SUBMIT) /opt/app/src/storage/stream_to_bronze.py

stream-drain: ## xử lý offsets hiện có rồi thoát; giữ checkpoint để resume
	$(SPARK_SUBMIT) /opt/app/src/storage/stream_to_bronze.py --source kafka --available-now

test-checkpoint-binding: ## từ chối checkpoint cũ không có nhận diện nguồn; giữ artifact
	$(SPARK_SUBMIT) /opt/app/tests/integration/checkpoint_binding_checks.py

maintenance: ## OPTIMIZE ngày đã đóng + VACUUM DRY RUN; CLOSED_BEFORE là mốc đóng độc quyền
	$(SPARK_SUBMIT) /opt/app/src/storage/maintenance.py --day $(DAY) --closed-before "$(CLOSED_BEFORE)"

test-maintenance: ## fixture E5 riêng: giảm file nhỏ, giữ dữ liệu/time travel; không xóa
	$(SPARK_SUBMIT) /opt/app/tests/integration/maintenance_checks.py

sink:      ## đăng ký Kafka Connect S3 Sink -> lake/raw/
	curl -s -X POST -H "Content-Type: application/json" --data @configs/connect/s3-sink.json http://localhost:8083/connectors

verify:    ## đối soát số dòng nguồn vs bronze theo ngày
	$(SPARK_SUBMIT) /opt/app/src/monitoring/reconcile.py --day "$(DAY)"

verify-sample: ## đối soát đúng cohort mẫu 100k và chạy query bàn giao C4
	$(SPARK_SUBMIT) /opt/app/src/monitoring/reconcile.py --sample --day "$(DAY)"

test-processing-queries: ## fixture SQL hai ngày, lỗi C5, pruning, negative cases; giữ artifact
	$(SPARK_SUBMIT) /opt/app/tests/integration/processing_query_checks.py

bench:     ## chạy E1..E5, kết quả vào benchmarks/results/
	for f in benchmarks/e*.sh; do bash $$f || exit 1; done

bench-e3:  ## E3 thật:6 cấu hình × ít nhất3 lần, giữ toàn bộ artifact
	bash benchmarks/e3_latency.sh

test:
	python -m pytest -q
