COMPOSE      := docker compose
DAY          ?= 2019-10-01
SPEED        ?= 100
SPARK_SUBMIT := $(COMPOSE) exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077

.PHONY: up up-core down topics download sample replay stream sink verify bench test

up:        ## toàn bộ stack (core + monitoring)
	$(COMPOSE) --profile monitoring up -d --build

up-core:   ## chỉ Kafka, Connect, MinIO, Spark, Collector (máy yếu)
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) --profile monitoring down

topics:    ## tạo topic theo configs/kafka_topics.yaml
	python -m src.ingestion.create_topics

download:  ## tải CSV gốc từ Kaggle vào data/raw/
	python -m src.sources.download

sample:    ## cắt data/sample/events_sample_100k.csv
	python -m src.sources.make_sample

replay:    ## make replay DAY=2019-10-01 SPEED=100
	python -m src.sources.simulator --day $(DAY) --speed $(SPEED)

stream:    ## Spark Structured Streaming Kafka -> bronze + quarantine
	$(SPARK_SUBMIT) /opt/app/src/storage/stream_to_bronze.py

sink:      ## đăng ký Kafka Connect S3 Sink -> lake/raw/
	curl -s -X POST -H "Content-Type: application/json" --data @configs/connect/s3-sink.json http://localhost:8083/connectors

verify:    ## đối soát số dòng nguồn vs bronze theo ngày
	$(SPARK_SUBMIT) /opt/app/src/monitoring/reconcile.py

bench:     ## chạy E1..E5, kết quả vào benchmarks/results/
	for f in benchmarks/e*.sh; do bash $$f || exit 1; done

test:
	python -m pytest -q
