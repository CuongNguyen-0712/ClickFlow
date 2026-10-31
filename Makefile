# Mọi target gọi run.py để Windows không có GNU Make vẫn chạy được: `python run.py <target>`.
# Ví dụ: make replay DAY=2019-11-02 SPEED=1000   (DAY/SPEED export sang run.py)
DAY   ?= 2019-11-01
SPEED ?= 100
export DAY SPEED

TARGETS := up up-core down topics download upload sample replay stream sink verify bench test
.PHONY: $(TARGETS)

up:        ## toàn bộ stack (core + monitoring)
up-core:   ## chỉ Kafka, Connect, MinIO, Spark, Collector (máy yếu)
down:
topics:    ## tạo topic theo configs/kafka_topics.yaml
download:  ## tải CSV trong pipeline.yaml -> simulator.files từ Kaggle về data/raw/ (giải nén zip có sẵn nếu có)
upload:    ## đẩy data/raw/*.csv lên lake/raw/csv/; hoặc kéo thả trên MinIO Console :9003
sample:    ## cắt data/sample/events_sample_100k.csv
replay:    ## make replay DAY=2019-11-01 SPEED=100
stream:    ## Spark Structured Streaming Kafka -> bronze + quarantine
sink:      ## đăng ký Kafka Connect S3 Sink -> lake/raw/
verify:    ## đối soát số dòng nguồn vs bronze theo ngày
bench:     ## chạy E1..E5, kết quả vào benchmarks/results/
test:

$(TARGETS):
	python run.py $@
