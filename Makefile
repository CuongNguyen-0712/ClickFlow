# Mọi target gọi run.py để Windows không có GNU Make vẫn chạy được: `python run.py <target>`.
# Ví dụ: make replay DAY=2019-11-02 SPEED=1000   (DAY/SPEED export sang run.py)
DAY   ?= 2019-11-01
SPEED ?= 100
export DAY SPEED

TARGETS := up up-core up-storage down init-lake smoke-storage smoke-storage-real test-storage \
           topics download upload sample replay stream stream-sample test-sample-bronze sink verify bench test
.PHONY: $(TARGETS)

up:        ## toàn bộ stack (core + monitoring)
up-core:   ## chỉ Kafka, Connect, MinIO, Spark, Collector (máy yếu)
up-storage: ## MinIO + Spark master + 2 worker; không cần Kafka hay dữ liệu
down:
init-lake:          ## tạo prefix + bảng Delta rỗng C4/C5; chạy lại an toàn
smoke-storage:      ## CSV tổng hợp -> MinIO -> Spark -> Delta -> Spark
smoke-storage-real: ## đọc tối đa 100 dòng từ raw/csv/2019-Nov.csv
test-storage:       ## integration checks storage, gồm khởi tạo lại không mất dữ liệu
stream-sample:      ## ghi/kiểm chứng 100k sự kiện mẫu vào bronze/sample_events
test-sample-bronze: ## kiểm tra kiểu, partition, chạy lại; giữ artifact
topics:    ## tạo topic theo configs/kafka_topics.yaml
download:  ## tải CSV trong pipeline.yaml -> simulator.files từ Kaggle về data/raw/ (giải nén zip có sẵn nếu có)
upload:    ## đẩy data/raw/*.csv lên lake/raw/csv/; hoặc kéo thả trên MinIO Console :9003
sample:    ## cắt data/sample/events_sample_100k.csv từ raw/csv bằng Spark
replay:    ## make replay DAY=2019-11-01 SPEED=100
stream:    ## chạy nguồn cấu hình trong stream.source (hiện: sample)
sink:      ## đăng ký Kafka Connect S3 Sink -> lake/raw/
verify:    ## đối soát số dòng nguồn vs bronze theo ngày
bench:     ## chạy E1..E5, kết quả vào benchmarks/results/
test:

$(TARGETS):
	python run.py $@
