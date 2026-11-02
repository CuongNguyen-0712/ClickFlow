# C4/C5 — bàn giao Storage cho Processing và nhóm dữ liệu

**Phiên bản 1.0.0, sẵn sàng review ngày 01/11/2026; phê duyệt nhóm đang chờ.**
Nguồn sự thật là [C4](../contracts/bronze_events.yaml) và
[C5](../contracts/quarantine_events.yaml). Metadata `status: review_ready`,
`approval.status: pending`, `approvals: []` phản ánh đúng trạng thái này.
Không có xác nhận duyệt của thành viên nào được tự điền.

## C4: 14 cột dữ liệu + 1 cột phân vùng

Đích chính: `s3a://lake/bronze/events/`, format Delta, Storage ghi và
Processing đọc. **14 = 9 cột C1 + 5 metadata**. `event_date` là cột thứ 15
trong DataFrame/bảng Delta; `source_file` của C3 không được tính vào C4.

| # | Cột | Spark SQL type | Nullable | Ý nghĩa |
|---|---|---|---|---|
| 1 | event_time | timestamp | false | Thời điểm sự kiện UTC |
| 2 | event_type | string | false | view, cart hoặc purchase |
| 3 | product_id | bigint | false | ID sản phẩm |
| 4 | category_id | bigint | false | ID danh mục 19 chữ số, giữ kiểu long |
| 5 | category_code | string | true | Mã phân cấp danh mục |
| 6 | brand | string | true | Thương hiệu |
| 7 | price | double | false | Giá >= 0 |
| 8 | user_id | bigint | false | ID người dùng |
| 9 | user_session | string | true | Session từ nguồn |
| 10 | event_id | string | false | ID ổn định từ C3; khóa duy nhất toàn bảng |
| 11 | kafka_partition | int | false | Partition Kafka gốc, >= 0 |
| 12 | kafka_offset | bigint | false | Offset Kafka gốc, >= 0 |
| 13 | produced_at | timestamp | true | Thời điểm producer gửi, UTC |
| 14 | ingested_at | timestamp | false | Thời điểm ghi hợp lệ lần đầu, UTC |
| 15, partition | event_date | date | true trong schema | to_date(event_time), UTC |

Cột partition giữ `nullable: true` như schema Delta đã tạo để không yêu cầu
migration. Invariant dữ liệu vẫn yêu cầu `event_date` không null và bằng
ngày UTC của `event_time` không null. Tương tự với `ingest_date` ở C5.
Giá trị `produced_at=null` hợp lệ với C4; Processing bỏ các dòng này khỏi
phép tính latency sử dụng `produced_at`.

`event_id` được giữ nguyên từ C3. Retry/replay phải mang cùng ID và giữ dòng
đã ghi đầu tiên, bao gồm Kafka metadata và `ingested_at`. Xung đột chỉ xảy ra
khi cùng ID nhưng một trong **9 giá trị C1** khác nhau, so sánh đúng kiểu và
null-safe. `kafka_partition`, `kafka_offset`, `produced_at`, `ingested_at`
không tham gia so sánh xung đột: các metadata này có thể đổi khi replay.
`event_date` được suy ra từ `event_time`, không cần so sánh độc lập. Khi sink
quan sát xung đột nguồn, giữ nguyên toàn bộ dòng đầu và phát cảnh báo để kiểm tra.

Kafka writer dùng một query: nhánh hợp lệ sau luật 1–4 đi qua
`withWatermark("event_time", "1 hour").dropDuplicatesWithinWatermark(["event_id"])`,
rồi `unionByName` với nhánh lỗi trước một `foreachBatch`. Spark bỏ trùng
trong state và bỏ riêng dòng hợp lệ quá watermark, kể cả ID mới; các dòng
này không vào C5. Dòng lỗi không tham gia state hay watermark.

Insert-only MERGE theo `event_id` toàn bảng vẫn giữ uniqueness cho các dòng
sống sót, kể cả ID xuất hiện lại sau khi key state hết hạn. `unique_key`
trong YAML không tự tạo ràng buộc UNIQUE của Delta: bảo đảm này do writer,
với một driver ghi tại một thời điểm. Cảnh báo xung đột theo 9 giá trị C1
chỉ áp dụng cho dòng còn tới sink; bản trùng bị state bỏ chỉ tăng counter.
Writer chọn ứng viên còn lại trong cùng batch theo partition rồi offset;
stateful dedup không cam kết chọn bản sao theo thứ tự toàn cục giữa các
partition. Dòng C4 đã commit đầu tiên vẫn được giữ nguyên.

Batch mẫu đã được kiểm chứng ở `bronze/sample_events`, có 100.000 ID khác
nhau. Bảng thử dùng `kafka_partition=-1`, `kafka_offset=-1`,
`produced_at=null` và tự tính ID từ 9 giá trị C1. Ngoại lệ này chỉ áp dụng
cho bảng thử riêng theo `stream.sample_bronze_path`.

## C5: 5 cột dữ liệu + 1 cột phân vùng

Đích: `s3a://lake/quarantine/events/`, format Delta. Storage ghi các message
không đạt luật 1–4 để nhóm dữ liệu điều tra và nạp lại sau khi sửa lỗi.

| # | Cột | Spark SQL type | Nullable | Ý nghĩa |
|---|---|---|---|---|
| 1 | payload | string | true | Kafka value UTF-8 gốc; null chỉ với tombstone |
| 2 | error_reason | string | false | Một trong 5 lý do bên dưới |
| 3 | kafka_partition | int | false | Partition gốc |
| 4 | kafka_offset | bigint | false | Offset gốc |
| 5 | ingested_at | timestamp | false | Thời điểm xử lý lỗi lần đầu, UTC |
| 6, partition | ingest_date | date | true trong schema | to_date(ingested_at), UTC |

`payload` giữ nguyên nội dung chuỗi, khoảng trắng và thứ tự key, kể cả khi
không parse được JSON. Không serialize lại object đã parse để thay payload.
`ingest_date` theo ngày nhận lỗi, vì `event_time` có thể thiếu hoặc hỏng.

Mỗi message hỏng có **một dòng, một lý do chính**, theo thứ tự sau:

| Ưu tiên | Luật | error_reason | Trường hợp |
|---|---|---|---|
| 1 | 1 | parse_error | JSON hỏng/không phải object, sai kiểu, hoặc thiếu trường bắt buộc ngoài ngoại lệ của luật 2–4 |
| 2 | 2 | bad_event_time | event_time thiếu/null, chuỗi timestamp hỏng hoặc ngoài tháng của source_file |
| 3 | 3 | bad_event_type | event_type thiếu/null hoặc string ngoài view/cart/purchase |
| 4 | 4 | missing_id | product_id hoặc user_id thiếu/null |
| 5 | 4 | negative_price | price < 0 |

Luật 1 kiểm tra cấu trúc/kiểu nhưng nhường các kiểm tra giá trị nêu ở luật
2–4 cho đúng lý do chuyên biệt. Ví dụ, khi các trường khác hợp lệ:

- `event_time=null` → `bad_event_time`; timestamp dạng số sai kiểu → `parse_error`.
- `event_type="refund"` → `bad_event_type`; event_type dạng object → `parse_error`.
- `product_id=null` và `price=-1` → `missing_id` theo thứ tự ưu tiên.
- `product_id="abc"` hoặc `price=null` → `parse_error`.
- `category_code`, `brand`, `user_session` null hợp lệ, không vào quarantine.
- Với `source_file=2019-Nov.csv`, `2019-12-01 00:00:00 UTC` nằm ngoài biên
  `[2019-11-01 00:00:00 UTC, 2019-12-01 00:00:00 UTC)` → `bad_event_time`.

Message hợp lệ bị trùng `event_id` thuộc luật 5: bỏ và đếm metric, không
thêm `duplicate` vào enum C5. Message hợp lệ quá watermark cũng bị Spark
bỏ riêng, không thêm lý do lỗi vào C5. Các message hỏng được phân loại luật
1–4 rồi bỏ qua toàn bộ stateful dedup: timestamp null hoặc ngoài tháng,
kể cả timestamp tương lai, vẫn tới C5 và không làm tăng watermark.
Với một `kafka.topic` cố định, retry cùng
`(kafka_partition, kafka_offset)` không được tạo thêm dòng quarantine;
hai offset khác nhau là hai message, kể cả payload giống nhau.

Writer kiểm tra JSON/C3 nghiêm ngặt trước khi Spark cast, tách nhánh hợp lệ
và lỗi trước state, rồi union hai nhánh vào cùng query Kafka/`foreachBatch`.
C5 dùng insert-only MERGE theo
hai tọa độ Kafka. Topic không có cột riêng trong schema C5 nên checkpoint
và hai đích chỉ được gắn với một topic cố định. Metadata binding lưu topic,
broker, đường dẫn hai bảng, fingerprint C3 và phiên bản C4/C5. Marker riêng
checkpoint còn có `checkpoint_layout`: operator, keys, event-time,
watermark, routing và shuffle partitions. Identity nguồn/đích cũ tương thích
vẫn được giữ, nhưng checkpoint stateless hoặc layout thay đổi bị từ chối.
Default mới là `s3a://lake/_checkpoints/kafka_to_delta_watermark_v1/`; giữ
nguyên checkpoint stateless cũ `kafka_to_delta/`, không tái dùng cho plan
mới hoặc xóa để vượt guard. Checkpoint không rỗng/chưa binding bị từ chối
trước init; đổi topic hoặc đích cần
checkpoint/đích mới phù hợp hoặc migration rõ ràng, giữ nguyên bằng chứng cũ.

Hai MERGE tạo **hai commit Delta riêng**, không có tính nguyên tử chung
hai bảng. Lỗi sau commit bronze có thể để quarantine tạm thời chưa đủ;
retry từ checkpoint giữ nguyên dòng C4 đã ghi và bổ sung C5 còn thiếu.
Processing chỉ đối soát hai bảng sau batch thành công, không coi snapshot
đọc giữa hai commit là một transaction chung. Xem
[runbook Kafka](runbook.md#kafka--c4c5-streaming) để vận hành và khôi phục.

## Cách Processing đọc và kiểm chứng

Spark dùng timezone UTC, S3A và Delta như `configs/spark-defaults.conf`.
Chọn cột theo tên và lọc `event_date` để dùng partition pruning. Ví dụ
đọc/đếm theo ngày, kiểm tra ID trùng và thống kê quarantine nằm trong
[queries/sample_queries.sql](../queries/sample_queries.sql).

`src/common/schemas.py` cung cấp `BRONZE_COLUMNS` (14),
`BRONZE_TABLE_COLUMNS` (15), `QUARANTINE_COLUMNS` (5),
`QUARANTINE_TABLE_COLUMNS` (6) và `QUARANTINE_ERROR_REASONS`.
`init_lake.table_contract()` mở rộng partition từ khai báo YAML, giữ đúng
kiểu và nullable, thay vì tự giả định mọi partition đều là date.

Chạy `python -m pytest -q tests/test_schemas.py` để kiểm tra giao diện và
`python -m pytest -q` cho toàn bộ suite. Enum, thứ tự chọn lỗi, unique key và
biểu thức ngày là yêu cầu cho writer; test schema và kiểm thử classifier/replay
Kafka cung cấp các bằng chứng riêng. Đổi kiểu, xóa/đổi tên cột,
đổi khóa, partition hoặc nghĩa trường cần version contract mới, kế hoạch
migration và nhóm consumer duyệt trước khi áp dụng.

## Kiểm chứng kỹ thuật trước stateful Rule 5 — 01/11/2026

Các kết quả trong mục này dùng plan stateless trước Rule 5. Giữ nguyên
bằng chứng lịch sử; các nhận định replay/conflict không xác nhận hành vi
stateful mới. Đặc biệt, bản trùng trong state window hiện có thể bị Spark
bỏ trước khi tới bước kiểm tra xung đột của MERGE.

- Mốc đối chiếu contract ban đầu: `tests/test_schemas.py` **9 test đạt**;
  toàn bộ pytest lúc đó **12 test đạt**. Suite sau triển khai Kafka lúc đó có
  **187 unit test đạt**.
- Đối chiếu trực tiếp schema, thứ tự cột, type, nullable và partition của
  `bronze/events` (15 cột, 0 dòng), `quarantine/events` (6 cột, 0 dòng),
  `bronze/sample_events` (15 cột, 100.000 dòng và ID duy nhất): đều khớp.
- Bốn truy vấn đọc trong `queries/sample_queries.sql` thực thi thành công.
- Review độc lập đã kiểm tra C3/C4/C5, replay và payload; sau khi làm rõ
  phép so sánh xung đột theo 9 giá trị C1, không còn phát hiện quan trọng.
- Báo cáo: `data/raw/c4-c5-contract-verification-2026-11-01.json`;
  log: `data/raw/c4-c5-contract-verification.log`. Kiểm chứng Delta chỉ đọc,
  không chạy lệnh xóa hoặc thay đổi dữ liệu/bảng hiện có.

Kiểm chứng Kafka độc lập được giữ trong
`data/raw/kafka-stream-test-767d9cd4c267/` (`case.json`, YAML và báo cáo JSON
từng stage; log trong `data/raw/`). Harness
`tests/integration/kafka_stream_checks.py` chạy `setup`, tạo topic 2 partition
/ RF3 bằng Kafka CLI, rồi `fault`, `resume`, `append`, `verify`, mỗi stage
bằng một driver mới:

- Fault chủ động sau commit bronze: **1 C4, 0 C5**. Resume cùng checkpoint:
  **1 C4, 2 C5**, giữ nguyên toàn bộ dòng C4 đầu, kể cả metadata.
- Append: **2 C4, 7 C5**, đủ cả năm lý do lỗi; replay chỉ đổi metadata được
  bỏ qua, một conflict giữ dòng đầu và phát cảnh báo. Bảy tọa độ C5 duy nhất.
- Driver verify mới resume checkpoint đã commit không ghi thêm; version
  hai Delta vẫn **2**. Payload giữ đúng chuỗi gốc và null tombstone.
- Binding topic khác bị từ chối. Regression riêng đã tái hiện việc nhận
  checkpoint không rỗng/chưa binding trước guard (RED), và xác nhận guard từ
  chối trước init (GREEN).
- Metrics HTTP đã scrape được. Counter sink đếm số thực chèn sau commit;
  progress Spark có thể đếm lại input qua nhiều action. Counter reset khi
  driver restart, không thay cho audit Delta lâu dài.

Đây là bằng chứng chức năng fixture và replay, chưa phải benchmark toàn bộ
dataset. CLI continuous mặc định đã phục vụ metrics ở cổng 8002 và dừng bằng
SIGTERM đúng PID với exit code 0; xem log/mốc vận hành trong runbook.
Trạng thái phê duyệt nhóm vẫn giữ nguyên như đầu tài liệu.

## Rule 5 hiện tại và đối soát Processing

- `clickflow_stream_duplicates_dropped_total` cộng
  `stateOperators[].customMetrics.numDroppedDuplicateRows`.
- `clickflow_stream_late_records_dropped_total` cộng riêng
  `stateOperators[].numRowsDroppedByWatermark`.
- `clickflow_stream_duplicates_ignored_total` chỉ phản ánh guard MERGE bền
  sau stateful dedup; C4 giữ duy nhất toàn bảng sau state expiry.
- C5 và counter luật 1–4 vẫn chỉ đếm dòng lỗi mới commit theo tọa độ Kafka.

Hai counter stateful có 0 ngay khi driver khởi động và reset khi restart. Progress lặp
trong cùng query/run/batch không cộng lại state counts; log `dedup_progress`
ghi query/run/batch và số drop mới để đối chiếu. Manifest qua nhiều driver
phải cộng theo stage; không dùng input progress làm số dòng đã commit.

Prototype Spark 3.5.3 đã xác nhận bỏ 2 duplicates và nhánh lỗi timestamp
không làm tăng watermark. Guard checkpoint thật đã kiểm tra từ chối plan
stateless cũ/layout thay đổi; suite triển khai Rule 5 có **240 unit test đạt**.
Recipe `tests/integration/watermark_dedup_checks.py` gồm `setup`, tạo topic
2 partition/RF3, `seed`, `recover`, `within-hour` bằng các driver mới, nằm trong
[runbook Rule 5](runbook.md#luật-5-stateful-dedup-và-watermark).

Case `data/raw/watermark-dedup-test-386af898f66d` đã kiểm chứng thành công:
seed C4/C5 **5/2**, drop **10**; replay trong driver mới phục hồi state và bỏ
**15**; advance 3 giờ làm hết hạn ít nhất 5 keys; boundary bỏ riêng **2** late
records, MERGE giữ ID cũ sau expiry và hai lỗi vẫn tới C5. Gửi lại cùng ID
với timestamp **03:01 → 03:30 UTC** bị state loại thêm **1**, không tới MERGE.
Báo cáo `acceptance-report.json` xác nhận C4 **6 dòng/6 ID**, C5 **4**,
**26 duplicate drops khớp manifest**. Đối soát Kafka offsets:
**39 = 6 bronze + 4 quarantine + 26 state drops + 2 late drops + 1 durable ignored**.
Đã scrape counter qua HTTP; timestamp lỗi không tác động watermark và payload
C5 giữ nguyên. Những kết quả kỹ thuật này không thay cho phê duyệt nhóm.

## Bản ghi duyệt nhóm

| Vai trò | Nội dung cần xác nhận | Người duyệt / ngày / bằng chứng |
|---|---|---|
| Sources | 9 kiểu C1, nullable, ID 19 chữ số, tháng nguồn | Chờ xác nhận |
| Ingestion | ID C3 ổn định, payload gốc, Kafka metadata, ranh giới luật 1 | Chờ xác nhận |
| Storage | C4/C5, dedup toàn bảng, UTC, error precedence, replay quarantine | Chờ xác nhận |
| Infra | Bucket/path, schema Delta hiện có, khả năng đọc S3A/Delta | Chờ xác nhận |
| Processing | Cách đếm 14+1, ý nghĩa/nullability, truy vấn và bàn giao | Chờ xác nhận |

Sau khi đủ xác nhận thực tế, điền reviewer/ngày/bằng chứng vào `approvals`
của cả hai YAML và chuyển `status`, `approval.status` sang `approved`.
Điều kiện Done của task còn bước duyệt này; test xanh không thay cho phê duyệt.
