# Chương 2 — Data Lake, Delta Lake và Spark Structured Streaming

*Phần TV3. Các mục 2.2–2.4 được soạn để ghép sau phần Kafka của TV2; nội dung
thiết kế cụ thể được trình bày tại [mục 4.3](ch04_storage.md).*

## 2.2. Data Lake và tổ chức dữ liệu theo vùng

Data Lake là kho tập trung có thể giữ dữ liệu ở nhiều dạng, bao gồm bản gốc
chưa chuẩn hóa. Cách tiếp cận này cho phép đọc lại nguồn khi quy tắc xử lý hoặc
câu hỏi phân tích thay đổi. Việc giữ dữ liệu không thay thế yêu cầu quản trị:
người sử dụng vẫn cần biết nguồn, schema, chất lượng và phạm vi thời gian của
từng tập dữ liệu. [DL1](references_storage.md#dl1)

Trong ClickFlow, MinIO cung cấp object storage tương thích giao thức S3;
Spark truy cập qua S3A. Đường dẫn là prefix chứa object, không nên đồng nhất
với bảng quan hệ hoặc một transaction. Dự án phân biệt ba vai trò dữ liệu:

| Vùng | Vai trò trong dự án | Điều kiện sử dụng |
|---|---|---|
| raw | Giữ CSV nguồn và cấu hình lưu JSON gzip từ Kafka | Cho phép kiểm tra lại dữ liệu trước xử lý |
| bronze | Sự kiện đã chuẩn hóa kiểu và vượt kiểm tra chất lượng | Giao diện đầu vào cho nhóm Processing |
| quarantine | Chuỗi message lỗi và lý do từ chối | Điều tra, thống kê chất lượng và chuẩn bị sửa lỗi |

Việc tách vùng giúp bảo toàn dấu vết đầu vào trong khi cung cấp một giao diện
ổn định cho bước xử lý tiếp theo. Đây là lựa chọn thiết kế của ClickFlow;
bronze trong dự án có kiểm tra chất lượng, không mặc định mọi hệ thống khác
đều dùng cùng ý nghĩa cho tên vùng này. Schema-on-read phù hợp với việc giữ
raw, còn bronze cần schema và invariant được xác định trước khi ghi.

## 2.3. Delta Lake

### 2.3.1. Bảng dữ liệu và giao dịch ACID

Delta bổ sung transaction log cho các file dữ liệu của bảng. Reader sử dụng
một snapshot đã commit; writer kiểm tra xung đột trước khi công bố version mới.
Optimistic concurrency control cho phép phát hiện thay đổi không tương thích
thay vì để reader nhìn thấy một cập nhật dở dang. [DL2](references_storage.md#dl2)

| Thuộc tính | Ý nghĩa cần thiết đối với bảng Storage |
|---|---|
| Atomicity | Một cập nhật bảng thành công hoặc không xuất hiện như một commit hoàn chỉnh |
| Consistency | Dữ liệu được ghi tuân theo schema/ràng buộc mà bảng và pipeline thực sự áp dụng |
| Isolation | Truy vấn đọc snapshot nhất quán trong khi có cập nhật |
| Durability | Commit đã công bố được lưu trên storage để tiếp tục đọc và phục hồi |

Phạm vi áp dụng trong ClickFlow là **từng bảng**. Hai lần MERGE vào bronze
và quarantine không tạo thành một transaction chung. Hệ thống cần thêm cơ chế
ghi lặp an toàn để hội tụ sau lỗi giữa các bước; chương 4 trình bày cơ chế này.

### 2.3.2. Schema enforcement và data contract

Schema enforcement kiểm tra schema đầu ghi tương thích với bảng; schema
evolution là thay đổi schema có chủ đích. Hai khái niệm này cần phân biệt với
kiểm tra nghiệp vụ như giá không âm. [DL3](references_storage.md#dl3)

ClickFlow dùng schema tường minh và contract C1/C3/C4/C5. ID 19 chữ số được giữ
bằng `bigint`, không trung chuyển qua số thực. Ví dụ, kiểm tra `event_type`
thuộc tập cho phép là trách nhiệm của `quality_rules.py`; khai báo `unique_key`
trong YAML cũng không tự tạo ràng buộc UNIQUE của Delta. Vì vậy, bảo đảm dữ liệu
phải được mô tả bằng cả contract lẫn cơ chế writer, không chỉ bằng tên định dạng.

### 2.3.3. Time travel và retention

Time travel cho phép truy vấn version cũ, thuận tiện để tái lập kết quả và
kiểm tra trước/sau thay đổi. Khả năng đọc lịch sử cần giữ cả transaction log
lẫn file dữ liệu của version đó; đặt thời hạn giữ log không đủ nếu file đã bị
thu hồi. [DL3](references_storage.md#dl3)

E5 sử dụng version trước/sau OPTIMIZE trên cùng bảng để đo xen kẽ. Các version
cũ đã được đọc lại và so sánh với nguồn, nhưng đây chưa phải thực nghiệm chạy
liên tục 30 ngày. Yêu cầu time travel 30 ngày là chính sách retention của dự án,
được kiểm tra trong cấu hình và metadata bảng thử.

### 2.3.4. Partition pruning, compaction và ZORDER

Ba cơ chế phục vụ các mục tiêu khác nhau:

| Cơ chế | Cách giảm công việc đọc trong thiết kế |
|---|---|
| Partition pruning | Chọn partition phù hợp bộ lọc, ví dụ `event_date=DATE '2019-11-01'` |
| OPTIMIZE/compaction | Gom file nhỏ, giảm số file cần quản lý và mở |
| ZORDER và data skipping | Sắp xếp dữ liệu liên quan gần nhau, hỗ trợ bỏ qua file qua thống kê cột |

Hiệu quả ZORDER phụ thuộc dữ liệu và predicate; việc sắp xếp không bảo đảm
mọi truy vấn đều nhanh hơn. OPTIMIZE tạo layout mới mà vẫn giữ kết quả logic
của bảng. [DL4](references_storage.md#dl4) E5 đo việc áp dụng đồng thời OPTIMIZE và ZORDER,
không tách được lợi ích độc lập của từng thành phần.

VACUUM thu hồi file dữ liệu hết thời hạn giữ. Đây là thao tác khác với việc
giảm active file count của snapshot. [DL5](references_storage.md#dl5) Trong E5,
active files giảm nhưng physical footprint vẫn chứa version cũ; báo cáo cần
ghi rõ đang dùng đại lượng nào khi so sánh dung lượng.

## 2.4. Spark Structured Streaming

### 2.4.1. Micro-batch và checkpoint

Structured Streaming biểu diễn xử lý luồng qua phép toán DataFrame/SQL.
Chế độ micro-batch xử lý các phần dữ liệu kế tiếp và lưu tiến độ/state vào
checkpoint để có thể khởi động lại. `foreachBatch` cho phép tự ghi nhiều đích,
nhưng callback mặc định có thể được thực hiện lại sau lỗi; sink phải xử lý
retry một cách an toàn. [SP1](references_storage.md#sp1)

Trong ClickFlow, Kafka là nguồn có thể đọc lại theo offset. Checkpoint lưu
offset/state của query; nó khác transaction log `_delta_log` quản lý version
của từng bảng. Nếu Delta đã commit nhưng callback chưa được checkpoint xác
nhận, batch có thể chạy lại. Vì vậy checkpoint và Delta ACID là hai thành phần
bổ sung cho nhau, không tự thay thế yêu cầu idempotent của writer.

### 2.4.2. Event-time và watermark

`event_time` là thời điểm sự kiện; `ingested_at` trong triển khai là timestamp
đầu micro-batch. Watermark đặt trên event-time để quản lý dữ liệu đến trễ và
state của operator, không phải đồng hồ đếm một giờ kể từ lúc job khởi động.
`dropDuplicatesWithinWatermark` giới hạn state khử trùng theo watermark;
dữ liệu trễ vượt ngưỡng có thể bị loại. [SP2](references_storage.md#sp2)

Pipeline áp dụng watermark một giờ cho nhánh hợp lệ và theo dõi riêng dòng
trùng, dòng trễ. Nhánh lỗi đi vòng qua stateful operator để timestamp hỏng
vẫn được giữ ở quarantine. MERGE theo khóa bền tiếp tục ngăn chèn lại ID đã
có sau khi state hết hạn; watermark riêng lẻ không bảo đảm uniqueness toàn bảng.

### 2.4.3. Liên hệ với kiểm chứng của dự án

E3 kiểm tra đánh đổi trigger và file nhỏ; E4b kiểm tra replay sau khi driver
bị SIGKILL; E5 kiểm tra định dạng và layout. Các kết quả được tổng hợp ở
[mục 4.3.8](ch04_storage.md#438-minh-chứng-e3-e4b-và-e5) và nguồn tương ứng trong
[danh mục minh chứng](references_storage.md). Chúng phục vụ việc đánh giá
thiết kế đã chọn, không phải bảo đảm hiệu năng phổ quát của Spark hoặc Delta.
