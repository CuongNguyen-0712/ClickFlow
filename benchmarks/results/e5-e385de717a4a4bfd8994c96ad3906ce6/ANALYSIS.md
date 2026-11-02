# E5 — nhận xét số liệu và kịch bản chương 6–7

E5 **đạt** trên 100.000 dòng thật: 50.000 dòng đầu mỗi ngày 01–02/11/2019,
giữ timestamp và multiplicity. Đo ngày **02/11/2026 (UTC+7)** trên một máy,
Spark3.5.3/Delta3.2.1, hai worker/four executor cores, MinIO local Docker.
Tám cấu hình, ba truy vấn, ba lần/cell: **72 lần đo**, tất cả kết quả khớp nguồn.

Dùng [bảng đầy đủ](publication/comparison.csv), [bảng mean/SD/median](publication/query-summary.csv),
[biểu đồ dung lượng/file](publication/storage-comparison.png) và
[biểu đồ truy vấn](publication/query-comparison.png) để đưa vào báo cáo.
Bản [publication](publication/README.md) dùng chính số đo gốc, chỉ giảm cỡ nhãn
trục cho dễ đọc; [provenance](publication/provenance.json) lưu tùy chọn render.

## Chương 6: kết quả quan sát

| Bố trí | Active MiB | Active files | Q1 median ms | Q2 median ms | Q3 median ms |
|---|---:|---:|---:|---:|---:|
| CSV flat | 13.706 | 8 | 249.8 | 249.2 | 311.3 |
| CSV partitioned | 12.657 | 16 | 239.3 | 162.5 | 211.6 |
| JSON gzip flat | 3.610 | 8 | 290.6 | 279.2 | 297.8 |
| JSON gzip partitioned | 3.539 | 16 | 306.1 | 150.3 | 217.5 |
| Delta flat trước | 3.869 | 8 | 994.4 | 1044.0 | 1021.9 |
| Delta flat sau | 2.349 | 1 | 385.3 | 314.9 | 407.7 |
| Delta partitioned trước | 3.937 | 16 | 1067.6 | 1117.8 | 860.5 |
| Delta partitioned sau | 2.354 | 2 | 403.2 | 312.8 | 341.8 |

Q1 là daily event types/distinct users; Q2 là top product activity/revenue;
Q3 là lookup user557746614/product1801881, có 25 dòng thật trong ngày01/11.

**Định dạng:** JSON gzip flat nhỏ hơn CSV flat **73,66%** về active bytes.
Delta sau OPTIMIZE có active footprint khoảng2,35MiB. Đây là cùng schema logic
9cột C1 + event_date; khi partitioned, event_date nằm trong đường dẫn thay vì
lặp trong từng dòng file. Vì thế chênh lệch bytes bao gồm cả cách lưu cột partition.

**File nhỏ:** OPTIMIZE ZORDER giảm Delta flat **8→1 file**, partitioned **16→2 file**
(giảm87,5% số file ở cả hai). Active bytes của Delta partitioned giảm **40,22%**.
Median trước/sau trên bảng partitioned cải thiện lần lượt **2,65x / 3,57x / 2,52x**
cho Q1/Q2/Q3. Đây là treatment gộp compaction và ZORDER; cohort nhỏ không tách
được lợi ích ZORDER riêng khỏi việc gom file và thay đổi nén/thứ tự dữ liệu.

**Partition pruning:** cả **36/36** trial partitioned có filter event_date và số
file được chọn thấp hơn tổng active: CSV/JSON/Delta trước là **8/16**, Delta sau
là **1/2**. [pruning.csv](pruning.csv) lưu từng lần; [plan Delta sau](plans/delta_partitioned_after--user_product_lookup.txt)
ghi `PartitionFilters` ngày01/11 và pushed filters user/product. File sizes trong
metric là tổng kích thước file được chọn, không phải bytes IO thực đọc.

**Thời gian truy vấn:** CSV/JSON nhanh hơn Delta trong phép đo nhỏ này. Wall clock
bao gồm tạo reader/view, metadata/planning, execute và collect; bản Delta được
pin version. Chi phí metadata/planning và khởi tạo task có thể chi phối khi
dataset nhỏ; chưa đo riêng các thành phần để quy toàn bộ chênh lệch cho một nguyên nhân.
Pruning không bảo đảm mọi truy vấn nhanh hơn: Q1 JSON partitioned306,1ms so với
flat290,6ms dù pruning thực sự xảy ra. Có ba lần/cell, SD được giữ đầy đủ;
không tuyên bố độ khác biệt có ý nghĩa thống kê.

**Dung lượng giữ lại:** physical footprint của Delta flat là **6,231MiB**,
partitioned **6,316MiB**, vì giữ file/version trước OPTIMIZE và metadata.
Hai hàng trước/sau cùng bảng dùng chung physical directory sau bảo trì, nên
không cộng chúng thành hai bản lưu độc lập. Active footprint giảm không đồng
nghĩa dung lượng vật lý đã được thu hồi. Không thực hiện VACUUM/xóa trong E5;
table retention30ngày và version cũ vẫn đọc được khi kiểm tra multiset.

## Chương 7: trình bày minh chứng

1. Mở [verification.json](verification.json):100k rows,8variants,72trials,36proofs,
   mọi multiset/query result khớp. [Kiểm tra chéo](evidence-validation.json) xác nhận
   SHA nguồn/code, matrix, snapshots và plans; driver kết thúc exit0.
   [Unit tests](checks/unit-tests.log):412pass; [runtime/static checks](checks/runtime-static-checks.json)
   xác nhận hai container benchmark/audit exit0. Metadata tạo bảng trong `checks/`
   xác nhận retention thực tế30ngày; [SHA-256 artifacts](artifact-sha256.json) dùng
   kiểm tra tính toàn vẹn của toàn bộ bộ minh chứng.
2. Hiển thị hai biểu đồ publication và bảng phía trên; chỉ ra trade-off giữa
   bytes, file count và wall time, giữ nguyên cả những trường hợp Delta chậm hơn.
3. Mở [plan Q3](plans/delta_partitioned_after--user_product_lookup.txt), chỉ
   `PartitionFilters: ... event_date=2019-11-01`, rồi đối chiếu `selected_files=1`,
   `total_active_files=2` trong [pruning.csv](pruning.csv).
4. Dùng [demo.sql](demo.sql) nếu cần chạy lại truy vấn/EXPLAIN trên đúng version3
   của bảng thử. Đây là truy vấn đọc, không thay đổi dữ liệu. `manifest.json`
   cũng lưu đầy đủ cả ba SQL đã đo và lookup IDs.

Để đo mới, chạy `bash benchmarks/e5_storage.sh` hoặc `make bench-e5`, tạo scope mới.
Không cần chạy lại benchmark để trình bày số liệu đã có. Mẫu first-N hai ngày,
cache JVM/OS/MinIO warm, một máy và ba repetition giới hạn phạm vi suy luận;
không suy rộng thành throughput toàn tháng hay môi trường nhiều host.
