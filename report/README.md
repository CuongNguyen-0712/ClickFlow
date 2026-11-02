# Ghép phần báo cáo Storage của TV3

- [Chương2: Data Lake, Delta, Structured Streaming](ch02_data_lake_delta.md):
  mục 2.2–2.4, ghép sau lý thuyết Kafka của TV2.
- [Chương4: thiết kế Storage](ch04_storage.md): mục 4.3 và các tiểu mục,
  gồm raw/bronze/quarantine, C4/C5, quality/dedup, checkpoint, partition,
  maintenance và bảng minh chứng E3/E4b/E5.
- [Tài liệu tham khảo và nguồn số liệu](references_storage.md): dùng chung
  cho hai phần, có link tới tài liệu chính thức và artifact gốc.
- [Hình 4.1 PNG](figures/ch04_storage_flow.png) và [Mermaid source](figures/ch04_storage_flow.mmd).

Nội dung chỉ bao phủ phần TV3 của hai chương. Số mục/hình có thể đổi khi ghép
chương Kafka/kiến trúc chung; giữ liên kết tới nguồn khi xuất PDF. Ngày dữ liệu
2019 khác ngày thực nghiệm01–02/11/2026. Contract C4/C5 còn chờ duyệt nhóm;
không sửa trạng thái đó bằng việc hoàn thành bản viết.

Các bảng E3/E5 ở chương 4 là số liệu đối chiếu thiết kế. Chương6 trình bày đầy đủ
phương pháp, trial, SD, pace miss và giới hạn; chương 7 dùng artifacts demo đã có.
Không chuyển E3 thành latency đến Delta commit, không ghép cohort E4b/E5 thành
một lần đo, không đổi active bytes thành physical bytes. Benchmark gốc được giữ nguyên.

Kiểm tra bản viết: 60 liên kết nội bộ/anchor hợp lệ; 76 giá trị trong bảng E3,
E4b và E5 khớp CSV/JSON gốc sau làm tròn. Hình PNG1800×1100 đã được xem lại.
Review kỹ thuật độc lập không thấy sai lệch quan trọng; đã làm rõ mapping ngày
cho mọi lỗi parse_error/bad_event_time và hai nhánh dữ liệu C4/C5 trên sơ đồ.
