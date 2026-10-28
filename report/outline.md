# Báo cáo ClickFlow — dàn ý (theo mục 6.1 của đề, ~30 trang)

Mỗi chương một file `report/chNN_*.md`, hình vào `report/figures/`, số liệu lấy từ `benchmarks/results/`. TV4 ghép và xuất PDF ở tuần 6.

| Chương | Nội dung | Người viết | Nguồn số liệu | Jira |
|---|---|---|---|---|
| 1. Giới thiệu | Bài toán, 4 câu hỏi kỹ thuật, phạm vi Sources → Ingestion → Storage, vì sao cần Big Data (Volume/Velocity/Veracity/phân tán) | TV4 Nguyễn Quốc Cường | README, EDA | BIG-55 |
| 2. Cơ sở lý thuyết | Kafka (KRaft, partition, RF, ISR, idempotence); Data Lake, Delta Lake (ACID, time travel, OPTIMIZE/ZORDER); Spark Structured Streaming (checkpoint, watermark) | TV2 Tiến An (Kafka), TV3 Thanh Trúc (Delta/Spark) | — | BIG-53, BIG-54 |
| 3. Dữ liệu và yêu cầu | Nguồn Kaggle, schema C1, kết quả EDA 2019-Nov, các mức Dev/Demo/Full, cách tạo Velocity/Veracity | TV1 Thơ Trần | `notebooks/01_eda.ipynb` | BIG-52 |
| 4. Thiết kế | Kiến trúc, data contract C1–C5, thiết kế topic/producer (ingestion), vùng lake/phân vùng/luật kiểm tra (storage), cấu trúc repo | TV2 (ingestion), TV3 (storage) | README, `contracts/` | BIG-53, BIG-54 |
| 5. Cài đặt | Docker Compose, phiên bản ghim, Makefile, cách chạy, giám sát | TV4 | `docker-compose.yml`, README | BIG-55 |
| 6. Thực nghiệm | E1–E6: biến, cách đo, bảng/biểu đồ, ≥ 3 lần/cấu hình | TV4 tổng hợp; mỗi người viết E của mình | `benchmarks/results/` | BIG-55 |
| 7. Demo | Kịch bản 5–7 phút, ảnh Grafana/Kafka UI/MinIO, kết quả `make verify` | TV4, TV1 (video) | — | BIG-55, BIG-57 |
| 8. Thảo luận | Trả lời 4 câu hỏi bằng số liệu; đánh đổi độ trễ – file nhỏ, acks – throughput; hạn chế 1 máy | cả nhóm | ch. 6 | BIG-56 |
| 9. Kết luận | Kết quả đạt được, hướng mở rộng (Oct 2019, cloud) | cả nhóm | — | BIG-56 |
| Phụ lục | Cấu hình Kafka (TV2), schema C1–C5 (TV3), `contribution.md`, `ai_usage.md` | TV2, TV3, TV4 | `configs/`, `contracts/`, `docs/` | BIG-58, BIG-59 |

Quy ước:
- Hình: `report/figures/<chương>_<tên>.png`, kèm chú thích và nguồn số liệu.
- Số liệu trong báo cáo phải có file gốc trong `benchmarks/results/` hoặc output notebook.
- Viết song song với thực nghiệm từ tuần 5; tuần 6 chỉ ghép, review chéo, xuất PDF.
