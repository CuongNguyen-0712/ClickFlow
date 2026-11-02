# Diễn giải E3 — 01/11/2026

Chạy từ **22:18:15 đến 22:38:42, Asia/Saigon (UTC+07)**: sáu cấu hình,
**3 lần/cấu hình**, mỗi lần100.000 dòng thật; tổng **1.800.000 dòng đo** và
3.600 dòng warm-up riêng. Nguồn có cùng SHA256, span3.452 giây event-time và
cùng giá trị C1 cho mọi trial. Topic/Delta/checkpoint tách riêng; nguồn và
production sinks không bị sửa hoặc xóa.

Xem [bảng đầy đủ](README.md), [trial CSV](trials.csv), [summary CSV](summary.csv),
[biểu đồ latency](latency.png) và [biểu đồ đánh đổi](tradeoff.png).

**Đọc đúng metric:** hiệu có dấu từ `produced_at` đến batch timestamp
`ingested_at`, chưa gồm Delta commit. Percentile từng trial được tính chính xác;
bảng/biểu đồ là **mean của các percentile**, error bar là sample SD, không phải
percentile pooled hay confidence interval. Tốc độ trong legend là **cấu hình
đích**; actual speed nằm trong CSV và bảng dưới.

| Trigger | Target | n | Actual speed mean ± SD | p95 giây mean ± SD | File nhỏ mean ± SD | Pace miss |
|---:|---:|---:|---:|---:|---:|---:|
| 5s | 100x | 3 | 99,999 ± 0,000 | 6,911 ± 3,478 | 7,00 ± 1,00 | 0 |
| 5s | 1000x | 3 | 999,720 ± 0,277 | 5,758 ± 1,880 | 1,67 ± 0,58 | 0 |
| 10s | 100x | 3 | 80,972 ± 16,973 | 56,138 ± 50,855 | 3,00 ± 1,00 | **2** |
| 10s | 1000x | 3 | 999,902 ± 0,036 | 7,586 ± 2,565 | 1,33 ± 0,58 | 0 |
| 30s | 100x | 3 | 99,999 ± 0,000 | 28,315 ± 0,081 | 2,00 ± 0,00 | 0 |
| 30s | 1000x | 3 | 999,943 ± 0,016 | 14,794 ± 10,000 | 1,00 ± 0,00 | 0 |

Các giá trị nhỏ làm tròn trong bảng; CSV giữ precision gốc.

## Quan sát được

- Ở100x, hai cấu hình có đủ3 lần đạt pace cho thấy đánh đổi: trigger5s sinh
  trung bình7 file, p95 trung bình6,911s; trigger30s sinh2 file, p95 khoảng28,315s.
  Không suy ra kết quả tương tự cho mọi nguồn, phần cứng hoặc lưu lượng.
- Ở1000x, cả chín lần phát đạt target trong ±10%. Cohort chỉ phát khoảng3,452s,
  nên thường vào1–2 micro-batch: trigger5/10/30s lần lượt sinh khoảng1,67/1,33/1,00
  file, p95 khoảng5,758/7,586/14,794s. SD của30s lớn vì ba pha phát khác nhau.
- Tất cả48 file sinh sau warm-up đều <16MiB; warm-up không bị tính vào48.
  Không chạy OPTIMIZE/VACUUM để làm thay đổi file layout trong phép đo.

## Hai outlier được giữ nguyên

**16/18 trial đạt pace mục tiêu.** Không loại hai lần miss khỏi bảng/biểu đồ:

- [trial09](trial-09.json), trigger10s/100x, repetition2: actual75,533x,
  thời gian phát45,702s thay vì34,52s, p95=48,678s; hai batch khoảng50s.
- [trial15](trial-15.json), trigger10s/100x, repetition3: actual67,386x,
  p95=110,311s. Cả hai vẫn commit đủ100.000 dòng, không quarantine/drop.

Vì actual input rate không được giữ đúng ở hai lần này, **không dùng cell
10s/100x để kết luận tác động riêng của trigger ở actual100x**. Đây là kết quả
của cấu hình yêu cầu100x trên hệ thống đang đo, không phải ba lần actual100x.
Không thay số, đổi tên tốc độ hoặc chọn lại trial để làm đẹp biểu đồ.

[Resource context](resource-context.txt) ghi Docker VM12vCPU, khoảng7,558GiB
RAM; Spark dùng4cores, driver1g và hai executor1500m. [Snapshot](resource-snapshot-after-trial09.txt)
được lấy **sau** trial09 cho thấy tải CPU/memory đáng kể, nhưng không đủ để
chứng minh nguyên nhân outlier. Không tự quy kết lỗi cho Kafka/Spark hay phần cứng.

## Kiểm chứng độc lập và giới hạn

[Audit](independent-audit.json) và [script](independent-audit.py) đọc lại toàn bộ
1,8 triệu cặp timestamp từ các Delta version đã lưu, tính NumPy linear
percentiles, parse AddFile JSON trực tiếp sau warm-up và kiểm tra Kafka offsets.
Tất cả18 trial khớp; source/code hashes không đổi trong phép đo. Không có
NULL timestamp, độ trễ âm, duplicate, late/durable drop hay quarantine trong
lượt chính. Max sai lệch pha phát quan sát so với epoch-grid target khoảng50,10ms.

Đây là benchmark cohort cố định trên máy hiện tại, chưa phải steady-state
throughput, latency đến commit, hoặc số liệu toàn bộ ngày Kaggle. Các trial1000x
ngắn nên phase đầu vào có ảnh hưởng lớn; SD từ3 lần chưa phải một CI95.

Hai hạn chế nhỏ của công cụ đã được ghi trong runbook: wrapper chỉ tạo session
mới, resume dùng Spark CLI với stage path cũ; khi enqueue bị BufferError,
`produced_at` vẫn là timestamp trước lần thử đầu nên bao gồm thời gian retry.
**Lượt chính có0 BufferError retries**, nên trường hợp thứ hai không tác động
đến số đo này. Tất cả topic/checkpoint/Delta và artifact được giữ lại.
