# Benchmark results

| Thí nghiệm | Cohort | Số lần | Bảng và biểu đồ | Kiểm chứng |
|---|---|---:|---|---|
| E3 — 01/11/2026 | Mẫu thật100k/lần, trigger5/10/30s ×100/1000x | 18, mỗi cell3 | [Bảng](e3-3eb2dde82e154bf09283424977d28cf6/README.md), [latency](e3-3eb2dde82e154bf09283424977d28cf6/latency.png), [tradeoff](e3-3eb2dde82e154bf09283424977d28cf6/tradeoff.png) | [Audit1,8M dòng](e3-3eb2dde82e154bf09283424977d28cf6/independent-audit.json) |

[Phân tích E3](e3-3eb2dde82e154bf09283424977d28cf6/ANALYSIS.md) ghi rõ hai
pacing miss ở10s/100x, biến thiên giữa lần chạy và định nghĩa batch-start latency.
Giữ nguyên outlier; không coi configured speed là actual speed khi không đạt target.
