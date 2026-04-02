# Cẩm nang Xử lý Sự cố (Troubleshooting Guide)

> [!NOTE]
> Tài liệu này liệt kê các lỗi thường gặp trong quá trình vận hành pipeline Churn Prediction và cách khắc phục nhanh.

## 1. ETL/Ingestion Pipeline
### Lỗi: Không thể kéo dữ liệu (Connection Refused)
- **Dấu hiệu:** `# TODO(author): Ví dụ DAG IngestionTask bị đỏ (fail).`
- **Nguyên nhân có thể:** `# TODO(author): Ví dụ sai Credentials DB CSKH, hoặc bị chặn Firewall.`
- **Cách xử lý:** `# TODO(author): Check Secrets trên K8s, VPN...`

### Lỗi: Thiếu dữ liệu (Missing Data)
- **Dấu hiệu:** `SchemaMismatch` hoặc `DataQualityError`.
- **Cách xử lý:** `# TODO(author): Báo cho team DBA hoặc bypass check nếu được duyệt.`

## 2. API / Inference
### Lỗi: Pod CrashLoopBackOff (Hết RAM / OOMKilled)
- **Dấu hiệu:** `/health` endpoint báo timeout 502/504 Bad Gateway. Grafana báo Memory Limit Exceeded.
- **Cách xử lý:** `# TODO(author): Nâng Limit/Request trên Helm, Restart Pod.`

### Lỗi: Dự đoán không nhất quán (Model Drift Warning)
- **Dấu hiệu:** Tỉ lệ Churn dự đoán đột ngột chênh lệc 5x so với quá khứ.
- **Cách xử lý:** `# TODO(author): Thu thập data gần nhất, chạy Retraining Pipeline thủ công, verify trước khi đẩy model lên Production.`
