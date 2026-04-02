# Hướng dẫn Giám sát (Monitoring Guide)

> [!NOTE]
> Tham khảo thêm các quy tắc chung về Observability tại dự án (Convention 18).

## 1. URL Các hệ thống giám sát
- **Grafana Dashboard:** # TODO(author): Điền Link
- **Prometheus UI:** # TODO(author): Điền Link
- **Airflow UI:** # TODO(author): Điền Link
- **MLflow Tracking:** # TODO(author): Điền Link

## 2. Các chỉ số sống còn (Gold Metrics / RED)
Khi trực hệ thống, người trực cần theo dõi 3 bảng Dashboard quan trọng nhất:
1. **API Mức độ đáp ứng:** `Request Rate`, `Error Rate (5xx)`, `Duration (Latency 95th)`.
2. **Pipeline Sức khỏe:** Tổng số Airflow Tasks thất bại hôm nay.
3. **Data Quality / Model Drift:** Tỉ lệ cảnh báo Outliers (Từ evidently hoặc custom metrics).

## 3. Danh sách Alert (Alerting Rules)
Hệ thống bắn cảnh báo qua Kênh nào?
- # TODO(author): Kênh Slack/Teams? Email? Nếu nhận được Alert `ChurnPipelineFailed` thì phải đọc log ở đâu?
- `ModelDriftDetected` (Mức độ độ ưu tiên: Cảnh báo - Warning).
- `DBConnectionLost` (Mức độ ưu tiên: Khẩn cấp - Critical).
