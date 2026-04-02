# Hướng dẫn Triển khai Hệ thống (Deployment Guide)

> [!IMPORTANT]
> Tài liệu này là "Nguồn chân lý" (Source of Truth) cho việc thiết lập môi trường Local và triển khai Production (Kubernetes/Helm). Mọi thay đổi về cấu trúc hạ tầng phải được cập nhật ngay tại đây.

## 1. Yêu cầu Hệ thống (Prerequisites)

Dự án yêu cầu các công cụ sau trước khi có thể chạy thử nghiệm hoặc triển khai:
- **Docker & Docker Desktop**: Bản mới nhất (v24+).
- **Kubernetes**: 
  - Local: Bắt buộc phải **Enable Kubernetes** ngay trong cài đặt của Docker Desktop (Vào Settings -> Kubernetes -> Tích "Enable Kubernetes" -> Bấm Apply & Restart). Hệ thống không khuyến khích dùng Minikube.
  - Production: Managed Kubernetes (EKS/GKE).
- **Helm**: v3+ (Windows có thể cài qua `choco install kubernetes-helm`).
- **Kubectl**: Tương thích API server v1.25+.
- **Git**: Đã config SSH key có quyền đọc repository dự án.

---

## 2. Triển khai Local (Local Development Setup)

Dành cho Data Scientist và Data Engineer test pipelines. Có 2 cách chạy Local: Dùng Docker Compose (truyền thống) hoặc chạy bằng K8s Local (giả lập chuẩn Production).

### 2.1. Cách 1: Sử dụng Docker Compose (Nhanh, nhẹ)
Phù hợp để test syntax DAG, test thao tác DB cơ bản.
```bash
cd infrastructure
echo -e "AIRFLOW_UID=$(id -u)" > .env
# Khởi tạo DB & chạy ngầm
docker-compose run --rm airflow-cli airflow db init
docker-compose up -d --build
```
> [!TIP]
> - Truy cập Web UI: `http://localhost:8080` (`airflow`/`airflow`).
> - Tắt: `docker-compose down --volumes --remove-orphans`.

### 2.2. Cách 2: Sử dụng K8s Local (Docker Desktop) - KHUYÊN DÙNG
Đây là cách test chuẩn xác nhất vì nó mô phỏng 1:1 luồng `KubernetesPodOperator` trên Production (1 Task - 1 Container).

**Bước 1: Build Docker Images chuẩn Production**
Tại thư mục gốc dự án:
```bash
# 1. Build image ứng dụng cốt lõi (chứa code Machine Learning model, pipeline)
docker build -t churn_app:latest -f infrastructure/Dockerfile.app .

# 2. Build image tuỳ chỉnh của Airflow (cài sẵn provider K8s)
docker build -t churn_app_airflow:latest -f infrastructure/Dockerfile.airflow .
```

**Bước 2: Deploy Airflow qua Helm**
Sử dụng 2 file config: `values.yaml` (chuẩn) và `values-local.yaml` (ghi đè resource cho nhẹ máy).
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm upgrade --install airflow apache-airflow/airflow \
  --namespace default \
  -f infrastructure/helm/airflow/values.yaml \
  -f infrastructure/helm/airflow/values-local.yaml
```

**Bước 3: Truy cập và Theo dõi**
```bash
# Ánh xạ cổng để truy cập UI ở http://localhost:8080 (Tài khoản: admin / admin)
kubectl port-forward svc/airflow-webserver 8080:8080 -n default
```
Khi bạn bật DAG (vd: `ds_churn_pipeline`), K8s sẽ tự động sinh ra các Pod độc lập (tên dạng `churn-pipeline-v2-pod-xxxxx`) để chạy task và tự hủy khi xong. Giám sát bằng: `kubectl get pods --watch -n default`.

---

## 3. Triển khai K8s Production (Production K8s Setup)

Trên Production, luồng triển khai gần giống K8s Local nhưng yêu cầu khắt khe hơn về bảo mật và Resource HA.

### 3.1. Build & Push Image
Push 2 images `churn_app` và `churn_app_airflow` lên Private Container Registry (ECR/ACR/GitLab Registry).

### 3.2. Setup Secret cho Git-Sync (Bắt buộc)
Airflow Prod sử dụng `git-sync` để tự động kéo code DAGs mới từ repo Git mà không cần build lại image Airflow.
```bash
kubectl create secret generic airflow-git-ssh-key \
  --from-file=gitSshKey=/path/to/your/private_ssh_key \
  -n churn-prod
```

### 3.3. Cài đặt bằng Helm
```bash
helm upgrade --install airflow apache-airflow/airflow \
  --namespace churn-prod --create-namespace \
  -f infrastructure/helm/airflow/values.yaml
```
*(Chỉ dùng `values.yaml`, tuyệt đối bỏ `values-local.yaml` ra khỏi lệnh này để giữ cấu hình HA 2 Replicas của Scheduler).*

---

## 4. Giám sát & Khắc phục Sự cố (Observability)

- **Lỗi `git-sync`:** Pod báo `Init:CrashLoopBackOff`, kiểm tra lại SSH Key Secret có quyền đọc repo không.
- **Tiết kiệm tài nguyên:** Các task nặng chạy qua `KubernetesPodOperator` (dùng image `churn_app`) sẽ tự giải phóng Node Memory ngay sau khi hoàn tất.
- Tham khảo thêm Alert và Dashboard tại `docs/operations/monitoring_guide.md`.
