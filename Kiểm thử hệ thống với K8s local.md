# Hướng dẫn Kiểm thử Hệ thống với K8s Local

Để hệ thống hoàn chỉnh chạy được trên máy cá nhân theo thiết kế "1 Task - 1 Container", bạn cần thực hiện tuần tự các bước sau đây:

## Bước 1: Kích hoạt Kubernetes trên Docker Desktop
> [!IMPORTANT]
> Đây là bước sống còn vì chúng ta không dùng Minikube.
1. Mở **Docker Desktop**.
2. Bấm vào biểu tượng bánh răng **Settings** ở góc trên bên phải.
3. Chọn tab **Kubernetes** bên trái.
4. Tích vào ô **Enable Kubernetes** và bấm **Apply & Restart**.
5. Đợi đến khi biểu tượng Kubernetes ở góc dưới bên trái chuyển sang màu xanh lá cây.

## Bước 2: Build các Docker Image chuẩn Production
Mở terminal (như PowerShell hoặc CMD) tại thư mục dự án `d:\Churn_Prediction_Product` và build 2 image:

```bash
# 1. Build image ứng dụng cốt lõi (chứa pipeline chạy ML)
docker build -t churn_app:latest -f infrastructure/Dockerfile.app .

# 2. Build image tuỳ chỉnh của Airflow (cài sẵn thư viện K8s)
docker build -t churn_app_airflow:latest -f infrastructure/Dockerfile.airflow .
```

## Bước 3: Deploy Airflow thông qua Helm
> [!NOTE]
> Để dùng Helm, bạn có thể cần [cài đặt Helm trên Windows](https://helm.sh/docs/intro/install/) nếu chưa có. Thường có thể dùng lệnh `choco install kubernetes-helm`.

Thêm Airflow Repo vào Helm và chạy cài đặt:
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install/upgrade
helm upgrade --install airflow apache-airflow/airflow \
  --namespace default \
  --values infrastructure/helm/airflow/values.yaml \
  --values infrastructure/helm/airflow/values-local.yaml
```

## Bước 4: Chạy thử và xác minh
1. **Forward port (Ánh xạ cổng)** để truy cập giao diện Airflow:
   ```bash
   kubectl port-forward svc/airflow-webserver 8080:8080 -n default
   ```
2. Mở trình duyệt vào `http://localhost:8080` (tài khoản mặc định thường là `admin` / `admin`).
3. Bật DAG `ds_churn_pipeline`. 
4. Quan sát log trên Airflow UI hoặc dùng lệnh để theo dõi pod mới sinh ra khi task chạy:
   ```bash
   kubectl get pods --watch -n default
   ```
   Bạn sẽ thấy một Pod có tên dạng `churn-pipeline-v2-pod-xxxxx` được sinh ra tự động để chạy logic và tự huỷ sau khi hoàn tất.

> [!TIP]
> Việc dùng `KubernetesPodOperator` là giải pháp Clean nhất cho scale lớn vì nó không trói buộc môi trường code ML với môi trường Airflow worker, đáp ứng hoàn toàn yêu cầu tách rời container của bạn.
