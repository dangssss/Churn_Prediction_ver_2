Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "[*] REBUILD LOCAL KUBERNETES CLUSTER (KIND)" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan

# 1. Download kind.exe if not present
$kind_exe = "kind.exe"
$kind_url = "https://kind.sigs.k8s.io/dl/v0.22.0/kind-windows-amd64"
if (-not (Get-Command kind -ErrorAction SilentlyContinue)) {
    if (-not (Test-Path ".\kind.exe")) {
        Write-Host "`n[*] Downloading Kind tool..." -ForegroundColor Yellow
        Invoke-WebRequest -Uri $kind_url -OutFile ".\kind.exe"
        Write-Host "[+] Downloaded kind.exe" -ForegroundColor Green
    }
    $kind_cmd = ".\kind.exe"
} else {
    $kind_cmd = "kind"
}

# 2. Delete old cluster
Write-Host "`n[-] Deleting old cluster (if any)..." -ForegroundColor Yellow
& $kind_cmd delete cluster --name churn-local-k8s 2>&1 | Out-Null

# 3. Create new cluster
Write-Host "`n[*] Creating new Kubernetes cluster with extraMounts..." -ForegroundColor Yellow
& $kind_cmd create cluster --config infrastructure\kind\kind-config.yaml
if ($LASTEXITCODE -ne 0) {
    Write-Host "[!] Error creating cluster. Please ensure Kubernetes is disabled in Docker Desktop!" -ForegroundColor Red
    Pause
    exit 1
}

# 4. Build Docker image and load into Kind
Write-Host "`n[*] Building Docker Image and loading into Kind Node..." -ForegroundColor Yellow
docker build -t churn_app:v2 -f infrastructure/Dockerfile.app .
& $kind_cmd load docker-image churn_app:v2 --name churn-local-k8s

docker build -t churn_app_airflow:latest -f infrastructure/Dockerfile.airflow .
& $kind_cmd load docker-image churn_app_airflow:latest --name churn-local-k8s

# 5. Setup K8s Secrets & Storage
Write-Host "`n[*] Creating Kubernetes Secrets and PVC..." -ForegroundColor Yellow
$home_dir = $env:USERPROFILE
if (-not (Test-Path "$home_dir\.ssh\id_rsa_airflow_local")) {
    Write-Host "[*] Generating new SSH Key for Airflow GitSync..."
    ssh-keygen -t rsa -b 4096 -f "$home_dir\.ssh\id_rsa_airflow_local" -N '""'
}
kubectl create secret generic airflow-git-ssh-key --from-file=gitSshKey="$home_dir\.ssh\id_rsa_airflow_local" -n default
kubectl create secret generic churn-db-secret --from-env-file=".env" -n default

# Create PVC for logs explicitly due to kind RWO limitation
@"
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: local-logs-pvc
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
"@ | Out-File -FilePath ".\local-logs-pvc.yaml" -Encoding UTF8
kubectl apply -f .\local-logs-pvc.yaml

# 6. Deploy Airflow via Helm
Write-Host "`n[*] Deploying Airflow via Helm..." -ForegroundColor Yellow
if (Test-Path ".\airflow-1.21.0.tgz") {
    Write-Host "[*] Found offline chart airflow-1.21.0.tgz. Installing offline..." -ForegroundColor Yellow
    .\helm upgrade --install airflow .\airflow-1.21.0.tgz --namespace default -f infrastructure/helm/airflow/values.yaml -f infrastructure/helm/airflow/values-local.yaml
} else {
    Write-Host "[*] Installing from online repository..." -ForegroundColor Yellow
    .\helm repo add apache-airflow https://airflow.apache.org
    .\helm repo update
    .\helm upgrade --install airflow apache-airflow/airflow --namespace default -f infrastructure/helm/airflow/values.yaml -f infrastructure/helm/airflow/values-local.yaml
}

Write-Host "`n[+] DONE!" -ForegroundColor Green
Write-Host "Local K8s Cluster created successfully with D: drive mounted." -ForegroundColor Cyan
Write-Host "Please wait a few minutes for the Pods to start. (Use command: kubectl get pods)" -ForegroundColor Cyan
Pause
