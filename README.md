# DS Churn — Hệ thống Dự đoán Rời bỏ Khách hàng

Hệ thống ML pipeline end-to-end dự đoán khách hàng có nguy cơ rời bỏ (churn) dịch vụ chuyển phát, được thiết kế theo kiến trúc **Modular Monolith** và orchestrate qua **Apache Airflow**.

---

## 1. Tổng quan Bài toán

### 1.1 Mục tiêu
Dự đoán khách hàng nào sẽ ngừng sử dụng dịch vụ trong **H tháng tới** (mặc định H=2), từ đó:
- Phát hiện sớm khách hàng có xu hướng rời bỏ
- Cung cấp danh sách rủi ro hàng tháng cho bộ phận CSKH
- Đưa ra lý do churn (top-3 feature ảnh hưởng) cho mỗi khách hàng

### 1.2 Định nghĩa Churn
- **y = 1** (churn): Khách hàng **không có item lẫn không có doanh thu** (`item_count == 0 AND total_fee == 0`) trong horizon [T+1, T+H]
- **y = 0** (active): Khách hàng có ít nhất 1 item HOẶC doanh thu > 0 trong horizon

### 1.3 Dữ liệu đầu vào
| Nguồn | Mô tả |
|-------|--------|
| `public.cas_customer` | Bảng giao dịch hàng tháng (item_count, total_fee, complaint, satisfaction, delay...) |
| `data_window.cus_feature_*` | Bảng sliding window features đã tính |
| CSKH evaluation files | Danh sách confirmed churners từ bộ phận Chăm sóc Khách hàng |

---

## 2. Kỹ thuật & Phương pháp

### 2.1 Pipeline tổng quan

```
┌─────────────┐    ┌──────────────┐    ┌───────────────────────────────────────┐
│   Ingestion  │───▶│ Feature Gen  │───▶│         Dataset Prep + Modeling       │
│  (ZIP → DB)  │    │ (Sliding Win)│    │ (Walk-forward → Train → Score → Export│
└─────────────┘    └──────────────┘    └───────────────────────────────────────┘
```

### 2.2 Dataset Preparation (7 bước)

| Bước | Module | Kỹ thuật |
|------|--------|----------|
| 1 | **Scope Filter** | Lọc KH đủ điều kiện (active_months ≥ min, loại KH mới/inactive quá lâu) |
| 2 | **Activity Tiering** | Phân tier: `active` / `at_risk` / `dormant` / `new_customer` dựa trên recency + frequency |
| 3 | **EWMA** (7 signals) | Exponentially Weighted Moving Average trên 7 metric: item, revenue, complaint, delay, nodone, order, satisfaction |
| 4 | **Walk-forward W\*** | Tìm window size tối ưu W* bằng walk-forward validation trên nhiều tháng lịch sử |
| 5 | **Leading Prototype** | Xây dựng prototype từ confirmed churners ở T-2 → tính mean (μ) + regularized inverse covariance (Σ⁻¹) → Mahalanobis similarity |
| 6 | **Pseudo-labeling** | Gán nhãn cho tập active: `confirmed` / `pseudo_churn` / `reliable_neg` / `pu_unlabeled` |
| 7 | **Sample Weighting** | PU Learning weights (P(s=1\|y=1) estimation) + Label smoothing per label source |

### 2.3 EWMA — Multi-signal Exponentially Weighted Moving Average

Với mỗi metric prefix (ví dụ: `item`, `revenue`), sử dụng các cột pivot theo thời gian:

```
item_2m_ago → item_1m_ago → item_last    (oldest → newest)
```

Công thức EWMA:
```
ewma(t) = α × value(t) + (1 - α) × ewma(t-1)
```

Output:
- `ewma_{prefix}` — giá trị EWMA tại thời điểm hiện tại
- `delta_ewma_{prefix}` — `ewma_current - ewma_penultimate` (xu hướng tức thời)

### 2.4 Leading Prototype & Pseudo-labeling

```
Confirmed churners (CSKH)
    ↓ Features ở T-2 months
    ↓ Compute μ (mean vector) + Σ⁻¹ (regularized inverse covariance)
    ↓
Similarity Score = Mahalanobis distance(customer, prototype)
    ↓
Pseudo-churn nếu: sim_score > threshold AND delta_ewma < 0 AND item_last < 85% × item_avg
```

### 2.5 PU Learning (Positive-Unlabeled)

Vì chỉ có confirmed churners (positive) và không có ground-truth negative:

```
pu_weight_c = n_confirmed / n_unlabeled   (clamped to min 0.01)
```

Sample weights:
| Label source | Weight | Label (smoothed) |
|-------------|--------|-------------------|
| confirmed | 1.0 | 1.00 |
| pseudo_churn | pu_weight_c | 0.90 |
| reliable_neg | 1.0 | 0.00 |
| pu_unlabeled | pu_weight_c | pu_weight_c |

### 2.6 Modeling (XGBoost)

| Aspect | Detail |
|--------|--------|
| Algorithm | XGBoost (`binary:logistic`) |
| Eval metric | `logloss` + `aucpr` (PR-AUC) |
| Threshold | Optimal F1 threshold (auto-selected) |
| Guardrail | min_f1=0.10, min_pr_auc=0.05 |
| Accept/Reject | New F1 > prev_F1 + ε |

---

## 3. Kiến trúc Hệ thống

### 3.1 Cấu trúc thư mục

```
ds_churn/
├── dags/                           # Airflow DAGs
│   ├── ds_churn_ingest.py          #   Scheduled: scan ZIP → validate
│   ├── ds_churn_features.py        #   Triggered: feature generation
│   ├── ds_churn_pipeline.py        #   Triggered: dataset prep + model v2
│   └── ds_churn_housekeeping.py    #   Scheduled: cleanup old bundles
│
├── src/                            # Source code (pip install -e .)
│   ├── config/                     #   Centralized config (Pydantic-based)
│   ├── shared/                     #   Shared utilities (DB, logging)
│   ├── data/
│   │   ├── preprocessing/          #   Data handling logic (transformations)
│   │   │   └── dataset_prep/       #   10 modules (scope→tier→EWMA→W*→prototype→pseudo→weight→sanity)
│   │   └── validation/             #   Data quality checks (schema validation)
│   ├── features/
│   │   └── engineering/
│   │       └── feature_gen/        #   Sliding window SQL + aggregation
│   ├── modeling/
│   │   ├── config/                 #   ModelConfig (XGBoost hyperparams)
│   │   ├── train/                  #   trainer, evaluator, guardrail
│   │   ├── export/                 #   scorer, risk_table
│   │   ├── common/                 #   artifacts (save/load bundles)
│   │   └── config_store/           #   best_config (accept/reject history)
│   ├── pipelines/
│   │   ├── ingestion/              #   Ingestion orchestration (jobs, ops, sensors, schedules)
│   │   └── monthly/                #   monthly_v2.py & monthly_v2_cli.py (8-step orchestrator)
│   └── monitoring/                 #   Production monitoring
│
├── tests/                          # Unit tests (105 tests)
├── Coding_conventions/             # Project coding standards
├── pyproject.toml                  # Packaging & tool config
├── .env                            # Database credentials (not committed)
└── docker-compose.yaml             # Docker setup
```

### 3.2 Airflow DAG Chain

```
ds_churn_ingest          ds_churn_housekeeping
 0 9 13,23 * *           0 3 * * *
       │
       ▼
ds_churn_features
(Feature Gen)
       │
       ▼
ds_churn_pipeline
(Dataset Prep → Train → Evaluate → Guardrail → Score → Export)
```

### 3.3 Monthly v2 Pipeline — 8 Steps

```
Step 1: Dataset Prep         → DatasetResult (x_train, y_train, w_train, x_eval, y_eval, x_predict)
Step 2: Train XGBoost        → Booster model
Step 3: Evaluate             → F1, PR-AUC, threshold
Step 4: Guardrail            → Pass/Fail (min quality gates)
Step 5: Accept/Reject        → Compare F1 vs previous accepted model
Step 6: Save Bundle          → Pickle model + metadata to disk
Step 7: Score All            → churn_probability + churn_flag for all active customers
Step 8: Export Risk Table    → Insert predictions to PostgreSQL
```

---

## 4. Cài đặt & Triển khai

### 4.1 Prerequisites

- Python ≥ 3.10
- PostgreSQL ≥ 14
- Docker + Docker Compose (cho Airflow)

### 4.2 Development Setup

```bash
# Clone repository
git clone <repo-url> ds_churn
cd ds_churn

# Tạo virtual environment
python -m venv .venv
source .venv/bin/activate       # Linux/Mac
# .venv\Scripts\activate        # Windows

# Install (editable mode + dev tools)
pip install -e ".[dev]"

# Cấu hình database
cp .env.example .env
# Sửa .env với credentials thực tế:
#   PG_USER=...
#   PG_PASSWORD=...
#   PG_HOST=...
#   PG_PORT=5432
#   PG_DBNAME=ds_churn
#   CHURN_MODEL_DIR=/path/to/model/output

# Chạy tests
pytest
```

### 4.3 Kubernetes Deployment (Tiêu chuẩn K8s Local/Production)

Hệ thống được thiết kế bắt buộc triển khai qua **KubernetesPodOperator** trên môi trường K8s bằng Helm. Tuyệt đối không dùng docker-compose cho môi trường chạy Dataset Prep / Modeling vì sẽ vượt quá giới hạn RAM của 1 máy chủ ảo đơn lẻ.

**1. Chuẩn bị K8s Secrets**
Tạo khóa cho kết nối Database (thay vì để public file `.env`):
```bash
kubectl create secret generic churn-db-secret --from-env-file=".env" -n default
```
*(Nếu dùng GitSync, tạo thêm secret `airflow-git-ssh-key`).*

**2. Cài đặt Airflow bằng Helm**
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Sử dụng file override cho local (resources & logs persistence)
.\helm upgrade --install airflow apache-airflow/airflow \
  --namespace default \
  -f infrastructure/helm/airflow/values.yaml \
  -f infrastructure/helm/airflow/values-local.yaml
```

**3. Giám sát & Dashboard (Prometheus/Grafana)**
Mọi tác vụ đào tạo Model bằng XGBoost sẽ được đóng gói thành các K8s Pods riêng biệt. Cần chạy kèm hệ thống Mắt thần để giám sát RAM/CPU.
```bash
# Xem hướng dẫn cấu hình Dashboard tại docs/operations/monitoring_guide.md
kubectl port-forward svc/monitoring-grafana 3000:80 -n monitoring
```

> **Lưu ý Path trên Windows:** Nếu test bằng Docker Desktop, đường dẫn Mount Database Data trong YAML phải đổi thành định dạng Linux Node VM: `/run/desktop/mnt/host/d/...`

### 4.4 Chạy Pipeline thủ công (không qua Airflow)

```bash
# Từ thư mục gốc, với PYTHONPATH=src
export PYTHONPATH=src

# Chạy full pipeline
python -m pipelines.monthly.monthly_v2_cli

# Chỉ chạy dataset_prep (debug)
python -c "
from shared.db import get_engine
from data.preprocessing.dataset_prep.pipeline_config import DatasetPipelineConfig
from data.preprocessing.dataset_prep.run_dataset_pipeline import run_dataset_pipeline

engine = get_engine()
config = DatasetPipelineConfig(horizon_months=2)
result = run_dataset_pipeline(engine, config)
print(f'Train: {len(result.x_train)}, Eval: {len(result.x_eval)}')
"
```

### 4.5 Chạy Tests

```bash
# Tất cả tests
pytest

# Với coverage
pytest --cov=src --cov-report=html

# Chỉ module cụ thể
pytest tests/test_ewma.py -v
pytest tests/test_guardrail.py -v
```

---

## 5. Cấu hình

### 5.1 Dataset Pipeline Config

| Parameter | Default | Mô tả |
|-----------|---------|--------|
| `horizon_months` | 2 | Số tháng horizon dự đoán |
| `w_min` / `w_max` | 3 / 12 | Phạm vi tìm kiếm window size |
| `alpha_ewma` | 0.3 | Smoothing factor cho EWMA |
| `sim_threshold` | 0.7 | Ngưỡng similarity cho pseudo-churn |
| `trend_down_ratio` | 0.85 | Tỷ lệ giảm xu hướng (item_last < ratio × item_avg) |
| `pu_weight_min` | 0.01 | Giá trị tối thiểu cho PU weight |
| `min_prototype_samples` | 10 | Số confirmed churners tối thiểu cho prototype |

### 5.2 Model Config

| Parameter | Default | Mô tả |
|-----------|---------|--------|
| `max_depth` | 6 | Độ sâu cây XGBoost |
| `learning_rate` | 0.05 | Tốc độ học |
| `n_estimators` | 500 | Số boosting rounds tối đa |
| `early_stopping_rounds` | 30 | Early stopping |
| `min_f1` | 0.10 | Guardrail: F1 tối thiểu |
| `min_pr_auc` | 0.05 | Guardrail: PR-AUC tối thiểu |
| `risk_threshold_pct` | 70.0 | Percentile cho ngưỡng risk |

---

## 6. Testing

| Test suite | Tests | Coverage |
|------------|-------|----------|
| `test_config.py` | 9 | PostgresConfig validation |
| `test_ewma.py` | 10 | Multi-signal EWMA, delta penultimate |
| `test_pipeline_config.py` | 11 | DatasetPipelineConfig validation |
| `test_pseudo_labeling.py` | 4 | Pseudo-label assignment |
| `test_sample_weighting.py` | 6 | PU weights, label smoothing |
| `test_sanity_checks.py` | 4 | Dataset sanity validation |
| `test_model_config.py` | 20 | ModelConfig validation + serialization |
| `test_evaluator.py` | 6 | F1 threshold selection |
| `test_guardrail.py` | 17 | Quality gates + accept/reject |
| `test_scorer.py` | 14 | Score stats + reasons generation |
| **Total** | **105** | |

---

## 7. License

Internal use only — proprietary.
