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

## 2. Kỹ thuật & Phương pháp (Luồng Xử lý & Prototype)

### 2.1 Pipeline Tổng Quan (System Flow)

Dưới đây là sơ đồ luồng dữ liệu toàn hệ thống từ khâu kéo dữ liệu (Ingestion) qua xử lý (Window Aggregation), tính toán đặc trưng (Dataset Prep) đến dự đoán và xuất kết quả:

```mermaid
flowchart TD
    %% 1. Ingestion
    subgraph Kéo_dữ_liệu ["1. Data Ingestion"]
        ZIP["Files ZIP (Giao dịch hàng tháng)"] --> |Giải nén & Validate| DB_Raw[("DB: public.cas_customer\n(Raw Transactions)")]
    end

    %% 2. Sliding Window Feature Gen
    subgraph Feature_Gen ["2. Sliding Window Feature Generation"]
        DB_Raw --> |Nhóm theo tháng (Từ T-W đến T)| Aggregate["Tính toán Aggregation:\n- item_sum\n- revenue_sum\n- delay_sum,..."]
        Aggregate --> Pivot["Xoay chiều (Pivot) thời gian:\nitem_last, item_1m_ago,..."]
        Pivot --> DB_Win[("DB: data_window.cus_feature_{W}m\n(Các bảng Window size W)")]
    end

    %% 3. Dataset Prep
    subgraph Dataset_Prep ["3. Dataset Transformation & Preparation"]
        DB_Win --> Scope["Lọc Scope KH"]
        Scope --> EWMA["Tính tín hiệu EWMA & Delta EWMA"]
        EWMA --> Prototype["Tính Leading Prototype (μ, Σ⁻¹)"]
        Prototype --> PseudoLabel["Gán nhãn Pseudo (Mahalanobis Distance)"]
        PseudoLabel --> PUWeight["Tính Trọng số PU Learning (P(s=1|y=1))"]
        PUWeight --> DatasetResult[["Dataset_Result\n(Train, Eval, Predict)"]]
    end

    %% 4. Modeling & Export
    subgraph Modeling ["4. Training & Export"]
        DatasetResult --> Train["Train mô hình XGBoost"]
        DatasetResult --> Eval["Tính Evaluate Metrics\n(Cắt ngưỡng Threshold ưu tiên F2)"]
        Train -.-> Eval
        Eval --> Guardrail{"Quét Guardrail\n(F2 > min_f2,\nROC-AUC > min_roc_auc)"}
        Guardrail -- Pass --> ScoreAll["Score KH Active\n(Tính Probability & Reasons)"]
        Guardrail -- Fail --> Stop((Dừng Pipeline))
        ScoreAll --> Export[("DB: cas_risk_prediction\n(Dữ liệu rủi ro cho CSKH)")]
    end

    Kéo_dữ_liệu --> Feature_Gen
    Feature_Gen --> Dataset_Prep
    Dataset_Prep --> Modeling
```

### 2.2 Công thức Xử lý Sliding Window ($W$ tháng)

Với mỗi khách hàng tại thời điểm quan sát $T$, dữ liệu được tổng hợp lùi về quá khứ qua $W$ tháng. Hệ thống tổng hợp các thông số như `item_sum`, `revenue_sum`, `delay_sum`, `satisfaction_avg`, v.v.

Ký hiệu $X_{t}$ là giá trị của metrics tại tháng $t$. Các cột feature được trải phẳng (pivot) thành các mốc thời gian tương đối:
- **Tháng hiện tại**: $X_{last} = X_{T}$
- **Tháng trước đó**: $X_{1m\_ago} = X_{T-1}$
- **...**
- **Tháng cũ nhất trong Window**: $X_{(W-1)m\_ago} = X_{T-W+1}$

### 2.3 Công thức EWMA (Exponentially Weighted Moving Average)

Để nắm bắt xu hướng thay đổi hành vi dài hạn (có trượt trọng số ưu tiên dữ liệu gần), hệ thống áp dụng EWMA cho 7 nhóm signals:

$$ EWMA_t = \alpha \cdot X_t + (1 - \alpha) \cdot EWMA_{t-1} $$

Ngoài ra, hệ thống trích xuất động lượng thay đổi biểu hiện rủi ro ngắn hạn (Delta Trend) bằng cách trừ hai kỳ EWMA gần nhất:

$$ \Delta EWMA = EWMA_{last} - EWMA_{penultimate} $$

Output features:
- `ewma_{prefix}`: Thể hiện quỹ đạo xu hướng đã làm mượt.
- `delta_ewma_{prefix}`: Ghi nhận sự sụt giảm/tăng tốc ($<0$ mang ý nghĩa cảnh báo sụt giảm hoạt động).

### 2.4 Tính toán Leading Prototype & Pseudo-labeling

Do giới hạn về nhãn thực tế, hệ thống trích xuất đặc trưng của tập khách hàng đã rời bỏ (Confirmed Churners) tại mốc thời gian T-2 tháng để xây dựng **Prototype**:

1. **Center Vector (Trọng tâm)** $\mu$:
$$ \mu_j = \frac{1}{N_{churn}} \sum_{i=1}^{N_{churn}} x_{i,j} $$

2. **Inverse Covariance (Ma trận hiệp phương sai nghịch đảo)** $\Sigma^{-1}$:
Sử dụng Covariance có Regularization (LW Shrinkage hoặc Ledoit-Wolf) để bù đắp biểu hiện nhiễu (noise) lấn át các features quan trọng.

3. **Mahalanobis Distance ($D_M$)**: Tính khoảng cách đặc trưng phân phối của mọi khách hàng active $x$ so với Prototype $\mu$:
$$ D_M(x) = \sqrt{(x - \mu)^T \Sigma^{-1} (x - \mu)} $$

4. **Biến đổi thành Similarity Score**:
Biến đổi nghịch đảo khoảng cách thành điểm Similarity để đối chiếu với ngưỡng (`sim_threshold`). Khách hàng có cấu trúc hành vi (distance) gần nhóm Churn nhất + có Tín hiệu rớt số (`delta_ewma < 0`) $\rightarrow$ Gán nhãn `pseudo_churn`.

### 2.5 Ước lượng Trọng số PU Learning (Positive-Unlabeled)

Theo định lý Elkan-Noto cho bài toán PU Learning, xác suất để một mẫu Positive ($y=1$) thực sự được dán nhãn (labeled $s=1$) được coi là hằng số:
$$ c = P(s=1|y=1) $$
Vì tập Negative không có ground-truth thực sự, ta ước lượng hằng số $c$ theo tỷ lệ:
$$ c = \frac{N_{confirmed}}{N_{unlabeled}} $$
*(Được kẹp giới hạn clamp tại giá trị min là `0.01`)*

Sau đó, tiến hành thiết lập cơ chế đánh trọng số mẫu (Sample Weights $w_i$) và nhãn mềm (Label Smoothing) áp dụng trực tiếp cho mô hình XGBoost:

| Nguồn Nhãn (Label Source) | Trọng số ($w_i$) | Nhãn mục tiêu ($y_i$) |
|-----------------------|----------------|---------------------|
| `confirmed` (Rời bỏ thật)| $1.0$ | $1.00$ |
| `pseudo_churn` (Giống rời bỏ)| $c$ | $0.90$ |
| `reliable_neg` (Đang phát triển)| $1.0$ | $0.00$ |
| `pu_unlabeled` (Chưa rõ)| $c$ | $c$ |

### 2.6 Modeling (XGBoost) & Phương pháp tính toán đánh giá

| Thuộc tính | Chi tiết |
|--------|--------|
| Algorithm | XGBoost (`binary:logistic`) |
| Eval metric | `logloss` + `auc` (ROC-AUC) |
| Threshold Selection | Cắt ngưỡng (threshold) phân loại tối ưu dựa trên đường cong Precision-Recall. Lựa chọn threshold sao cho **tối đa hóa F2-Score (β=2.0)**. Việc này ưu tiên recall cao hơn precision, đảm bảo mô hình thà dự đoán nhầm còn hơn bỏ sót KH rủi ro. |
| Guardrail | `min_f2=0.10`, `min_roc_auc=0.05` |
| Accept/Reject | Chấp nhận mô hình mới nếu `New F2 > prev_F2_accepted + ε` |

---

## 3. Kiến trúc Hệ thống

### 3.1 Cấu trúc thư mục

```text
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
│   │   ├── ingestion/              #   Data extraction and loading logic
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
│   │   ├── ingestion/              #   Ingestion orchestration schedules
│   │   └── monthly/                #   monthly_v2.py & monthly_v2_cli.py (8-step orchestrator)
│   └── monitoring/                 #   Production monitoring
│
├── infrastructure/                 # Deployment configs
│   └── docker-compose.yaml         # Docker setup
│
├── data/                           # Data storage (mount data volumes)
├── logs/                           # Runtime logs
├── tests/                          # Unit tests (105 tests)
├── Coding_conventions/             # Project coding standards
├── requirements.txt                # Python dependencies
├── pyproject.toml                  # Packaging & tool config
└── .env                            # Database credentials (not committed)
```

### 3.2 Logic Xử lý Pipeline (Monthly v2)

Hệ thống xử lý logic dự đoán hàng tháng qua một chu trình 8 bước tuần tự chặt chẽ, tách biệt rạch ròi quá trình Train và Inference:

1. **Dataset Prep**: 
   - Chạy 7 bước xử lý dữ liệu (đã nêu ở mục 2.2). 
   - Kết xuất `DatasetResult` cấu trúc chặt chẽ gồm: `x_train, y_train, w_train` (dữ liệu huấn luyện kèm sample weights), `x_eval, y_eval` (dữ liệu khách hàng đã rời bỏ thực tế để đánh giá), và `x_predict` (tập khách hàng đang hoạt động cần đánh giá rủi ro).

2. **Train XGBoost**:
   - Huấn luyện XGBoost Booster trên tập train dựa trên cấu hình siêu tham số (Hyperparameters configuration).
   - Truy xuất giá trị feature importance nội tại sau huấn luyện.

3. **Evaluate (Đánh giá & Chọn ngưỡng)**:
   - Inference xác suất (probability) trên tập evaluate.
   - Tìm ra mức `threshold` quyết định phân loại (0 hay 1) bằng cách quét dải Threshold để tối đa hoá điểm **F2-score (beta=2.0)** nhằm ưu tiên Recall theo quy chiếu nghiệp vụ.
   - Xuất các độ đo: `f2`, `roc_auc`, `precision`, `recall`.

4. **Guardrail Check (Chốt chặn chất lượng)**:
   - Đối chiếu metrics với ngưỡng tối thiểu: Phải thỏa mãn `F2 >= min_f2` VÀ `ROC-AUC >= min_roc_auc`.
   - Nếu trượt, pipeline lập tức báo lỗi và dừng toàn bộ (Tránh rủi ro xả rác xuống CSDL nghiệp vụ).

5. **Accept/Reject (Lọc model)**:
   - Truy xuất model F2-score tốt nhất từng được nghiệm thu (ở các lần chạy trước).
   - So sánh cải thiện (Improvement check): Mô hình chỉ được `Accepted` nếu $F2_{new} > F2_{prev\_accepted} + \epsilon$.
   - Pipeline vẫn ghi nhận lịch sử vào Data Store (`model_best_config`) phục vụ tracing.

6. **Save Bundle**:
   - Chỉ diễn ra nếu model được Accept. Tiến hành serialize (bằng `joblib`/`pickle`) cấu hình, model booster, meta features, threshold vào bundle tĩnh để tái lập inference trong tương lai.

7. **Score All**:
   - Tải pipeline model (Mới nếu vừa được accept, hoặc fallback lại model cũ nhất định nếu reject).
   - Dự đoán `churn_probability` cho tập `x_predict`. 
   - Áp mức `threshold` đã lưu để sinh cờ `churn_flag` (Rủi ro: 1, An toàn: 0).
   - **Xác định lý do rời bỏ (Compute Reasons)**: Giải cấu trúc dự đoán để chọn top 3 features là nguyên nhân thúc đẩy điểm rủi ro lớn nhất cho riêng từng khách hàng.

8. **Export Risk Table**:
   - Ghi kết quả vào Database PostgreSQL (`cas_risk_prediction`) theo Schema đã định để bảng điều khiển bộ phận CSKH có khả năng đọc.

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

### 4.3 Docker Deployment (Production)

```yaml
# docker-compose.yaml key volumes:
volumes:
  - ../src:/churn_source/src       # Source code
  - ../dags:/opt/airflow/dags      # DAG files
  - /data:/churn_data              # Data directories
environment:
  - PYTHONPATH=/churn_source/src
  - CHURN_MODEL_DIR=/churn_data/models
```

```bash
# Start Airflow
docker compose up -d

# Verify DAGs
docker exec airflow-worker airflow dags list | grep ds_churn
```

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
| `min_f2` | 0.10 | Guardrail: Điểm F2 tối thiểu (ưu tiên Recall) |
| `min_roc_auc` | 0.05 | Guardrail: ROC-AUC tối thiểu |
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
