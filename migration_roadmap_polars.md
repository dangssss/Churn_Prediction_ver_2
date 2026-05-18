# Migration Roadmap: Pandas → Polars + sklearn set_output + XGBoost native

**Giải pháp:** Solution 2 — Polars toàn bộ pipeline, sklearn v1.4+ `set_output("polars")`, XGBoost native polars input  
**Tổng số file cần đụng:** 48 src files + 11 test files  
**Thời gian ước tính:** 7–9 tuần  
**Nguyên tắc:** Migrate từng phase, test so sánh output pandas vs polars trước khi xóa pandas

---

## Quy ước xuyên suốt lộ trình

```
🟢 Dễ — chỉ đổi API call
🟡 Trung bình — cần refactor logic
🔴 Khó — cần thiết kế lại
✅ Done
⚠️ Cần test kỹ
```

---

## Phase 0 — Setup & Infrastructure (Tuần 1, ngày 1–2)

### 0.1 Cập nhật `requirements.txt`

```diff
- pandas==2.2.3
- numpy==1.26.4
+ polars>=1.0.0
+ connectorx>=0.3.3          # thay pd.read_sql — nhanh hơn 5-10x
+ pyarrow>=15.0.0             # polars dùng arrow internally
+ numpy>=1.26.4               # vẫn giữ cho ewma numpy ops
  psycopg2-binary==2.9.9
  sqlalchemy
- scikit-learn==1.7.2
+ scikit-learn>=1.4.0         # bắt buộc để có set_output("polars")
+ xgboost>=2.0.0              # bắt buộc để có native polars input
  joblib
  matplotlib>=3.8
  python-dotenv==1.0.1
  python-dateutil
  apscheduler==3.10.4
  pydantic==2.5.0
  tenacity==8.2.3
```

### 0.2 Tạo helper `src/shared/polars_db.py` — centralize DB reading

```python
"""Centralized Polars-native database reader (replaces pd.read_sql everywhere)."""
from __future__ import annotations
import polars as pl
from sqlalchemy.engine import Engine
from sqlalchemy import text


def read_sql(
    query: str,
    engine: Engine,
    params: dict | None = None,
) -> pl.DataFrame:
    """Drop-in replacement cho pd.read_sql(), trả về pl.DataFrame.

    Dùng connectorx nếu có connection string, fallback về SQLAlchemy mapping.
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        rows = result.mappings().all()
        if not rows:
            # Trả về empty DataFrame với đúng schema
            cols = list(result.keys())
            return pl.DataFrame({c: [] for c in cols})
        return pl.DataFrame([dict(r) for r in rows])
```

> **Lý do tạo helper:** 14 chỗ dùng `pd.read_sql` trong codebase. Nếu sau này muốn đổi
> sang connectorx hay `pl.read_database_uri`, chỉ sửa 1 file.

### 0.3 Checklist trước khi bắt đầu

- [ ] Chạy toàn bộ test suite hiện tại, ghi lại baseline pass/fail
- [ ] Pin version polars, sklearn, xgboost vào requirements
- [ ] Tạo branch `feature/polars-migration`
- [ ] Setup CI chạy test tự động trên branch

---

## Phase 1 — I/O Layer: Thay toàn bộ `pd.read_sql` (Tuần 1–2, ngày 3–10)

> **Mục tiêu:** Sau phase này, toàn bộ data đọc từ DB/file đều là `pl.DataFrame`.
> Đây là phase ít rủi ro nhất và cho speedup ngay lập tức.

### 1.1 🟢 `src/features/engineering/feature_gen/run_feature_generation.py`

**Thay đổi:**
```python
# TRƯỚC
import pandas as pd
df = pd.read_sql(bccp_sql, engine)
cas_df = pd.read_sql(cas_sql, engine)
cas_max = _to_date(cas_df.iloc[0, 0])

# SAU
import polars as pl
from shared.polars_db import read_sql
df = read_sql(bccp_sql, engine)
cas_df = read_sql(cas_sql, engine)
cas_max = _to_date(cas_df[0, 0])   # polars: df[row, col]
```

### 1.2 🟢 `src/features/engineering/feature_gen/static_aggregation.py`

1 dòng pandas duy nhất — thay import và dùng `read_sql` helper.

### 1.3 🟢 `src/data/preprocessing/dataset_prep/scope_filter.py`

```python
# TRƯỚC
df = pd.read_sql(sql, conn, params={"min_orders": min_lifetime_orders})
cskh_df = pd.read_csv(path)

# SAU
df = read_sql(sql, engine, params={"min_orders": min_lifetime_orders})
cskh_df = pl.read_csv(path)
```

### 1.4 🟢 `src/data/preprocessing/dataset_prep/cskh_loader.py`

```python
# TRƯỚC
df = pd.read_excel(file_path)
df = pd.read_csv(file_path)
df = pd.read_sql(sql, conn)

# SAU
df = pl.read_excel(file_path)     # polars hỗ trợ xlsx natively từ v0.19
df = pl.read_csv(file_path)
df = read_sql(sql, engine)
```

### 1.5 🟢 `src/data/preprocessing/dataset_prep/activity_tiering.py` *(I/O part)*

```python
# TRƯỚC
tbl_df = pd.read_sql(sql, conn)
result = pd.read_sql(sql_fallback, conn)
t_obs = pd.Timestamp(result.iloc[0, 0])

# SAU
tbl_df = read_sql(sql, engine)
result = read_sql(sql_fallback, engine)
t_obs = pd.Timestamp(result[0, 0])   # giữ pd.Timestamp nếu downstream cần
```

### 1.6 🟢 `src/data/preprocessing/dataset_prep/label_construction.py` *(I/O part)*

```python
# TRƯỚC
df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
label_df = pd.read_sql(query, conn)

# SAU
df = read_sql(f"SELECT * FROM {table_name}", engine)
label_df = read_sql(query, engine)
```

### 1.7 🟢 `src/monitoring/model_quality/monitoring/score.py`

```python
# TRƯỚC
df_hist = pd.read_sql(q_hist, engine, params={"h": int(horizon), "n": int(anomaly_window)})

# SAU
df_hist = read_sql(q_hist, engine, params={"h": int(horizon), "n": int(anomaly_window)})
```

### 1.8 🟢 `src/monitoring/model_quality/monitoring/backtest.py`

```python
# TRƯỚC
if pd.read_sql(q_exists, engine, params={"t": hist_tbl}).empty:
df_list = pd.read_sql(q_list, engine, params={"pred_month": int(pred_month)})

# SAU
if len(read_sql(q_exists, engine, params={"t": hist_tbl})) == 0:
df_list = read_sql(q_list, engine, params={"pred_month": int(pred_month)})
```

### 1.9 🟢 `src/modeling/config_store/best_config.py`

```python
# TRƯỚC
df = pd.read_sql(q, engine, params={"h": horizon})
return df.iloc[0].to_dict()
return None if df.empty else df.iloc[0].to_dict()

# SAU
df = read_sql(q, engine, params={"h": horizon})
return df.row(0, named=True)
return None if len(df) == 0 else df.row(0, named=True)
```

### 1.10 🟢 `src/data/eda/run_eda.py` *(I/O part)*

```python
# TRƯỚC
df = pd.read_sql(f"SELECT * FROM {table}", conn)

# SAU
df = read_sql(f"SELECT * FROM {table}", engine)
```

### ✅ Checklist Phase 1

- [ ] `read_sql` helper hoạt động với PostgreSQL + params
- [ ] Tất cả `pd.read_sql` đã được thay
- [ ] `pl.read_csv` / `pl.read_excel` đọc đúng schema
- [ ] Empty DataFrame trả về đúng schema (không lỗi downstream)
- [ ] Chạy test `test_feature_gen.py`, `test_ingest_*` — pass

---

## Phase 2 — Core Data Transformations (Tuần 2–3, ngày 11–18)

> **Mục tiêu:** Migrate logic transformation trong preprocessing pipeline.
> Sau phase này, `ewma`, `leading_prototype`, `label_construction`, `activity_tiering` 
> chạy hoàn toàn trên `pl.DataFrame`.

### 2.1 🟢 `src/data/preprocessing/dataset_prep/ewma.py`

File này hầu như không dùng pandas — chỉ `df.copy()`, `.fillna(0).values`, add columns.

```python
# TRƯỚC
result = df.copy()
monthly_data = result[monthly_cols].fillna(0).values   # → numpy (N, T)
result[f"ewma_{prefix}"] = ewma_all[:, -1]

# SAU
result = df.clone()
monthly_data = result.select(monthly_cols).fill_null(0).to_numpy()  # → numpy
result = result.with_columns(pl.Series(f"ewma_{prefix}", ewma_all[:, -1]))
```

> `_vectorized_ewma_series` dùng numpy thuần — **không đổi gì**.

### 2.2 🟡 `src/data/preprocessing/dataset_prep/leading_prototype.py`

```python
# TRƯỚC — trả về pd.Series với index để caller align
return pd.Series(0.0, index=df.index)
return pd.Series(scores, index=df.index)

# SAU — trả về pl.Series, caller dùng with_columns để gán
return pl.Series("sim_score", [0.0] * len(df))
return pl.Series("sim_score", scores)
```

**Lưu ý:** Caller (`pseudo_labeling.py`) phải được update đồng thời ở Phase 3.

### 2.3 🟡 `src/data/preprocessing/dataset_prep/label_construction.py`

```python
# TRƯỚC
all_rows: list[pd.DataFrame] = []
# ...
result = pd.concat(all_rows, ignore_index=True)
feat_df = feat_df.merge(label_df, on=["cms_code_enc", "window_end"], how="left")
feat_df["item_in_horizon"] = feat_df["item_in_horizon"].fillna(0)
return pd.DataFrame()

# SAU
all_rows: list[pl.DataFrame] = []
# ...
result = pl.concat(all_rows)
feat_df = feat_df.join(label_df, on=["cms_code_enc", "window_end"], how="left")
feat_df = feat_df.with_columns(pl.col("item_in_horizon").fill_null(0))
return pl.DataFrame()
```

### 2.4 🟡 `src/data/preprocessing/dataset_prep/activity_tiering.py`

```python
# TRƯỚC
result = working_df.merge(recency_df, on="cms_code_enc", how="left")
result["recency_days"] = result["recency_days"].fillna(9999).astype(int)

# SAU
result = working_df.join(recency_df, on="cms_code_enc", how="left")
result = result.with_columns(
    pl.col("recency_days").fill_null(9999).cast(pl.Int32)
)
```

### 2.5 🟡 `src/data/preprocessing/dataset_prep/scope_filter.py` *(transformation part)*

```python
# TRƯỚC — boolean filtering trả về pandas
def filter_active_scope(...) -> pd.DataFrame:
    df = pd.read_sql(...)
    return df[df["order_count"] >= min_orders]

# SAU
def filter_active_scope(...) -> pl.DataFrame:
    df = read_sql(...)
    return df.filter(pl.col("order_count") >= min_orders)
```

### ✅ Checklist Phase 2

- [ ] `ewma.py` — output shape và values giống pandas (test với `test_ewma.py`)
- [ ] `leading_prototype.py` — `compute_similarity` trả về `pl.Series` đúng length
- [ ] `label_construction.py` — `pd.concat` → `pl.concat` giữ đúng schema
- [ ] `activity_tiering.py` — `.join()` left join kết quả giống `.merge()`
- [ ] Không còn `pd.DataFrame()` empty return — dùng `pl.DataFrame()`

---

## Phase 3 — Business Logic (Tuần 3–4, ngày 19–26) ⚠️

> **Phase quan trọng nhất.** Chứa các pattern phức tạp nhất: `.loc[]` conditional
> assignment, `pd.Series(val, index=df.index)`, `pd.concat(axis=1)`.
> **Bắt buộc viết comparison test trước khi merge.**

### 3.1 🔴 `src/data/preprocessing/dataset_prep/pseudo_labeling.py`

Đây là file có nhiều `.loc[mask, col] = val` nhất. Polars immutable → dùng `when/then` chain.

```python
# TRƯỚC — 3 lần gán conditional, thứ tự sau đè trước
result["label_source"] = "pu_unlabeled"
result.loc[pseudo_churn, "label_source"] = "pseudo_churn"
result.loc[reliable_neg, "label_source"] = "reliable_neg"
result.loc[result["cms_code_enc"].isin(eval_ids), "label_source"] = "confirmed"

# SAU — when/then chain, confirmed có priority cao nhất (để trên cùng)
# ⚠️ Thứ tự NGƯỢC với pandas: first match wins trong polars
result = result.with_columns(
    pl.when(pl.col("cms_code_enc").is_in(list(eval_ids)))
      .then(pl.lit("confirmed"))
      .when(reliable_neg)
      .then(pl.lit("reliable_neg"))
      .when(pseudo_churn)
      .then(pl.lit("pseudo_churn"))
      .otherwise(pl.lit("pu_unlabeled"))
      .alias("label_source")
)
```

**Đồng thời update caller của `compute_similarity`:**
```python
# TRƯỚC
result["sim_score"] = compute_similarity(result, prototype)

# SAU — compute_similarity trả về pl.Series
result = result.with_columns(
    compute_similarity(result, prototype).alias("sim_score")
)
```

**Đồng thời update masks:**
```python
# TRƯỚC — pandas boolean Series
sim_high = result["sim_score"] > sim_threshold

# SAU — polars expression (dùng trong when/then trực tiếp)
sim_high = pl.col("sim_score") > sim_threshold
ewma_down = pl.col("delta_ewma") < 0
pseudo_churn = sim_high & ewma_down & trend_down
reliable_neg = (pl.col("recency_days") <= recency_reliable_neg) & (pl.col("delta_ewma") >= 0)
```

### 3.2 🟡 `src/data/preprocessing/dataset_prep/sanity_checks.py`

```python
# TRƯỚC
train_ids = set(result.active_df.loc[~result.active_df["cms_code_enc"].isin(eval_ids), "cms_code_enc"])

# SAU
train_ids = set(
    result.active_df
    .filter(~pl.col("cms_code_enc").is_in(eval_ids))
    ["cms_code_enc"]
    .to_list()
)
```

### 3.3 🔴 `src/modeling/common/churn_type.py`

File phức tạp nhất. Có `pd.Series(val, index=df.index)` × 6 và `pd.concat(axis=1)`.

```python
# TRƯỚC — tạo Series aligned theo index
nan = pd.Series(np.nan, index=df.index)
zero = pd.Series(0, index=df.index)

# SAU — dùng pl.lit() trong context of with_columns
# Không cần tạo Series standalone — dùng trực tiếp trong expression
# nan_col = pl.lit(None).cast(pl.Float64)
# zero_col = pl.lit(0)
```

```python
# TRƯỚC — _mean_of_cols dùng .apply(pd.to_numeric)
mat = df[avail].apply(pd.to_numeric, errors="coerce")
n_used = mat.notna().sum(axis=1).astype(int)
mean = mat.mean(axis=1, skipna=True)

# SAU
mat = df.select([pl.col(c).cast(pl.Float64, strict=False) for c in avail])
n_used = mat.select([pl.sum_horizontal([pl.col(c).is_not_null() for c in avail])]).to_series()
mean = mat.select(pl.mean_horizontal([pl.col(c) for c in avail])).to_series()
```

```python
# TRƯỚC — pd.concat(rpi_cols, axis=1) column concat dùng index alignment
mat = pd.concat(rpi_cols, axis=1)

# SAU — horizontal concat (giữ nguyên row order, không có index)
mat = pl.concat(rpi_cols, how="horizontal")
```

```python
# TRƯỚC — pd.NA với nullable integer
d["ref_month"] = pd.Series([pd.NA] * len(d), index=d.index, dtype="Int64")

# SAU — polars dùng None cho null, Int64 là nullable by default
d = d.with_columns(pl.lit(None).cast(pl.Int64).alias("ref_month"))
```

```python
# TRƯỚC — lambda với pd.notna
d["window_end"].apply(lambda x: prev_yymm(int(x), 1) if pd.notna(x) else pd.NA).astype("Int64")

# SAU — polars map_elements hoặc when/then
d.with_columns(
    pl.when(pl.col("window_end").is_not_null())
      .then(pl.col("window_end").map_elements(lambda x: prev_yymm(int(x), 1), return_dtype=pl.Int64))
      .otherwise(None)
      .alias("ref_month")
)
```

> **⚠️ Warning:** `churn_type.py` là file dài và phức tạp nhất. Recommend tạo riêng 
> `test_churn_type_polars.py` với fixture so sánh output pandas vs polars row-by-row
> trước khi merge.

### 3.4 🟡 `src/data/preprocessing/dataset_prep/walkforward.py`

```python
# TRƯỚC
all_months: pd.DatetimeIndex
training_data = build_training_windows(...)   # returns pd.DataFrame
window_ids = sorted(training_data["_end_month"].unique())
train_fold = training_data[training_data["_end_month"].isin(train_windows)]
x_train = train_fold[feats].fillna(0)
y_train = train_fold["y_raw"]

# SAU
all_months: list[date]   # đổi type hint, DatetimeIndex → list
training_data = build_training_windows(...)   # returns pl.DataFrame
window_ids = sorted(training_data["_end_month"].unique().to_list())
train_fold = training_data.filter(pl.col("_end_month").is_in(train_windows))
x_train = train_fold.select(feats).fill_null(0)
y_train = train_fold["y_raw"].to_numpy()   # sklearn Pipeline nhận numpy

# sklearn Pipeline với StandardScaler — không cần set_output ở đây
# vì output cuối là predict_proba (numpy), không cần polars out
pipe = Pipeline([("scaler", StandardScaler()), ("lr", LogisticRegression(...))])
pipe.fit(x_train, y_train)   # sklearn nhận polars DataFrame từ v1.4
```

### ✅ Checklist Phase 3

- [ ] `pseudo_labeling.py` — label distribution giống hệt pandas output (so sánh dict)
- [ ] `sanity_checks.py` — `train_ids` set giống nhau
- [ ] `churn_type.py` — từng column output giống pandas (tolerance 1e-6 cho float)
- [ ] `walkforward.py` — AUC per fold giống nhau (tolerance 1e-4)
- [ ] Chạy `test_pseudo_labeling.py`, `test_ewma.py` — pass

---

## Phase 4 — ML Boundary: DatasetResult Migration (Tuần 4–5, ngày 27–33) ⚠️

> **Phase then chốt.** Đổi `DatasetResult` từ pandas sang polars, update 
> `StandardScaler.set_output("polars")`, và XGBoost native polars input.

### 4.1 🔴 `src/data/preprocessing/dataset_prep/sample_weighting.py`

**Bước 1: Đổi DatasetResult type annotations**

```python
# TRƯỚC
import pandas as pd
from sklearn.preprocessing import StandardScaler

@dataclass
class DatasetResult:
    x_train: pd.DataFrame
    y_train: pd.Series
    w_train: pd.Series
    x_eval: pd.DataFrame
    y_eval: pd.Series
    x_predict: pd.DataFrame
    scaler: StandardScaler
    feature_names: list[str]
    active_df: pd.DataFrame

# SAU
import polars as pl
from sklearn.preprocessing import StandardScaler

@dataclass
class DatasetResult:
    x_train: pl.DataFrame
    y_train: pl.Series
    w_train: pl.Series
    x_eval: pl.DataFrame
    y_eval: pl.Series
    x_predict: pl.DataFrame
    scaler: StandardScaler
    feature_names: list[str]
    active_df: pl.DataFrame
```

**Bước 2: Thay index-based split bằng key-based**

```python
# TRƯỚC — dùng index để track rows sau train_test_split
_, eval_unlabeled_df = train_test_split(
    unlabeled_df, test_size=0.2, random_state=42, stratify=unlabeled_df["y_label"]
)
is_eval_unlabeled = active_df.index.isin(eval_unlabeled_df.index)

# SAU — dùng cms_code_enc làm key (business identifier)
_, eval_unlabeled_df = train_test_split(
    unlabeled_df, test_size=0.2, random_state=42,
    stratify=unlabeled_df["y_label"].to_numpy()
)
eval_unlabeled_ids = set(eval_unlabeled_df["cms_code_enc"].to_list())
is_eval_unlabeled = pl.col("cms_code_enc").is_in(eval_unlabeled_ids)
```

**Bước 3: Thay `.loc[mask, cols]` bằng `.filter()`**

```python
# TRƯỚC
x_train_active = active_df.loc[train_mask, feats].fillna(0)
y_train_active = active_df.loc[train_mask, "y_smooth"]
w_train_active = active_df.loc[train_mask, "sample_weight"]

# SAU
train_df = active_df.filter(train_mask)
x_train_active = train_df.select(feats).fill_null(0)
y_train_active = train_df["y_smooth"]
w_train_active = train_df["sample_weight"]
```

**Bước 4: StandardScaler với `set_output("polars")` — quan trọng nhất**

```python
# TRƯỚC — wrap lại numpy array với index gốc
scaler = StandardScaler()
x_train = pd.DataFrame(
    scaler.fit_transform(x_train_raw),
    columns=feats,
    index=x_train_raw.index    # ← đây là lý do dùng index
)

# SAU — set_output("polars") → scaler trả về pl.DataFrame trực tiếp
scaler = StandardScaler()
scaler.set_output(transform="polars")

x_train = scaler.fit_transform(x_train_raw)    # trả về pl.DataFrame
x_eval  = scaler.transform(x_eval_raw)         # trả về pl.DataFrame
x_predict = scaler.transform(
    active_df.select(feats).fill_null(0)
)                                               # trả về pl.DataFrame
```

**Bước 5: `pd.concat` → `pl.concat`**

```python
# TRƯỚC
x_train_raw = pd.concat([x_train_active, x_train_hist], ignore_index=True)
y_train = pd.concat([y_train_active, y_train_hist], ignore_index=True)
w_train = pd.concat([w_train_active, w_train_hist], ignore_index=True)

# SAU
x_train_raw = pl.concat([x_train_active, x_train_hist])
y_train = pl.concat([y_train_active, y_train_hist])
w_train = pl.concat([w_train_active, w_train_hist])
```

### 4.2 🟡 `src/modeling/train/trainer.py`

XGBoost 2.0+ accept polars DataFrame trực tiếp, nhưng `label` và `weight` cần numpy:

```python
# TRƯỚC
dtrain = xgb.DMatrix(
    ds.x_train,
    label=ds.y_train,
    weight=ds.w_train,
    feature_names=ds.feature_names,
)

# SAU — x_train là pl.DataFrame (native), label/weight cần .to_numpy()
dtrain = xgb.DMatrix(
    ds.x_train,                        # polars native ✅
    label=ds.y_train.to_numpy(),       # numpy array
    weight=ds.w_train.to_numpy(),      # numpy array
    feature_names=ds.feature_names,
)
```

### 4.3 🟡 `src/modeling/train/evaluator.py`

```python
# TRƯỚC
deval = xgb.DMatrix(ds.x_eval, feature_names=ds.feature_names)
y_true = ds.y_eval.values.astype(int)   # .values là pandas

# SAU
deval = xgb.DMatrix(ds.x_eval, feature_names=ds.feature_names)  # polars native
y_true = ds.y_eval.to_numpy().astype(int)   # polars Series → numpy
```

### 4.4 🟡 `src/modeling/export/scorer.py`

```python
# TRƯỚC
dpredict = xgb.DMatrix(ds.x_predict, feature_names=ds.feature_names)
scored = ds.active_df.copy()
scored["churn_probability"] = y_prob
scored["churn_flag"] = (y_prob >= effective_threshold).astype(int)
risk_count = int(scored_df.get("churn_flag", pd.Series(dtype=int)).sum())
median_val = result[feat].median()

# SAU
dpredict = xgb.DMatrix(ds.x_predict, feature_names=ds.feature_names)  # polars native
scored = ds.active_df.with_columns([
    pl.Series("churn_probability", y_prob),
    pl.Series("churn_flag", (y_prob >= effective_threshold).astype(int)),
])
risk_count = int(scored_df.get_column("churn_flag").sum()) if "churn_flag" in scored_df.columns else 0
median_val = result[feat].median()   # polars Series.median() ✅ tương tự
```

### ✅ Checklist Phase 4

- [ ] `DatasetResult` fields đều là `pl.DataFrame` / `pl.Series`
- [ ] `scaler.set_output("polars")` — `x_train` output là `pl.DataFrame`
- [ ] `xgb.DMatrix(polars_df)` hoạt động không lỗi
- [ ] `trainer.py` — model train được, loss giống nhau (tolerance 0.001)
- [ ] `evaluator.py` — F1, PR-AUC giống nhau so với pandas baseline
- [ ] `scorer.py` — churn_probability distribution giống nhau
- [ ] Chạy `test_scorer.py`, `test_evaluator.py`, `test_sample_weighting.py` — pass

---

## Phase 5 — Monitoring & Export (Tuần 5–6, ngày 34–40)

### 5.1 🟡 `src/monitoring/model_quality/monitoring/drift.py`

```python
# TRƯỚC — iterrows để build DB payload
for _, r in drift_df.iterrows():
    payload.append({
        "feature_name": str(r.get("feature_name")),
        "psi": float(r.get("psi")) if ...,
        ...
    })

# SAU — to_dicts() nhanh hơn iterrows 10-50x
for r in drift_df.to_dicts():
    psi = r.get("psi")
    payload.append({
        "feature_name": str(r.get("feature_name")),
        "psi": float(psi) if psi is not None and psi == psi else None,
        ...
    })
```

```python
# TRƯỚC — fillna, value_counts
s = df_train[c].astype(str).fillna("NA")
vc = s.value_counts(dropna=False).head(30)

# SAU
s = df_train[c].cast(pl.String).fill_null("NA")
vc = s.value_counts().head(30)   # polars value_counts trả về DataFrame
vc_dict = dict(zip(vc[""].to_list(), vc["count"].to_list()))
```

### 5.2 🟡 `src/monitoring/model_quality/monitoring/psi.py`

```python
# TRƯỚC — nhận pd.Series
def make_numeric_profile(x: pd.Series, n_bins: int = 10) -> dict | None:
def counts_on_profile(x: pd.Series, profile: dict) -> np.ndarray:

# SAU — nhận pl.Series
def make_numeric_profile(x: pl.Series, n_bins: int = 10) -> dict | None:
    arr = x.drop_nulls().to_numpy()   # convert sang numpy cho np.percentile

def counts_on_profile(x: pl.Series, profile: dict) -> np.ndarray:
    arr = x.to_numpy()
```

### 5.3 🟡 `src/modeling/export/risk_table.py`

```python
# TRƯỚC
for _, row in flagged.iterrows():
    # build insert payload

# SAU
for row in flagged.to_dicts():
    # build insert payload — dict access giống nhau
```

### ✅ Checklist Phase 5

- [ ] `drift.py` — PSI và KS values giống nhau so với pandas
- [ ] `risk_table.py` — số rows insert đúng, không mất data
- [ ] `psi.py` — `.to_numpy()` conversion không có side effect

---

## Phase 6 — EDA Module (Tuần 6–7, ngày 41–50)

> EDA chạy offline, ít ảnh hưởng production. Có thể làm sau cùng.

### 6.1 🟡 `src/data/eda/stats/` (missing, descriptive, correlation, outliers)

**Pattern chung cho tất cả:**
```python
# TRƯỚC — trả về pd.DataFrame từ list of dicts
return pd.DataFrame(rows)

# SAU
return pl.DataFrame(rows)
```

```python
# TRƯỚC — iterrows
for _, row in desc.iterrows():

# SAU
for row in desc.to_dicts():
```

### 6.2 🔴 `src/data/eda/stats/correlation.py`

```python
# TRƯỚC — corr_matrix là kết quả của df.corr(), dùng iloc[i,j]
val = corr_matrix.iloc[i, j]

# SAU — polars .pearson_corr() trả về scalar, không có corr matrix
# Cần build correlation matrix thủ công
corr_matrix_data = {}
for col_i in numeric_cols:
    corr_matrix_data[col_i] = [
        df[col_i].pearson_corr(df[col_j]) for col_j in numeric_cols
    ]
corr_matrix = pl.DataFrame(corr_matrix_data)
val = corr_matrix[i, j]   # polars positional access
```

### 6.3 🟡 `src/data/eda/visualize/charts.py`

```python
# TRƯỚC — matplotlib dùng .index làm axis label
ax.set_yticklabels(top.index, fontsize=LABEL_SIZE)
subset = corr.iloc[:n, :n]

# SAU — extract column làm label
ax.set_yticklabels(top["feature"].to_list(), fontsize=LABEL_SIZE)
subset = corr.head(n).select(numeric_cols[:n])
```

### 6.4 🟡 `src/data/eda/target/feature_target.py`

```python
# TRƯỚC
feat_v = feat[valid].reset_index(drop=True)
target_v = target[valid].reset_index(drop=True)
grouped = pd.DataFrame({"bin": bins, "target": target_v})
agg = grouped.groupby("bin", observed=True)["target"].agg(["count", "sum", "mean"])

# SAU — polars không có index, filter + group_by
feat_v = feat.filter(valid)
target_v = target.filter(valid)
grouped = pl.DataFrame({"bin": bins, "target": target_v})
agg = grouped.group_by("bin").agg([
    pl.len().alias("count"),
    pl.col("target").sum().alias("sum"),
    pl.col("target").mean().alias("mean"),
])
```

### ✅ Checklist Phase 6

- [ ] EDA stats output — values giống pandas (tolerance 1e-6)
- [ ] Charts render được với polars data
- [ ] `test_eda/` — tất cả test pass

---

## Phase 7 — Tests & Cleanup (Tuần 7–8, ngày 51–56)

### 7.1 Update toàn bộ test files (11 files)

**Pattern chung — đổi fixture:**

```python
# TRƯỚC
import pandas as pd

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "cms_code_enc": ["A", "B", "C"],
        "y_label": [0, 1, 0],
    })

# SAU
import polars as pl

@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "cms_code_enc": ["A", "B", "C"],
        "y_label": [0, 1, 0],
    })
```

**Files cần update:**
- `tests/test_pseudo_labeling.py` — fixtures → pl.DataFrame
- `tests/test_scorer.py` — fixtures → pl.DataFrame
- `tests/test_sample_weighting.py` — DatasetResult fields
- `tests/test_sanity_checks.py` — fixtures
- `tests/test_eda/conftest.py` — EDA fixtures
- `tests/test_eda/test_*.py` — 4 files
- `tests/test_ewma.py` — input/output assertions
- `tests/test_guardrail.py` — nếu dùng DataFrame
- `tests/test_evaluator.py` — metrics assertions

### 7.2 Thêm comparison tests (bắt buộc cho Phase 3 & 4)

```python
# tests/test_migration_parity.py — chạy song song pandas vs polars, so sánh output

def test_pseudo_labeling_parity(sample_active_df_pd, sample_active_df_pl):
    """Đảm bảo polars output giống pandas output."""
    result_pd = assign_pseudo_labels_pandas(sample_active_df_pd, ...)
    result_pl = assign_pseudo_labels_polars(sample_active_df_pl, ...)
    
    # So sánh label distribution
    assert result_pd["label_source"].value_counts().to_dict() == \
           dict(zip(*result_pl["label_source"].value_counts().to_pandas().values.T))
```

### 7.3 Final cleanup

```bash
# Kiểm tra không còn pandas nào sót
grep -r "import pandas" --include="*.py" src/
grep -r "from pandas" --include="*.py" src/

# Nếu sạch hoàn toàn, xóa khỏi requirements
# Xóa dòng pandas khỏi requirements.txt
```

### ✅ Checklist Phase 7

- [ ] Không còn `import pandas` trong `src/`
- [ ] Toàn bộ test pass
- [ ] Parity tests pass (tolerance đã định)
- [ ] `pandas` đã xóa khỏi `requirements.txt`
- [ ] Benchmark so sánh runtime trước/sau migration

---

## Bảng tổng hợp timeline

| Phase | Nội dung | Files | Tuần | Độ khó |
|-------|----------|-------|------|--------|
| 0 | Setup & Infrastructure | requirements, helper | 1 | 🟢 |
| 1 | I/O Layer (read_sql, read_csv) | 10 files | 1–2 | 🟢 |
| 2 | Core Transformations | ewma, label_construction, activity_tiering | 2–3 | 🟡 |
| 3 | Business Logic | pseudo_labeling, churn_type, walkforward | 3–4 | 🔴 |
| 4 | ML Boundary (DatasetResult) | sample_weighting, trainer, evaluator, scorer | 4–5 | 🔴 |
| 5 | Monitoring & Export | drift, psi, risk_table | 5–6 | 🟡 |
| 6 | EDA Module | 12 eda files | 6–7 | 🟡 |
| 7 | Tests & Cleanup | 11 test files | 7–8 | 🟡 |

---

## Rủi ro còn lại cần theo dõi

**1. `sklearn.StandardScaler.set_output("polars")` — internal copy**  
Sklearn hiện tại vẫn copy data từ polars sang numpy internally. Nghĩa là `x_train` 
output là polars nhưng transformation chạy trên numpy. Không ảnh hưởng correctness, 
chỉ ảnh hưởng một phần performance. Theo dõi sklearn updates cho native arrow support.

**2. `train_test_split` với polars**  
`sklearn.model_selection.train_test_split` nhận polars DataFrame nhưng trả về 
pandas hoặc numpy tùy version. Kiểm tra output type và `.to_list()` nếu cần.

**3. `xgb.DMatrix` label/weight**  
XGBoost nhận polars DataFrame làm feature matrix, nhưng `label=` và `weight=` 
parameter cần numpy array. Luôn dùng `.to_numpy()` cho y và w.

**4. `value_counts()` API khác nhau**  
Pandas: trả về `pd.Series(index=values, values=counts)`.  
Polars: trả về `pl.DataFrame` với 2 cột. Cần update tất cả code dùng `.value_counts()`.

**5. `df.empty` → `len(df) == 0`**  
Polars không có `.empty` property. Tìm và thay tất cả.

---

*Document này được tạo tự động từ phân tích codebase thực tế.*  
*Cập nhật lần cuối: dựa trên polars ≥1.0, sklearn ≥1.4, xgboost ≥2.0*
