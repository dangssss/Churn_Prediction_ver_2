# Churn Prediction v1 vs v2 - Logic Comparison

Nguon so sanh:

- v1 zip: `B:/Churn_v1/Churn_Prediction_ver_1.zip`
- v1 extracted de phan tich: `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1`
- v2 workspace: `B:/ssss/Churn_Prediction`

Muc tieu cua tai lieu nay la so sanh logic tong the, khong chi so sanh diff file. Ket qua phuc vu viec chon phan da chay production o v1 de tich hop sang v2.

## 1. Tong ket nhanh

| Hang muc | v1 production | v2 hien tai | Danh gia migrate |
|---|---|---|---|
| Kien truc | Tach thanh 3 source tree: `ingestion`, `preprocessing`, `modeling`; Airflow goi BashOperator truc tiep. | Gom thanh modular monolith `src/{data,features,modeling,monitoring,pipelines}`; Airflow chay KubernetesPodOperator. | Giu kien truc v2. Chi migrate logic thuc chien tu v1. |
| Ingestion | Logic ZIP/CSV/Data_pull kha day du, co naming, unzip, copy, maintenance. | Da duoc dua vao `src/data/ingestion`, co test cho ingest job/log repo. | Thap. Kiem tra patch production neu co khac biet nho. |
| Feature generation | Lifetime + sliding window; co chunk checkpoint, skip/recompute table rong, recompute last N, resume. | Lifetime + sliding window; co staging UNLOGGED, incremental skip existing, parallel insert. | Cao. Nen merge checkpoint/recompute-empty cua v1 vao engine v2. |
| Dataset/label | Label da tin hieu C0/C1/C2/C3 tren future table; active gating; outlier clipping. | Label chinh thuc `no item AND no revenue trong horizon`; them CSKH, prototype, pseudo-label, PU learning. | Cao nhung can chon ky. v1 label co the tang recall, v2 label sach hon. |
| Window search | Sweep K tat ca bang available, dung Logistic Regression baseline de chon `best_k`. | Walk-forward W* search trong dataset prep. | Trung binh. Nen tham khao guard/ablation cua v1, khong copy y nguyen. |
| Training | XGBClassifier, native categorical fallback one-hot, 5000 estimators, learning rate 0.01, sanity guardrail. | xgb.train Booster, numeric scaled features, PU sample weights, 500 rounds, learning rate 0.05. | Cao. Nen migrate sanity guardrails va metadata compatibility. |
| Accept/retrain | Co mandatory retrain moi 3 thang, prevalence guard, active-count guard, DB vs bundle F1 cross-check. | Guardrail F0.5/PR-AUC, accept neu F0.5 cai thien, reject van score bang model cu. | Rat cao. Nen migrate cac guard thuc chien cua v1. |
| Scoring threshold | Default config threshold 95%, ghi bang `data_static.cus_risk_95`, xuat CSV. | Score top 10% dynamic threshold, insert `data_static.churn_risk_predictions`. | Cao. Can thong nhat contract voi CSKH. |
| Reasons | SHAP per-customer map vao 8 nhom reason tieng Viet, fallback rule-based. | Global feature importance, reason dang `High/Low feature`. | Rat cao. Nen migrate reason engine v1 sang v2. |
| Monitoring | Score drift, feature drift, backtest precision-in-list, run log. | Co module monitoring tuong tu nhung monthly v2 chua goi day du. | Cao. Nen gan lai monitoring vao `run_monthly_v2`. |
| Config/artifact | Config DB `best_config`, bundle metadata co `cfg`, `feat_cols`, `cat_cols`, `feature_profile`; co fix mismatch K DB vs bundle. | Bundle latest va best_config, nhung metadata don gian hon. | Cao. Nen migrate compatibility checks. |
| Tests | It hon, gan voi production code cu. | Nhieu unit tests hon cho v2. | Giu tests v2, them regression tests cho logic migrate tu v1. |

Ket luan ngan: v2 co kien truc tot hon va pipeline moi cho CSKH/pseudo-label/PU learning. v1 co nhieu guardrail production quan trong hon, dac biet quanh retrain, scoring output, reason, drift, va kha nang chong du lieu thang chua hoan thanh. Nen tich hop theo huong "v2 core + v1 operational guardrails".

## 2. Luong pipeline tong the

| Buoc | v1 | v2 | Khac biet logic |
|---|---|---|---|
| 1. Ingest | `dags/ds_churn_ingest.py` goi luong Data_pull, insert raw vao public schema. | `dags/ds_churn_ingest.py` chay pod image `churn_app:v2`, module `data.ingestion`. | v2 da productize theo k8s, v1 la Bash/local style. |
| 2. Feature | `ds_churn_features` -> `preprocessing/src/operations/run/run_feature_generation.py --start 2025-01-01`. | `ds_churn_features` -> `python -m features.engineering.feature_gen.run_feature_generation --start 2025-01-01 --incremental`. | v2 co incremental mac dinh, v1 co checkpoint/chunk resume manh hon. |
| 3. Modeling | `ds_churn_model_monthly` -> `modeling/main.py run-monthly --horizon 2 --risk-threshold-pct 95`. | `ds_churn_pipeline` -> `pipelines.monthly.monthly_v2_cli`. | v1 threshold/output contract gan production CSKH hon. |
| 4. Train decision | Sweep K -> accept/reject -> retrain neu accepted -> score luon. | Dataset prep -> train -> eval -> guardrail -> accept/reject -> score luon. | v1 co guard data completeness va mandatory cycle. |
| 5. Export | Bang percentile-specific `data_static.cus_risk_{pct}`, CSV `churn_predict_update_YYMMDD.csv`, SHAP raw. | Bang `data_static.churn_risk_predictions`. | Can quyet dinh output contract cu hay moi. |
| 6. Monitoring | Run log, score drift, feature drift, backtest trong monthly pipeline. | Module ton tai, nhung `run_monthly_v2` chua wire day du. | Nen migrate orchestration monitoring tu v1. |

## 3. So sanh chi tiet theo module

### 3.1 Ingestion

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Vi tri | `src/ingestion/Data_pull/...` | `src/data/ingestion/...` | Giu v2 layout. |
| Validate schema | `jobs/csv_schema.py`, `jobs/table_schema.py` | `config/csv_schema.py`, `config/table_schema.py`, them `data/validation` | V2 co cau truc tot hon. |
| Unzip/copy/insert | `ops/unzip_and_discover.py`, `copy_and_insert_to_production.py`, `post_ingest_maintenance.py` | Cung nhom logic trong `src/data/ingestion/ops` | So diff chi tiet truoc khi deploy, vi ingestion de phat sinh bug schema thuc te. |
| Ingest log/idempotency | V1 co luong ingest log. | V2 co `ingest_log_repository.py` va test. | Giu v2, them regression neu v1 co case production moi. |

Nhan dinh: ingestion v2 da duoc migrate kha nhieu tu v1. Khong nen copy nguyen folder v1. Chi nen so diff cac file `copy_and_insert_to_production.py`, `naming.py`, `post_ingest_maintenance.py` neu can dam bao bugfix production khong bi mat.

### 3.2 Feature generation

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Static lifetime | `static_runner.run_static_aggregate` tao `data_static.cus_lifetime`. | `render_and_execute_templates.run_static_aggregate`. | Cung concept, can so SQL lifetime de bao toan feature. |
| Sliding window | `window_runner.render_and_run_all`. | `window_aggregation.render_and_run_all`. | Nen ket hop uu diem 2 ben. |
| Parallel/staging | v1 co chunk execution + checkpoint. | v2 co UNLOGGED staging tables + parallel insert 4 workers. | Giu staging v2, migrate checkpoint/retry cua v1. |
| Incremental | v1: `split_tables_to_keep_and_recompute`, `list_empty_tables`, `truncate_tables`, `recompute_last_n`, checkpoint resume. | v2: `--incremental` skip table da ton tai, khong thay check table rong/recompute last N. | Migrate logic check empty/recompute/resume tu v1. |
| Date planning | v1 co `resolve_month_plan`. | v2 auto-detect latest month tu `bccp_orderitem_YYMM` hoac `cas_customer`. | Giu auto-detect v2, them guard neu data thang moi chua du. |

File v1 lien quan:

- `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/preprocessing/src/features/window_features/window_runner.py`
- `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/preprocessing/src/features/window_features/chunk_checkpoint.py`
- `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/preprocessing/src/features/window_features/table_planner.py`

File v2 lien quan:

- `src/features/engineering/feature_gen/window_aggregation.py`
- `src/features/engineering/feature_gen/run_feature_generation.py`

### 3.3 Label va dataset

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Dinh nghia churn | Da tin hieu `C0 OR C1 OR C2 OR C3`: mat hoan toan, item giam >50%, revenue giam >50%, revenue_slope am va revenue thap. Khach vang mat o future table duoc gan churn. | `y_raw = 1` neu khong co item va revenue trong horizon. | Khong copy y nguyen. Nen bien v1 multi-signal thanh tuy chon config hoac auxiliary label. |
| Active gating | `is_active_now = item_last > 0 OR revenue_last > 0`. | v2 co working set, tier active/at_risk/churned theo recency. | Co the giu tiering v2, them gate item/revenue cua v1 cho scoring/export. |
| Outlier | V1 clip numeric outlier theo percentile 0.1/99.9 khi max qua xa nguong. | V2 chua thay outlier clipping trong dataset prep. | Nen migrate co config, fit tren train de tranh leakage. |
| Static/lifetime ratio | V1 tao ratio giua `last` va lifetime total/percent. | V2 numeric feature list chua thay ratio lifetime. | Rat nen migrate, vi day la feature gan nghiep vu va production. |
| CSKH confirmed churn | V1 khong thay pipeline CSKH/prototype nhu v2. | V2 load CSKH file/DB, prototype cache, pseudo-label, PU weight. | Giu v2. V1 khong thay the duoc phan nay. |
| PU/sample weights | V1 dung class_weight/scale_pos_weight theo churn ratio. | V2 co confirmed/pseudo/reliable_neg/pu_unlabeled + smoothing. | Giu v2 PU, them guard v1 cho high prevalence. |

Risk lon nhat: v1 label da tin hieu co the danh dau "suy giam manh" la churn, con v2 label la "mat hoan toan". Neu tron khong co config, metric va output se doi nghia nghiep vu. De an toan:

1. Giu label v2 lam target chinh cho "churn that".
2. Them cac signal C1/C2/C3 cua v1 thanh feature/risk signal hoac pseudo-label rule.
3. Neu business muon du bao "nguy co roi bo/suy giam", tao `label_strategy = strict_zero_activity | multi_signal`.

### 3.4 Window/K selection

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Search space | Sweep tat ca K co bang feature, `k_min=3`. | Walk-forward W* search trong dataset prep. | Giu W* v2 neu dung voi pipeline moi. |
| Model sweep | Logistic Regression baseline cho moi K va use_static true/false. | Walk-forward AUC, sau do XGBoost train chinh. | Co the migrate v1 ablation static/no-static thanh bao cao phu. |
| Selection metric | Sort F1 roi PR-AUC. | v2 dung F0.5/PR-AUC cho eval. | Nen thong nhat precision-first F0.5 neu output la CSKH top list. |
| Degenerate guard | V1 skip model predict-all-positive. | v2 chua co guard degenerate ro nhu v1. | Nen migrate. |

### 3.5 Training

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Model API | `xgboost.XGBClassifier`. | `xgboost.train` voi `DMatrix`. | Ca hai ok. V2 Booster gon hon, v1 sklearn API de handle categorical metadata hon. |
| Feature type | V1 support numeric, categorical, date-like to ordinal, native categorical fallback one-hot. | V2 dung numeric features + StandardScaler. | Neu v2 chi numeric thi don gian. Neu muon static categorical, migrate v1 type handling. |
| Hyperparams | 5000 estimators, lr 0.01, max_depth 6, max_leaves 63, reg manh, early stopping 200. | 500 rounds, lr 0.05, max_depth 6, early stopping 30. | Can benchmark lai. V1 params co ve production-tuned. |
| Sanity guardrail | V1 so main AP voi dummy const0/random/simple2feat LR; score range guard; predict-all-positive guard. | V2 chi metric guardrail F0.5/PR-AUC. | Rat nen migrate. |
| Feature profile | V1 luu `feature_profile` trong bundle de drift. | V2 bundle luu feature importance, config, feature names. | Nen migrate `feature_profile`. |

File v1 quan trong: `src/modeling/main_model/runner.py`.

### 3.6 Accept/reject va retrain policy

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Previous model metric | V1 lay max giua DB best_config va bundle metadata de tranh DB bi overwrite thu cong. | V2 doc latest accepted config DB. | Migrate DB-vs-bundle cross-check. |
| Mandatory retrain | V1 bat buoc retrain moi 3 thang, anchor `2603`. | V2 khong co. | Nen migrate voi config, khong hardcode anchor. |
| High prevalence guard | V1 block retrain neu churn_ratio_train > 0.45. | V2 khong co guard nay. | Nen migrate vi bao ve khi label thang chua san sang/du lieu loi. |
| Active-count guard | V1 neu khong mandatory, so active count thang hien tai voi thang truoc; ratio < 0.80 thi reject retrain, van score bang model cu. | V2 khong thay guard du lieu thang chua hoan thanh. | Rat nen migrate. |
| Accept metric | V1 `cand_f1 > prev_f1 + eps`, co mandatory override. | V2 `new_f05 > prev_f05 + eps`. | Giu F0.5 v2 neu business uu tien precision, them guard v1. |

Day la cum logic v1 dang gia nhat de tich hop sang v2.

### 3.7 Scoring va export

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Threshold | Config `risk_threshold_pct=95`, filter `churn_rate >= 95`. | `score_all` dung max(eval threshold, top 10% percentile). | Can chon lai theo yeu cau CSKH: fixed high threshold hay top N%. |
| Output DB | `data_static.cus_risk_{pct}` va `_hist` cho backtest. | `data_static.churn_risk_predictions`. | Neu downstream CSKH dang doc v1 table, can migrate compatibility view/table. |
| Output CSV | V1 xuat `churn_predict_update_YYMMDD.csv`. | V2 chua thay xuat CSV. | Neu production dang can CSV, migrate. |
| Feature mismatch scoring | V1 neu DB config K khac bundle K, dung K trong bundle; pad missing features. | V2 scoring dua vao `DatasetResult` cung run, reject thi load model cu nhung features co the khong khop neu W/feature set khac. | Rat nen migrate compatibility check. |
| Active filter reasons | V1 reasons chi giu khach co don 2 thang gan nhat (`item_last > 0 AND item_1m_ago > 0`). | V2 score all active theo tier. | Can thong nhat voi nghiep vu. |

### 3.8 Reason engine

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Approach | SHAP per-customer, map feature vao 8 bucket nghiep vu, render reason tieng Viet bang so lieu thuc. Fallback rule-based. | Dung global feature importance top-N, tao text `High feature`/`Low feature`. | Migrate v1 reason engine. |
| Reason buckets | Item giam, complaint tang, delay tang, nodone tang, bien dong cao, gia tri don giam, da dang dich vu giam, khach moi. | Generic feature names. | V1 tot hon nhieu cho CSKH. |
| SHAP raw | V1 xuat raw SHAP CSV. | V2 khong co. | Nen migrate de audit/explainability. |

### 3.9 Monitoring

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Run log | `ml_monitor.run_log` start/finish trong monthly pipeline. | Module `monitoring/model_quality/monitoring/run_log.py` ton tai. | Wire vao `run_monthly_v2`. |
| Score drift | V1 upsert score drift sau export. | Module v2 co `score.py`. | Wire lai. |
| Feature drift | V1 load `feature_profile` tu bundle, compute PSI current feature table. | V2 co drift/psi module, nhung bundle v2 chua luu profile. | Migrate profile save + monthly call. |
| Backtest | V1 run precision-in-list voi risk hist table. | V2 co backtest module. | Can tao hist/output contract roi wire lai. |

### 3.10 Airflow/deployment

| Noi dung | v1 | v2 | Khuyen nghi |
|---|---|---|---|
| Operator | BashOperator local path `/churn_source`. | KubernetesPodOperator image `churn_app:v2`. | Giu v2. |
| Image | `dockerhub.vnpost.vn/airflow-churn:1.0.0`. | Local/prod `churn_app:v2`, Helm/K8s infra. | Giu v2 infra. |
| DAG chain | ingest -> features -> model. | ingest -> features + EDA -> pipeline + housekeeping. | Giu v2, them monitoring/backtest task neu tach task se de debug hon. |

## 4. Cac diem v2 can sua truoc hoac trong luc migrate

| Muc | Van de | Anh huong | De xuat |
|---|---|---|---|
| Config naming | `run_monthly_v2` goi `model_config.min_f1`, `min_pr_auc`, `f1_improve_eps`, nhung `ModelConfig` hien co `min_f2`, `min_roc_auc`, `f2_improve_eps`. | Co nguy co runtime `AttributeError` khi chay monthly v2. | Doi `ModelConfig` sang ten F0.5/PR-AUC dung README, hoac sua monthly_v2 ve field hien co. |
| Guardrail tests | Tests dang goi `check_guardrail(..., min_f2=..., min_roc_auc=...)`, trong khi function nhan `min_f05`, `min_pr_auc`. | Test suite co nguy co fail hoac lech doc/code. | Chuan hoa metric naming: `f05`, `min_f05`, `min_pr_auc`. |
| Scoring reject old model | V2 reject thi load old bundle, nhung `DatasetResult` moi co the khac feature set/scaler voi old model. | Risk scoring fail hoac score sai neu feature_names mismatch. | Migrate v1 logic bundle metadata K/feature compatibility, pad missing, hoac luu full preprocessing artifact trong bundle. |
| Reasons | Reason v2 chua dung cho CSKH. | Output kho giai thich cho nguoi dung cuoi. | Migrate SHAP/rule reason v1. |
| Monitoring not wired | V2 co module nhung pipeline monthly chua day du run log/drift/backtest. | Mat observability production. | Migrate orchestration v1. |

## 5. De xuat lo trinh tich hop

### Phase 1 - Fix blocking/runtime and preserve production contract

| Task | Nguon | Dich |
|---|---|---|
| Chuan hoa config/guardrail metric naming F0.5/PR-AUC. | v2 | `modeling/config/model_config.py`, `modeling/train/guardrail.py`, tests. |
| Them old-model feature compatibility khi reject. | v1 `export_risk_mode/insert_predictions.py` | v2 `modeling/export/scorer.py` hoac bundle scoring adapter. |
| Xac nhan output contract CSKH: bang v1 `cus_risk_95`/CSV hay bang v2 `churn_risk_predictions`. | v1 production | v2 export. |

### Phase 2 - Migrate operational guardrails

| Task | Nguon v1 | Dich v2 |
|---|---|---|
| Mandatory retrain cycle co config. | `pipeline/monthly.py:is_mandatory_retrain_month` | `pipelines/monthly/monthly_v2.py` |
| Active-count data completeness guard. | `get_active_count_for_month`, active ratio block | `monthly_v2` truoc train accept. |
| High prevalence guard. | `prevalence_blocked` trong monthly v1 | Sau dataset prep/eval trong v2. |
| DB-vs-bundle previous metric cross-check. | `load_bundle` + prev F1 max | `monthly_v2` accept/reject. |

### Phase 3 - Migrate feature/reason quality

| Task | Nguon v1 | Dich v2 |
|---|---|---|
| Lifetime ratio features. | `preprocess/static_features.py` | `data/preprocessing/dataset_prep` hoac `features` layer. |
| Outlier clipping co config. | `preprocess/dataset.py:clip_and_log_outliers` | Dataset prep train-only transform/artifact. |
| SHAP + 8 reason buckets. | `export_risk_mode/insert_predictions.py` | `modeling/export/reasons.py` moi. |
| CSV export neu CSKH can. | `export_risk_mode/runner.py` | `modeling/export` hoac DAG artifact output. |

### Phase 4 - Monitoring and feature generation robustness

| Task | Nguon v1 | Dich v2 |
|---|---|---|
| Feature profile luu trong bundle. | `main_model/runner.py` | `modeling/common/artifacts.py` metadata. |
| Score drift, feature drift, backtest orchestration. | `pipeline/monthly.py` | `monthly_v2` hoac tach tasks Airflow. |
| Window checkpoint/recompute-empty/resume. | `preprocessing/src/features/window_features` | `features/engineering/feature_gen/window_aggregation.py`. |

## 6. Cac phan khong nen copy y nguyen

| Phan v1 | Ly do |
|---|---|
| Toan bo tree `src/modeling` | v2 da co kien truc moi, CSKH/prototype/PU learning khac ban chat. Copy se lam mat loi the v2. |
| Multi-signal label lam target mac dinh | Co the doi nghia churn tu "mat hoat dong" thanh "suy giam". Nen dua vao config/feature/pseudo-label. |
| BashOperator deployment | v2 K8s/pod isolation tot hon. |
| Hardcoded mandatory anchor `2603` | Nen dua vao config/env va ghi ro policy. |
| Table output `cus_risk_{pct}` neu v2 da co consumer moi | Chi giu neu downstream CSKH van can. Co the lam compatibility view thay vi table moi. |

## 7. Mapping file quan trong

| Logic | v1 file | v2 file de tich hop |
|---|---|---|
| Monthly orchestration | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/pipeline/monthly.py` | `src/pipelines/monthly/monthly_v2.py` |
| CLI model | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/main.py` | `src/pipelines/monthly/monthly_v2_cli.py` |
| Sweep K | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/baseline/sweep.py` | `src/data/preprocessing/dataset_prep/walkforward.py` |
| Label/gating/outlier | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/preprocess/dataset.py` | `src/data/preprocessing/dataset_prep/label_construction.py`, `scope_filter.py`, `activity_tiering.py` |
| Lifetime ratio | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/preprocess/static_features.py` | `src/data/preprocessing/dataset_prep` hoac `src/features/engineering` |
| XGB train guardrails | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/main_model/runner.py` | `src/modeling/train/trainer.py`, `evaluator.py`, `guardrail.py` |
| Scoring compatibility | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/export_risk_mode/insert_predictions.py` | `src/modeling/export/scorer.py` |
| Risk export | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/export_risk_mode/runner.py` | `src/modeling/export/risk_table.py` |
| Monitoring | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/modeling/monitoring` | `src/monitoring/model_quality/monitoring` |
| Feature checkpoint | `B:/Churn_v1/v1_extracted_for_comparison/Churn_Prediction_ver_1/src/preprocessing/src/features/window_features` | `src/features/engineering/feature_gen/window_aggregation.py` |

## 8. Uu tien migrate de xuat

| Uu tien | Hang muc | Ly do |
|---|---|---|
| P0 | Sua mismatch config/guardrail v2. | Co the lam monthly v2 fail ngay khi chay. |
| P0 | Old model scoring compatibility khi reject. | Neu reject model moi, phai score bang bundle cu an toan. |
| P1 | Active-count guard + high-prevalence guard. | Bao ve production khi data thang hien tai chua day du. |
| P1 | SHAP/rule reason engine v1. | Tang gia tri output cho CSKH nhieu nhat. |
| P1 | Monitoring orchestration. | Can cho production observability. |
| P2 | Lifetime ratio features va outlier clipping. | Co the cai thien model, can benchmark. |
| P2 | Window checkpoint/recompute-empty. | Tang do ben feature pipeline khi chay du lieu lon. |
| P3 | Multi-signal label C1/C2/C3. | Nen thu nghiem offline truoc khi thay doi nghia label. |
