# Feature Generation Integration Handover

## Scope

This note records the v1-to-v2 feature-generation integration work completed
through Phase 3B. Read this file before continuing later feature work.

The target remains v2. Existing technologies and architecture are preserved.
Production guardrails from v1 are integrated selectively instead of copying
whole v1 modules.

## Completed Work

### Phase 1: Incremental Window Planning

- Added DB-backed manifest table:
  `data_static.feature_generation_windows`.
- Added incremental window planning for:
  - new windows
  - empty existing windows
  - windows whose latest run failed
  - latest N windows per size requested for recomputation
- Added CLI options:
  - `--incremental`
  - `--recompute-last-n`
  - `--feature-run-id`
- Added validation before a window is marked successful:
  - non-empty table
  - non-blank `cms_code_enc`
  - one row per `cms_code_enc`

Key modules:

- `src/features/engineering/feature_gen/window_planner.py`
- `src/features/engineering/feature_gen/window_manifest.py`
- `src/features/engineering/feature_gen/window_aggregation.py`

### Phase 2: SQL Ownership and Source Contracts

- Kept one canonical SQL tree:
  `src/features/engineering/database/sql`.
- Removed the duplicate unused SQL tree under:
  `src/data/preprocessing/database/sql`.
- Added strict unresolved-placeholder validation in the template engine.
- Added source schema contracts and strict BCCP source discovery.
- Corrected `cas_info.contract_service` to integer schema handling.
- Corrected lifetime and sliding SQL boundaries:
  - complaint end day uses a half-open boundary
  - lifetime source reads are bounded through `CURRENT_DATE`
  - BCCP complaint denominator uses `NULLIF(COUNT(*), 0)`
  - inactive days no longer become null without BCCP activity
  - lifetime upsert refreshes `contract_classify`
  - `cas_info` selection is deterministic by latest update date

Key modules:

- `src/features/engineering/feature_gen/feature_source_schema.py`
- `src/features/engineering/feature_gen/db_utils.py`
- `src/features/engineering/feature_gen/template_engine.py`

Operational note:

Existing databases with a timestamp-typed `cas_info.contract_service` column
require a reviewed migration. The pipeline does not alter that column
automatically.

### Phase 3A: Feature Quality Gates

- Corrected sliding `recency` semantics:
  inclusive ordinal day of latest BCCP service activity relative to the
  window start. Example: `2025-01-01` through `2025-02-02` yields `33`.
- Corrected `lifetime_months_active` semantics:
  number of distinct `report_month` values with `item_count > 0`, bounded
  from `2025-01-01` through `CURRENT_DATE`.
- Added persisted quality audit table:
  `data_static.feature_generation_quality`.
- Added lifetime validation:
  - unique and non-blank `cms_code_enc`
  - critical null checks
  - non-finite numeric checks
  - ratio range checks
  - non-negative total checks
  - activity range checks
- Added per-window validation:
  - critical null checks
  - non-finite numeric checks
  - ratio range checks
  - non-negative total checks
  - activity range checks
  - embedded metadata checks
- Added batch consistency validation:
  - longer nested windows must not have lower item or revenue totals than
    shorter windows ending in the same month
  - largest current window must not exceed lifetime totals
  - largest current window customers must exist in lifetime features
- Incremental runs now audit skipped existing windows before accepting the
  batch.

Key modules:

- `src/features/engineering/feature_gen/window_quality.py`
- `src/features/engineering/feature_gen/window_aggregation.py`
- `src/features/engineering/feature_gen/run_feature_generation.py`
- `docs/feature_generation_quality.md`

## Verification Evidence

Completed locally:

- Python compilation for touched source and test modules
- `git diff --check`
- focused `pytest` and `ruff` checks after creating the project virtual
  environment
- focused smoke assertions for:
  - quality gate evaluation
  - inclusive window-day calculation
  - batch audit parameters
  - JSON sanitization for non-finite values
  - nested-window pair generation
  - incremental skipped-window validation order
  - Phase 3B sliding-template rendering
  - Phase 3B no-service gap reference cases
  - Phase 3B quality-summary mapping

Not completed locally:

- Full PostgreSQL feature-generation integration run is pending completion of
  the local source-data ingestion.

### Phase 3B: No-Service Gap Features

- Replaced the approximate `avg_noservice_days` formula with real BCCP
  activity-gap calculation.
- Replaced hard-coded `max_consecutive_inactive = 0` with the longest actual
  no-service gap.
- Included leading, internal, and trailing gaps inside each sliding window.
- Defined no-activity fallback:
  both features equal the full inclusive window duration.
- Refreshed activity fields during upsert so incremental recomputation updates
  existing window rows.
- Extended window quality gates:
  - both fields are required
  - values must be non-negative
  - values must not exceed `inactive_days`
  - average gap must not exceed maximum gap
  - `avg_noservice_days` must be finite
- Added SQL regression assertions and updated feature-quality documentation.

## Next Starting Point

Feature generation and raw-feature EDA have completed local acceptance.
The next starting point is Phase 4 model-dataset validation before any retrain.
Keep `ds_churn_pipeline` paused until the baseline-versus-candidate evaluation
is run with a fixed snapshot, split, seed, and config.

## Local Feature Acceptance Run

- Airflow DAG run:
  `manual_phase3_acceptance_20260602_retry6`.
- Feature application run ID:
  `5dfaf3730aba4f92887b621ec23d6c3a`.
- Lifetime snapshots:
  - 17 monthly snapshots through `2026-05-01`
  - 3,410,474 total snapshot rows
  - 258,403 customers in the latest compatibility snapshot
- Window planning:
  - 81 retained manifests
  - 23 empty windows
  - 39 computed windows
  - 39 successful windows
- Batch consistency quality gate:
  `success`.
- Runtime guardrails added during acceptance:
  - complaint rates remain non-negative but are not incorrectly capped at 1
  - negative raw revenue is neutralized to zero in feature aggregates while
    raw source data remains unchanged
  - static snapshots support incremental skip and trailing recomputation
  - window aggregation worker count is configurable through
    `FEATURE_MAX_PARALLEL_WORKERS`
  - ephemeral staging tables disable autovacuum and are explicitly analysed
- Local PostgreSQL temporary spill files use the reviewed `churn_temp`
  tablespace on drive `B:`. This is a local environment setting, not an
  application migration.

## Local EDA Acceptance Run

- Airflow DAG run:
  `manual_phase4_eda_20260602_retry4`.
- EDA application run ID:
  `eda_20260602_075834_f75449a6`.
- EDA temporal loading is bounded and repeatable:
  - maximum 50,000 rows per monthly snapshot
  - PostgreSQL `TABLESAMPLE SYSTEM (25) REPEATABLE (42)`
  - configured by `EDA_TEMPORAL_SAMPLE_ROWS` and
    `EDA_TEMPORAL_SAMPLE_PERCENT`
- Primary EDA automatically selects the latest generated feature-window table
  instead of falling back to lifetime data when `EDA_WINDOW_END` is unset.
- Acceptance persistence:
  - 45,863 sampled primary rows
  - 50 raw window feature statistics
  - 31 high-correlation pairs
  - 500 temporal-drift rows across the two successful acceptance runs
  - latest HTML report:
    `data/reports/eda/eda_report_20260602_075841.html`
- `NUMERIC_FEATURES` currently has 66 fields. The remaining 16 EWMA fields are
  created later in dataset preparation, so they are intentionally outside the
  raw feature-window EDA surface.
- Airflow 3 manual DAG triggers no longer pass the legacy `{{ ds }}` template,
  because downstream DAGs do not consume that value.

## Phase 4 Feature Ablation Entry Point

- Use the manual Airflow DAG:
  `ds_churn_feature_ablation`.
- Do not use `ds_churn_pipeline` until the feature list is accepted.
- The experiment runs dataset preparation once with the union feature set,
  then trains two branches against the same snapshot, labels, split, W*,
  calibration, and random seed:
  - baseline: current `NUMERIC_FEATURES`
  - candidate: baseline plus `max_consecutive_inactive`
- The experiment does not execute model accept/reject, save a model bundle, or
  write `data_static.churn_risk_predictions`.
- JSON reports are written under:
  `data/reports/model_experiments`.
- Each report includes:
  - ROC-AUC, PR-AUC, F0.5, F1, precision, recall, and selected threshold
  - confusion matrix
  - metric deltas from baseline to candidate
  - feature importance
  - true-holdout metrics grouped by confirmation month when possible
- Current dataset preparation reserves only the latest true CSKH cohort as
  holdout. Until multiple out-of-sample cohorts are retained, report stability
  is explicitly marked `insufficient_holdout_months`; older cohorts must not be
  reused as fake holdout because they participate in training.

## Local Environment Setup

- Project virtual environment is created with:
  `uv sync --extra dev`.
- Python is pinned to `3.12` through `.python-version`.
- `pyproject.toml` constrains Python to `>=3.10,<3.13` because the current
  `numpy<2` dependency is not compatible with Python 3.14.
- Local PostgreSQL settings belong in the ignored `.env` file:
  - `PG_HOST=localhost`
  - `PG_PORT=5433`
  - `PG_DB=churn_prediction`
  - `PG_USER=postgres`
  - `PG_PW` must be filled locally and must never be committed.
- Local data files are stored under the repository `data/` directory and are
  mounted into Docker Compose containers and Kind pods at `/data`.
- Runtime data paths use the same `/data` contract:
  - `INCOMING_DIR=/data/incoming`
  - `SAVED_DIR=/data/saved`
  - `FAIL_DIR=/data/failed`
  - `CSKH_DIR=/data/cskh`

## Local PostgreSQL Integration Run

- Dedicated local database created: `churn_prediction`.
- Schemas initialized:
  - `cskh`
  - `data_static`
  - `data_window`
  - `ingest`
- CSKH ZIP ingestion completed from `data/cskh`:
  - 12 ZIP files loaded without errors
  - 3,805 rows persisted in `cskh.customer_labels`
  - 1,330 direct CMS rows persisted in `cskh.confirmed_churners`
  - CRM-only labels remain in the raw table and are resolved through
    `public.cas_info` after source-data ingestion.
- Source-data ingestion is running from `data/incoming`.
- Current committed source-data checkpoint:
  - `public.cas_customer`: 1,924,445 rows, validation passed
  - `public.cas_info`: 1,211,096 rows, validation passed
  - `public.bccp_orderitem_2604`: 4,678,201 rows, validation passed
- The first source scan continues sequentially through the remaining BCCP
  months. A hidden one-shot watcher is waiting for that scan to finish and
  will rerun ingestion once so MD5 audit skips successful files and retries
  `cms_complaint` with the corrected DDL.
- Local progress files are intentionally ignored under `data/logs`:
  - `data/logs/active_ingestion.json`
  - `data/logs/ingestion_retry_watcher.json`
- PostgreSQL compatibility fixes applied during the integration run:
  - ingestion DDL time columns now use PostgreSQL `TIMESTAMP` instead of
    unsupported `DATETIME`
  - encoded customer and item key columns now use `VARCHAR(100)` because the
    current encoded values are longer than the legacy `VARCHAR(20)` limit
  - `cms_complaint` ingestion DDL now includes the transformed
    `etl_date TIMESTAMP` column
  - `bccp_orderitem.total_fee` now uses `BIGINT`; source values above the
    PostgreSQL `INTEGER` range blocked five historical monthly ZIPs
- CRM fallback resolution is deterministic:
  - 1,609 CRM-only keys were inspected after `public.cas_info` ingestion
  - 767 keys map to exactly one CMS ID
  - 839 keys remain unresolved
  - 3 ambiguous CRM keys map to multiple CMS IDs and are intentionally
    excluded from PU labels
- CSKH label selection is time-scoped to prevent temporal leakage:
  - `load_eval_id_cohorts_from_db` requires an inclusive `label_to_yymm`
    cutoff and preserves the source `label_yymm` for each resolved CMS ID
  - the default rolling history is `label_months_back=6`
  - direct CMS labels, CRM fallback labels, and CRM resolution statistics all
    use the same bounded month range
  - a legacy single-file `CSKH_FILE_PATH` is rejected if its filename cannot
    identify the label month or if its label month is after the run cutoff
  - the dataset pipeline detects `t_obs` before loading labels and uses the
    run month as the label cutoff
  - production target is exact next-month inactivity: feature origin `T`
    predicts activity outcome in `T+1`, with `horizon_months=1`;
  - historical training rows retain `y_raw` from their own next-month outcome;
    current CSKH confirmations no longer overwrite labels for past windows
  - PU prototype samples are time-aligned by confirmation cohort: for
    `label_yymm=2503` and `lead_offset=1`, the feature snapshot ends at
    `2502`; repeated CMS IDs contribute only their earliest cohort sample
- Next-month target regression checks:
  - latest confirmed CSKH cohort is a strict temporal holdout
  - older confirmed cohorts enter training with their matching `T-1` snapshots
  - holdout negatives are customers with actual activity in the outcome month;
    unlabeled rows are no longer assumed negative for evaluation
  - pseudo-label thresholds are calibrated from current population quantiles
  - auxiliary label weights are calibrated by volume relative to confirmed
    historical training rows; confirmed labels retain weight `1.0`
  - scoring uses tunable `risk_top_percentile` plus the operational
    `risk_max_customers=5000` cap agreed for the monthly CSKH list
  - missing complete holdout switches monthly execution to scoring-only mode
    using the latest accepted bundle
  - canonical model quality contract is `F0.5`, `PR-AUC`, and recall floor;
    F2/ROC-AUC legacy persistence columns are removed during schema ensure
  - unwired v1-shaped model-quality monitoring modules were retired because
    they depended on removed imports and F1/percentile-history tables;
    `monitoring.model_quality.monitoring.psi` remains because EDA uses it
  - operational monitoring for the new model must read
    `data_static.churn_risk_predictions` and use F0.5 semantics
  - full `pytest` suite: 242 passed
  - touched-file `ruff` checks: passed
  - touched-module Python compilation: passed
- Lifetime features are point-in-time scoped:
  - canonical history is stored in
    `data_static.cus_lifetime_snapshot(snapshot_month, cms_code_enc)`
  - each monthly snapshot aggregates only source rows available through its
    own month cutoff; tenure is recomputed in months from `contract_sig_first`
    through that cutoff instead of reusing the current-state `cas_info.tenure`
  - `data_static.cus_lifetime` remains the latest compatibility snapshot
  - historical feature windows merge the matching lifetime snapshot by their
    own `end_month`
  - dataset scope filtering uses the prediction feature cutoff month instead
    of the latest lifetime state
  - scope filtering applies minimum lifetime orders, lifetime GMV, and
    account age in months; inactive-duration filtering remains in tiering
  - recency calculation excludes `cas_customer` activity at or after `t_obs`
- PostgreSQL scoped-label integration checks:
  - cutoff `2501`, range `2501..2501`: 67 CRM-only keys, 43 resolved CMS IDs
  - cutoff `2503`, range `2501..2503`: 613 resolved/direct CMS IDs
  - cutoff `2512`, range `2507..2512`: 808 resolved/direct CMS IDs
- Added focused ingestion-DDL regression tests:
  `tests/test_ingestion_table_schema.py`.

## Local Worktree Boundaries

At the time this note was written, unrelated local changes existed and were
intentionally excluded from the feature-generation commit:

- `README.md`
- `data/cskh/README.md`
- `data/cskh/ds_churn - Copy.code-workspace`
- `infrastructure/kind/local-logs-pvc.yaml`
