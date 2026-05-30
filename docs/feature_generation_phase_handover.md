# Feature Generation Integration Handover

## Scope

This note records the v1-to-v2 feature-generation integration work completed
through Phase 3A. Read this file before continuing Phase 3B.

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
- focused smoke assertions for:
  - quality gate evaluation
  - inclusive window-day calculation
  - batch audit parameters
  - JSON sanitization for non-finite values
  - nested-window pair generation
  - incremental skipped-window validation order

Not completed locally:

- `pytest`: bundled runtime does not currently include `pytest`
- `ruff`: bundled runtime does not currently include `ruff`
- PostgreSQL integration run: local DB service was not available

## Phase 3B Starting Point

Confirm business definitions before changing these existing fields:

- `avg_noservice_days`
- `max_consecutive_inactive`

Current behavior:

- `avg_noservice_days` divides inactive days by a month-like value derived
  from the window duration.
- `max_consecutive_inactive` is currently hard-coded to `0`.

Recommended Phase 3B flow:

1. Confirm definitions and edge cases for the two fields.
2. Implement SQL logic using the canonical feature SQL tree only.
3. Add quality rules and regression tests for the confirmed semantics.
4. Run PostgreSQL integration validation when a local DB is available.

## Local Worktree Boundaries

At the time this note was written, unrelated local changes existed and were
intentionally excluded from the feature-generation commit:

- `README.md`
- `data/cskh/README.md`
- `data/cskh/ds_churn - Copy.code-workspace`
- `infrastructure/kind/local-logs-pvc.yaml`
