from __future__ import annotations

"""
DAG: ds_churn_pipeline
Runs the full churn prediction pipeline v2:
  Dataset Prep (7 steps) → Train → Evaluate → Guardrail
  → Accept/Reject → Save → Score → Export

Schedule: None (triggered by ds_churn_features)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
    dag_id="ds_churn_pipeline",
    start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,          # triggered by ds_churn_features
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["ds_churn", "pipeline", "model"],
    doc_md="""
    ## DS Churn Pipeline v2

    Runs the full monthly churn prediction pipeline:
    1. **Dataset Prep** (scope filter → tiering → EWMA → walk-forward W*
       → prototype → pseudo-labeling → sample weighting)
    2. **Train** XGBoost on DatasetResult
    3. **Evaluate** on confirmed churner eval set
    4. **Guardrail** check (min F1/PR-AUC)
    5. **Accept/Reject** vs previous model
    6. **Save** model bundle
    7. **Score** all active customers
    8. **Export** risk predictions to DB

    Triggered by `ds_churn_features` after feature generation completes.
    """,
) as dag:

    run_pipeline = BashOperator(
        task_id="run_monthly_v2",
        bash_command="python -m pipelines.monthly.monthly_v2_cli",
        env={
            "TZ": "Asia/Ho_Chi_Minh",
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": "/churn_source/src",
            "CSKH_FILE_PATH": "/churn_data/cskh/confirmed_churners.csv",
            "CHURN_MODEL_DIR": "/churn_data/models",
        },
        append_env=True,
    )
