from __future__ import annotations

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

with DAG(
    dag_id="ds_churn_ingest",
    start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 9 13,23 * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["ds_churn", "ingest"],
) as dag:

    import os
    from airflow.operators.bash import BashOperator
    
    # Calculate Project Root dynamically (dags/.. -> root)
    # This ensures paths work regardless of AIRFLOW_HOME
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    # Run ingestion script locally
    # Assumes code is at {PROJECT_ROOT}/src/ingestion/run_job_now.py
    ingest_scan_and_load = BashOperator(
        task_id="ingest_scan_and_load",
        bash_command=f"python -m pipelines.ingestion.jobs.ingest_zip_job",
        env={
            # Env vars are loaded from .env by Airflow, append_env=True, but specific overrides can go here
            "TZ": "Asia/Ho_Chi_Minh",
            "PYTHONPATH": "/churn_source/src",  # Ensure imports work
        },
        append_env=True
    )

    # NEW: Validation Step
    validate_data = BashOperator(
        task_id="validate_data",
        bash_command=f"python -m pipelines.ingestion.ops.post_ingest_maintenance",
        env={
            "TZ": "Asia/Ho_Chi_Minh",
            "PYTHONPATH": "/churn_source/src", # Ensure imports work
        },
        append_env=True,
    )

    trigger_features = TriggerDagRunOperator(
        task_id="trigger_features",
        trigger_dag_id="ds_churn_features",
        conf={
            "upstream_run_id": "{{ run_id }}",
            "logical_date": "{{ ds }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Flow: Ingest -> Validate -> Trigger
    ingest_scan_and_load >> validate_data >> trigger_features
