from __future__ import annotations

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

with DAG(
    dag_id="ds_churn_features",
    start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,          # chỉ chạy khi ingest trigger
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["ds_churn", "features"],
) as dag:

    # GIỮ NGUYÊN LOGIC: dùng đúng entrypoint hiện tại
    from airflow.operators.bash import BashOperator

    # GIỮ NGUYÊN LOGIC: dùng đúng entrypoint hiện tại
    # Assumes code is at /churn_source/src/ops/run_feature_generation.py
    run_features = BashOperator(
        task_id="run_features",
        bash_command="python -m features.engineering.feature_gen.run_feature_generation --start 2025-01-01",
        env={
            "WINDOW_SCHEMA": "data_window",
            "TZ": "Asia/Ho_Chi_Minh",
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": "/churn_source/src", # Ensure imports work
            # DB Config loaded from .env
        },
        append_env=True,
    )

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_pipeline",
        trigger_dag_id="ds_churn_pipeline",
        conf={
            "upstream_features_run_id": "{{ run_id }}",
            "logical_date": "{{ ds }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    run_features >> trigger_pipeline
