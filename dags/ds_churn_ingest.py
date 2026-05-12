"""DAG: ds_churn_ingest

Scans for new ZIP data files, loads them into the database (atomic
TRUNCATE-and-reload per ZIP), then triggers the features DAG.

Skip / re-ingest decision is md5-based and lives inside the pod's
``data.ingestion.cli scan`` entrypoint (see
:mod:`data.ingestion.ops.ingest_log_repository`).

Schedule: 09:00 on the 13th and 23rd of every month.
"""
from __future__ import annotations

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from pendulum import datetime

# Airflow XCom return-value sidecar reads this exact path.
_XCOM_RETURN_PATH = "/airflow/xcom/return.json"

with DAG(
    dag_id="ds_churn_ingest",
    start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 9 13,23 * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["ds_churn", "ingest"],
) as dag:

    # Common volume — host path mounted into container at /churn_data.
    volume = k8s.V1Volume(
        name="churn-data-mount",
        # prod: path="/data/churn_prediction/ftp_churn"
        host_path=k8s.V1HostPathVolumeSource(
            path="/run/desktop/mnt/host/d/Churn_Prediction_v2/data"
        ),
    )
    volume_mount = k8s.V1VolumeMount(
        name="churn-data-mount",
        mount_path="/churn_data",
        sub_path=None,
        read_only=False,
    )

    ingest_scan_and_load = KubernetesPodOperator(
        task_id="ingest_scan_and_load_k8s",
        name="churn-ingestion-pod",
        namespace="default",
        image="churn_app:latest",
        image_pull_policy="IfNotPresent",
        container_security_context=k8s.V1SecurityContext(run_as_user=0),
        cmds=["python", "-m", "data.ingestion.cli"],
        arguments=[
            "scan",
            "--prod-schema", "public",
            "--ingest-schema", "ingest",
            "--xcom-out", _XCOM_RETURN_PATH,
        ],
        env_vars={"TZ": "Asia/Ho_Chi_Minh"},
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="churn-db-secret"))
        ],
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=True,
    )

    trigger_features = TriggerDagRunOperator(
        task_id="trigger_features",
        trigger_dag_id="ds_churn_features",
        conf={
            "upstream_run_id": "{{ run_id }}",
            "logical_date": "{{ ds }}",
            "ingest_summary": "{{ ti.xcom_pull(task_ids='ingest_scan_and_load_k8s') }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    trigger_eda = TriggerDagRunOperator(
        task_id="trigger_eda",
        trigger_dag_id="ds_churn_eda",
        conf={
            "upstream_run_id": "{{ run_id }}",
            "logical_date": "{{ ds }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Flow: ingest (atomic, validates internally) → features + EDA in parallel
    ingest_scan_and_load >> [trigger_features, trigger_eda]
