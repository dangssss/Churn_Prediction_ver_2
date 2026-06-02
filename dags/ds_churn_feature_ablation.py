"""DAG: baseline-versus-candidate feature experiment."""

from __future__ import annotations

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from pendulum import datetime

with DAG(
    dag_id="ds_churn_feature_ablation",
    start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["ds_churn", "experiment", "feature_ablation"],
) as dag:
    volume = k8s.V1Volume(
        name="churn-data-mount",
        host_path=k8s.V1HostPathVolumeSource(path="/data"),
    )
    volume_mount = k8s.V1VolumeMount(
        name="churn-data-mount",
        mount_path="/data",
        read_only=False,
    )

    run_feature_ablation = KubernetesPodOperator(
        task_id="run_feature_ablation_k8s",
        name="churn-feature-ablation-pod",
        namespace="default",
        image="churn_app:v2",
        image_pull_policy="IfNotPresent",
        container_security_context=k8s.V1SecurityContext(run_as_user=0),
        cmds=["python", "-m", "modeling.experiments.feature_ablation_cli"],
        env_vars={
            "TZ": "Asia/Ho_Chi_Minh",
            "PYTHONUNBUFFERED": "1",
            "FEATURE_ABLATION_CANDIDATE": "max_consecutive_inactive",
            "FEATURE_ABLATION_REPORT_DIR": "/data/reports/model_experiments",
        },
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="churn-db-secret")
            )
        ],
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
    )
