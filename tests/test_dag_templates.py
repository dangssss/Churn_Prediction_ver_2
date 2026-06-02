"""Source-level checks for trigger configuration shared by Airflow DAGs."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]


def test_manual_dag_triggers_do_not_depend_on_ds_template() -> None:
    """Airflow 3 manual DagRuns do not expose the legacy ``ds`` template."""
    for relative_path in ("dags/ds_churn_features.py", "dags/ds_churn_ingest.py"):
        source = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
        assert "{{ ds }}" not in source


def test_trigger_lineage_ids_are_retained() -> None:
    """Downstream runs still receive their upstream run lineage."""
    features_source = (PROJECT_ROOT / "dags/ds_churn_features.py").read_text(
        encoding="utf-8"
    )
    ingest_source = (PROJECT_ROOT / "dags/ds_churn_ingest.py").read_text(
        encoding="utf-8"
    )

    assert '"upstream_features_run_id": "{{ run_id }}"' in features_source
    assert ingest_source.count('"upstream_run_id": "{{ run_id }}"') == 2


def test_feature_ablation_dag_uses_non_production_cli() -> None:
    source = (PROJECT_ROOT / "dags/ds_churn_feature_ablation.py").read_text(
        encoding="utf-8"
    )

    assert "modeling.experiments.feature_ablation_cli" in source
    assert "pipelines.monthly.monthly_v2_cli" not in source
