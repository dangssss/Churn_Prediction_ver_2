"""Compare one candidate feature against the production baseline feature set."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

import xgboost as xgb

from data.preprocessing.dataset_prep.pipeline_config import (
    NUMERIC_FEATURES,
    DatasetPipelineConfig,
)
from data.preprocessing.dataset_prep.run_dataset_pipeline import (
    run_dataset_pipeline,
)
from data.preprocessing.dataset_prep.sample_weighting import DatasetResult
from modeling.config.model_config import ModelConfig
from modeling.train.evaluator import evaluate_model, evaluate_predictions
from modeling.train.trainer import get_feature_importance, train_model

logger = logging.getLogger(__name__)

DEFAULT_CANDIDATE_FEATURE = "max_consecutive_inactive"
_COMPARISON_METRICS = (
    "f05",
    "pr_auc",
    "precision",
    "recall",
    "f1",
    "roc_auc",
    "threshold",
)


@dataclass(frozen=True)
class FeatureAblationConfig:
    """Configuration for a baseline-versus-candidate experiment."""

    candidate_feature: str = DEFAULT_CANDIDATE_FEATURE
    report_dir: Path = Path("/data/reports/model_experiments")

    def validate(self) -> None:
        """Reject ambiguous feature ablation requests."""
        if not self.candidate_feature:
            raise ValueError("candidate_feature must not be blank")
        if self.candidate_feature in NUMERIC_FEATURES:
            raise ValueError(
                "candidate_feature is already in NUMERIC_FEATURES: "
                f"{self.candidate_feature}"
            )

    def to_safe_dict(self) -> dict:
        """Return logging-safe experiment configuration."""
        return {
            "candidate_feature": self.candidate_feature,
            "report_dir": str(self.report_dir),
        }


def run_feature_ablation(
    engine,
    *,
    pipeline_config: DatasetPipelineConfig | None = None,
    model_config: ModelConfig | None = None,
    experiment_config: FeatureAblationConfig | None = None,
) -> dict:
    """Train baseline and candidate branches without publishing a model."""
    pipeline_config = pipeline_config or DatasetPipelineConfig()
    model_config = model_config or ModelConfig()
    experiment_config = experiment_config or FeatureAblationConfig()
    pipeline_config.validate()
    model_config.validate()
    experiment_config.validate()

    run_id = _generate_run_id()
    candidate_features = [*NUMERIC_FEATURES, experiment_config.candidate_feature]
    logger.info(
        "Feature ablation %s starting: %s",
        run_id,
        experiment_config.to_safe_dict(),
    )

    dataset = run_dataset_pipeline(
        engine,
        pipeline_config,
        feature_names=candidate_features,
    )
    _validate_holdout(dataset)
    if experiment_config.candidate_feature not in dataset.feature_names:
        raise ValueError(
            "Candidate feature is unavailable after dataset preparation: "
            f"{experiment_config.candidate_feature}"
        )

    baseline_dataset = _project_dataset(dataset, NUMERIC_FEATURES)
    candidate_dataset = _project_dataset(dataset, candidate_features)
    baseline_result = _train_and_evaluate(
        "baseline",
        baseline_dataset,
        model_config,
    )
    candidate_result = _train_and_evaluate(
        "candidate",
        candidate_dataset,
        model_config,
    )
    stability = _build_holdout_stability(
        dataset,
        baseline_result.pop("_model"),
        candidate_result.pop("_model"),
    )

    report = {
        "run_id": run_id,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "status": "success",
        "model_side_effect_policy": {
            "save_bundle": False,
            "write_risk_table": False,
            "accept_reject": False,
        },
        "pipeline_config": pipeline_config.to_safe_dict(),
        "model_config": model_config.to_safe_dict(),
        "experiment_config": experiment_config.to_safe_dict(),
        "baseline": baseline_result,
        "candidate": candidate_result,
        "comparison": _compare_metrics(
            baseline_result["metrics"],
            candidate_result["metrics"],
        ),
        "holdout_stability": stability,
    }
    report_path = _write_report(report, experiment_config.report_dir)
    report["report_path"] = str(report_path)
    logger.info("Feature ablation %s completed: %s", run_id, report_path)
    return report


def _project_dataset(
    dataset: DatasetResult,
    feature_names: list[str],
) -> DatasetResult:
    available_features = [
        name for name in feature_names if name in dataset.feature_names
    ]
    return replace(
        dataset,
        x_train=dataset.x_train[available_features].copy(),
        x_eval=dataset.x_eval[available_features].copy(),
        x_predict=dataset.x_predict[available_features].copy(),
        feature_names=available_features,
    )


def _train_and_evaluate(
    branch_name: str,
    dataset: DatasetResult,
    model_config: ModelConfig,
) -> dict:
    logger.info(
        "Training experiment branch=%s features=%d",
        branch_name,
        len(dataset.feature_names),
    )
    model = train_model(dataset, model_config)
    return {
        "_model": model,
        "feature_names": dataset.feature_names,
        "feature_count": len(dataset.feature_names),
        "metrics": evaluate_model(model, dataset),
        "feature_importance": get_feature_importance(model),
    }


def _compare_metrics(baseline: dict, candidate: dict) -> dict:
    return {
        name: {
            "baseline": baseline[name],
            "candidate": candidate[name],
            "delta": candidate[name] - baseline[name],
        }
        for name in _COMPARISON_METRICS
    }


def _build_holdout_stability(
    dataset: DatasetResult,
    baseline_model: xgb.Booster,
    candidate_model: xgb.Booster,
) -> dict:
    metadata = dataset.eval_metadata
    cohort_column = "_confirmed_label_yymm"
    if metadata.empty or cohort_column not in metadata:
        return {"status": "insufficient_holdout_months", "monthly_metrics": []}

    cohort_months = metadata[cohort_column].dropna().astype(int).unique()
    monthly_metrics = []
    for cohort_month in sorted(cohort_months):
        mask = metadata[cohort_column].eq(cohort_month).to_numpy()
        monthly_metrics.append(
            {
                "label_yymm": int(cohort_month),
                "baseline": _evaluate_slice(
                    baseline_model,
                    dataset,
                    mask,
                    feature_names=list(NUMERIC_FEATURES),
                ),
                "candidate": _evaluate_slice(
                    candidate_model,
                    dataset,
                    mask,
                    feature_names=dataset.feature_names,
                ),
            }
        )

    status = (
        "available"
        if len(monthly_metrics) >= 2
        else "insufficient_holdout_months"
    )
    return {"status": status, "monthly_metrics": monthly_metrics}


def _evaluate_slice(
    model: xgb.Booster,
    dataset: DatasetResult,
    mask,
    *,
    feature_names: list[str],
) -> dict:
    available_features = [
        name for name in feature_names if name in dataset.x_eval
    ]
    matrix = xgb.DMatrix(
        dataset.x_eval.loc[mask, available_features],
        feature_names=available_features,
    )
    probabilities = model.predict(matrix)
    labels = dataset.y_eval.loc[mask].to_numpy(dtype=int)
    return evaluate_predictions(labels, probabilities)


def _validate_holdout(dataset: DatasetResult) -> None:
    if dataset.x_eval.empty or dataset.y_eval.nunique() < 2:
        raise ValueError("Feature ablation requires a complete true-label holdout")


def _write_report(report: dict, report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{report['run_id']}.json"
    report["report_path"] = str(report_path)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report_path


def _generate_run_id() -> str:
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"feature_ablation_{timestamp}_{uuid.uuid4().hex[:8]}"
