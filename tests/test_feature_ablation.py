"""Tests for baseline-versus-candidate feature ablation."""

from __future__ import annotations

import pandas as pd
import pytest
from sklearn.preprocessing import StandardScaler

from data.preprocessing.dataset_prep.pipeline_config import NUMERIC_FEATURES
from data.preprocessing.dataset_prep.sample_weighting import DatasetResult
from modeling.experiments.feature_ablation import (
    FeatureAblationConfig,
    _compare_metrics,
    _project_dataset,
)


def _dataset() -> DatasetResult:
    feature_names = [*NUMERIC_FEATURES, "max_consecutive_inactive"]
    frame = pd.DataFrame({name: [0.0, 1.0] for name in feature_names})
    return DatasetResult(
        x_train=frame.copy(),
        y_train=pd.Series([0.0, 1.0]),
        w_train=pd.Series([1.0, 1.0]),
        x_eval=frame.copy(),
        y_eval=pd.Series([0.0, 1.0]),
        x_predict=frame.copy(),
        scaler=StandardScaler(),
        feature_names=feature_names,
        active_df=pd.DataFrame({"cms_code_enc": ["A", "B"]}),
    )


def test_should_remove_candidate_feature_from_baseline_projection() -> None:
    projected = _project_dataset(_dataset(), list(NUMERIC_FEATURES))

    assert projected.feature_names == NUMERIC_FEATURES
    assert "max_consecutive_inactive" not in projected.x_train


def test_should_keep_candidate_feature_in_candidate_projection() -> None:
    feature_names = [*NUMERIC_FEATURES, "max_consecutive_inactive"]

    projected = _project_dataset(_dataset(), feature_names)

    assert projected.feature_names == feature_names
    assert "max_consecutive_inactive" in projected.x_train


def test_should_report_metric_delta_for_candidate() -> None:
    baseline = {
        "f05": 0.5,
        "pr_auc": 0.4,
        "precision": 0.6,
        "recall": 0.3,
        "f1": 0.4,
        "roc_auc": 0.7,
        "threshold": 0.8,
    }
    candidate = {name: value + 0.1 for name, value in baseline.items()}

    comparison = _compare_metrics(baseline, candidate)

    assert comparison["f05"]["delta"] == pytest.approx(0.1)


def test_should_reject_candidate_already_in_production_features() -> None:
    config = FeatureAblationConfig(candidate_feature="item_sum")

    with pytest.raises(ValueError, match="already in NUMERIC_FEATURES"):
        config.validate()
