"""Tests for data-driven pseudo-label calibration."""

from __future__ import annotations

import pandas as pd

from data.preprocessing.dataset_prep.label_calibration import (
    calibrate_label_weights,
    calibrate_pseudo_label_thresholds,
)


def test_calibrate_pseudo_label_thresholds_uses_population_quantiles(monkeypatch) -> None:
    active_df = pd.DataFrame(
        {
            "recency_days": [1, 2, 3, 4],
            "item_avg": [10.0, 10.0, 10.0, 10.0],
            "item_last": [1.0, 2.0, 3.0, 4.0],
        }
    )
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.label_calibration.compute_similarity",
        lambda df, prototype: pd.Series([0.1, 0.2, 0.3, 0.4]),
    )

    scores, thresholds = calibrate_pseudo_label_thresholds(
        active_df,
        {},
        similarity_quantile=0.75,
        reliable_neg_recency_quantile=0.25,
        trend_down_quantile=0.50,
    )

    assert scores.tolist() == [0.1, 0.2, 0.3, 0.4]
    assert thresholds.sim_threshold == 0.325
    assert thresholds.recency_reliable_neg == 2
    assert thresholds.trend_down_ratio == 0.25


def test_calibrate_label_weights_prioritizes_confirmed_rows() -> None:
    weights = calibrate_label_weights(
        pd.Series(["pseudo_churn"] * 20 + ["reliable_neg"] * 40 + ["pu_unlabeled"] * 100),
        n_confirmed_training=10,
        n_rule_based=200,
        min_aux_weight=0.01,
        max_aux_weight=0.80,
    )

    assert weights.confirmed == 1.0
    assert weights.pseudo_churn == 0.5
    assert weights.reliable_neg == 0.25
    assert weights.pu_unlabeled == 0.1
    assert weights.rule_based == 0.05
