"""Tests for temporal CSKH confirmed-sample preparation."""

from __future__ import annotations

import pandas as pd

from data.preprocessing.dataset_prep.confirmed_samples import (
    build_confirmed_holdout_rows,
    build_confirmed_training_rows,
    split_confirmed_cohorts,
)


def test_split_confirmed_cohorts_reserves_latest_month_and_deduplicates_ids() -> None:
    split = split_confirmed_cohorts(
        {
            2501: {"A", "B"},
            2502: {"B", "C"},
            2503: {"C", "D"},
        }
    )

    assert split.holdout_yymm == 2503
    assert split.holdout_ids == {"C", "D"}
    assert split.training_cohorts == {2501: {"A", "B"}}


def test_build_confirmed_training_rows_loads_previous_month_snapshot(monkeypatch) -> None:
    loaded_months = []

    def fake_load_window_features(engine, window_size, end_month):
        loaded_months.append(end_month)
        return pd.DataFrame({"cms_code_enc": ["A", "B"], "feat": [1.0, 2.0]})

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.confirmed_samples.load_window_features",
        fake_load_window_features,
    )
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.confirmed_samples.compute_ewma",
        lambda df, window_size, alpha_ewma: df,
    )

    result = build_confirmed_training_rows(
        object(),
        {2503: {"A"}},
        window_size=3,
        alpha_ewma=0.3,
    )

    assert loaded_months == [pd.Timestamp("2025-02-01")]
    assert result["cms_code_enc"].tolist() == ["A"]
    assert result["label_source"].tolist() == ["confirmed"]
    assert result["y_raw"].tolist() == [1]


def test_build_confirmed_holdout_rows_uses_actual_active_negatives(monkeypatch) -> None:
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.confirmed_samples.load_window_features",
        lambda engine, window_size, end_month: pd.DataFrame(
            {"cms_code_enc": ["POS", "NEG", "UNKNOWN"], "feat": [1.0, 2.0, 3.0]}
        ),
    )
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.confirmed_samples.compute_ewma",
        lambda df, window_size, alpha_ewma: df,
    )
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.confirmed_samples.build_label",
        lambda engine, feature_end, horizon_months: pd.DataFrame(
            {"cms_code_enc": ["NEG"], "item_in_horizon": [1], "rev_in_horizon": [10]}
        ),
    )

    result = build_confirmed_holdout_rows(
        object(),
        2503,
        {"POS"},
        window_size=3,
        alpha_ewma=0.3,
    )

    assert result["cms_code_enc"].tolist() == ["POS", "NEG"]
    assert result["y_label"].tolist() == [1.0, 0.0]
    assert result["label_source"].tolist() == ["confirmed", "actual_active"]
