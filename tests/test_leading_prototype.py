"""Tests for time-aligned leading prototype construction."""

from __future__ import annotations

import pandas as pd
import pytest

from data.preprocessing.dataset_prep.leading_prototype import (
    _parse_yymm,
    build_leading_prototype,
)


def test_parse_yymm_handles_year_boundary() -> None:
    assert _parse_yymm(2501) == pd.Timestamp("2025-01-01")
    assert _parse_yymm(2412) == pd.Timestamp("2024-12-01")


def test_parse_yymm_rejects_invalid_month() -> None:
    with pytest.raises(ValueError, match="Invalid label_yymm"):
        _parse_yymm(2513)


def test_build_leading_prototype_loads_snapshot_for_each_label_cohort(monkeypatch) -> None:
    loaded_months = []

    def fake_load_window_features(engine, window_size, end_month):
        loaded_months.append(end_month)
        if end_month == pd.Timestamp("2025-01-01"):
            return pd.DataFrame(
                {"cms_code_enc": ["CMS001", "CMS002"], "item_sum": [1.0, 3.0]}
            )
        return pd.DataFrame(
            {"cms_code_enc": ["CMS002", "CMS003"], "item_sum": [100.0, 5.0]}
        )

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.leading_prototype.load_window_features",
        fake_load_window_features,
    )
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.leading_prototype.compute_ewma",
        lambda df, window_size, alpha_ewma: df,
    )

    prototype = build_leading_prototype(
        object(),
        {2503: {"CMS001", "CMS002"}, 2504: {"CMS002", "CMS003"}},
        window_size=3,
        alpha_ewma=0.5,
        sigma_reg=0.1,
        lead_offset=2,
        min_prototype_samples=1,
    )

    assert loaded_months == [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-02-01")]
    assert prototype["n_confirmed"] == 3
    assert prototype["feature_names"] == ["item_sum"]
    assert prototype["mu"].tolist() == [3.0]


def test_build_leading_prototype_skips_missing_cohort_window(monkeypatch) -> None:
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.leading_prototype.load_window_features",
        lambda engine, window_size, end_month: pd.DataFrame(),
    )

    prototype = build_leading_prototype(
        object(),
        {2503: {"CMS001"}},
        window_size=3,
        alpha_ewma=0.5,
        sigma_reg=0.1,
        min_prototype_samples=1,
    )

    assert prototype == {}


def test_build_leading_prototype_defaults_to_previous_month(monkeypatch) -> None:
    loaded_months = []

    def fake_load_window_features(engine, window_size, end_month):
        loaded_months.append(end_month)
        return pd.DataFrame(
            {"cms_code_enc": ["CMS001", "CMS002"], "item_sum": [1.0, 2.0]}
        )

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.leading_prototype.load_window_features",
        fake_load_window_features,
    )
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.leading_prototype.compute_ewma",
        lambda df, window_size, alpha_ewma: df,
    )

    build_leading_prototype(
        object(),
        {2503: {"CMS001", "CMS002"}},
        window_size=3,
        alpha_ewma=0.5,
        sigma_reg=0.1,
        min_prototype_samples=1,
    )

    assert loaded_months == [pd.Timestamp("2025-02-01")]
