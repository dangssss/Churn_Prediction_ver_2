"""Tests for point-in-time feature window loading."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy.sql.elements import TextClause

from data.preprocessing.dataset_prep.label_construction import (
    build_label,
    build_training_windows,
    load_window_features,
)


def test_load_window_features_merges_matching_lifetime_snapshot(monkeypatch) -> None:
    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    calls = []

    def fake_read_sql(sql, conn, params=None):
        calls.append((sql, params))
        if len(calls) == 1:
            return pd.DataFrame({"cms_code_enc": ["CMS001"], "item_sum": [5]})
        return pd.DataFrame(
            {
                "snapshot_month": [pd.Timestamp("2025-03-01")],
                "cms_code_enc": ["CMS001"],
                "lifetime_total_items": [12],
            }
        )

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.label_construction.pd.read_sql",
        fake_read_sql,
    )

    result = load_window_features(FakeEngine(), 3, pd.Timestamp("2025-03-01"))

    assert result["lifetime_total_items"].tolist() == [12]
    assert isinstance(calls[1][0], TextClause)
    assert calls[1][1] == {"snapshot_month": pd.Timestamp("2025-03-01")}


def test_build_label_queries_exact_next_month(monkeypatch) -> None:
    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    calls = []

    def fake_read_sql(sql, conn, params=None):
        calls.append((str(sql), params))
        return pd.DataFrame(
            {
                "cms_code_enc": ["CMS001"],
                "item_in_horizon": [1],
                "rev_in_horizon": [10],
            }
        )

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.label_construction.pd.read_sql",
        fake_read_sql,
    )

    result = build_label(FakeEngine(), pd.Timestamp("2025-03-01"), horizon_months=1)

    assert result["cms_code_enc"].tolist() == ["CMS001"]
    assert calls[0][1] == {
        "label_start": "2025-04-01",
        "label_end": "2025-04-01",
    }


def test_build_label_rejects_multi_month_horizon() -> None:
    with pytest.raises(ValueError, match="next-month inactivity target"):
        build_label(object(), pd.Timestamp("2025-03-01"), horizon_months=2)


def test_build_training_windows_rejects_multi_month_horizon() -> None:
    with pytest.raises(ValueError, match="next-month inactivity target"):
        build_training_windows(
            object(),
            window_size=3,
            all_months=pd.date_range("2025-01-01", periods=6, freq="MS"),
            horizon_months=2,
            alpha_ewma=0.5,
        )
