"""Tests for point-in-time activity tiering."""

from __future__ import annotations

import pandas as pd

from data.preprocessing.dataset_prep.activity_tiering import compute_recency


def test_compute_recency_excludes_activity_at_or_after_observation_month(monkeypatch) -> None:
    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    captured = {}

    def fake_read_sql(sql, conn, params):
        captured["sql"] = str(sql)
        captured["params"] = params
        return pd.DataFrame(
            {
                "cms_code_enc": ["CMS001"],
                "last_active_month": [pd.Timestamp("2025-03-01")],
                "recency_days": [31],
            }
        )

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.activity_tiering.pd.read_sql",
        fake_read_sql,
    )

    result = compute_recency(
        FakeEngine(),
        pd.DataFrame({"cms_code_enc": ["CMS001"]}),
        pd.Timestamp("2025-04-01"),
        pd.Timestamp("2025-01-01"),
    )

    assert result["recency_days"].tolist() == [31]
    assert "AND report_month < :t_obs" in captured["sql"]
