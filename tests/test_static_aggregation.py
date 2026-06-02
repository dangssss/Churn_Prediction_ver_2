"""Tests for monthly lifetime snapshot orchestration."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from features.engineering.feature_gen.run_feature_generation import (
    _get_static_recompute_last_n,
)
from features.engineering.feature_gen.static_aggregation import run_lifetime_snapshots


def test_static_recompute_tail_accepts_operational_zero_override(monkeypatch) -> None:
    monkeypatch.setenv("FEATURE_STATIC_RECOMPUTE_LAST_N", "0")

    assert _get_static_recompute_last_n(2) == 0


def test_static_recompute_tail_rejects_negative_override(monkeypatch) -> None:
    monkeypatch.setenv("FEATURE_STATIC_RECOMPUTE_LAST_N", "-1")

    with pytest.raises(ValueError, match="must be >= 0"):
        _get_static_recompute_last_n(2)


def test_run_lifetime_snapshots_uses_month_end_cutoffs(monkeypatch) -> None:
    cutoffs = []

    monkeypatch.setattr(
        "features.engineering.feature_gen.static_aggregation.create_bccp_indexes",
        lambda engine: None,
    )
    monkeypatch.setattr(
        "features.engineering.feature_gen.static_aggregation.run_static_aggregate",
        lambda engine, as_of_date, create_indexes: cutoffs.append(
            (as_of_date, create_indexes)
        ),
    )
    monkeypatch.setattr(
        "features.engineering.feature_gen.static_aggregation._sync_latest_lifetime",
        lambda engine, snapshot_month: None,
    )

    run_lifetime_snapshots(
        object(),
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2025-03-12"),
    )

    assert cutoffs == [
        (pd.Timestamp("2025-01-31"), False),
        (pd.Timestamp("2025-02-28"), False),
        (pd.Timestamp("2025-03-12"), False),
    ]


def test_incremental_lifetime_snapshots_skip_history_and_recompute_tail(monkeypatch) -> None:
    cutoffs = []
    synced_months = []

    monkeypatch.setattr(
        "features.engineering.feature_gen.static_aggregation.create_bccp_indexes",
        lambda engine: None,
    )
    monkeypatch.setattr(
        "features.engineering.feature_gen.static_aggregation._existing_snapshot_months",
        lambda engine: {
            pd.Timestamp("2025-01-01"),
            pd.Timestamp("2025-02-01"),
            pd.Timestamp("2025-03-01"),
        },
    )
    monkeypatch.setattr(
        "features.engineering.feature_gen.static_aggregation.run_static_aggregate",
        lambda engine, as_of_date, create_indexes: cutoffs.append(as_of_date),
    )
    monkeypatch.setattr(
        "features.engineering.feature_gen.static_aggregation._sync_latest_lifetime",
        lambda engine, snapshot_month: synced_months.append(snapshot_month),
    )

    run_lifetime_snapshots(
        object(),
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2025-04-01"),
        incremental=True,
        recompute_last_n=2,
    )

    assert cutoffs == [
        pd.Timestamp("2025-03-31"),
        pd.Timestamp("2025-04-01"),
    ]
    assert synced_months == [pd.Timestamp("2025-04-01")]


def test_lifetime_snapshot_recomputes_tenure_at_cutoff() -> None:
    sql_path = (
        Path(__file__).parents[1]
        / "src/features/engineering/database/sql/data_static/lifetime_aggregate.sql"
    )
    sql = sql_path.read_text(encoding="utf-8")

    assert "COALESCE(i.tenure," not in sql
    assert "EXTRACT(year from age(DATE '{AS_OF_DATE}'" in sql
    assert "SUM(GREATEST(total_fee, 0))" in sql
