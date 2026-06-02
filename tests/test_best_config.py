"""Tests for the canonical v2 model best-config persistence contract."""

from __future__ import annotations

from modeling.config_store.best_config import (
    ensure_best_config_table,
    upsert_best_config,
)


class _FakeConnection:
    def __init__(self, calls: list[tuple[str, dict | None]]) -> None:
        self._calls = calls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql, params=None):
        self._calls.append((str(sql), params))


class _FakeEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []

    def begin(self):
        return _FakeConnection(self.calls)


def test_ensure_best_config_table_uses_only_canonical_v2_metrics() -> None:
    engine = _FakeEngine()

    ensure_best_config_table(engine)

    sql = "\n".join(statement for statement, _ in engine.calls)
    assert "metric_f05_val" in sql
    assert "metric_pr_auc_val" in sql
    assert "prev_accepted_f05" in sql
    assert "DROP COLUMN IF EXISTS metric_f2_val" in sql
    assert "DROP COLUMN IF EXISTS metric_roc_auc_val" in sql


def test_upsert_best_config_persists_f05_contract() -> None:
    engine = _FakeEngine()
    record = {
        "as_of_month": 2503,
        "horizon": 1,
        "best_k": 3,
        "use_static": True,
        "best_threshold": 0.7,
        "best_spw": 1.0,
        "metric_f05_val": 0.4,
        "metric_pr_auc_val": 0.3,
        "val_month": 2502,
        "target_month": 2503,
        "notes": "test",
        "is_accepted": True,
        "prev_accepted_f05": 0.35,
        "accept_rule": "accepted_f05_improved",
        "accepted_at": "2025-03-01T00:00:00",
    }

    upsert_best_config(engine, record)

    sql, params = engine.calls[-1]
    assert "metric_f05_val" in sql
    assert "metric_pr_auc_val" in sql
    assert "prev_accepted_f05" in sql
    assert "metric_f2_val" not in sql
    assert params == record
