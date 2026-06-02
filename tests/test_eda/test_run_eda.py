"""Tests for bounded temporal snapshot loading."""

from __future__ import annotations

import importlib

import pytest

from data.eda.config import EdaConfig
from data.eda.run_eda import _build_temporal_sample_query, _load_feature_data

run_eda_module = importlib.import_module("data.eda.run_eda")


def test_should_build_bounded_repeatable_query_for_temporal_snapshot() -> None:
    config = EdaConfig(temporal_sample_rows=1234, temporal_sample_percent=12.5)

    query = _build_temporal_sample_query("cus_feature_9m_2509_2605", config)

    assert "TABLESAMPLE SYSTEM (12.5)" in query
    assert "REPEATABLE (42)" in query
    assert query.endswith("LIMIT 1234")


def test_should_reject_unsafe_temporal_snapshot_table_name() -> None:
    with pytest.raises(ValueError, match="Unsafe temporal snapshot"):
        _build_temporal_sample_query("snapshot; DROP TABLE x", EdaConfig())


def test_should_load_sampled_latest_window_for_primary_eda(monkeypatch) -> None:
    config = EdaConfig(temporal_sample_rows=123)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_eda_module,
        "_find_latest_feature_snapshot",
        lambda *_: "cus_feature_9m_2509_2605",
    )

    def fake_read_sql(query: str, connection: object) -> str:
        captured["query"] = query
        captured["connection"] = connection
        return "sampled-frame"

    monkeypatch.setattr(run_eda_module.pd, "read_sql", fake_read_sql)

    class _Connection:
        def __enter__(self) -> object:
            return "connection"

        def __exit__(self, *_: object) -> None:
            return None

    class _Engine:
        def connect(self) -> _Connection:
            return _Connection()

    result = _load_feature_data(_Engine(), config, window_end=None)  # type: ignore[arg-type]

    assert result == "sampled-frame"
    assert "cus_feature_9m_2509_2605" in str(captured["query"])
    assert str(captured["query"]).endswith("LIMIT 123")
