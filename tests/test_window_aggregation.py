from pathlib import Path

import pytest

from features.engineering.feature_gen import window_aggregation


def test_parallel_workers_default_to_conservative_value(monkeypatch) -> None:
    monkeypatch.delenv("FEATURE_MAX_PARALLEL_WORKERS", raising=False)

    assert window_aggregation._get_max_parallel_workers() == 2


def test_parallel_workers_reject_non_positive_override(monkeypatch) -> None:
    monkeypatch.setenv("FEATURE_MAX_PARALLEL_WORKERS", "0")

    with pytest.raises(ValueError, match="must be >= 1"):
        window_aggregation._get_max_parallel_workers()


def test_staging_neutralizes_negative_total_fee() -> None:
    source = Path(window_aggregation.__file__).read_text(encoding="utf-8")

    assert "SUM(GREATEST(total_fee, 0))::bigint AS revenue_sum" in source
    assert "Neutralizing %d cas_customer rows with negative total_fee" in source
    assert source.count("SET (autovacuum_enabled = false)") == 3


def test_validate_kept_windows_runs_basic_validation_before_quality(monkeypatch):
    calls = []
    specs = (
        {
            "table_name": "data_window.cus_feature_3m_2501_2503",
            "window_size": 3,
            "start_ym": "2501",
            "end_ym": "2503",
        },
    )

    monkeypatch.setattr(
        window_aggregation,
        "validate_window_table",
        lambda engine, table_name: calls.append(("basic", table_name)),
    )
    monkeypatch.setattr(
        window_aggregation,
        "validate_and_record_window_quality",
        lambda engine, run_id, spec: calls.append(("quality", spec["table_name"])),
    )

    window_aggregation._validate_kept_windows(object(), "run-1", specs)

    assert calls == [
        ("basic", "data_window.cus_feature_3m_2501_2503"),
        ("quality", "data_window.cus_feature_3m_2501_2503"),
    ]
