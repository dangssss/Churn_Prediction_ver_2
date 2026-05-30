import pytest

from features.engineering.feature_gen.window_planner import plan_incremental_windows


def _spec(window_size: int, start_yymm: str, end_yymm: str) -> dict:
    return {
        "table_name": f"data_window.cus_feature_{window_size}m_{start_yymm}_{end_yymm}",
        "window_size": window_size,
        "start_ym": start_yymm,
        "end_ym": end_yymm,
    }


def _table_names(specs: tuple[dict, ...]) -> set[str]:
    return {spec["table_name"].split(".")[-1] for spec in specs}


def test_plan_incremental_windows_classifies_new_and_kept_tables():
    specs = [
        _spec(3, "2501", "2503"),
        _spec(3, "2502", "2504"),
    ]

    plan = plan_incremental_windows(
        specs,
        existing_tables={"cus_feature_3m_2501_2503"},
        empty_tables=set(),
        retry_tables=set(),
        recompute_last_n=0,
    )

    assert _table_names(plan.keep) == {"cus_feature_3m_2501_2503"}
    assert _table_names(plan.compute_new) == {"cus_feature_3m_2502_2504"}
    assert plan.summary()["to_compute"] == 1


def test_plan_incremental_windows_recomputes_latest_tables_per_window_size():
    specs = [
        _spec(3, "2501", "2503"),
        _spec(3, "2502", "2504"),
        _spec(3, "2503", "2505"),
        _spec(4, "2501", "2504"),
        _spec(4, "2502", "2505"),
    ]
    existing_tables = _table_names(tuple(specs))

    plan = plan_incremental_windows(
        specs,
        existing_tables=existing_tables,
        empty_tables=set(),
        retry_tables=set(),
        recompute_last_n=1,
    )

    assert _table_names(plan.recompute_recent) == {
        "cus_feature_3m_2503_2505",
        "cus_feature_4m_2502_2505",
    }


def test_plan_incremental_windows_classifies_empty_before_recent():
    specs = [
        _spec(3, "2501", "2503"),
        _spec(3, "2502", "2504"),
    ]
    existing_tables = _table_names(tuple(specs))

    plan = plan_incremental_windows(
        specs,
        existing_tables=existing_tables,
        empty_tables={"cus_feature_3m_2502_2504"},
        retry_tables=set(),
        recompute_last_n=1,
    )

    assert _table_names(plan.recompute_empty) == {"cus_feature_3m_2502_2504"}
    assert plan.recompute_recent == ()


def test_plan_incremental_windows_classifies_retry_before_recent():
    specs = [
        _spec(3, "2501", "2503"),
        _spec(3, "2502", "2504"),
    ]
    existing_tables = _table_names(tuple(specs))

    plan = plan_incremental_windows(
        specs,
        existing_tables=existing_tables,
        empty_tables=set(),
        retry_tables={"cus_feature_3m_2502_2504"},
        recompute_last_n=1,
    )

    assert _table_names(plan.recompute_retry) == {"cus_feature_3m_2502_2504"}
    assert plan.recompute_recent == ()


def test_plan_incremental_windows_does_not_recompute_old_table_when_latest_is_new():
    specs = [
        _spec(3, "2501", "2503"),
        _spec(3, "2502", "2504"),
        _spec(3, "2503", "2505"),
    ]

    plan = plan_incremental_windows(
        specs,
        existing_tables={
            "cus_feature_3m_2501_2503",
            "cus_feature_3m_2502_2504",
        },
        empty_tables=set(),
        retry_tables=set(),
        recompute_last_n=1,
    )

    assert _table_names(plan.compute_new) == {"cus_feature_3m_2503_2505"}
    assert plan.recompute_recent == ()


def test_plan_incremental_windows_rejects_negative_recompute_count():
    with pytest.raises(ValueError, match="recompute_last_n"):
        plan_incremental_windows([], set(), set(), set(), recompute_last_n=-1)
