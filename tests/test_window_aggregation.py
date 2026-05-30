from features.engineering.feature_gen import window_aggregation


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
