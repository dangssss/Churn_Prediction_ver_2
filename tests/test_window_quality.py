from features.engineering.feature_gen.window_quality import (
    BatchConsistencyMetrics,
    LifetimeQualityMetrics,
    WindowQualityMetrics,
    _audit_params,
    _nested_window_pairs,
    _sanitize_json_value,
    _window_days,
    evaluate_batch_consistency,
    evaluate_lifetime_quality,
    evaluate_window_quality,
    metrics_from_mapping,
)


def _metrics(**overrides) -> WindowQualityMetrics:
    values = {
        "row_count": 10,
        "critical_null_rows": 0,
        "critical_null_rate": 0.0,
        "non_finite_rows": 0,
        "ratio_out_of_range_rows": 0,
        "volume_out_of_range_rows": 0,
        "activity_out_of_range_rows": 0,
        "metadata_mismatch_rows": 0,
        "summary": {},
    }
    values.update(overrides)
    return WindowQualityMetrics(**values)


def test_evaluate_window_quality_accepts_clean_metrics():
    assert evaluate_window_quality(_metrics()) == []


def test_evaluate_window_quality_reports_each_failed_gate():
    metrics = _metrics(
        critical_null_rows=2,
        non_finite_rows=1,
        ratio_out_of_range_rows=3,
        volume_out_of_range_rows=4,
        activity_out_of_range_rows=5,
        metadata_mismatch_rows=6,
    )

    assert evaluate_window_quality(metrics) == [
        "critical_null_rows=2",
        "non_finite_rows=1",
        "ratio_out_of_range_rows=3",
        "volume_out_of_range_rows=4",
        "activity_out_of_range_rows=5",
        "metadata_mismatch_rows=6",
    ]


def test_evaluate_window_quality_rejects_empty_window():
    assert evaluate_window_quality(_metrics(row_count=0)) == [
        "window table is empty"
    ]


def test_window_days_is_inclusive():
    assert _window_days("2025-01-01", "2025-02-28") == 59


def test_audit_params_do_not_require_window_dates():
    assert _audit_params(
        {
            "table_name": "__batch_consistency__",
            "window_size": 0,
            "start_ym": "2501",
            "end_ym": "2503",
        }
    ) == {
        "window_table": "__batch_consistency__",
        "window_size": 0,
        "start_yymm": "2501",
        "end_yymm": "2503",
    }


def test_metrics_from_mapping_builds_summary():
    row = {
        "row_count": 2,
        "critical_null_rows": 0,
        "non_finite_rows": 0,
        "ratio_out_of_range_rows": 0,
        "volume_out_of_range_rows": 0,
        "activity_out_of_range_rows": 0,
        "metadata_mismatch_rows": 0,
    }
    for column in (
        "item_sum",
        "revenue_sum",
        "complaint_sum",
        "active_months",
        "active_days",
        "inactive_days",
        "recency",
        "frequency",
        "monetary",
    ):
        row[f"{column}_min"] = 0
        row[f"{column}_max"] = 2
        row[f"{column}_avg"] = 1.0
        row[f"{column}_p50"] = 1.0

    metrics = metrics_from_mapping(row)

    assert metrics.row_count == 2
    assert metrics.critical_null_rate == 0.0
    assert metrics.summary["recency_min"] == 0
    assert metrics.summary["recency_max"] == 2
    assert metrics.summary["recency_p50"] == 1.0


def test_sanitize_json_value_converts_non_finite_numbers():
    assert _sanitize_json_value(
        {"minimum": float("-inf"), "maximum": float("inf"), "average": float("nan")}
    ) == {
        "minimum": "-inf",
        "maximum": "inf",
        "average": "nan",
    }


def test_evaluate_batch_consistency_reports_failed_checks():
    metrics = BatchConsistencyMetrics(
        compared_window_pairs=2,
        cross_window_violation_rows=3,
        lifetime_violation_rows=4,
        missing_lifetime_rows=5,
    )

    assert evaluate_batch_consistency(metrics) == [
        "cross_window_violation_rows=3",
        "lifetime_violation_rows=4",
        "missing_lifetime_rows=5",
    ]


def test_evaluate_lifetime_quality_reports_failed_checks():
    metrics = LifetimeQualityMetrics(
        row_count=10,
        distinct_customer_count=9,
        blank_customer_rows=1,
        critical_null_rows=2,
        non_finite_rows=3,
        ratio_out_of_range_rows=4,
        volume_out_of_range_rows=5,
        activity_out_of_range_rows=6,
        summary={},
    )

    assert evaluate_lifetime_quality(metrics) == [
        "duplicate cms_code_enc rows=1",
        "blank_customer_rows=1",
        "critical_null_rows=2",
        "non_finite_rows=3",
        "ratio_out_of_range_rows=4",
        "volume_out_of_range_rows=5",
        "activity_out_of_range_rows=6",
    ]


def test_nested_window_pairs_compares_adjacent_sizes_for_each_end_month():
    specs = [
        {"table_name": "data_window.cus_feature_3m_2501_2503", "window_size": 3, "end_ym": "2503"},
        {"table_name": "data_window.cus_feature_4m_2501_2504", "window_size": 4, "end_ym": "2504"},
        {"table_name": "data_window.cus_feature_3m_2502_2504", "window_size": 3, "end_ym": "2504"},
        {"table_name": "data_window.cus_feature_5m_2501_2505", "window_size": 5, "end_ym": "2505"},
    ]

    assert _nested_window_pairs(specs) == [
        (specs[2], specs[1]),
    ]
