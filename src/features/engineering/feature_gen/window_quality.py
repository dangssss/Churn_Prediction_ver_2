"""Quality gates and persisted audit metrics for generated feature windows."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import text

from features.engineering.feature_gen.window_manifest import (
    _validate_qualified_window_name,
)

QUALITY_TABLE = "data_static.feature_generation_quality"

RATIO_COLUMNS = (
    "pct_delay",
    "pct_refund",
    "pct_noaccepted",
    "pct_lost_order",
    "pct_complaint_per_item",
    "pct_successful_item",
    "pct_intra_province",
    "pct_international",
    "dominant_service_ratio",
)

NON_NEGATIVE_RATE_COLUMNS = ("pct_complaint",)

CRITICAL_COLUMNS = (
    "item_sum",
    "revenue_sum",
    "complaint_sum",
    "active_months",
    "inactive_months",
    "active_days",
    "inactive_days",
    "avg_noservice_days",
    "max_consecutive_inactive",
    "recency",
    "frequency",
    "monetary",
    *RATIO_COLUMNS,
)

NON_FINITE_COLUMNS = (
    "item_avg",
    "revenue_avg",
    "complaint_avg",
    "avg_noservice_days",
    "frequency",
    "monetary",
    *RATIO_COLUMNS,
)

SUMMARY_COLUMNS = (
    "item_sum",
    "revenue_sum",
    "complaint_sum",
    "active_months",
    "active_days",
    "inactive_days",
    "avg_noservice_days",
    "max_consecutive_inactive",
    "recency",
    "frequency",
    "monetary",
)

LIFETIME_RATIO_COLUMNS = (
    "lifetime_pct_delay",
    "lifetime_pct_refund",
    "lifetime_pct_noaccepted",
    "lifetime_pct_lost_order",
    "lifetime_pct_complaint_per_item",
    "lifetime_pct_successful_item",
    "lifetime_pct_international",
    "lifetime_pct_intra_province",
)

LIFETIME_NON_NEGATIVE_RATE_COLUMNS = ("lifetime_pct_complaint",)

LIFETIME_NON_FINITE_COLUMNS = (
    "lifetime_total_weight",
    "lifetime_avg_revenue_per_item",
    "lifetime_avg_weight_per_item",
    "lifetime_avg_delayday",
    "lifetime_avg_order_score",
    "lifetime_avg_satisfaction",
    *LIFETIME_RATIO_COLUMNS,
)


@dataclass(frozen=True)
class WindowQualityMetrics:
    """Aggregated quality metrics for one generated feature-window table."""

    row_count: int
    critical_null_rows: int
    critical_null_rate: float
    non_finite_rows: int
    ratio_out_of_range_rows: int
    volume_out_of_range_rows: int
    activity_out_of_range_rows: int
    metadata_mismatch_rows: int
    summary: dict[str, float | int | None]


@dataclass(frozen=True)
class BatchConsistencyMetrics:
    """Cross-table consistency metrics for one feature-generation batch."""

    compared_window_pairs: int
    cross_window_violation_rows: int
    lifetime_violation_rows: int
    missing_lifetime_rows: int


@dataclass(frozen=True)
class LifetimeQualityMetrics:
    """Aggregated quality metrics for the static lifetime feature table."""

    row_count: int
    distinct_customer_count: int
    blank_customer_rows: int
    critical_null_rows: int
    non_finite_rows: int
    ratio_out_of_range_rows: int
    volume_out_of_range_rows: int
    activity_out_of_range_rows: int
    summary: dict[str, float | int | None]


def ensure_window_quality_table(engine: Any) -> None:
    """Create the feature-window quality audit table idempotently."""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_TABLE} (
            run_id          VARCHAR(100) NOT NULL,
            window_table    VARCHAR(255) NOT NULL,
            window_size     INT NOT NULL,
            start_yymm      VARCHAR(4) NOT NULL,
            end_yymm        VARCHAR(4) NOT NULL,
            status          VARCHAR(20) NOT NULL,
            violations      JSONB NOT NULL,
            metrics         JSONB NOT NULL,
            checked_at      TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_id, window_table)
        )
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS data_static"))
        conn.execute(text(ddl))
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_feature_quality_table_status "
                f"ON {QUALITY_TABLE}(window_table, status)"
            )
        )


def validate_and_record_window_quality(
    engine: Any,
    run_id: str,
    spec: dict,
) -> WindowQualityMetrics:
    """Collect, persist, and enforce quality metrics for one feature window."""
    metrics = collect_window_quality_metrics(engine, spec)
    violations = evaluate_window_quality(metrics)
    record_window_quality(engine, run_id, spec, metrics, violations)
    if violations:
        raise ValueError(
            f"Window quality validation failed for {spec['table_name']}: "
            f"{'; '.join(violations)}"
        )
    return metrics


def validate_and_record_lifetime_quality(
    engine: Any,
    run_id: str,
) -> LifetimeQualityMetrics:
    """Collect, persist, and enforce quality metrics for lifetime features."""
    metrics = collect_lifetime_quality_metrics(engine)
    violations = evaluate_lifetime_quality(metrics)
    synthetic_spec = {
        "table_name": "__lifetime__",
        "window_size": 0,
        "start_ym": "2501",
        "end_ym": datetime.now().strftime("%y%m"),
    }
    _record_quality_artifact(engine, run_id, synthetic_spec, metrics, violations)
    if violations:
        raise ValueError(
            "Lifetime quality validation failed: "
            f"{'; '.join(violations)}"
        )
    return metrics


def collect_lifetime_quality_metrics(engine: Any) -> LifetimeQualityMetrics:
    """Collect one-row SQL quality summary for static lifetime features."""
    sql = text(f"""
        SELECT
            COUNT(*)::bigint AS row_count,
            COUNT(DISTINCT cms_code_enc)::bigint AS distinct_customer_count,
            COUNT(*) FILTER (
                WHERE cms_code_enc IS NULL OR TRIM(cms_code_enc) = ''
            )::bigint AS blank_customer_rows,
            COUNT(*) FILTER (
                WHERE lifetime_total_items IS NULL
                   OR lifetime_total_revenue IS NULL
                   OR lifetime_total_weight IS NULL
                   OR lifetime_total_complaint IS NULL
                   OR lifetime_months_active IS NULL
                   OR lifetime_days_active IS NULL
                   OR {_join_conditions(LIFETIME_RATIO_COLUMNS, "IS NULL")}
            )::bigint AS critical_null_rows,
            COUNT(*) FILTER (
                WHERE {_non_finite_condition(LIFETIME_NON_FINITE_COLUMNS)}
            )::bigint AS non_finite_rows,
            COUNT(*) FILTER (
                WHERE {_ratio_condition(LIFETIME_RATIO_COLUMNS)}
                   OR {_negative_condition(LIFETIME_NON_NEGATIVE_RATE_COLUMNS)}
            )::bigint AS ratio_out_of_range_rows,
            COUNT(*) FILTER (
                WHERE lifetime_total_items < 0
                   OR lifetime_total_revenue < 0
                   OR lifetime_total_complaint < 0
            )::bigint AS volume_out_of_range_rows,
            COUNT(*) FILTER (
                WHERE lifetime_months_active < 0
                   OR lifetime_months_active > (
                       EXTRACT(YEAR FROM age(CURRENT_DATE, DATE '2025-01-01'))::int * 12
                       + EXTRACT(MONTH FROM age(CURRENT_DATE, DATE '2025-01-01'))::int
                       + 1
                   )
                   OR lifetime_days_active < 0
                   OR lifetime_days_active > (CURRENT_DATE - DATE '2025-01-01' + 1)
            )::bigint AS activity_out_of_range_rows,
            MIN(lifetime_total_items) AS lifetime_total_items_min,
            MAX(lifetime_total_items) AS lifetime_total_items_max,
            AVG(lifetime_total_items)::double precision AS lifetime_total_items_avg,
            MIN(lifetime_total_revenue) AS lifetime_total_revenue_min,
            MAX(lifetime_total_revenue) AS lifetime_total_revenue_max,
            AVG(lifetime_total_revenue)::double precision AS lifetime_total_revenue_avg,
            MIN(lifetime_months_active) AS lifetime_months_active_min,
            MAX(lifetime_months_active) AS lifetime_months_active_max,
            AVG(lifetime_months_active)::double precision AS lifetime_months_active_avg
        FROM data_static.cus_lifetime
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().one()
    return lifetime_metrics_from_mapping(row)


def lifetime_metrics_from_mapping(row: dict) -> LifetimeQualityMetrics:
    """Build typed lifetime quality metrics from a SQL mapping."""
    summary_keys = (
        "lifetime_total_items_min",
        "lifetime_total_items_max",
        "lifetime_total_items_avg",
        "lifetime_total_revenue_min",
        "lifetime_total_revenue_max",
        "lifetime_total_revenue_avg",
        "lifetime_months_active_min",
        "lifetime_months_active_max",
        "lifetime_months_active_avg",
    )
    return LifetimeQualityMetrics(
        row_count=int(row["row_count"]),
        distinct_customer_count=int(row["distinct_customer_count"]),
        blank_customer_rows=int(row["blank_customer_rows"]),
        critical_null_rows=int(row["critical_null_rows"]),
        non_finite_rows=int(row["non_finite_rows"]),
        ratio_out_of_range_rows=int(row["ratio_out_of_range_rows"]),
        volume_out_of_range_rows=int(row["volume_out_of_range_rows"]),
        activity_out_of_range_rows=int(row["activity_out_of_range_rows"]),
        summary={key: row[key] for key in summary_keys},
    )


def evaluate_lifetime_quality(metrics: LifetimeQualityMetrics) -> list[str]:
    """Return violations for static lifetime feature quality."""
    violations = []
    if metrics.row_count == 0:
        violations.append("lifetime table is empty")
    if metrics.row_count != metrics.distinct_customer_count:
        violations.append(
            "duplicate cms_code_enc rows="
            f"{metrics.row_count - metrics.distinct_customer_count}"
        )
    for field_name in (
        "blank_customer_rows",
        "critical_null_rows",
        "non_finite_rows",
        "ratio_out_of_range_rows",
        "volume_out_of_range_rows",
        "activity_out_of_range_rows",
    ):
        value = getattr(metrics, field_name)
        if value > 0:
            violations.append(f"{field_name}={value}")
    return violations


def collect_window_quality_metrics(engine: Any, spec: dict) -> WindowQualityMetrics:
    """Collect one-row SQL quality summary for a generated window table."""
    safe_name = _validate_qualified_window_name(spec["table_name"])
    summary_selects = ",\n            ".join(
        f"MIN({column}) AS {column}_min, "
        f"MAX({column}) AS {column}_max, "
        f"AVG({column})::double precision AS {column}_avg, "
        f"percentile_cont(0.5) WITHIN GROUP (ORDER BY {column}) "
        f"AS {column}_p50"
        for column in SUMMARY_COLUMNS
    )
    sql = text(f"""
        SELECT
            COUNT(*)::bigint AS row_count,
            COUNT(*) FILTER (
                WHERE {_join_conditions(CRITICAL_COLUMNS, "IS NULL")}
            )::bigint AS critical_null_rows,
            COUNT(*) FILTER (
                WHERE {_non_finite_condition(NON_FINITE_COLUMNS)}
            )::bigint AS non_finite_rows,
            COUNT(*) FILTER (
                WHERE {_ratio_condition(RATIO_COLUMNS)}
                   OR {_negative_condition(NON_NEGATIVE_RATE_COLUMNS)}
            )::bigint AS ratio_out_of_range_rows,
            COUNT(*) FILTER (
                WHERE item_sum < 0
                   OR revenue_sum < 0
                   OR complaint_sum < 0
            )::bigint AS volume_out_of_range_rows,
            COUNT(*) FILTER (
                WHERE active_months < 0
                   OR active_months > :window_size
                   OR inactive_months <> :window_size - active_months
                   OR active_days < 0
                   OR active_days > :window_days
                   OR inactive_days <> :window_days - active_days
                   OR avg_noservice_days < 0
                   OR avg_noservice_days > inactive_days
                   OR max_consecutive_inactive < 0
                   OR max_consecutive_inactive > inactive_days
                   OR avg_noservice_days > max_consecutive_inactive
                   OR recency < 0
                   OR recency > :window_days
            )::bigint AS activity_out_of_range_rows,
            COUNT(*) FILTER (
                WHERE window_size IS DISTINCT FROM :window_size
                   OR window_start IS DISTINCT FROM :start_yymm
                   OR window_end IS DISTINCT FROM :end_yymm
            )::bigint AS metadata_mismatch_rows,
            {summary_selects}
        FROM {safe_name}
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, _quality_params(spec)).mappings().one()
    return metrics_from_mapping(row)


def metrics_from_mapping(row: dict) -> WindowQualityMetrics:
    """Build typed quality metrics from a SQL mapping for isolated testing."""
    summary = {
        key: row[key]
        for column in SUMMARY_COLUMNS
        for key in (
            f"{column}_min",
            f"{column}_max",
            f"{column}_avg",
            f"{column}_p50",
        )
    }
    return WindowQualityMetrics(
        row_count=int(row["row_count"]),
        critical_null_rows=int(row["critical_null_rows"]),
        critical_null_rate=_rate(row["critical_null_rows"], row["row_count"]),
        non_finite_rows=int(row["non_finite_rows"]),
        ratio_out_of_range_rows=int(row["ratio_out_of_range_rows"]),
        volume_out_of_range_rows=int(row["volume_out_of_range_rows"]),
        activity_out_of_range_rows=int(row["activity_out_of_range_rows"]),
        metadata_mismatch_rows=int(row["metadata_mismatch_rows"]),
        summary=summary,
    )


def evaluate_window_quality(metrics: WindowQualityMetrics) -> list[str]:
    """Return human-readable gate violations for one quality summary."""
    violations = []
    if metrics.row_count == 0:
        violations.append("window table is empty")
    for field_name in (
        "critical_null_rows",
        "non_finite_rows",
        "ratio_out_of_range_rows",
        "volume_out_of_range_rows",
        "activity_out_of_range_rows",
        "metadata_mismatch_rows",
    ):
        value = getattr(metrics, field_name)
        if value > 0:
            violations.append(f"{field_name}={value}")
    return violations


def validate_and_record_batch_consistency(
    engine: Any,
    run_id: str,
    window_specs: list[dict],
) -> BatchConsistencyMetrics:
    """Persist and enforce cross-window and lifetime consistency checks."""
    metrics = collect_batch_consistency_metrics(engine, window_specs)
    violations = evaluate_batch_consistency(metrics)
    record_batch_consistency(engine, run_id, window_specs, metrics, violations)
    if violations:
        raise ValueError(
            "Feature batch consistency validation failed: "
            f"{'; '.join(violations)}"
        )
    return metrics


def collect_batch_consistency_metrics(
    engine: Any,
    window_specs: list[dict],
) -> BatchConsistencyMetrics:
    """Compare nested windows and the largest current window against lifetime."""
    if not window_specs:
        return BatchConsistencyMetrics(0, 0, 0, 0)

    table_pairs = _nested_window_pairs(window_specs)
    cross_window_violation_rows = 0
    if table_pairs:
        checks = " UNION ALL ".join(
            f"""
                SELECT COUNT(*)::bigint AS violation_rows
                FROM {_validate_qualified_window_name(longer["table_name"])} longer
                JOIN {_validate_qualified_window_name(shorter["table_name"])} shorter
                  USING (cms_code_enc)
                WHERE longer.item_sum < shorter.item_sum
                   OR longer.revenue_sum < shorter.revenue_sum
            """
            for shorter, longer in table_pairs
        )
        with engine.connect() as conn:
            cross_window_violation_rows = int(
                conn.execute(
                    text(
                        f"SELECT COALESCE(SUM(violation_rows), 0)::bigint "
                        f"FROM ({checks}) checks"
                    )
                ).scalar()
            )

    largest_spec = max(
        window_specs,
        key=lambda spec: (spec["end_ym"], int(spec["window_size"])),
    )
    largest_table = _validate_qualified_window_name(largest_spec["table_name"])
    lifetime_sql = text(f"""
        SELECT
            COUNT(*) FILTER (
                WHERE lifetime.cms_code_enc IS NULL
            )::bigint AS missing_lifetime_rows,
            COUNT(*) FILTER (
                WHERE lifetime.cms_code_enc IS NOT NULL
                  AND (
                      lifetime.lifetime_total_items < win.item_sum
                      OR lifetime.lifetime_total_revenue < win.revenue_sum
                  )
            )::bigint AS lifetime_violation_rows
        FROM {largest_table} win
        LEFT JOIN data_static.cus_lifetime_snapshot lifetime
          ON lifetime.cms_code_enc = win.cms_code_enc
         AND lifetime.snapshot_month = :snapshot_month
    """)
    with engine.connect() as conn:
        lifetime_row = conn.execute(
            lifetime_sql,
            {"snapshot_month": datetime.strptime(largest_spec["end_ym"], "%y%m").date()},
        ).mappings().one()

    return BatchConsistencyMetrics(
        compared_window_pairs=len(table_pairs),
        cross_window_violation_rows=cross_window_violation_rows,
        lifetime_violation_rows=int(lifetime_row["lifetime_violation_rows"]),
        missing_lifetime_rows=int(lifetime_row["missing_lifetime_rows"]),
    )


def evaluate_batch_consistency(metrics: BatchConsistencyMetrics) -> list[str]:
    """Return violations for nested-window and lifetime comparisons."""
    violations = []
    for field_name in (
        "cross_window_violation_rows",
        "lifetime_violation_rows",
        "missing_lifetime_rows",
    ):
        value = getattr(metrics, field_name)
        if value > 0:
            violations.append(f"{field_name}={value}")
    return violations


def record_batch_consistency(
    engine: Any,
    run_id: str,
    window_specs: list[dict],
    metrics: BatchConsistencyMetrics,
    violations: list[str],
) -> None:
    """Persist one aggregate consistency audit for the feature batch."""
    if not window_specs:
        return

    latest_spec = max(
        window_specs,
        key=lambda spec: (spec["end_ym"], int(spec["window_size"])),
    )
    synthetic_spec = {
        "table_name": "__batch_consistency__",
        "window_size": 0,
        "start_ym": latest_spec["start_ym"],
        "end_ym": latest_spec["end_ym"],
    }
    _record_quality_artifact(engine, run_id, synthetic_spec, metrics, violations)


def record_window_quality(
    engine: Any,
    run_id: str,
    spec: dict,
    metrics: WindowQualityMetrics,
    violations: list[str],
) -> None:
    """Persist one success or failed quality audit idempotently."""
    _record_quality_artifact(engine, run_id, spec, metrics, violations)


def _record_quality_artifact(
    engine: Any,
    run_id: str,
    spec: dict,
    metrics: WindowQualityMetrics | BatchConsistencyMetrics | LifetimeQualityMetrics,
    violations: list[str],
) -> None:
    sql = text(f"""
        INSERT INTO {QUALITY_TABLE}
            (run_id, window_table, window_size, start_yymm, end_yymm,
             status, violations, metrics, checked_at)
        VALUES
            (:run_id, :window_table, :window_size, :start_yymm, :end_yymm,
             :status, CAST(:violations AS JSONB), CAST(:metrics AS JSONB), NOW())
        ON CONFLICT (run_id, window_table) DO UPDATE SET
            status = EXCLUDED.status,
            violations = EXCLUDED.violations,
            metrics = EXCLUDED.metrics,
            checked_at = NOW()
    """)
    params = _audit_params(spec)
    params.update(
        {
            "run_id": run_id,
            "status": "failed" if violations else "success",
            "violations": json.dumps(violations),
            "metrics": json.dumps(_sanitize_json_value(asdict(metrics)), default=str),
        }
    )
    with engine.begin() as conn:
        conn.execute(sql, params)


def _quality_params(spec: dict) -> dict:
    params = _audit_params(spec)
    params["window_days"] = _window_days(spec["start_date"], spec["end_date"])
    return params


def _audit_params(spec: dict) -> dict:
    return {
        "window_table": spec["table_name"],
        "window_size": int(spec["window_size"]),
        "start_yymm": spec["start_ym"],
        "end_yymm": spec["end_ym"],
    }


def _window_days(start_date: str, end_date: str) -> int:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    return (end - start).days + 1


def _join_conditions(columns: tuple[str, ...], suffix: str) -> str:
    return "\n                   OR ".join(f"{column} {suffix}" for column in columns)


def _non_finite_condition(columns: tuple[str, ...]) -> str:
    return "\n                   OR ".join(
        f"{column}::text IN ('NaN', 'Infinity', '-Infinity')"
        for column in columns
    )


def _ratio_condition(columns: tuple[str, ...]) -> str:
    return "\n                   OR ".join(
        f"{column} < 0 OR {column} > 1"
        for column in columns
    )


def _negative_condition(columns: tuple[str, ...]) -> str:
    return "\n                   OR ".join(f"{column} < 0" for column in columns)


def _nested_window_pairs(window_specs: list[dict]) -> list[tuple[dict, dict]]:
    by_end_yymm: dict[str, list[dict]] = {}
    for spec in window_specs:
        by_end_yymm.setdefault(spec["end_ym"], []).append(spec)

    pairs = []
    for specs in by_end_yymm.values():
        ordered_specs = sorted(specs, key=lambda spec: int(spec["window_size"]))
        pairs.extend(zip(ordered_specs, ordered_specs[1:], strict=False))
    return pairs


def _rate(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value
