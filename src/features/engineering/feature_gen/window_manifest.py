"""Database-backed checkpoint and validation helpers for feature windows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

MANIFEST_TABLE = "data_static.feature_generation_windows"


@dataclass(frozen=True)
class WindowValidation:
    """Validation metrics persisted after each window computation."""

    row_count: int
    distinct_customer_count: int


def ensure_window_manifest_table(engine: Any) -> None:
    """Create the DB-backed feature-window manifest idempotently."""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
            run_id                  VARCHAR(100) NOT NULL,
            window_table            VARCHAR(255) NOT NULL,
            window_size             INT NOT NULL,
            start_yymm              VARCHAR(4) NOT NULL,
            end_yymm                VARCHAR(4) NOT NULL,
            status                  VARCHAR(20) NOT NULL,
            plan_reason             VARCHAR(30) NOT NULL,
            row_count               BIGINT,
            distinct_customer_count BIGINT,
            error_message           TEXT,
            started_at              TIMESTAMP,
            finished_at             TIMESTAMP,
            updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_id, window_table)
        )
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS data_static"))
        conn.execute(text(ddl))
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_feature_windows_table_status "
                f"ON {MANIFEST_TABLE}(window_table, status)"
            )
        )


def record_window_started(engine: Any, run_id: str, spec: dict, plan_reason: str) -> None:
    """Record a window attempt before its INSERT starts."""
    sql = text(f"""
        INSERT INTO {MANIFEST_TABLE}
            (run_id, window_table, window_size, start_yymm, end_yymm,
             status, plan_reason, started_at, updated_at)
        VALUES
            (:run_id, :window_table, :window_size, :start_yymm, :end_yymm,
             'running', :plan_reason, NOW(), NOW())
        ON CONFLICT (run_id, window_table) DO UPDATE SET
            status = 'running',
            plan_reason = EXCLUDED.plan_reason,
            row_count = NULL,
            distinct_customer_count = NULL,
            error_message = NULL,
            started_at = NOW(),
            finished_at = NULL,
            updated_at = NOW()
    """)
    with engine.begin() as conn:
        conn.execute(sql, _manifest_params(run_id, spec, plan_reason))


def record_window_succeeded(
    engine: Any,
    run_id: str,
    spec: dict,
    plan_reason: str,
    validation: WindowValidation,
) -> None:
    """Mark one window as validated and complete."""
    sql = text(f"""
        UPDATE {MANIFEST_TABLE}
        SET status = 'success',
            row_count = :row_count,
            distinct_customer_count = :distinct_customer_count,
            error_message = NULL,
            finished_at = NOW(),
            updated_at = NOW()
        WHERE run_id = :run_id
          AND window_table = :window_table
    """)
    params = _manifest_params(run_id, spec, plan_reason)
    params.update(
        {
            "row_count": validation.row_count,
            "distinct_customer_count": validation.distinct_customer_count,
        }
    )
    with engine.begin() as conn:
        conn.execute(sql, params)


def record_window_failed(
    engine: Any,
    run_id: str,
    spec: dict,
    plan_reason: str,
    error_message: str,
) -> None:
    """Mark one failed window attempt for operational diagnosis."""
    sql = text(f"""
        UPDATE {MANIFEST_TABLE}
        SET status = 'failed',
            error_message = :error_message,
            finished_at = NOW(),
            updated_at = NOW()
        WHERE run_id = :run_id
          AND window_table = :window_table
    """)
    params = _manifest_params(run_id, spec, plan_reason)
    params["error_message"] = error_message[:2000]
    with engine.begin() as conn:
        conn.execute(sql, params)


def validate_window_table(engine: Any, table_name: str) -> WindowValidation:
    """Require a non-empty table with one row per encoded CMS customer."""
    safe_name = _validate_qualified_window_name(table_name)
    sql = text(f"""
        SELECT
            COUNT(*)::bigint AS row_count,
            COUNT(DISTINCT cms_code_enc)::bigint AS distinct_customer_count,
            COUNT(*) FILTER (
                WHERE cms_code_enc IS NULL OR TRIM(cms_code_enc) = ''
            )::bigint AS blank_customer_count
        FROM {safe_name}
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().one()

    row_count = int(row["row_count"])
    distinct_customer_count = int(row["distinct_customer_count"])
    blank_customer_count = int(row["blank_customer_count"])
    if row_count == 0:
        raise ValueError(f"Window table is empty: {safe_name}")
    if blank_customer_count > 0:
        raise ValueError(f"Window table has {blank_customer_count} blank cms_code_enc rows: {safe_name}")
    if row_count != distinct_customer_count:
        raise ValueError(
            f"Window table has duplicate cms_code_enc rows: {safe_name} "
            f"(rows={row_count}, distinct={distinct_customer_count})"
        )
    return WindowValidation(row_count=row_count, distinct_customer_count=distinct_customer_count)


def find_empty_window_tables(engine: Any, table_names: set[str]) -> set[str]:
    """Return existing data_window tables that contain no rows."""
    empty_tables: set[str] = set()
    with engine.connect() as conn:
        for short_name in sorted(table_names):
            safe_name = _validate_short_window_name(short_name)
            has_rows = conn.execute(
                text(f"SELECT EXISTS (SELECT 1 FROM data_window.{safe_name} LIMIT 1)")
            ).scalar()
            if not has_rows:
                empty_tables.add(short_name)
    return empty_tables


def find_retry_window_tables(engine: Any, table_names: set[str]) -> set[str]:
    """Return windows whose latest manifest attempt did not complete."""
    if not table_names:
        return set()

    params = {}
    placeholders = []
    short_names = {}
    for idx, short_name in enumerate(sorted(table_names)):
        qualified_name = f"data_window.{_validate_short_window_name(short_name)}"
        param_name = f"table_{idx}"
        params[param_name] = qualified_name
        placeholders.append(f":{param_name}")
        short_names[qualified_name] = short_name

    sql = text(f"""
        SELECT DISTINCT ON (window_table)
            window_table,
            status
        FROM {MANIFEST_TABLE}
        WHERE window_table IN ({", ".join(placeholders)})
        ORDER BY window_table, updated_at DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings()
        return {
            short_names[row["window_table"]]
            for row in rows
            if row["status"] != "success"
        }


def truncate_window_tables(engine: Any, table_names: set[str]) -> None:
    """Clear target tables that must be recomputed."""
    if not table_names:
        return

    with engine.begin() as conn:
        for short_name in sorted(table_names):
            safe_name = _validate_short_window_name(short_name)
            conn.execute(text(f"TRUNCATE TABLE data_window.{safe_name}"))


def _manifest_params(run_id: str, spec: dict, plan_reason: str) -> dict:
    return {
        "run_id": run_id,
        "window_table": spec["table_name"],
        "window_size": int(spec["window_size"]),
        "start_yymm": spec["start_ym"],
        "end_yymm": spec["end_ym"],
        "plan_reason": plan_reason,
    }


def _validate_qualified_window_name(table_name: str) -> str:
    prefix = "data_window."
    if not table_name.startswith(prefix):
        raise ValueError(f"Invalid data_window table name: {table_name}")
    return f"{prefix}{_validate_short_window_name(table_name[len(prefix):])}"


def _validate_short_window_name(table_name: str) -> str:
    parts = table_name.split("_")
    if (
        len(parts) != 5
        or parts[0:2] != ["cus", "feature"]
        or not parts[2].endswith("m")
        or not parts[2][:-1].isdigit()
        or not parts[3].isdigit()
        or len(parts[3]) != 4
        or not parts[4].isdigit()
        or len(parts[4]) != 4
    ):
        raise ValueError(f"Invalid feature window table name: {table_name}")
    return table_name
