import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import inspect, text

from features.engineering.feature_gen.db_utils import discover_bccp_tables
from features.engineering.feature_gen.template_engine import render_template
from features.engineering.feature_gen.window_manifest import (
    ensure_window_manifest_table,
    find_empty_window_tables,
    find_retry_window_tables,
    record_window_failed,
    record_window_started,
    record_window_succeeded,
    truncate_window_tables,
    validate_window_table,
)
from features.engineering.feature_gen.window_planner import plan_incremental_windows
from features.engineering.feature_gen.window_quality import (
    ensure_window_quality_table,
    validate_and_record_batch_consistency,
    validate_and_record_window_quality,
)
from shared.logging_config import get_logger

logger = get_logger("window_aggregation")

# ── Configuration ──────────────────────────────────────────
MAX_PARALLEL_WORKERS = 4  # Parallel INSERT workers (GP-3)

# Staging table names (UNLOGGED for cross-connection visibility)
_STG_MONTHLY = "data_window._stg_monthly"
_STG_COMPLAINT = "data_window._stg_complaint"
_STG_BCCP = "data_window._stg_bccp"


def build_relative_suffix(month_offset: int) -> str:
    """Build relative suffix for column names: _last, _1m_ago, _2m_ago, etc."""
    if month_offset == 0:
        return "last"
    return f"{month_offset}m_ago"


def _create_staging_tables(engine, global_start: str, bccp_tables: list[str]):
    """Create UNLOGGED staging tables for the entire date range.

    UNLOGGED tables are visible across all connections (needed for parallel
    INSERT) and don't write WAL (fast). They are explicitly dropped at the
    end of the pipeline via ``_cleanup_staging_tables()``.

    Args:
        engine: SQLAlchemy engine.
        global_start: Earliest date to include (YYYY-MM-DD).
        bccp_tables: Sorted list of bccp_orderitem table names.
    """
    logger.info("Creating staging UNLOGGED tables (one-time source scan)...")
    t0 = time.time()

    with engine.begin() as conn:
        # Drop any leftover staging from previous failed runs
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_STG_MONTHLY}")
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_STG_COMPLAINT}")
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_STG_BCCP}")

        # ── 1. _stg_monthly: pre-aggregated monthly sums with ALL columns ──
        conn.exec_driver_sql(f"""
            CREATE UNLOGGED TABLE {_STG_MONTHLY} AS
            SELECT
                cms_code_enc,
                to_char(report_month, 'YYMM') AS month_key,
                to_char(report_month, 'YYMM')::bigint AS month_key_num,
                MIN(report_month) AS report_month,
                SUM(item_count)::bigint AS item_sum,
                SUM(total_fee)::bigint AS revenue_sum,
                SUM(total_complaint)::bigint AS complaint_sum,
                SUM(delay_count)::bigint AS delay_sum,
                SUM(nodone)::bigint AS nodone_sum,
                AVG(order_score)::double precision AS order_score_avg,
                AVG(satisfaction_score)::double precision AS satisfaction_avg,
                COUNT(*)::int AS month_record_count,
                COUNT(CASE WHEN total_complaint > 0 THEN 1 END)::int AS months_with_complaint,
                SUM(weight_kg)::double precision AS weight_sum,
                SUM(delay_day)::double precision AS delay_day_sum,
                SUM(refunded)::double precision AS refund_sum,
                SUM(noaccepted)::double precision AS noaccepted_sum,
                SUM(lost_order)::double precision AS lost_sum,
                SUM(intra_province)::double precision AS intra_prov_sum,
                SUM(international)::double precision AS intl_sum,
                AVG(lastday)::double precision AS avg_lastday,
                SUM(COALESCE(ser_c, 0))::bigint AS ser_c,
                SUM(COALESCE(ser_e, 0))::bigint AS ser_e,
                SUM(COALESCE(ser_m, 0))::bigint AS ser_m,
                SUM(COALESCE(ser_p, 0))::bigint AS ser_p,
                SUM(COALESCE(ser_r, 0))::bigint AS ser_r,
                SUM(COALESCE(ser_u, 0))::bigint AS ser_u,
                SUM(COALESCE(ser_l, 0))::bigint AS ser_l,
                SUM(COALESCE(ser_q, 0))::bigint AS ser_q
            FROM public.cas_customer
            WHERE report_month >= DATE '{global_start}'
            GROUP BY cms_code_enc, to_char(report_month, 'YYMM'), to_char(report_month, 'YYMM')::bigint
        """)
        conn.exec_driver_sql(f"CREATE INDEX ON {_STG_MONTHLY} (cms_code_enc, month_key)")
        conn.exec_driver_sql(f"CREATE INDEX ON {_STG_MONTHLY} (report_month)")
        conn.exec_driver_sql(f"ANALYZE {_STG_MONTHLY}")

        stg_count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {_STG_MONTHLY}").fetchone()[0]
        logger.info(f"  _stg_monthly: {stg_count:,} rows")

        # ── 2. _stg_complaint ──
        conn.exec_driver_sql(f"""
            CREATE UNLOGGED TABLE {_STG_COMPLAINT} AS
            SELECT cms_code_enc, create_complaint_date, complaint_code
            FROM public.cms_complaint
            WHERE create_complaint_date >= DATE '{global_start}'
        """)
        conn.exec_driver_sql(
            f"CREATE INDEX ON {_STG_COMPLAINT} (cms_code_enc, create_complaint_date)"
        )
        conn.exec_driver_sql(f"ANALYZE {_STG_COMPLAINT}")

        cmp_count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {_STG_COMPLAINT}").fetchone()[0]
        logger.info(f"  _stg_complaint: {cmp_count:,} rows")

        # ── 3. _stg_bccp ──
        if bccp_tables:
            bccp_union = " UNION ALL ".join(
                f"SELECT cms_code_enc, DATE(sending_time) AS send_date FROM public.{t}"
                for t in bccp_tables
            )
            conn.exec_driver_sql(f"""
                CREATE UNLOGGED TABLE {_STG_BCCP} AS
                SELECT cms_code_enc, send_date
                FROM ({bccp_union}) AS raw
                WHERE send_date >= DATE '{global_start}'
            """)
        else:
            conn.exec_driver_sql(f"""
                CREATE UNLOGGED TABLE {_STG_BCCP} AS
                SELECT cms_code_enc, DATE(sending_time) AS send_date
                FROM public.bccp_orderitem
                WHERE sending_time >= DATE '{global_start}'
            """)
        conn.exec_driver_sql(f"CREATE INDEX ON {_STG_BCCP} (cms_code_enc, send_date)")
        conn.exec_driver_sql(f"ANALYZE {_STG_BCCP}")

        bccp_count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {_STG_BCCP}").fetchone()[0]
        logger.info(f"  _stg_bccp: {bccp_count:,} rows")

    elapsed = time.time() - t0
    logger.info(f"Staging tables created in {elapsed:.1f}s")


def _cleanup_staging_tables(engine):
    """Drop staging tables after pipeline completes."""
    logger.info("Cleaning up staging tables...")
    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_STG_MONTHLY}")
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_STG_COMPLAINT}")
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {_STG_BCCP}")
    logger.info("Staging tables dropped")


def _insert_one_window(
    engine,
    spec: dict,
    insert_sql: str,
    idx: int,
    total: int,
    run_id: str,
    plan_reason: str,
):
    """Execute a single window INSERT on its own connection.

    Each call gets its own connection from the pool, so multiple calls
    can run in parallel without blocking each other.
    """
    table_name = spec["table_name"]
    t0 = time.time()
    record_window_started(engine, run_id, spec, plan_reason)
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(insert_sql)
        validation = validate_window_table(engine, table_name)
        quality = validate_and_record_window_quality(engine, run_id, spec)
        record_window_succeeded(engine, run_id, spec, plan_reason, validation)
        elapsed = time.time() - t0
        logger.info(
            "  [%d/%d] %s (%s, rows=%d, null_rate=%.4f, recency=%s..%s, %.1fs)",
            idx,
            total,
            table_name,
            plan_reason,
            validation.row_count,
            quality.critical_null_rate,
            quality.summary["recency_min"],
            quality.summary["recency_max"],
            elapsed,
        )
    except Exception as exc:
        record_window_failed(engine, run_id, spec, plan_reason, str(exc))
        raise


def _validate_kept_windows(engine, run_id: str, specs: tuple[dict, ...]) -> None:
    """Audit skipped incremental windows before the batch is accepted."""
    for spec in specs:
        validate_window_table(engine, spec["table_name"])
        validate_and_record_window_quality(engine, run_id, spec)


def render_and_run_all(
    engine,
    months,
    window_sizes,
    incremental: bool = False,
    recompute_last_n: int = 2,
    run_id: str | None = None,
):
    run_id = run_id or uuid.uuid4().hex
    ensure_window_manifest_table(engine)
    ensure_window_quality_table(engine)
    logger.info(
        f"Starting window feature aggregation "
        f"({len(window_sizes)} sizes × {len(months)} months, "
        f"incremental={incremental}, run_id={run_id})"
    )

    default_start = pd.Timestamp("2025-01-01")

    # ── Discover bccp tables ──
    inspector = inspect(engine)
    all_tables = set(inspector.get_table_names(schema="public"))
    bccp_tables = discover_bccp_tables(all_tables)
    logger.debug(f"Found {len(bccp_tables)} bccp_orderitem tables")

    # ── Pre-generate all window specs ──
    window_specs = []
    for window_size in window_sizes:
        for end_month in months:
            start_month = end_month - pd.offsets.DateOffset(months=window_size - 1)
            if start_month < default_start:
                continue

            start_ym = start_month.strftime("%y%m")
            end_ym = end_month.strftime("%y%m")
            start_date = start_month.strftime("%Y-%m-01")
            end_date = (end_month + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
            table_name = f"data_window.cus_feature_{window_size}m_{start_ym}_{end_ym}"

            window_specs.append(
                {
                    "table_name": table_name,
                    "start_date": start_date,
                    "end_date": end_date,
                    "start_ym": start_ym,
                    "end_ym": end_ym,
                    "window_size": window_size,
                }
            )

    expected_window_specs = list(window_specs)
    kept_specs: tuple[dict, ...] = ()

    # ── GP-5: Incremental — skip existing windows ──
    plan_reasons = {
        spec["table_name"]: "full_refresh"
        for spec in window_specs
    }
    summary = {
        "kept": 0,
        "new": len(window_specs),
        "empty": 0,
        "retry": 0,
        "recent": 0,
        "to_compute": len(window_specs),
    }

    if incremental:
        dw_tables = set(inspector.get_table_names(schema="data_window"))
        # Exclude staging table names from the check
        staging_names = {"_stg_monthly", "_stg_complaint", "_stg_bccp"}
        dw_tables -= staging_names
        expected_tables = {
            spec["table_name"].split(".")[-1]
            for spec in window_specs
        }
        existing_tables = dw_tables & expected_tables
        empty_tables = find_empty_window_tables(engine, existing_tables)
        retry_tables = find_retry_window_tables(engine, existing_tables)
        plan = plan_incremental_windows(
            window_specs,
            existing_tables,
            empty_tables,
            retry_tables,
            recompute_last_n,
        )
        summary = plan.summary()
        logger.info(
            "Incremental plan: kept=%d, new=%d, empty=%d, retry=%d, recent=%d, to_compute=%d",
            summary["kept"],
            summary["new"],
            summary["empty"],
            summary["retry"],
            summary["recent"],
            summary["to_compute"],
        )
        recompute_specs = plan.recompute_empty + plan.recompute_retry + plan.recompute_recent
        truncate_window_tables(
            engine,
            {spec["table_name"].split(".")[-1] for spec in recompute_specs},
        )
        plan_reasons = {
            spec["table_name"]: reason
            for reason, specs in (
                ("new", plan.compute_new),
                ("empty", plan.recompute_empty),
                ("retry", plan.recompute_retry),
                ("recent", plan.recompute_recent),
            )
            for spec in specs
        }
        kept_specs = plan.keep
        window_specs = list(plan.to_compute)
        if not window_specs:
            _validate_kept_windows(engine, run_id, kept_specs)
            validate_and_record_batch_consistency(engine, run_id, expected_window_specs)
            logger.info("No new windows to compute. Skipping.")
            return summary

    logger.info(f"Will compute {len(window_specs)} window specifications")

    # ── Create source table indexes (one-time, idempotent) ──
    logger.info("Ensuring source table indexes...")
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_cas_customer_code_month "
            "ON public.cas_customer(cms_code_enc, report_month)"
        )
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_cas_customer_month_range "
            "ON public.cas_customer(report_month)"
        )
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_cms_complaint_code_date "
            "ON public.cms_complaint(cms_code_enc, create_complaint_date)"
        )
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_cms_complaint_date_range "
            "ON public.cms_complaint(create_complaint_date)"
        )

    # ── GP-1: Create staging UNLOGGED tables (ONE source scan) ──
    global_start = default_start.strftime("%Y-%m-%d")
    _create_staging_tables(engine, global_start, bccp_tables)

    try:
        # ── Render all SQL pairs ──
        all_sql_pairs = []
        for spec in window_specs:
            create_sql, insert_sql = _render_window_sqls(
                spec["table_name"],
                spec["start_date"],
                spec["end_date"],
                spec["start_ym"],
                spec["end_ym"],
                spec["window_size"],
            )
            all_sql_pairs.append((spec, create_sql, insert_sql))

        # ── Create all target tables (sequential — DDL is fast) ──
        with engine.begin() as conn:
            for spec, create_sql, _ in all_sql_pairs:
                table_name = spec["table_name"]
                try:
                    conn.exec_driver_sql(create_sql)
                except Exception as e:
                    logger.warning(f"Table {table_name} creation failed: {e}")

        # ── GP-3: Parallel INSERT (each window to its own table) ──
        total = len(all_sql_pairs)
        t0 = time.time()

        logger.info(
            f"Starting parallel INSERT with {MAX_PARALLEL_WORKERS} workers "
            f"({total} windows)..."
        )

        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as pool:
            futures = {}
            for idx, (spec, _, insert_sql) in enumerate(all_sql_pairs, 1):
                table_name = spec["table_name"]
                future = pool.submit(
                    _insert_one_window,
                    engine,
                    spec,
                    insert_sql,
                    idx,
                    total,
                    run_id,
                    plan_reasons[table_name],
                )
                futures[future] = table_name

            # Wait for all and propagate any exceptions
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"INSERT failed for {table_name}: {e}")
                    raise

        elapsed = time.time() - t0
        logger.info(
            f"Window feature aggregation complete: "
            f"{total} windows in {elapsed:.1f}s "
            f"({elapsed / max(total, 1):.1f}s/window avg)"
        )
        _validate_kept_windows(engine, run_id, kept_specs)
        batch_quality = validate_and_record_batch_consistency(
            engine,
            run_id,
            expected_window_specs,
        )
        logger.info(
            "Batch consistency validated: pairs=%d, cross_window_violations=%d, "
            "lifetime_violations=%d, missing_lifetime=%d",
            batch_quality.compared_window_pairs,
            batch_quality.cross_window_violation_rows,
            batch_quality.lifetime_violation_rows,
            batch_quality.missing_lifetime_rows,
        )
        return summary

    finally:
        # ── Always cleanup staging tables ──
        _cleanup_staging_tables(engine)


def _render_window_sqls(
    table_name: str, start_date: str, end_date: str,
    start_ym: str, end_ym: str, window_size: int,
):
    """Render CREATE TABLE and INSERT SQL for one window spec.

    No longer needs ``all_tables`` — bccp data is in staging table.
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    months = pd.date_range(start, end, freq="MS")
    month_keys = [m.strftime("%y%m") for m in months]
    num_months = len(month_keys)

    # Pre-compute relative suffixes in one pass
    rel_suffixes = [build_relative_suffix(num_months - 1 - idx) for idx in range(num_months)]

    # Build all SQL components efficiently in single loops
    case_parts = []
    cols_parts = []
    select_parts = []
    insert_parts = []

    for idx, (month_key, rel_suffix) in enumerate(zip(month_keys, rel_suffixes)):
        case_parts.append(
            f"MAX(CASE WHEN month_key = '{month_key}' THEN item_sum END) AS \"item_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN revenue_sum END) AS \"revenue_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN complaint_sum END) AS \"complaint_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN delay_sum END) AS \"delay_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN nodone_sum END) AS \"nodone_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN order_score_avg END) AS \"order_score_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN satisfaction_avg END) AS \"satisfaction_{rel_suffix}\""
        )

        cols_parts.append(
            f'    "item_{rel_suffix}" BIGINT,\n'
            f'    "revenue_{rel_suffix}" BIGINT,\n'
            f'    "complaint_{rel_suffix}" BIGINT,\n'
            f'    "delay_{rel_suffix}" BIGINT,\n'
            f'    "nodone_{rel_suffix}" BIGINT,\n'
            f'    "order_score_{rel_suffix}" DOUBLE PRECISION,\n'
            f'    "satisfaction_{rel_suffix}" DOUBLE PRECISION'
        )

        select_parts.append(
            f'COALESCE(mp."item_{rel_suffix}", 0) AS "item_{rel_suffix}", '
            f'COALESCE(mp."revenue_{rel_suffix}", 0) AS "revenue_{rel_suffix}", '
            f'COALESCE(mp."complaint_{rel_suffix}", 0) AS "complaint_{rel_suffix}", '
            f'COALESCE(mp."delay_{rel_suffix}", 0) AS "delay_{rel_suffix}", '
            f'COALESCE(mp."nodone_{rel_suffix}", 0) AS "nodone_{rel_suffix}", '
            f'COALESCE(mp."order_score_{rel_suffix}", 0) AS "order_score_{rel_suffix}", '
            f'COALESCE(mp."satisfaction_{rel_suffix}", 0) AS "satisfaction_{rel_suffix}"'
        )

        insert_parts.append(
            f'"item_{rel_suffix}", "revenue_{rel_suffix}", "complaint_{rel_suffix}", '
            f'"delay_{rel_suffix}", "nodone_{rel_suffix}", "order_score_{rel_suffix}", '
            f'"satisfaction_{rel_suffix}"'
        )

    # Join all parts
    monthly_case_str = ",\n        ".join(case_parts)
    monthly_cols_str = ",\n".join(cols_parts)
    monthly_select_str = ", ".join(select_parts)
    monthly_insert_cols_str = ", ".join(insert_parts)

    # Render CREATE and INSERT SQL
    table_safe = table_name.replace(".", "_")
    create_sql = render_template(
        "sliding_table", TABLE_NAME=table_name, TABLE_SAFE=table_safe, MONTHLY_COLUMNS=monthly_cols_str
    )

    insert_sql = render_template(
        "sliding_aggregate",
        TABLE_NAME=table_name,
        START_DATE=start_date,
        END_DATE=end_date,
        WINDOW_SIZE=window_size,
        START_YM=start_ym,
        END_YM=end_ym,
        MONTHLY_CASE_STATEMENTS=monthly_case_str,
        MONTHLY_SELECT_COLUMNS=monthly_select_str,
        MONTHLY_COLUMNS_LIST=monthly_insert_cols_str,
    )

    return create_sql, insert_sql
