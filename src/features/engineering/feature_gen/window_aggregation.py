import pandas as pd
from features.engineering.feature_gen.template_engine import render_template

from features.engineering.feature_gen.db_utils import build_bccp_src
from shared.logging_config import get_logger

logger = get_logger("window_aggregation")


def build_relative_suffix(month_offset: int) -> str:
    """Build relative suffix for column names: _last, _1m_ago, _2m_ago, etc."""
    if month_offset == 0:
        return "last"
    return f"{month_offset}m_ago"


def render_and_run_all(engine, months, window_sizes):
    logger.info(f"Starting window feature aggregation ({len(window_sizes)} sizes × {len(months)} months)")

    default_start = pd.Timestamp("2025-01-01")

    # Pre-generate all window specs to avoid repeated computation
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

    logger.debug(f"Generated {len(window_specs)} window specifications")

    # Batch: Pre-generate all SQL + create tables first (quick DDL pass)
    all_sql_pairs = []
    total = len(window_specs)

    for idx, spec in enumerate(window_specs, 1):
        create_sql, insert_sql = _render_window_sqls(
            engine,
            spec["table_name"],
            spec["start_date"],
            spec["end_date"],
            spec["start_ym"],
            spec["end_ym"],
            spec["window_size"],
        )
        all_sql_pairs.append((spec["table_name"], create_sql, insert_sql))

    # Execute: Batch create tables first (parallel-friendly prep work)
    with engine.begin() as conn:
        for table_name, create_sql, _ in all_sql_pairs:
            try:
                conn.exec_driver_sql(create_sql)
            except Exception as e:
                logger.warning(f"Table {table_name} creation failed: {e}")

    # Then batch inserts in optimal transaction size (e.g., 5 per transaction)
    batch_size = 5
    for batch_idx in range(0, len(all_sql_pairs), batch_size):
        batch = all_sql_pairs[batch_idx : batch_idx + batch_size]
        with engine.begin() as conn:
            for table_name, _, insert_sql in batch:
                idx = all_sql_pairs.index((table_name, _, insert_sql)) + 1
                logger.info(f"  [{idx}/{total}] {table_name}...")
                try:
                    conn.exec_driver_sql(insert_sql)
                except Exception as e:
                    logger.error(f"Insert to {table_name} failed: {e}")
                    raise

    logger.info(f"Window feature aggregation complete ({total} windows)")


def _render_window_sqls(
    engine, table_name: str, start_date: str, end_date: str, start_ym: str, end_ym: str, window_size: int
):
    """Pre-render both CREATE and INSERT SQL (cached BCCP lookups)."""
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

    # Single loop to build all components efficiently
    for idx, (month_key, rel_suffix) in enumerate(zip(month_keys, rel_suffixes)):
        # CASE statements for pivoting (minimal, uses monthly_sums not monthly_metrics)
        case_parts.append(
            f"MAX(CASE WHEN month_key = '{month_key}' THEN item_sum END) AS \"item_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN revenue_sum END) AS \"revenue_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN complaint_sum END) AS \"complaint_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN delay_sum END) AS \"delay_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN nodone_sum END) AS \"nodone_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN order_score_avg END) AS \"order_score_{rel_suffix}\", "
            f"MAX(CASE WHEN month_key = '{month_key}' THEN satisfaction_avg END) AS \"satisfaction_{rel_suffix}\""
        )

        # DDL column definitions - with quotes for problematic column names
        cols_parts.append(
            f'    "item_{rel_suffix}" BIGINT,\n'
            f'    "revenue_{rel_suffix}" BIGINT,\n'
            f'    "complaint_{rel_suffix}" BIGINT,\n'
            f'    "delay_{rel_suffix}" BIGINT,\n'
            f'    "nodone_{rel_suffix}" BIGINT,\n'
            f'    "order_score_{rel_suffix}" DOUBLE PRECISION,\n'
            f'    "satisfaction_{rel_suffix}" DOUBLE PRECISION'
        )

        # SELECT columns from pivoted table - with quotes
        select_parts.append(
            f'COALESCE(mp."item_{rel_suffix}", 0) AS "item_{rel_suffix}", '
            f'COALESCE(mp."revenue_{rel_suffix}", 0) AS "revenue_{rel_suffix}", '
            f'COALESCE(mp."complaint_{rel_suffix}", 0) AS "complaint_{rel_suffix}", '
            f'COALESCE(mp."delay_{rel_suffix}", 0) AS "delay_{rel_suffix}", '
            f'COALESCE(mp."nodone_{rel_suffix}", 0) AS "nodone_{rel_suffix}", '
            f'COALESCE(mp."order_score_{rel_suffix}", 0) AS "order_score_{rel_suffix}", '
            f'COALESCE(mp."satisfaction_{rel_suffix}", 0) AS "satisfaction_{rel_suffix}"'
        )

        # INSERT column names - with quotes
        insert_parts.append(
            f'"item_{rel_suffix}", "revenue_{rel_suffix}", "complaint_{rel_suffix}", '
            f'"delay_{rel_suffix}", "nodone_{rel_suffix}", "order_score_{rel_suffix}", '
            f'"satisfaction_{rel_suffix}"'
        )

    # Join all parts efficiently
    monthly_case_str = ",\n        ".join(case_parts)
    monthly_cols_str = ",\n".join(cols_parts)
    monthly_select_str = ", ".join(select_parts)
    monthly_insert_cols_str = ", ".join(insert_parts)

    # Build bccp_src once (expensive operation)
    bccp_src = build_bccp_src(engine, start_date, end_date)

    # Execute both CREATE and INSERT in single transaction
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
        BCCP_SRC=bccp_src,
        MONTHLY_CASE_STATEMENTS=monthly_case_str,
        MONTHLY_SELECT_COLUMNS=monthly_select_str,
        MONTHLY_COLUMNS_LIST=monthly_insert_cols_str,
    )

    return create_sql, insert_sql
