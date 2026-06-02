# SQL identifiers are introspected from the internal lifetime table contract.
# ruff: noqa: S608

from __future__ import annotations

import pandas as pd
from sqlalchemy import inspect, text

from features.engineering.feature_gen.db_utils import (
    build_bccp_src,
    create_bccp_indexes,
    execute_sql,
)
from features.engineering.feature_gen.template_engine import render_template
from shared.logging_config import get_logger

logger = get_logger("static_aggregation")


def run_static_aggregate(
    engine,
    as_of_date: pd.Timestamp | None = None,
    *,
    create_indexes: bool = True,
):
    """Aggregate one point-in-time lifetime snapshot and refresh latest state."""
    as_of_date = (as_of_date or pd.Timestamp.today()).normalize()
    snapshot_month = as_of_date.to_period("M").to_timestamp()
    logger.info("Starting static feature aggregation as_of=%s", as_of_date.date())

    # Create bccp indexes first (one-time cost, huge speedup)
    if create_indexes:
        create_bccp_indexes(engine)

    # Render SQL with dynamic bccp tables
    start_date = "2025-01-01"
    end_date = as_of_date.strftime("%Y-%m-%d")
    bccp_src = build_bccp_src(engine, start_date, end_date)

    logger.debug("Rendering lifetime_aggregate.sql (date range: %s to %s)", start_date, end_date)
    sql = render_template(
        "lifetime_aggregate",
        BCCP_SRC=bccp_src,
        AS_OF_DATE=end_date,
        SNAPSHOT_MONTH=snapshot_month.strftime("%Y-%m-%d"),
    )

    # Execute aggregation
    logger.info("Executing lifetime_aggregate SQL...")
    execute_sql(engine, sql)
    _sync_latest_lifetime(engine, snapshot_month)

    logger.info("Static feature aggregation complete")


def run_lifetime_snapshots(
    engine,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    *,
    incremental: bool = False,
    recompute_last_n: int = 2,
) -> None:
    """Backfill monthly point-in-time lifetime snapshots through end_date."""
    start_date = pd.Timestamp(start_date).normalize()
    end_date = pd.Timestamp(end_date).normalize()
    if recompute_last_n < 0:
        raise ValueError("recompute_last_n must be >= 0")

    create_bccp_indexes(engine)
    snapshot_months = list(pd.date_range(start_date, end_date, freq="MS"))
    existing_months = _existing_snapshot_months(engine) if incremental else set()
    trailing_months = set(snapshot_months[-recompute_last_n:]) if recompute_last_n else set()

    for snapshot_month in snapshot_months:
        if incremental and snapshot_month in existing_months and snapshot_month not in trailing_months:
            logger.info("Skipping existing static snapshot snapshot_month=%s", snapshot_month.date())
            continue
        as_of_date = min(snapshot_month + pd.offsets.MonthEnd(0), end_date)
        run_static_aggregate(engine, as_of_date=as_of_date, create_indexes=False)

    if snapshot_months:
        _sync_latest_lifetime(engine, snapshot_months[-1])


def _existing_snapshot_months(engine) -> set[pd.Timestamp]:
    """Return canonical snapshot months already persisted in the database."""
    sql = text("SELECT DISTINCT snapshot_month FROM data_static.cus_lifetime_snapshot")
    with engine.connect() as conn:
        rows = conn.execute(sql)
        return {pd.Timestamp(row[0]).normalize() for row in rows}


def _sync_latest_lifetime(engine, snapshot_month: pd.Timestamp) -> None:
    """Refresh the compatibility table from one canonical monthly snapshot."""
    column_names = [
        column["name"]
        for column in inspect(engine).get_columns("cus_lifetime", schema="data_static")
    ]
    columns_sql = ", ".join(f'"{name}"' for name in column_names)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE data_static.cus_lifetime"))
        conn.execute(
            text(
                f"""
                INSERT INTO data_static.cus_lifetime ({columns_sql})
                SELECT {columns_sql}
                FROM data_static.cus_lifetime_snapshot
                WHERE snapshot_month = :snapshot_month
                """
            ),
            {"snapshot_month": snapshot_month.date()},
        )
