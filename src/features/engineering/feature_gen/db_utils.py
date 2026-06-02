"""Database utilities: connection, index creation, table discovery, and validation."""
# SQL identifiers in this module are generated from strict internal table-name patterns.
# ruff: noqa: S608

import re

import pandas as pd
from sqlalchemy import inspect, text

from features.engineering.feature_gen.feature_source_schema import (
    BCCP_ORDERITEM_CONTRACT,
    FEATURE_SOURCE_CONTRACTS,
    collect_schema_errors,
)
from shared.logging_config import get_logger

logger = get_logger("db_utils")

BCCP_TABLE_PATTERN = re.compile(r"^bccp_orderitem_\d{4}$")


def execute_sql(engine, sql_str: str):
    with engine.begin() as conn:
        try:
            conn.exec_driver_sql(sql_str)
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            raise


def build_bccp_src(engine, start_date: str, end_date: str) -> str:
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    months = pd.date_range(start, end, freq="MS")

    # Get all tables once and convert to set for O(1) lookup
    inspector = inspect(engine)
    all_tables = set(inspector.get_table_names(schema="public"))

    # Filter tables with single pass
    selects = []
    for m in months:
        tbl = f"bccp_orderitem_{m.strftime('%y%m')}"
        if tbl in all_tables:
            selects.append(f"SELECT * FROM public.{tbl}")

    if not selects:
        if "bccp_orderitem" in all_tables:
            logger.warning(
                "No monthly bccp_orderitem tables found for %s to %s, using base table",
                start_date,
                end_date,
            )
            return "public.bccp_orderitem"
        raise RuntimeError(
            f"No bccp_orderitem source found for {start_date} to {end_date}"
        )

    result = "(" + " UNION ALL ".join(selects) + ") AS bccp"
    logger.debug(f"Built bccp_src with {len(selects)} tables")
    return result


def create_bccp_indexes(engine):
    inspector = inspect(engine)
    available = set(inspector.get_table_names(schema="public"))
    bccp_tables = discover_bccp_tables(available)
    if "bccp_orderitem" in available:
        bccp_tables.append("bccp_orderitem")

    if not bccp_tables:
        logger.info("No bccp_orderitem tables found, skipping bccp indexes")
        return

    logger.info(f"Creating indexes on {len(bccp_tables)} bccp_orderitem tables...")

    with engine.begin() as conn:
        for table in sorted(bccp_tables):
            idx_name = f"idx_{table}_code_time"
            crm_map_idx_name = f"idx_{table}_crm_cms"
            try:
                conn.execute(
                    text(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name}
                    ON public.{table}(cms_code_enc, sending_time)
                """)
                )
                conn.execute(
                    text(f"""
                    CREATE INDEX IF NOT EXISTS {crm_map_idx_name}
                    ON public.{table}(crm_code_enc, cms_code_enc)
                """)
                )
            except Exception as e:
                logger.warning(f"Warning creating index on {table}: {e}")

        # Analyze for query planner optimization
        try:
            logger.debug("Running ANALYZE on source tables...")
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_cas_info_crm_cms
                ON public.cas_info(crm_code_enc, cms_code_enc)
            """)
            )
            conn.execute(text("ANALYZE public.cas_customer"))
            conn.execute(text("ANALYZE public.cas_info"))
            conn.execute(text("ANALYZE public.cms_complaint"))
        except Exception as e:
            logger.warning(f"Warning analyzing tables: {e}")

    logger.info(f"Created {len(bccp_tables)} bccp indexes")


def ensure_public_tables_exist(engine, required_tables=None):
    inspector = inspect(engine)
    available = set(inspector.get_table_names(schema="public"))

    if required_tables is None:
        required_tables = ["cas_customer", "cms_complaint", "cas_info", "bccp_orderitem"]

    missing = []
    for tbl in required_tables:
        if tbl == "bccp_orderitem":
            # Accept either a base table or partitioned monthly tables
            has_base = "bccp_orderitem" in available
            has_partition = bool(discover_bccp_tables(available))
            if not (has_base or has_partition):
                missing.append(tbl)
        else:
            if tbl not in available:
                missing.append(tbl)

    if missing:
        msg = f"Missing required public tables: {', '.join(missing)}"
        logger.error(msg)
        raise RuntimeError(msg)


def discover_bccp_tables(table_names) -> list[str]:
    """Return strictly named monthly BCCP source tables in stable order."""
    return sorted(table for table in table_names if BCCP_TABLE_PATTERN.fullmatch(table))


def ensure_feature_source_schema(engine) -> None:
    """Validate columns and type families consumed by feature-generation SQL."""
    inspector = inspect(engine)
    available = set(inspector.get_table_names(schema="public"))
    errors = []

    for table_name, contract in FEATURE_SOURCE_CONTRACTS.items():
        if table_name not in available:
            errors.append(f"Missing table: {table_name}")
            continue
        errors.extend(
            collect_schema_errors(
                table_name,
                inspector.get_columns(table_name, schema="public"),
                contract,
            )
        )

    bccp_tables = discover_bccp_tables(available)
    if "bccp_orderitem" in available:
        bccp_tables.append("bccp_orderitem")
    if not bccp_tables:
        errors.append("Missing table: bccp_orderitem or bccp_orderitem_YYMM")

    for table_name in bccp_tables:
        errors.extend(
            collect_schema_errors(
                table_name,
                inspector.get_columns(table_name, schema="public"),
                BCCP_ORDERITEM_CONTRACT,
            )
        )

    if errors:
        msg = "; ".join(errors)
        logger.error(msg)
        raise RuntimeError(msg)
