import argparse
import datetime as dt
import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables (centralized .env at project root)
load_dotenv()

from config.db_config import PostgresConfig
from features.engineering.feature_gen.db_utils import ensure_public_table_columns_exist, ensure_public_tables_exist
from features.engineering.feature_gen.render_and_execute_templates import render_and_run_all, run_static_aggregate
from shared.logging_config import configure_logging, get_logger

# Setup logging at module level
configure_logging()
logger = get_logger("run_feature_generation")

BASE = Path(__file__).resolve().parents[1]
DB_STATIC_SQL = BASE / "database" / "sql" / "data_static" / "lifetime_template.sql"


def _drop_recreate_schema(engine, schema_name: str):
    """Drop and recreate schema for fresh start."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"))
        conn.execute(text(f"CREATE SCHEMA {schema_name};"))


def run(args):
    """Execute full feature generation pipeline."""
    logger.info("=" * 60)
    logger.info("FEATURE GENERATION PIPELINE STARTED")
    logger.info("=" * 60)

    try:
        # Get database URL
        database_url = args.database_url or os.environ.get("DATABASE_URL")
        if not database_url:
            cfg = PostgresConfig.from_env()
            database_url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.dbname}"
            logger.info("DATABASE_URL built from PostgresConfig.from_env()")
        else:
            logger.info("DATABASE_URL provided via argument")

        engine = create_engine(database_url)
        logger.info("Database connection established")

        # Validate source tables and columns
        logger.info("Validating source tables...")
        ensure_public_tables_exist(engine)
        required_columns = {
            "cas_customer": ["cms_code_enc", "item_count"],
            "cms_complaint": ["cms_code_enc"],
            "cas_info": ["cms_code_enc"],
        }
        ensure_public_table_columns_exist(engine, required_columns)
        logger.info("Source tables validated")

        # Date range setup
        default_start = pd.Timestamp("2025-01-01")
        today = dt.date.today()
        start_date = pd.to_datetime(args.start) if getattr(args, "start", None) else default_start

        def _to_date(x) -> dt.date | None:
            if x is None:
                return None
            ts = pd.Timestamp(x)
            if ts.tz is not None:
                ts = ts.tz_convert(None)
            return ts.date()

        def _yymm_to_date(yymm: str) -> dt.date:
            yy = int(yymm[:2])
            mm = int(yymm[2:])
            return dt.date(2000 + yy, mm, 1)

        def _auto_end_date_from_db(engine, fallback_end=None) -> dt.date:
            # 1) ưu tiên bccp_orderitem_YYMM
            bccp_sql = """
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname = 'public' AND tablename LIKE 'bccp_orderitem_%'
                ORDER BY tablename
            """
            bccp_max = None
            try:
                df = pd.read_sql(bccp_sql, engine)
                yymm_list = []
                for t in df["tablename"].tolist():
                    m = re.match(r"bccp_orderitem_(\d{4})$", t)
                    if m:
                        yymm_list.append(m.group(1))
                if yymm_list:
                    bccp_max = _yymm_to_date(max(yymm_list))
            except Exception:
                bccp_max = None

            if bccp_max is not None:
                return bccp_max

            # 2) fallback: cas_customer (force DATE to avoid tz)
            cas_sql = """
                SELECT MAX(date_trunc('month', report_month))::date AS cas_max_month
                FROM public.cas_customer
            """
            try:
                cas_df = pd.read_sql(cas_sql, engine)
                cas_max = _to_date(cas_df.iloc[0, 0])
                if cas_max is not None:
                    return cas_max
            except Exception:
                pass

            # 3) fallback_end / today
            fb = _to_date(fallback_end) if fallback_end is not None else None
            return fb if fb is not None else dt.date.today()

        if getattr(args, "end", None):
            end_date = pd.to_datetime(args.end)
        else:
            # Auto-detect latest available month from DB (bccp_orderitem_YYMM + cas_customer.report_month)
            end_date = _auto_end_date_from_db(engine, fallback_end=today)
        # Normalize to month starts for monthly windows
        start_date = pd.Timestamp(start_date.year, start_date.month, 1)
        end_date = pd.Timestamp(end_date.year, end_date.month, 1)

        months = pd.date_range(start_date, end_date, freq="MS")
        if len(months) == 0:
            raise SystemExit("No months in given range")

        # Compute window sizes (3 to max available)
        max_window = max(3, len(pd.date_range(default_start, end_date, freq="MS")))
        window_sizes = list(range(3, max_window + 1))
        logger.info(f"Date range: {len(months)} months ({start_date.date()} to {end_date.date()})")
        logger.info(f"Window sizes: {window_sizes}")

        # Initialize schemas - batch in single transaction
        logger.info("Recreating schemas...")
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS data_static CASCADE;"))
            conn.execute(text("CREATE SCHEMA data_static;"))
            conn.execute(text("DROP SCHEMA IF EXISTS data_window CASCADE;"))
            conn.execute(text("CREATE SCHEMA data_window;"))
            # Create static table in same transaction
            conn.execute(text(DB_STATIC_SQL.read_text(encoding="utf-8")))
        logger.info("Schemas initialized")

        def run_lifetime(engine):
            logger.info("Aggregating static features...")
            run_static_aggregate(engine)
            with engine.begin() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM data_static.cus_lifetime;"))
                count = result.scalar()
                logger.info(f"Static feature aggregation complete. Total customers: {count}")
                if count == 0:
                    logger.error("Static feature table is empty! Check data_static aggregation.")
                    raise ValueError("Static feature table is empty!")

        def run_sliding(engine, months, window_sizes):
            logger.info("Aggregating window features...")
            render_and_run_all(engine, months, window_sizes)

        run_lifetime(engine)
        run_sliding(engine, months, window_sizes)

        engine.dispose()  # Clean up connection pool

        logger.info("=" * 60)
        logger.info("FEATURE GENERATION PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Log file: {Path('logs/pipeline.log').resolve()}")

    except Exception as e:
        logger.error("=" * 60)
        logger.error("FEATURE GENERATION PIPELINE FAILED")
        logger.error("=" * 60)
        logger.exception(f"Error: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate churn prediction features")
    parser.add_argument("--start", default="2025-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--database-url", default=None, help="Database URL")
    args = parser.parse_args()
    run(args)
