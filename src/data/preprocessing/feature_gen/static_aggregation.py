import pandas as pd

from data.preprocessing.feature_gen.db_utils import build_bccp_src, create_bccp_indexes, execute_sql
from data.preprocessing.feature_gen.template_engine import render_template
from shared.logging_config import get_logger

logger = get_logger("static_aggregation")


def run_static_aggregate(engine):
    logger.info("Starting static feature aggregation...")

    # Create bccp indexes first (one-time cost, huge speedup)
    create_bccp_indexes(engine)

    # Render SQL with dynamic bccp tables
    start_date = "2025-01-01"
    end_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    bccp_src = build_bccp_src(engine, start_date, end_date)

    logger.debug(f"Rendering lifetime_aggregate.sql (date range: {start_date} to {end_date})")
    sql = render_template("lifetime_aggregate", BCCP_SRC=bccp_src)

    # Execute aggregation
    logger.info("Executing lifetime_aggregate SQL...")
    execute_sql(engine, sql)

    logger.info("Static feature aggregation complete")
