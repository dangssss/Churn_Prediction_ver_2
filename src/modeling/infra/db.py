"""Modeling infra.db — thin wrapper around shared.db.

This file remains for backward compatibility with existing imports
within the modeling module. All actual logic lives in shared.db and
config.db_config.

Convention: 08-Security §3.2 — no hardcoded credentials.
"""

from __future__ import annotations

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config.db_config import PostgresConfig
from shared.db import get_engine as _shared_get_engine

# Ensure .env is loaded before accessing any config
load_dotenv()


def get_engine(
    database_url: str | None = None,
    pool_pre_ping: bool = True,
) -> Engine:
    """Create a SQLAlchemy engine.

    Args:
        database_url: Explicit connection URL. If None, builds from env.
        pool_pre_ping: Enable connection health checks.

    Returns:
        SQLAlchemy Engine instance.
    """
    if database_url:
        from sqlalchemy import create_engine

        return create_engine(database_url, pool_pre_ping=pool_pre_ping)

    cfg = PostgresConfig.from_env()
    return _shared_get_engine(cfg)


def smoke_test(engine: Engine) -> tuple:
    """Return (current_database, current_user, version)."""
    with engine.connect() as conn:
        return conn.execute(text("SELECT current_database(), current_user, version()")).fetchone()
