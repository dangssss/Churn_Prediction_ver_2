"""Centralized Polars-native database reader (replaces pd.read_sql everywhere)."""
from __future__ import annotations
import polars as pl
from sqlalchemy.engine import Engine
from sqlalchemy import text


def read_sql(
    query: str,
    engine: Engine,
    params: dict | None = None,
) -> pl.DataFrame:
    """Drop-in replacement cho pd.read_sql(), trả về pl.DataFrame.

    Dùng connectorx nếu có connection string, fallback về SQLAlchemy mapping.
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        rows = result.mappings().all()
        if not rows:
            # Trả về empty DataFrame với đúng schema
            cols = list(result.keys())
            return pl.DataFrame({c: [] for c in cols})
        return pl.DataFrame([dict(r) for r in rows])
