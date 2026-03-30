"""CSKH confirmed churner CSV loader.

Reads CSV files from the CSKH directory (Roi_bo_MM_YY.csv),
loads them into ``cskh.confirmed_churners`` table, and provides
functions to query confirmed IDs by month.

File naming convention:
    Roi_bo_MM_YY.csv  (e.g. Roi_bo_01_25 = tháng 01/2025)

Conventions applied:
  - 13-Data_ML §5.1: Dedicated adapter per data source.
  - 13-Data_ML §9.1: Idempotent — re-loading same file is safe (UPSERT).
  - 02-Config §4.3: No os.getenv here — paths from caller.
  - 08-Security §7.1: No credentials in logs.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Regex: Roi_bo_MM_YY with optional .csv extension
_CSKH_FILENAME_RE = re.compile(
    r"^Roi_bo_(\d{2})_(\d{2})(?:\.csv)?$", re.IGNORECASE
)

CSKH_SCHEMA = "cskh"
CSKH_TABLE = "confirmed_churners"


def ensure_cskh_schema(engine: Engine) -> None:
    """Create the cskh schema and confirmed_churners table if not exists.

    Convention: 13-Data_ML §9.1 — idempotent DDL.
    """
    ddl = [
        "CREATE SCHEMA IF NOT EXISTS cskh",
        f"""CREATE TABLE IF NOT EXISTS {CSKH_SCHEMA}.{CSKH_TABLE} (
            id              SERIAL PRIMARY KEY,
            cms_code_enc    VARCHAR(100) NOT NULL,
            file_month      INT NOT NULL,
            file_year       INT NOT NULL,
            source_file     VARCHAR(255),
            loaded_at       TIMESTAMP DEFAULT NOW(),
            UNIQUE(cms_code_enc, file_month, file_year)
        )""",
    ]
    with engine.begin() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))
    logger.debug("CSKH schema ensured: %s.%s", CSKH_SCHEMA, CSKH_TABLE)


def parse_cskh_filename(filename: str) -> tuple[int, int] | None:
    """Parse month and year from CSKH filename.

    Args:
        filename: Filename like ``Roi_bo_01_25.csv``.

    Returns:
        Tuple of (month, year_2digit) or None if no match.
        Example: ``("Roi_bo_03_25.csv")`` → ``(3, 25)``.
    """
    match = _CSKH_FILENAME_RE.match(filename)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def load_cskh_csv_to_db(
    engine: Engine,
    csv_path: Path,
    *,
    skip_if_exists: bool = True,
) -> int:
    """Load one CSKH CSV file into the DB.

    Args:
        engine: SQLAlchemy engine.
        csv_path: Path to CSKH CSV file (Roi_bo_MM_YY.csv).
        skip_if_exists: Skip if data for this month/year already in DB.

    Returns:
        Number of rows inserted.

    Raises:
        ValueError: If filename format is invalid or CSV lacks cms_code_enc.
    """
    parsed = parse_cskh_filename(csv_path.name)
    if parsed is None:
        raise ValueError(
            f"Invalid CSKH filename: {csv_path.name}. "
            f"Expected: Roi_bo_MM_YY.csv (e.g. Roi_bo_01_25.csv)"
        )

    month, year = parsed

    # Check if already loaded
    if skip_if_exists:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM {CSKH_SCHEMA}.{CSKH_TABLE} "
                    f"WHERE file_month = :m AND file_year = :y"
                ),
                {"m": month, "y": year},
            )
            count = result.scalar()
            if count and count > 0:
                logger.info(
                    "CSKH %02d/%02d already loaded (%d rows) — skipping",
                    month, year, count,
                )
                return 0

    # Read CSV
    df = pd.read_csv(csv_path)
    if "cms_code_enc" not in df.columns:
        raise ValueError(
            f"CSKH file {csv_path.name} must contain 'cms_code_enc' column. "
            f"Found columns: {list(df.columns)}"
        )

    ids = df["cms_code_enc"].astype(str).str.strip().unique()

    # Insert with ON CONFLICT DO NOTHING (idempotent)
    insert_sql = text(f"""
        INSERT INTO {CSKH_SCHEMA}.{CSKH_TABLE}
            (cms_code_enc, file_month, file_year, source_file)
        VALUES
            (:cms, :m, :y, :src)
        ON CONFLICT (cms_code_enc, file_month, file_year) DO NOTHING
    """)

    rows = [
        {"cms": cms_id, "m": month, "y": year, "src": csv_path.name}
        for cms_id in ids
    ]

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    logger.info(
        "CSKH loaded: %s → %d IDs for %02d/%02d",
        csv_path.name, len(rows), month, year,
    )
    return len(rows)


def scan_and_load_cskh_dir(
    engine: Engine,
    cskh_dir: Path,
) -> dict[str, int]:
    """Scan CSKH directory and load any new CSV files into DB.

    Args:
        engine: SQLAlchemy engine.
        cskh_dir: Directory containing Roi_bo_MM_YY.csv files.

    Returns:
        Dict mapping filename → rows inserted.
    """
    ensure_cskh_schema(engine)

    if not cskh_dir.exists():
        logger.warning("CSKH directory not found: %s", cskh_dir)
        return {}

    results: dict[str, int] = {}
    csv_files = sorted(cskh_dir.glob("Roi_bo_*.csv"))

    if not csv_files:
        logger.info("No CSKH files found in %s", cskh_dir)
        return {}

    for csv_path in csv_files:
        try:
            n = load_cskh_csv_to_db(engine, csv_path)
            results[csv_path.name] = n
        except (ValueError, Exception) as exc:
            logger.error("Failed to load %s: %s", csv_path.name, exc)
            results[csv_path.name] = -1

    return results


def load_eval_ids_from_db(
    engine: Engine,
    working_ids: set[str],
    *,
    months_back: int = 6,
) -> set[str]:
    """Load confirmed churn IDs from DB (all months within range).

    Args:
        engine: SQLAlchemy engine.
        working_ids: Set of CMS codes in scope (for intersection).
        months_back: How many months of confirmed data to include.

    Returns:
        Set of confirmed churn IDs that are in scope.
    """
    ensure_cskh_schema(engine)

    sql = text(f"""
        SELECT DISTINCT cms_code_enc
        FROM {CSKH_SCHEMA}.{CSKH_TABLE}
        ORDER BY cms_code_enc
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        logger.warning("No confirmed churners found in DB")
        return set()

    all_ids = set(df["cms_code_enc"].astype(str).str.strip())
    in_scope = all_ids & working_ids

    logger.info(
        "CSKH from DB: %d total, %d in scope",
        len(all_ids), len(in_scope),
    )
    return in_scope
