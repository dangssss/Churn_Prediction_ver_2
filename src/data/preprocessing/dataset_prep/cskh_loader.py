"""CSKH confirmed churner file loader.

Reads CSV/XLSX files from the CSKH directory (Roi_bo_MM_YY),
loads them into ``cskh.confirmed_churners`` table, and provides
functions to query confirmed IDs by month.

File naming convention:
    Roi_bo_MM_YY.csv or Roi_bo_MM_YY.xlsx  (e.g. Roi_bo_01_25 = thang 01/2025)

Conventions applied:
  - 13-Data_ML §5.1: Dedicated adapter per data source.
  - 13-Data_ML §9.1: Idempotent — re-loading same file is safe (UPSERT).
  - 02-Config §4.3: No os.getenv here — paths from caller.
  - 08-Security §7.1: No credentials in logs.
"""
# SQL identifiers in this module are fixed internal constants; row values are bound parameters.
# ruff: noqa: S608

from __future__ import annotations

import logging
import re
import unicodedata
import zipfile
from io import TextIOWrapper
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from sqlalchemy import inspect, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Regex: legacy Roi_bo_MM_YY and current label_YYMM naming.
_CSKH_FILENAME_RE = re.compile(r"^Roi_bo_(\d{2})_(\d{2})(?:\.(?:csv|xlsx))?$", re.IGNORECASE)
_LABEL_FILENAME_RE = re.compile(r"^label_(\d{4})(?:\.csv)?$", re.IGNORECASE)
_BCCP_TABLE_RE = re.compile(r"^bccp_orderitem_(\d{4})$")

_ID_COLUMN_CANDIDATES = (
    "cms_code_enc",
    "crm_code_enc",
)

CSKH_SCHEMA = "cskh"
CSKH_TABLE = "confirmed_churners"
CSKH_RAW_LABEL_TABLE = "customer_labels"

_RAW_LABEL_COLUMNS = (
    "stt",
    "ma_kh",
    "ma_cms",
    "crm_code_enc",
    "cms_code_enc",
    "ma_don_vi",
    "ten_don_vi",
    "tinh_trang_kh",
    "thang_ks",
)


def _discover_bccp_mapping_tables(engine: Engine, label_to_yymm: int) -> list[tuple[str, int]]:
    """Return strictly named BCCP partitions available up to the label cutoff."""
    tables = inspect(engine).get_table_names(schema="public")
    matches = []
    for table in tables:
        match = _BCCP_TABLE_RE.fullmatch(table)
        if match and int(match.group(1)) <= label_to_yymm:
            matches.append((table, int(match.group(1))))
    return sorted(matches, key=lambda item: item[1])


def _build_crm_resolution_ctes(bccp_tables: list[tuple[str, int]]) -> str:
    """Build point-in-time CRM-to-CMS resolution CTEs for CSKH labels."""
    if bccp_tables:
        bccp_selects = "\nUNION\n".join(
            f"""
            SELECT DISTINCT
                rc.label_yymm,
                rc.crm_code_enc,
                NULLIF(b.cms_code_enc, '') AS cms_code_enc
            FROM raw_crm rc
            JOIN public.{table} b
                ON b.crm_code_enc = rc.crm_code_enc
            WHERE rc.label_yymm >= {table_yymm}
              AND NULLIF(b.cms_code_enc, '') IS NOT NULL
            """
            for table, table_yymm in bccp_tables
        )
    else:
        bccp_selects = """
            SELECT
                NULL::INT AS label_yymm,
                NULL::TEXT AS crm_code_enc,
                NULL::TEXT AS cms_code_enc
            WHERE FALSE
        """

    return f"""
        raw_crm AS (
            SELECT DISTINCT
                label_yymm,
                NULLIF(TRIM(crm_code_enc), '') AS crm_code_enc
            FROM {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE}
            WHERE NULLIF(TRIM(cms_code_enc), '') IS NULL
              AND NULLIF(TRIM(crm_code_enc), '') IS NOT NULL
              AND label_yymm BETWEEN :label_from_yymm AND :label_to_yymm
        ),
        bccp_crm_pairs AS (
            {bccp_selects}
        ),
        cas_info_pairs AS (
            SELECT DISTINCT
                rc.label_yymm,
                rc.crm_code_enc,
                NULLIF(ci.cms_code_enc, '') AS cms_code_enc
            FROM raw_crm rc
            JOIN public.cas_info ci
                ON ci.crm_code_enc = rc.crm_code_enc
            WHERE NULLIF(ci.cms_code_enc, '') IS NOT NULL
        ),
        raw_crm_resolved AS (
            SELECT label_yymm, crm_code_enc, cms_code_enc, 'bccp_history' AS resolve_source
            FROM bccp_crm_pairs
            UNION
            SELECT label_yymm, crm_code_enc, cms_code_enc, 'cas_info' AS resolve_source
            FROM cas_info_pairs
        )
    """


def shift_yymm(yymm: int, months: int) -> int:
    """Shift a YYMM integer by a number of calendar months."""
    year = 2000 + yymm // 100
    month = yymm % 100
    if not 1 <= month <= 12:
        raise ValueError(f"Invalid YYMM value: {yymm}")

    zero_based_month = year * 12 + month - 1 + months
    shifted_year, shifted_month = divmod(zero_based_month, 12)
    return shifted_year % 100 * 100 + shifted_month + 1


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
            source_zip      VARCHAR(255),
            source_member   VARCHAR(255),
            customer_key_type VARCHAR(50),
            label_yymm      INT,
            loaded_at       TIMESTAMP DEFAULT NOW(),
            UNIQUE(cms_code_enc, file_month, file_year)
        )""",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_TABLE} ADD COLUMN IF NOT EXISTS source_zip VARCHAR(255)",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_TABLE} ADD COLUMN IF NOT EXISTS source_member VARCHAR(255)",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_TABLE} ADD COLUMN IF NOT EXISTS customer_key_type VARCHAR(50)",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_TABLE} ADD COLUMN IF NOT EXISTS label_yymm INT",
        f"CREATE INDEX IF NOT EXISTS idx_confirmed_label_yymm ON {CSKH_SCHEMA}.{CSKH_TABLE}(label_yymm)",
        f"""CREATE TABLE IF NOT EXISTS {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE} (
            id              SERIAL PRIMARY KEY,
            label_yymm      INT NOT NULL,
            file_month      INT NOT NULL,
            file_year       INT NOT NULL,
            customer_key    VARCHAR(100) NOT NULL,
            customer_key_type VARCHAR(50) NOT NULL,
            stt             VARCHAR(50),
            ma_kh           VARCHAR(100),
            ma_cms          VARCHAR(100),
            crm_code_enc    VARCHAR(100),
            cms_code_enc    VARCHAR(100),
            ma_don_vi       VARCHAR(100),
            ten_don_vi      TEXT,
            tinh_trang_kh   TEXT,
            thang_ks        VARCHAR(50),
            source_file     VARCHAR(255),
            source_zip      VARCHAR(255),
            source_member   VARCHAR(255),
            loaded_at       TIMESTAMP DEFAULT NOW()
        )""",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE} ADD COLUMN IF NOT EXISTS customer_key VARCHAR(100)",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE} ADD COLUMN IF NOT EXISTS customer_key_type VARCHAR(50)",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE} ADD COLUMN IF NOT EXISTS thang_ks VARCHAR(50)",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE} ADD COLUMN IF NOT EXISTS source_zip VARCHAR(255)",
        f"ALTER TABLE {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE} ADD COLUMN IF NOT EXISTS source_member VARCHAR(255)",
        f"""CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_labels_yymm_key_type
            ON {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE}(label_yymm, customer_key_type, customer_key)""",
        (
            f"CREATE INDEX IF NOT EXISTS idx_customer_labels_label_yymm "
            f"ON {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE}(label_yymm)"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS idx_customer_labels_cms_code_enc "
            f"ON {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE}(cms_code_enc)"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS idx_customer_labels_crm_code_enc "
            f"ON {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE}(crm_code_enc)"
        ),
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
    legacy_match = _CSKH_FILENAME_RE.match(filename)
    if legacy_match:
        return int(legacy_match.group(1)), int(legacy_match.group(2))

    label_match = _LABEL_FILENAME_RE.match(Path(filename).name)
    if label_match:
        yymm = label_match.group(1)
        return int(yymm[2:]), int(yymm[:2])

    return None


def _read_csv_auto(source) -> pd.DataFrame:
    """Read CSKH CSV with delimiter sniffing for comma/semicolon files."""
    return pd.read_csv(source, sep=None, engine="python", dtype=str, keep_default_na=False)


def _normalize_source_column(column_name: Any) -> str:
    """Normalize source CSV headers to stable snake_case aliases."""
    value = str(column_name).strip().lstrip("\ufeff")
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^0-9a-zA-Z]+", "_", value).strip("_").lower()
    return value


def _clean_cell(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text_value = str(value).strip()
    if text_value.lower() in {"", "nan", "none", "nat", "<na>"}:
        return None
    return text_value


def _find_customer_key_columns(
    df: pd.DataFrame,
    *,
    source_name: str,
    allow_single_column: bool = False,
) -> list[tuple[str, Any]]:
    """Find encoded source columns that can identify a confirmed customer."""
    if df.empty:
        return []

    normalized = {_normalize_source_column(c): c for c in df.columns}
    key_columns: list[tuple[str, Any]] = []
    for candidate in _ID_COLUMN_CANDIDATES:
        if candidate in normalized:
            key_columns.append((candidate, normalized[candidate]))

    if key_columns:
        return key_columns

    if allow_single_column and len(df.columns) == 1:
        id_col = df.columns[0]
        logger.warning(
            "CSKH %s has one column %r; treating it as cms_code_enc",
            source_name,
            id_col,
        )
        return [("cms_code_enc", id_col)]

    raise ValueError(
        f"CSKH file {source_name} must contain an encoded customer id column. "
        f"Accepted encoded names: {list(_ID_COLUMN_CANDIDATES)}. Found: {list(df.columns)}"
    )


def _extract_confirmed_keys(df: pd.DataFrame, *, source_name: str) -> list[tuple[str, str]]:
    """Extract customer keys and their source column type from CSKH labels."""
    key_columns = _find_customer_key_columns(df, source_name=source_name, allow_single_column=True)
    if not key_columns:
        return []

    keys: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for _, row in df.iterrows():
        selected_key: tuple[str, str] | None = None
        for key_type, source_col in key_columns:
            clean_value = _clean_cell(row[source_col])
            if clean_value is not None:
                selected_key = (clean_value, key_type)
                break

        if selected_key is None or selected_key in seen:
            continue
        keys.append(selected_key)
        seen.add(selected_key)

    return keys


def _extract_confirmed_ids(df: pd.DataFrame, *, source_name: str) -> list[str]:
    """Extract confirmed churn IDs from a CSKH label DataFrame.

    The current CSKH files are named ``label_YYMM.csv`` and must identify
    customers with encoded keys. For multi-column files, only ``cms_code_enc``
    and ``crm_code_enc`` are accepted as keys.
    """
    return [customer_key for customer_key, _ in _extract_confirmed_keys(df, source_name=source_name)]


def _prepare_raw_label_rows(
    df: pd.DataFrame,
    *,
    month: int,
    year: int,
    source_file: str,
    source_name: str,
    source_zip: str | None = None,
    source_member: str | None = None,
) -> list[dict[str, Any]]:
    """Convert a CSKH label DataFrame to raw-table rows with in-file dedup."""
    key_columns = _find_customer_key_columns(df, source_name=source_name)
    if not key_columns:
        return []

    normalized_columns = {_normalize_source_column(c): c for c in df.columns}
    label_yymm = year * 100 + month
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    for _, row in df.iterrows():
        selected_key: tuple[str, str] | None = None
        for key_type, source_col in key_columns:
            customer_key = _clean_cell(row[source_col])
            if customer_key is not None:
                selected_key = (customer_key, key_type)
                break

        if selected_key is None or selected_key in rows_by_key:
            continue
        customer_key, key_type = selected_key

        raw_row: dict[str, Any] = {
            "label_yymm": label_yymm,
            "file_month": month,
            "file_year": year,
            "customer_key": customer_key,
            "customer_key_type": key_type,
            "source_file": source_file,
            "source_zip": source_zip,
            "source_member": source_member,
        }
        for column in _RAW_LABEL_COLUMNS:
            source_col = normalized_columns.get(column)
            raw_row[column] = _clean_cell(row[source_col]) if source_col is not None else None

        rows_by_key[selected_key] = raw_row

    return list(rows_by_key.values())


def _upsert_raw_label_rows(
    engine: Engine,
    *,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert full CSKH label rows into the raw label table."""
    if not rows:
        return 0

    ensure_cskh_schema(engine)
    insert_columns = (
        "label_yymm",
        "file_month",
        "file_year",
        "customer_key",
        "customer_key_type",
        *_RAW_LABEL_COLUMNS,
        "source_file",
        "source_zip",
        "source_member",
        "loaded_at",
    )
    value_columns = [column for column in insert_columns if column != "loaded_at"]
    update_columns = [column for column in insert_columns if column not in {"loaded_at"}]
    insert_sql = text(f"""
        INSERT INTO {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE}
            ({", ".join(insert_columns)})
        VALUES
            ({", ".join(f":{column}" for column in value_columns)}, NOW())
        ON CONFLICT (label_yymm, customer_key_type, customer_key) DO UPDATE SET
            {", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)},
            loaded_at = NOW()
    """)

    with engine.begin() as conn:
        result = conn.execute(insert_sql, rows)

    return int(result.rowcount or len(rows))


def _insert_confirmed_ids(
    engine: Engine,
    *,
    confirmed_keys: list[tuple[str, str]],
    month: int,
    year: int,
    source_file: str,
    source_zip: str | None = None,
    source_member: str | None = None,
) -> int:
    """Upsert confirmed IDs into the CSKH table without creating duplicates."""
    if not confirmed_keys:
        return 0

    ensure_cskh_schema(engine)
    label_yymm = year * 100 + month
    insert_sql = text(f"""
        INSERT INTO {CSKH_SCHEMA}.{CSKH_TABLE}
            (cms_code_enc, file_month, file_year, source_file, source_zip,
             source_member, customer_key_type, label_yymm, loaded_at)
        VALUES
            (:cms, :m, :y, :src, :zip, :member, :key_type, :label_yymm, NOW())
        ON CONFLICT (cms_code_enc, file_month, file_year) DO UPDATE SET
            source_file = EXCLUDED.source_file,
            source_zip = EXCLUDED.source_zip,
            source_member = EXCLUDED.source_member,
            customer_key_type = EXCLUDED.customer_key_type,
            label_yymm = EXCLUDED.label_yymm,
            loaded_at = NOW()
    """)

    rows = [
        {
            "cms": cms_id,
            "m": month,
            "y": year,
            "src": source_file,
            "zip": source_zip,
            "member": source_member,
            "key_type": key_type,
            "label_yymm": label_yymm,
        }
        for cms_id, key_type in confirmed_keys
    ]

    with engine.begin() as conn:
        result = conn.execute(insert_sql, rows)

    return int(result.rowcount or len(rows))


def load_cskh_file_to_db(
    engine: Engine,
    file_path: Path,
    *,
    skip_if_exists: bool = False,
) -> int:
    """Load one CSKH file (CSV or XLSX) into the DB.

    Args:
        engine: SQLAlchemy engine.
        file_path: Path to CSKH file (Roi_bo_MM_YY.csv or .xlsx).
        skip_if_exists: Skip if data for this month/year already in DB.

    Returns:
        Number of confirmed ``cms_code_enc`` rows upserted for PU learning.

    Raises:
        ValueError: If filename format is invalid or file lacks a customer id column.
    """
    ensure_cskh_schema(engine)

    parsed = parse_cskh_filename(file_path.name)
    if parsed is None:
        raise ValueError(
            f"Invalid CSKH filename: {file_path.name}. Expected Roi_bo_MM_YY.csv/.xlsx or label_YYMM.csv"
        )

    month, year = parsed

    # Check if already loaded
    if skip_if_exists:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM {CSKH_SCHEMA}.{CSKH_TABLE} "
                    "WHERE file_month = :m AND file_year = :y"
                ),
                {"m": month, "y": year},
            )
            count = result.scalar()
            if count and count > 0:
                logger.info(
                    "CSKH %02d/%02d already loaded (%d rows) — skipping",
                    month,
                    year,
                    count,
                )
                return 0

    # Read file (CSV or XLSX)
    suffix = file_path.suffix.lower()
    if suffix == ".xlsx":
        df = pd.read_excel(file_path)
    elif suffix == ".csv":
        df = _read_csv_auto(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")

    raw_rows = _prepare_raw_label_rows(
        df,
        month=month,
        year=year,
        source_file=file_path.name,
        source_name=file_path.name,
        source_member=file_path.name,
    )
    raw_count = _upsert_raw_label_rows(engine, rows=raw_rows)
    confirmed_keys = [
        (row["customer_key"], row["customer_key_type"])
        for row in raw_rows
        if row["customer_key_type"] == "cms_code_enc"
    ]
    n_rows = _insert_confirmed_ids(
        engine,
        confirmed_keys=confirmed_keys,
        month=month,
        year=year,
        source_file=file_path.name,
        source_member=file_path.name,
    )

    logger.info(
        "CSKH loaded: %s -> %d raw rows, %d confirmed cms IDs for %02d/%02d",
        file_path.name,
        raw_count,
        len(confirmed_keys),
        month,
        year,
    )
    return n_rows


def load_cskh_zip_to_db(engine: Engine, zip_path: Path) -> int:
    """Load label_YYMM CSV files from a CSKH ZIP into DB.

    Expected local layout: ``data/incoming/cskh/*.zip`` where each ZIP contains
    files named like ``label_2503.csv``.
    """
    ensure_cskh_schema(engine)

    total = 0
    with zipfile.ZipFile(zip_path) as zf:
        members = sorted(
            m for m in zf.namelist()
            if not m.endswith("/") and parse_cskh_filename(Path(m).name) is not None
        )

        if not members:
            logger.warning("CSKH ZIP has no label_YYMM/Roi_bo files: %s", zip_path.name)
            return 0

        for member in members:
            parsed = parse_cskh_filename(Path(member).name)
            if parsed is None:
                continue
            month, year = parsed

            with zf.open(member) as raw:
                df = _read_csv_auto(TextIOWrapper(raw, encoding="utf-8-sig", newline=""))

            source_name = f"{zip_path.name}:{member}"
            raw_rows = _prepare_raw_label_rows(
                df,
                month=month,
                year=year,
                source_file=Path(member).name,
                source_name=source_name,
                source_zip=zip_path.name,
                source_member=member,
            )
            raw_count = _upsert_raw_label_rows(engine, rows=raw_rows)
            confirmed_keys = [
                (row["customer_key"], row["customer_key_type"])
                for row in raw_rows
                if row["customer_key_type"] == "cms_code_enc"
            ]
            n_rows = _insert_confirmed_ids(
                engine,
                confirmed_keys=confirmed_keys,
                month=month,
                year=year,
                source_file=Path(member).name,
                source_zip=zip_path.name,
                source_member=member,
            )
            total += n_rows
            logger.info(
                "CSKH ZIP loaded: %s:%s -> %d raw rows, %d confirmed cms IDs for %02d/%02d",
                zip_path.name,
                member,
                raw_count,
                len(confirmed_keys),
                month,
                year,
            )

    return total


def scan_and_load_cskh_dir(
    engine: Engine,
    cskh_dir: Path,
) -> dict[str, int]:
    """Scan CSKH directory and load any new CSV/XLSX files into DB.

    Args:
        engine: SQLAlchemy engine.
        cskh_dir: Directory containing Roi_bo_MM_YY.csv/.xlsx files.

    Returns:
        Dict mapping filename -> rows inserted.
    """
    ensure_cskh_schema(engine)

    if not cskh_dir.exists():
        logger.warning("CSKH directory not found: %s", cskh_dir)
        return {}

    results: dict[str, int] = {}
    # Collect legacy standalone files, current label_YYMM CSV files, and label ZIPs.
    cskh_files = sorted(
        p for p in cskh_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() in {".csv", ".xlsx"}
        and parse_cskh_filename(p.name) is not None
    )
    cskh_zips = sorted(
        p for p in cskh_dir.iterdir()
        if p.is_file() and p.suffix.lower() == ".zip"
    )

    if not cskh_files and not cskh_zips:
        logger.info("No CSKH files found in %s", cskh_dir)
        return {}

    for zip_path in cskh_zips:
        try:
            n = load_cskh_zip_to_db(engine, zip_path)
            results[zip_path.name] = n
        except (ValueError, Exception) as exc:
            logger.error("Failed to load %s: %s", zip_path.name, exc)
            results[zip_path.name] = -1

    for file_path in cskh_files:
        try:
            n = load_cskh_file_to_db(engine, file_path)
            results[file_path.name] = n
        except (ValueError, Exception) as exc:
            logger.error("Failed to load %s: %s", file_path.name, exc)
            results[file_path.name] = -1

    return results


def load_eval_id_cohorts_from_db(
    engine: Engine,
    working_ids: set[str],
    *,
    label_to_yymm: int,
    months_back: int = 6,
) -> dict[int, set[str]]:
    """Load confirmed churn IDs grouped by label month.

    Args:
        engine: SQLAlchemy engine.
        working_ids: Set of CMS codes in scope (for intersection).
        label_to_yymm: Inclusive latest label month allowed for the run.
        months_back: How many months of confirmed data to include.

    Returns:
        Mapping from label YYMM to confirmed churn IDs that are in scope.
    """
    if months_back < 1:
        raise ValueError(f"months_back must be >= 1, got {months_back}")

    label_from_yymm = shift_yymm(label_to_yymm, -(months_back - 1))
    ensure_cskh_schema(engine)
    bccp_tables = _discover_bccp_mapping_tables(engine, label_to_yymm)
    crm_resolution_ctes = _build_crm_resolution_ctes(bccp_tables)

    sql = text(f"""
        WITH raw_direct AS (
            SELECT DISTINCT
                label_yymm,
                NULLIF(TRIM(cms_code_enc), '') AS cms_code_enc
            FROM {CSKH_SCHEMA}.{CSKH_RAW_LABEL_TABLE}
            WHERE NULLIF(TRIM(cms_code_enc), '') IS NOT NULL
              AND label_yymm BETWEEN :label_from_yymm AND :label_to_yymm
        ),
        {crm_resolution_ctes},
        confirmed_direct AS (
            SELECT DISTINCT
                label_yymm,
                NULLIF(TRIM(cms_code_enc), '') AS cms_code_enc
            FROM {CSKH_SCHEMA}.{CSKH_TABLE}
            WHERE (customer_key_type IS NULL OR customer_key_type = 'cms_code_enc')
              AND label_yymm BETWEEN :label_from_yymm AND :label_to_yymm
        )
        SELECT DISTINCT label_yymm, cms_code_enc
        FROM (
            SELECT label_yymm, cms_code_enc FROM confirmed_direct
            UNION
            SELECT label_yymm, cms_code_enc FROM raw_direct
            UNION
            SELECT label_yymm, cms_code_enc FROM raw_crm_resolved
        ) resolved
        WHERE cms_code_enc IS NOT NULL
        ORDER BY label_yymm, cms_code_enc
    """)

    stats_sql = text(f"""
        WITH {crm_resolution_ctes},
        crm_matches AS (
            SELECT
                rc.label_yymm,
                rc.crm_code_enc,
                COUNT(DISTINCT rr.cms_code_enc) AS n_cms,
                COUNT(DISTINCT rr.cms_code_enc)
                    FILTER (WHERE rr.resolve_source = 'bccp_history') AS n_bccp_cms,
                COUNT(DISTINCT rr.cms_code_enc)
                    FILTER (WHERE rr.resolve_source = 'cas_info') AS n_cas_info_cms
            FROM raw_crm rc
            LEFT JOIN raw_crm_resolved rr
                ON rr.label_yymm = rc.label_yymm
               AND rr.crm_code_enc = rc.crm_code_enc
            GROUP BY rc.label_yymm, rc.crm_code_enc
        )
        SELECT
            COUNT(*) AS raw_crm_keys,
            COUNT(*) FILTER (WHERE n_cms > 0) AS resolved_crm_keys,
            COUNT(*) FILTER (WHERE n_cms = 0) AS unresolved_crm_keys,
            COUNT(*) FILTER (WHERE n_cms > 1) AS multi_cms_crm_keys,
            COUNT(*) FILTER (WHERE n_bccp_cms > 0) AS bccp_resolved_crm_keys,
            COUNT(*) FILTER (WHERE n_cas_info_cms > 0) AS cas_info_resolved_crm_keys,
            COALESCE(SUM(n_cms), 0) AS resolved_cms_ids
        FROM crm_matches
    """)

    params = {
        "label_from_yymm": label_from_yymm,
        "label_to_yymm": label_to_yymm,
    }
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
        stats_df = pd.read_sql(stats_sql, conn, params=params)

    if df.empty:
        logger.warning("No confirmed churners found in DB")
        return {}

    df["cms_code_enc"] = df["cms_code_enc"].astype(str).str.strip()
    all_ids = set(df["cms_code_enc"])
    in_scope_df = df[df["cms_code_enc"].isin(working_ids)]
    cohorts = {
        int(label_yymm): set(group["cms_code_enc"])
        for label_yymm, group in in_scope_df.groupby("label_yymm")
    }
    in_scope_ids = set().union(*cohorts.values()) if cohorts else set()

    if not stats_df.empty:
        stats = stats_df.iloc[0].to_dict()
        logger.info(
            (
                "CSKH CRM resolve: %d raw CRM keys, %d resolved, %d unresolved, "
                "%d multi-CMS, %d via BCCP history, %d via cas_info, "
                "%d CMS candidates"
            ),
            int(stats.get("raw_crm_keys") or 0),
            int(stats.get("resolved_crm_keys") or 0),
            int(stats.get("unresolved_crm_keys") or 0),
            int(stats.get("multi_cms_crm_keys") or 0),
            int(stats.get("bccp_resolved_crm_keys") or 0),
            int(stats.get("cas_info_resolved_crm_keys") or 0),
            int(stats.get("resolved_cms_ids") or 0),
        )

    logger.info(
        "CSKH from DB: %d total, %d in scope, label range=%d..%d",
        len(all_ids),
        len(in_scope_ids),
        label_from_yymm,
        label_to_yymm,
    )
    return cohorts


def load_eval_ids_from_db(
    engine: Engine,
    working_ids: set[str],
    *,
    label_to_yymm: int,
    months_back: int = 6,
) -> set[str]:
    """Load the union of time-bounded confirmed churn IDs from the database."""
    cohorts = load_eval_id_cohorts_from_db(
        engine,
        working_ids,
        label_to_yymm=label_to_yymm,
        months_back=months_back,
    )
    return set().union(*cohorts.values()) if cohorts else set()
