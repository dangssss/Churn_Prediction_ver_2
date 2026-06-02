# ops/copy_and_insert_to_production.py
"""COPY CSV → production tables (atomic, with deadlock retry & validation).

Convention references:
  - 10 §3 Readability / §4.6 Named constants
  - 10 §5.1 SRP — module chỉ đảm nhiệm load CSV vào prod table
  - 04 §5.4 Infrastructure layer (DB persistence)

Design notes:
  - **Atomic**: toàn bộ DDL + TRUNCATE + COPY chạy trong 1 transaction.
    Nếu bất kỳ bước nào fail → rollback → bảng prod giữ nguyên trạng thái cũ.
  - **Truncate-and-reload** cho cả snapshot lẫn monthly (Kịch bản A):
    ZIP monthly mới chứa toàn bộ CSV của tháng đó (full snapshot per-month).
  - **Encryption mapping save** chạy TRƯỚC commit DB. Nếu save mapping fail →
    rollback DB để tránh dữ liệu encrypted không có khoá giải.
  - **Deadlock retry** ở tầng job (qua tenacity), không retry trong chính
    hàm này vì retry trong scope của 1 connection sẽ không reset transaction.
  - Hàm trả về :class:`IngestStats` để caller (post_ingest_maintenance) ghi
    đầy đủ metrics vào ``ingest_log`` trong 1 INSERT.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any

from data.ingestion.config.csv_schema import (
    BATCH_ROWS,
    CSV_INJECTION_GUARD,
    SOURCE_HAS_HEADER,
    get_table_config,
)
from data.ingestion.config.table_schema import (
    get_canonical_column_names,
    get_prod_table_ddl,
)
from data.ingestion.ops.data_transformations import (
    CustomerEncryption,
    SafeTypeCaster,
    transform_bccp_orderitem_row,
    transform_cas_customer_row,
    transform_cas_info_row,
    transform_cms_complaint_row,
)
from data.ingestion.resources import PostgresConfig, get_pg_conn
from shared.logging_config import get_logger

logger = get_logger(__name__)


# ---- Constants ----------------------------------------------------------

# Ngưỡng mismatch giữa số dòng CSV input và số dòng insert vào prod.
# Vượt ngưỡng này → validation_passed = False (vẫn commit, nhưng log warning).
# 0.0 = strict (mọi mismatch đều fail validation).
_VALIDATION_TOLERANCE_PCT = 0.0

# Header preview length khi log mismatch (tránh dump full schema 50+ cột).
_HEADER_PREVIEW_LEN = 10

# Sample size dùng cho debug log khi COPY fail.
_DEBUG_SAMPLE_LEN = 5


class CsvHeaderMismatchError(Exception):
    """Raise khi header CSV không khớp ``EXPECTED_HEADERS``.

    Caller (ingest_zip_job) bắt exception này → move ZIP sang ``fail_data``.
    """


# ============================================================
# EXPECTED HEADERS (canonical column order per table)
# Map CSV header → canonical header bằng thứ tự (position-based)
# ============================================================

COMPLAINT_CODES = [114, 115, 116, 134, 194, 554, 595, 314, 594, 274, 614, 654, 234, 174]

EXPECTED_HEADERS: dict[str, list[str]] = {
    "bccp_orderitem": [
        "crm_code_enc",
        "cms_code_enc",
        "item_code_enc",
        "service_code",
        "weight_kg",
        "length_size",
        "width_size",
        "height_size",
        "total_fee",
        "is_domestic",
        "country_code",
        "send_province_code",
        "send_district_code",
        "send_commune_code",
        "rec_province_code",
        "rec_district_code",
        "rec_commune_code",
        "region",
        "sending_time",
        "ending_time",
        "rec_success",
        "refunded",
        "no_accepted",
        "lost_order",
        "delay_day",
        "done",
        "total_complaint",
        *[f"complaint{c}" for c in COMPLAINT_CODES],
        "order_score",
        "bccp_update_date",
    ],
    "cas_customer": [
        "cms_code_enc",
        "report_month",
        "item_count",
        "weight_kg",
        "total_fee",
        "intra_province",
        "international",
        "ser_c",
        "ser_e",
        "ser_m",
        "ser_p",
        "ser_r",
        "ser_u",
        "ser_l",
        "ser_q",
        "delay_day",
        "delay_count",
        "nodone",
        "refunded",
        "noaccepted",
        "lost_order",
        "lastday",
        "noservice",
        "dev_item",
        "order_score",
        "satisfaction_score",
        "total_complaint",
        *[f"complaint{c}" for c in COMPLAINT_CODES],
        "updated_at",
    ],
    "cms_complaint": [
        "cms_code_enc",
        "item_code",
        "create_complaint_date",
        "exp_complaint_date",
        "close_complaint_date",
        "delay_complaint",
        "complaint_code",
        "complaint_content",
        "complaint_content_bit",
        "complaint_update_date",
        "etl_date",
    ],
    "cas_info": [
        "cms_code_enc",
        "crm_code_enc",
        "cus_province",
        "contract_service",
        "tenure",
        "custype",
        "customer_update_date",
        "contract_classify",
        "contract_sig_first",
        "contract_mgr_org",
        "cus_poscode",
    ],
}

# Transform function dispatch để tránh if-elif chain
TRANSFORM_DISPATCH = {
    "bccp_orderitem": transform_bccp_orderitem_row,
    "cms_complaint": transform_cms_complaint_row,
    "cas_customer": transform_cas_customer_row,
    "cas_info": transform_cas_info_row,
}

# Natural keys used to collapse duplicate source rows before they land in prod.
# These are the v1 production keys, kept local to ingestion so modeling logic is unchanged.
DEDUP_KEYS: dict[str, list[str]] = {
    "bccp_orderitem": ["item_code"],
    "cms_complaint": ["cms_code_enc", "item_code", "create_complaint_date", "complaint_code"],
    "cas_customer": ["cms_code_enc", "report_month"],
    "cas_info": ["cms_code_enc"],
}


# ---- Public dataclass ---------------------------------------------------

@dataclass(frozen=True)
class IngestStats:
    """Kết quả của 1 lần copy_and_insert_to_production.

    Attributes:
        rows_inserted: Số dòng được COPY vào prod table.
        rows_in_csv:   Tổng số dòng đọc từ CSV (trước khi transform/skip).
        validation_passed: ``True`` nếu chênh lệch ≤ ``_VALIDATION_TOLERANCE_PCT``.
        diff_pct: ``|rows_in_csv - rows_inserted| / max(rows_in_csv, 1)``.
    """

    rows_inserted: int
    rows_in_csv: int
    validation_passed: bool
    diff_pct: float
    rows_staged: int = 0
    rows_deduplicated: int = 0


# ---- Public API ---------------------------------------------------------

def get_csv_header(csv_file: Path) -> list[str]:
    """Đọc dòng đầu CSV (delimiter=';', encoding='utf-8-sig')."""
    with open(csv_file, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        first_row = next(reader, None)
        if not first_row:
            raise ValueError(f"CSV file is empty: {csv_file}")
        return first_row


def copy_and_insert_to_production(
    meta: dict[str, Any],
    pg_cfg: PostgresConfig,
    *,
    batch_rows: int = BATCH_ROWS,
    source_has_header: bool = SOURCE_HAS_HEADER,
    injection_mode: str = CSV_INJECTION_GUARD,
    use_encryption: bool = True,
    encryption_mapping_file: str | None = None,
) -> IngestStats:
    """Đọc CSV và COPY thẳng vào production (atomic, truncate-and-reload).

    Hỗ trợ 4 base: ``bccp_orderitem``, ``cms_complaint``, ``cas_customer``,
    ``cas_info`` ở cả 2 mode (snapshot / monthly). Theo Kịch bản A, ZIP
    monthly chứa toàn bộ CSV của tháng đó → cần TRUNCATE bảng per-month
    trước khi COPY để tránh duplicate.

    Returns:
        :class:`IngestStats` (caller dùng để ghi ``ingest_log``).

    Raises:
        CsvHeaderMismatchError: Header CSV không khớp ``EXPECTED_HEADERS``.
        Exception: Bất kỳ lỗi DB nào → caller (job) chịu trách nhiệm
            move ZIP sang ``fail_data``.
    """
    base = meta["base"]
    table_name = meta["table_name"]  # vd: bccp_orderitem_2501, cas_customer
    csv_files: list[Path] = meta.get("csv_files", [])
    mode = meta.get("mode", "monthly")
    prod_schema = "public"

    if not csv_files:
        logger.warning(f"No CSV files to ingest for {table_name}")
        return IngestStats(rows_inserted=0, rows_in_csv=0, validation_passed=True, diff_pct=0.0)

    # Get table config (text_cols, datetime_cols, mode)
    try:
        table_cfg = get_table_config(base)
    except ValueError as e:
        logger.error(f"{e}")
        raise

    # Lưu ý: text_cols / datetime_cols không dùng trực tiếp trong COPY pipeline
    # hiện tại (transform_*_row đã ép kiểu). Giữ lại để future-proof.
    _ = table_cfg.get("text_cols", set())
    _ = table_cfg.get("datetime_cols", set())

    prod_tbl = f'{prod_schema}."{table_name}"'

    # Setup encryption nếu cần
    encrypto: CustomerEncryption | None = None
    if use_encryption:
        encrypto = CustomerEncryption()
        if encryption_mapping_file and Path(encryption_mapping_file).exists():
            try:
                encrypto.load_mapping(encryption_mapping_file)
                logger.info(f"Loaded encryption mapping from {encryption_mapping_file}")
            except Exception as e:
                logger.warning(f"Could not load encryption mapping: {e}")

    logger.info(
        f"COPY & CAST -> production: {prod_tbl} | base={base} | mode={mode} | files={len(csv_files)}"
    )

    headers, header_map, headers_raw = _resolve_headers(csv_files[0], base)

    # ===== Atomic transaction: DDL + TRUNCATE + COPY =====
    conn = get_pg_conn(pg_cfg)
    conn.autocommit = False
    cur = conn.cursor()

    rows_inserted = 0
    rows_in_csv = 0
    rows_staged = 0
    rows_deduplicated = 0

    try:
        # 0) Schema prod
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {prod_schema};")

        # 1) DDL với đúng kiểu dữ liệu
        ddl = get_prod_table_ddl(base, table_name, prod_schema)
        cur.execute(ddl)

        # 2) TRUNCATE cho cả snapshot lẫn monthly (Kịch bản A: full snapshot
        #    per-month — ZIP mới luôn chứa toàn bộ CSV của tháng đó).
        cur.execute(f"TRUNCATE TABLE {prod_tbl};")
        if base == "bccp_orderitem":
            cur.execute(
                f'ALTER TABLE {prod_tbl} ALTER COLUMN "total_fee" TYPE BIGINT;'
            )
        logger.info(f"Truncated {prod_tbl} (mode={mode}, full reload)")

        # 3) COPY từng CSV
        prod_columns = get_canonical_column_names(base)
        for idx, csv_file in enumerate(csv_files, start=1):
            staging_tbl = _quote_ident(f"_stg_{table_name}_{idx}")
            cur.execute(f"DROP TABLE IF EXISTS {staging_tbl};")
            cur.execute(f"CREATE TEMP TABLE {staging_tbl} (LIKE {prod_tbl}) ON COMMIT DROP;")

            file_staged, file_seen = _copy_one_csv(
                cur=cur,
                csv_file=csv_file,
                prod_tbl=staging_tbl,
                base=base,
                headers_raw=headers_raw,
                header_map=header_map,
                encrypto=encrypto,
                batch_rows=batch_rows,
            )
            rows_staged += file_staged
            rows_in_csv += file_seen

            if file_staged > 0:
                cur.execute(f"ANALYZE {staging_tbl};")
                file_inserted, file_deleted = _upsert_staging_to_prod(
                    cur=cur,
                    prod_tbl=prod_tbl,
                    staging_tbl=staging_tbl,
                    base=base,
                    columns=prod_columns,
                )
                rows_deduplicated += max(file_staged - file_inserted, 0) + file_deleted
            else:
                file_inserted = 0
                file_deleted = 0

            rows_inserted += file_inserted
            cur.execute(f"DROP TABLE IF EXISTS {staging_tbl};")
            logger.info(
                f"[{base}] {csv_file.name}: read={file_seen:,}, staged={file_staged:,}, "
                f"inserted={file_inserted:,}, deleted_overlap={file_deleted:,} "
                f"(running final={rows_inserted:,})"
            )

        # 4) Tính validation TRƯỚC commit (quyết định có rollback không)
        diff = abs(rows_in_csv - rows_staged)
        diff_pct = diff / max(rows_in_csv, 1)
        validation_passed = diff_pct <= _VALIDATION_TOLERANCE_PCT
        if not validation_passed:
            logger.warning(
                f"[{base}] Row count mismatch: csv={rows_in_csv:,} "
                f"staged={rows_staged:,} diff_pct={diff_pct:.4%} "
                f"(tolerance={_VALIDATION_TOLERANCE_PCT:.4%})"
            )
        if rows_deduplicated:
            logger.info(
                "[%s] Deduplicated %d row(s) using natural keys before commit",
                base,
                rows_deduplicated,
            )

        # 5) Save encryption mapping TRƯỚC commit DB (Bug 5 fix).
        #    Nếu save fail → raise → rollback toàn bộ transaction.
        if use_encryption and encrypto and encryption_mapping_file:
            encrypto.save_mapping(encryption_mapping_file)
            logger.info(f"Saved encryption mapping to {encryption_mapping_file}")

        # 6) Commit toàn bộ (atomic).
        conn.commit()
        logger.info(
            f"Production {prod_tbl}: {rows_inserted:,} rows committed "
            f"(validation={'OK' if validation_passed else 'WARN'})"
        )

        return IngestStats(
            rows_inserted=rows_inserted,
            rows_in_csv=rows_in_csv,
            validation_passed=validation_passed,
            diff_pct=diff_pct,
            rows_staged=rows_staged,
            rows_deduplicated=rows_deduplicated,
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"copy_and_insert_to_production base={base}, table={table_name}: {e}")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# ---- Internal helpers ---------------------------------------------------

def _resolve_headers(
    first_csv: Path, base: str
) -> tuple[list[str], dict[str, str], list[str]]:
    """Đọc header CSV đầu tiên và map sang canonical header.

    Returns:
        ``(canonical_headers, header_map, headers_raw)``.

    Raises:
        CsvHeaderMismatchError: Số cột không khớp ``EXPECTED_HEADERS[base]``.
    """
    try:
        headers_raw = [h.strip() for h in get_csv_header(first_csv)]
        logger.info(
            f"Read header from {first_csv.name}: {len(headers_raw)} columns "
            f"(raw: {headers_raw[:_DEBUG_SAMPLE_LEN]}...)"
        )
    except Exception as e:
        logger.error(f"Failed to read header from {first_csv.name}: {e}")
        raise

    expected = EXPECTED_HEADERS.get(base)
    if expected is None:
        # Base chưa có EXPECTED_HEADERS → cho phép dùng header raw
        logger.info(
            "Using raw header from CSV (no EXPECTED_HEADERS for base=%s): %d columns",
            base,
            len(headers_raw),
        )
        return headers_raw[:], {h: h for h in headers_raw}, headers_raw

    if len(expected) != len(headers_raw):
        msg = (
            f"Header count mismatch for base={base}: "
            f"expected {len(expected)} columns, got {len(headers_raw)}. "
            f"CSV đang bị thiếu hoặc thừa cột so với schema EXPECTED_HEADERS.\n"
            f"Expected: {expected[:_HEADER_PREVIEW_LEN]}"
            f"{'...' if len(expected) > _HEADER_PREVIEW_LEN else ''}\n"
            f"Got:      {headers_raw[:_HEADER_PREVIEW_LEN]}"
            f"{'...' if len(headers_raw) > _HEADER_PREVIEW_LEN else ''}"
        )
        logger.error(msg)
        raise CsvHeaderMismatchError(msg)

    # Map theo vị trí: cột i CSV → cột i canonical
    header_map = {headers_raw[i]: expected[i] for i in range(len(expected))}
    mismatches = [
        (headers_raw[i], expected[i])
        for i in range(len(expected))
        if headers_raw[i] != expected[i]
    ]
    if mismatches:
        logger.warning(
            "Column name mismatches detected (will map by position for base=%s):", base
        )
        for csv_col, canonical_col in mismatches:
            logger.warning("  CSV: '%s' → Canonical: '%s'", csv_col, canonical_col)

    logger.info("Using canonical header order for base=%s (%d columns)", base, len(expected))
    return expected[:], header_map, headers_raw


def _quote_ident(identifier: str) -> str:
    """Quote a generated SQL identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def _upsert_staging_to_prod(
    *,
    cur,
    prod_tbl: str,
    staging_tbl: str,
    base: str,
    columns: list[str],
) -> tuple[int, int]:
    """Move staged rows into prod using v1 natural-key dedup semantics."""
    col_str = ", ".join([f'"{c}"' for c in columns])
    dedup_cols = [c for c in DEDUP_KEYS.get(base, []) if c in columns]

    if dedup_cols:
        key_conditions = " AND ".join([f'p."{col}" = s."{col}"' for col in dedup_cols])
        key_cols_sql = ", ".join([f'"{col}"' for col in dedup_cols])

        cur.execute(
            f"""
            DELETE FROM {prod_tbl} p
            USING {staging_tbl} s
            WHERE {key_conditions};
            """
        )
        rows_deleted = cur.rowcount

        cur.execute(
            f"""
            INSERT INTO {prod_tbl} ({col_str})
            SELECT {col_str}
            FROM (
                SELECT {col_str},
                       ROW_NUMBER() OVER (PARTITION BY {key_cols_sql} ORDER BY ctid) AS rn
                FROM {staging_tbl}
            ) sub
            WHERE rn = 1;
            """
        )
        return cur.rowcount, rows_deleted

    all_cols_cond = " AND ".join([f'p."{col}" = s."{col}"' for col in columns])
    cur.execute(
        f"""
        DELETE FROM {prod_tbl} p
        USING {staging_tbl} s
        WHERE {all_cols_cond};
        """
    )
    rows_deleted = cur.rowcount

    cur.execute(
        f"""
        INSERT INTO {prod_tbl} ({col_str})
        SELECT {col_str}
        FROM (
            SELECT {col_str},
                   ROW_NUMBER() OVER (PARTITION BY {col_str} ORDER BY ctid) AS rn
            FROM {staging_tbl}
        ) sub
        WHERE rn = 1;
        """
    )
    return cur.rowcount, rows_deleted


def _copy_one_csv(
    *,
    cur,
    csv_file: Path,
    prod_tbl: str,
    base: str,
    headers_raw: list[str],
    header_map: dict[str, str],
    encrypto: CustomerEncryption | None,
    batch_rows: int,
) -> tuple[int, int]:
    """Đọc 1 CSV, transform từng row, COPY theo batch.

    Trong transaction caller — KHÔNG commit, KHÔNG rollback ở đây.

    Returns:
        ``(rows_inserted, rows_seen_in_csv)``.
    """
    transform_func = TRANSFORM_DISPATCH.get(base)
    if transform_func is None:
        logger.warning(f"No transform function for base={base}, skipping {csv_file.name}")
        return 0, 0

    rows_inserted = 0
    rows_seen = 0

    with open(csv_file, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        source_fields = [h.strip() for h in (reader.fieldnames or [])]
        if len(source_fields) != len(headers_raw):
            msg = (
                f"Header mismatch in {csv_file.name}: "
                f"expected {len(headers_raw)} columns (from first CSV), "
                f"got {len(source_fields)} columns. "
                f"All CSVs in ZIP must have same header structure."
            )
            logger.error(msg)
            raise CsvHeaderMismatchError(msg)

        logger.debug(f"Processing: {len(source_fields)} columns from {csv_file.name}")

        rows_buffer: list[dict[str, Any]] = []
        for raw_row in reader:
            rows_seen += 1

            # Strip + apply header_map (CSV key → canonical key)
            raw_row = {(k.strip() if isinstance(k, str) else k): v for k, v in raw_row.items()}
            normalized_row = {header_map.get(k, k): v for k, v in raw_row.items()}

            transformed = transform_func(normalized_row, encrypto)
            if transformed is None:
                continue  # Skip invalid row

            rows_buffer.append(transformed)

            if len(rows_buffer) >= batch_rows:
                _bulk_insert_rows(cur, prod_tbl, rows_buffer, base)
                rows_inserted += len(rows_buffer)
                rows_buffer = []

        # Flush phần còn lại
        if rows_buffer:
            _bulk_insert_rows(cur, prod_tbl, rows_buffer, base)
            rows_inserted += len(rows_buffer)

    return rows_inserted, rows_seen


def _bulk_insert_rows(cur, prod_tbl: str, rows: list[dict[str, Any]], base: str) -> None:
    """COPY FROM STDIN với StringIO buffer (nhanh hơn executemany)."""
    if not rows:
        return

    # --- EXTRA GUARD cho cas_info: normalize lại 2 cột datetime ---
    if base == "cas_info":
        ts_fields = ["customer_update_date", "contract_sig_first"]
        for row in rows:
            for field in ts_fields:
                val = row.get(field)
                if isinstance(val, str) and val.strip():
                    row[field] = SafeTypeCaster.to_timestamp(val)

    columns = list(rows[0].keys())
    col_str = ", ".join([f'"{col}"' for col in columns])

    buffer = StringIO()
    for row in rows:
        line_values = []
        for col in columns:
            val = row.get(col)
            if val is None:
                line_values.append("\\N")
            else:
                val_str = (
                    str(val)
                    .replace("\\", "\\\\")
                    .replace("\t", "\\t")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                )
                line_values.append(val_str)
        buffer.write("\t".join(line_values) + "\n")
    buffer.seek(0)

    try:
        cur.copy_expert(
            f"COPY {prod_tbl} ({col_str}) FROM STDIN WITH (FORMAT TEXT, NULL '\\N')",
            buffer,
        )
    except Exception as e:
        logger.error(f"COPY FROM failed for base={base}: {e}")
        logger.debug(f"Columns: {columns}")
        logger.debug(f"First row sample: {rows[0] if rows else 'N/A'}")
        raise
