# ops/post_ingest_maintenance.py
"""ANALYZE prod table + ghi đầy đủ ``ingest_log`` trong 1 INSERT.

Convention:
  - 10 §5.1 SRP — module này không chịu trách nhiệm DDL của ``ingest_log``
    (đẩy về :mod:`ingest_log_repository`).
  - 04 §5.4 Infrastructure (DB persistence).

Pattern:
  - Mỗi ZIP run = 1 INSERT mới (audit history tự nhiên qua row mới, không
    UPDATE row cũ → tránh mất lịch sử).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from data.ingestion.ops.copy_and_insert_to_production import IngestStats
from data.ingestion.ops.ingest_log_repository import ensure_ingest_log_schema
from data.ingestion.resources import PostgresConfig, get_pg_conn
from shared.logging_config import get_logger

logger = get_logger(__name__)


def post_ingest_maintenance(
    meta: dict[str, Any],
    pg_cfg: PostgresConfig,
    *,
    stats: IngestStats,
    md5_hash: str,
    prod_schema: str = "public",
    ingest_schema: str = "ingest",
    zip_path: Path | None = None,
) -> None:
    """Hậu xử lý sau khi nạp 1 ZIP thành công.

    Steps:
      1. Đảm bảo schema ``ingest_log`` (delegate sang repository module).
      2. ANALYZE bảng prod (tối ưu query plan).
      3. INSERT 1 row vào ``ingest_log`` chứa đầy đủ metrics + md5 + validation.

    Args:
        meta: dict từ ``unzip_and_discover`` (cần ``base``, ``table_name``,
            ``period_key_month``).
        pg_cfg: PostgresConfig cho DB connection.
        stats: :class:`IngestStats` từ ``copy_and_insert_to_production``.
        md5_hash: MD5 hex digest của ZIP đã ingest (đã tính ở job-level).
        prod_schema: Schema chứa bảng prod (default ``public``).
        ingest_schema: Schema chứa ``ingest_log`` (default ``ingest``).
        zip_path: Path tới ZIP — dùng cho ``zip_name``, ``file_size``, ``file_mtime``.
    """
    base = meta.get("base", "")
    table_name = meta.get("table_name", "")
    period_key_month = meta.get("period_key_month")  # vd: "202403"
    zip_name = zip_path.name if zip_path is not None else ""
    file_size = zip_path.stat().st_size if zip_path is not None else 0
    file_mtime = zip_path.stat().st_mtime if zip_path is not None else 0.0

    conn = get_pg_conn(pg_cfg)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        logger.info(f'Running post_ingest_maintenance for {prod_schema}."{table_name}"')

        # 1) Schema ingest_log (single source of truth = ingest_log_repository).
        ensure_ingest_log_schema(conn, ingest_schema=ingest_schema)

        # 2) ANALYZE prod table.
        if table_name:
            cur.execute(f'ANALYZE {prod_schema}."{table_name}";')
            logger.info(f'Analyzed {prod_schema}."{table_name}"')

        # 3) INSERT log — đầy đủ metrics trong 1 statement.
        cur.execute(
            f"""
            INSERT INTO {ingest_schema}.ingest_log (
                zip_name, base, table_name, period_key_month,
                prod_schema, staging_rows, prod_rows,
                file_size, file_mtime, status,
                md5_hash, rows_inserted, rows_in_csv, validation_passed
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s
            );
            """,
            (
                zip_name,
                base,
                table_name,
                period_key_month,
                prod_schema,
                stats.rows_inserted,           # staging_rows = prod_rows (no staging)
                stats.rows_inserted,
                file_size,
                file_mtime,
                "success",
                md5_hash,
                stats.rows_inserted,
                stats.rows_in_csv,
                stats.validation_passed,
            ),
        )

        conn.commit()
        logger.info(
            f"Logged success for zip={zip_name}, table={prod_schema}.\"{table_name}\", "
            f"rows_inserted={stats.rows_inserted:,}, rows_in_csv={stats.rows_in_csv:,}, "
            f"validation_passed={stats.validation_passed}"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"post_ingest_maintenance for {table_name}: {e}")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()
