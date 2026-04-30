"""Repository và schema helpers cho bảng ``ingest.ingest_log``.

Module này là **single source of truth** cho:
  - DDL của ``ingest_log`` (CREATE SCHEMA + CREATE TABLE + ALTER ADD COLUMN)
  - Cách tính MD5 của file ZIP (``compute_zip_md5``)
  - Quyết định re-ingest dựa trên md5 + status (``IngestLogRepository.should_process``)

Convention:
  - 04 §5.4 — Infrastructure layer (DB persistence detail).
  - 10 §5.1 — SRP: chỉ xử lý ``ingest_log``, không TRUNCATE / COPY production tables.
  - 10 §3.1 — Readability: API ngắn gọn, không thêm lớp gián tiếp.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from shared.logging_config import get_logger

logger = get_logger(__name__)


# ---- Constants ----------------------------------------------------------

# 8KB là giá trị thường dùng cho streaming hash; đủ nhỏ để tránh giữ memory
# nhưng vẫn lớn hơn pagesize trên đa số hệ thống → ít syscall.
_MD5_CHUNK_SIZE = 8_192

# Backward-compat columns — được thêm vào bảng ingest_log đã tồn tại từ
# trước v1.2. ADD COLUMN IF NOT EXISTS làm phép migration idempotent.
_MIGRATION_COLUMNS: list[tuple[str, str]] = [
    ("md5_hash", "text"),
    ("rows_inserted", "bigint"),
    ("rows_in_csv", "bigint"),
    ("validation_passed", "boolean"),
]


# ---- Module-level helpers (single source of truth for schema + md5) -----

def ensure_ingest_log_schema(conn, *, ingest_schema: str = "ingest") -> None:
    """Đảm bảo schema + bảng ``ingest_log`` + các cột bổ sung tồn tại.

    Idempotent — có thể gọi nhiều lần. Commit nội bộ vì DDL không nên
    bị giữ trong transaction dài của ingest job (DDL chiếm
    ``AccessExclusiveLock`` → block các session khác).

    Args:
        conn: psycopg2 connection (autocommit=False khuyến nghị).
        ingest_schema: Tên schema chứa ``ingest_log``.
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ingest_schema};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ingest_schema}.ingest_log (
                id                bigserial PRIMARY KEY,
                zip_name          text        NOT NULL,
                base              text        NOT NULL,
                table_name        text        NOT NULL,
                period_key_month  varchar(6),
                prod_schema       text        NOT NULL,
                staging_rows      bigint,
                prod_rows         bigint,
                file_size         bigint,
                file_mtime        double precision,
                status            text        NOT NULL,
                started_at        timestamptz DEFAULT now(),
                finished_at       timestamptz DEFAULT now()
            );
            """
        )
        for col_name, col_type in _MIGRATION_COLUMNS:
            cur.execute(
                f"ALTER TABLE {ingest_schema}.ingest_log "
                f"ADD COLUMN IF NOT EXISTS {col_name} {col_type};"
            )
    conn.commit()
    logger.debug(f"[ingest_log] ensured schema={ingest_schema}.ingest_log")


def compute_zip_md5(zip_path: Path, chunk_size: int = _MD5_CHUNK_SIZE) -> str:
    """Tính MD5 hex digest của file ZIP (streaming, không load full vào RAM)."""
    md5 = hashlib.md5()
    with open(zip_path, "rb") as fh:
        while chunk := fh.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


# ---- Repository ---------------------------------------------------------

class IngestLogRepository:
    """Thin repository cho ``ingest.ingest_log``.

    Không sở hữu connection — caller cấp connection và chịu trách nhiệm
    đóng. Connection nên ở chế độ autocommit=False (mặc định psycopg2).
    """

    def __init__(self, conn, *, ingest_schema: str = "ingest") -> None:
        self._conn = conn
        self._schema = ingest_schema

    # -- Public API -------------------------------------------------------

    def ensure_schema(self) -> None:
        """Forward về helper module-level (cùng nguồn DDL)."""
        ensure_ingest_log_schema(self._conn, ingest_schema=self._schema)

    def get_zip_status(
        self, zip_name: str
    ) -> Optional[Tuple[str, Optional[datetime], Optional[str], Optional[int]]]:
        """Lấy record mới nhất của ``zip_name``.

        Returns:
            ``(status, finished_at, md5_hash, rows_inserted)`` hoặc ``None``.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT status, finished_at, md5_hash, rows_inserted
                FROM {self._schema}.ingest_log
                WHERE zip_name = %s
                ORDER BY finished_at DESC NULLS LAST
                LIMIT 1
                """,
                (zip_name,),
            )
            return cur.fetchone()

    def should_process(self, zip_path: Path) -> Tuple[bool, str]:
        """Quyết định có nên ingest file ZIP này không.

        Quyết định dựa trên md5 + status (file mtime KHÔNG dùng — không
        đáng tin khi file được copy qua nhiều stage).

        Logic:
          1. Chưa có record       → process (``new_file``)
          2. ``success`` + md5 khớp → skip   (``already_ingested_same_md5``)
          3. md5 khác             → process (``content_changed_md5_diff``)
          4. md5 khớp + non-success → process (``retry_previous_failure``)

        Returns:
            ``(should_process, reason)``.
        """
        record = self.get_zip_status(zip_path.name)
        if record is None:
            return True, "new_file"

        status, _finished_at, last_md5, _rows = record
        current_md5 = compute_zip_md5(zip_path)

        if status == "success" and current_md5 == last_md5:
            return False, "already_ingested_same_md5"

        if current_md5 != last_md5:
            return True, "content_changed_md5_diff"

        # md5 khớp nhưng status khác success → có thể là failed/partial → retry
        return True, "retry_previous_failure"
