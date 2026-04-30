# jobs/ingest_zip_job.py
"""Job xử lý 1 file ZIP: skip-check → unzip → COPY-to-prod → maintenance.

Convention:
  - 10 §5.1 SRP — orchestrator, không chứa DB/COPY logic chi tiết.
  - 04 §5.3 Application layer (use-case orchestration).

Re-ingest decision:
  Dùng :class:`IngestLogRepository.should_process` (md5-based) thay vì
  mtime so sánh — md5 ổn định khi file được copy qua nhiều stage.

Failure handling:
  - unzip/discover fail → ZIP đã được copy sang ``fail_data`` bên trong
    ``unzip_and_discover``.
  - COPY fail → copy ZIP sang ``fail_data`` (không xoá bản gốc).
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from data.ingestion.ops.copy_and_insert_to_production import (
    IngestStats,
    copy_and_insert_to_production,
)
from data.ingestion.ops.ingest_log_repository import (
    IngestLogRepository,
    compute_zip_md5,
)
from data.ingestion.ops.post_ingest_maintenance import post_ingest_maintenance
from data.ingestion.ops.unzip_and_discover import unzip_and_discover
from data.ingestion.resources import FSConfig, PostgresConfig, get_pg_conn
from shared.logging_config import get_logger

logger = get_logger(__name__)


# ---- Constants ----------------------------------------------------------

# Deadlock retry — psycopg2 raises ``DeadlockDetected`` /
# ``SerializationFailure`` khi 2 transaction tranh chấp lock.
_MAX_DEADLOCK_RETRIES = 3
_DEADLOCK_RETRY_BASE_DELAY = 1.0   # seconds, exponential backoff base
_DEADLOCK_RETRY_MAX_DELAY = 10.0   # seconds


def _import_psycopg_errors():
    """Lazy import để test môi trường không cài psycopg2 vẫn import được module."""
    from psycopg2.errors import DeadlockDetected, SerializationFailure  # type: ignore

    return (DeadlockDetected, SerializationFailure)


def ingest_zip_job(
    zip_path: Path,
    fs_cfg: FSConfig,
    pg_cfg: PostgresConfig,
    *,
    prod_schema: str = "public",
    ingest_schema: str = "ingest",
    batch_rows: int = 50_000,
    source_has_header: bool = True,
    injection_mode: str = "sanitize",
    use_encryption: bool = True,
    encryption_mapping_file: str | None = None,
    use_test_schema: bool | None = None,
) -> dict[str, Any]:
    """Xử lý 1 ZIP: skip-check (md5) → unzip → COPY → maintenance.

    Returns:
        dict với các trường: ``zip_name``, ``meta``, ``rows_inserted``,
        ``rows_in_csv``, ``validation_passed``, ``md5_hash``,
        ``skipped``, ``reason``, ``success``, ``error``.
    """
    # Auto-detect test schema từ env
    if use_test_schema is None:
        use_test_schema = os.getenv("USE_TEST_SCHEMA", "").lower() in ("1", "true", "yes")
    if use_test_schema:
        os.environ["USE_TEST_SCHEMA"] = "1"

    zip_name = zip_path.name
    result: dict[str, Any] = {
        "zip_name": zip_name,
        "meta": None,
        "rows_inserted": 0,
        "rows_in_csv": 0,
        "validation_passed": None,
        "md5_hash": None,
        "skipped": False,
        "reason": None,
        "success": False,
        "error": None,
    }

    # ===== 0) Skip check: md5-based via IngestLogRepository =====
    md5_hash: str | None = None
    try:
        skip, md5_hash, reason = _check_should_process(zip_path, pg_cfg, ingest_schema)
        result["md5_hash"] = md5_hash
        if skip:
            result["skipped"] = True
            result["reason"] = reason
            logger.info(f"[SKIP] {zip_name}: {reason}")
            return result
        logger.info(f"[PROCEED] {zip_name}: {reason} (md5={md5_hash[:8]}...)")
    except Exception as e:
        logger.warning(f"[WARN] should_process check failed for {zip_name}: {e}")
        # Fail-open: vẫn process để không kẹt pipeline
        if md5_hash is None:
            try:
                md5_hash = compute_zip_md5(zip_path)
                result["md5_hash"] = md5_hash
            except Exception as md5_err:
                logger.error(f"[FAIL] compute_zip_md5 for {zip_name}: {md5_err}")
                result["error"] = f"md5 compute failed: {md5_err}"
                return result

    # ===== 1) unzip & discover =====
    try:
        meta = unzip_and_discover(zip_path, fs_cfg)
        result["meta"] = meta
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"[FAIL] unzip_and_discover for {zip_name}: {e}")
        return result

    if not meta.get("csv_files"):
        result["skipped"] = True
        result["reason"] = "no_csv_after_unzip"
        logger.warning(f"[SKIP] {zip_name}: không có CSV sau khi unzip.")
        return result

    # ===== 2) COPY → production (with deadlock retry) =====
    try:
        stats = _copy_with_retry(
            meta=meta,
            pg_cfg=pg_cfg,
            batch_rows=batch_rows,
            source_has_header=source_has_header,
            injection_mode=injection_mode,
            use_encryption=use_encryption,
            encryption_mapping_file=encryption_mapping_file,
        )
        result["rows_inserted"] = stats.rows_inserted
        result["rows_in_csv"] = stats.rows_in_csv
        result["validation_passed"] = stats.validation_passed
    except Exception as e:
        result["error"] = f"copy_and_insert_to_production failed: {e}"
        logger.error(f"[FAIL] copy_and_insert_to_production for {zip_name}: {e}")
        _copy_zip_to_fail_dir(zip_path, meta, fs_cfg)
        return result

    result["success"] = True

    # ===== 3) maintenance: ANALYZE + log ingest_log =====
    try:
        post_ingest_maintenance(
            meta,
            pg_cfg,
            stats=stats,
            md5_hash=md5_hash or "",
            prod_schema=prod_schema,
            ingest_schema=ingest_schema,
            zip_path=zip_path,
        )
    except Exception as e:
        # Maintenance không critical — không đảo ngược success.
        logger.warning(f"[WARN] post_ingest_maintenance failed: {e}")

    logger.info(
        f"[SUCCESS] {zip_name}: inserted={stats.rows_inserted:,} "
        f"csv_rows={stats.rows_in_csv:,} validation={stats.validation_passed}"
    )
    return result


# ---- Internal helpers ---------------------------------------------------

def _check_should_process(
    zip_path: Path, pg_cfg: PostgresConfig, ingest_schema: str
) -> tuple[bool, str, str]:
    """Hỏi ``IngestLogRepository`` có nên process ZIP này không.

    Returns:
        ``(skip, md5_hash, reason)`` — ``skip=True`` nếu KHÔNG cần process.
    """
    conn = get_pg_conn(pg_cfg)
    try:
        repo = IngestLogRepository(conn, ingest_schema=ingest_schema)
        repo.ensure_schema()
        should_process, reason = repo.should_process(zip_path)
        # md5 cần thiết kể cả khi skip (để log/debug)
        md5_hash = compute_zip_md5(zip_path)
        return (not should_process, md5_hash, reason)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _copy_with_retry(**kwargs) -> IngestStats:
    """Wrap ``copy_and_insert_to_production`` với deadlock retry.

    Retry chỉ áp dụng cho ``DeadlockDetected`` / ``SerializationFailure``
    (transient lỗi do tranh chấp lock). Các exception khác → không retry.
    """
    try:
        deadlock_excs = _import_psycopg_errors()
    except Exception:
        # Môi trường không có psycopg2 (test) → chạy 1 lần, không retry.
        return copy_and_insert_to_production(**kwargs)

    @retry(
        retry=retry_if_exception_type(deadlock_excs),
        stop=stop_after_attempt(_MAX_DEADLOCK_RETRIES),
        wait=wait_exponential(
            multiplier=_DEADLOCK_RETRY_BASE_DELAY, max=_DEADLOCK_RETRY_MAX_DELAY
        ),
        reraise=True,
        before_sleep=lambda rs: logger.warning(
            f"[RETRY] Deadlock/serialization conflict (attempt "
            f"{rs.attempt_number}/{_MAX_DEADLOCK_RETRIES}): {rs.outcome.exception()}"
        ),
    )
    def _attempt() -> IngestStats:
        return copy_and_insert_to_production(**kwargs)

    try:
        return _attempt()
    except RetryError as re:
        # tenacity wraps last failure — unwrap để caller log message gốc.
        raise re.last_attempt.exception() if re.last_attempt else re


def _copy_zip_to_fail_dir(zip_path: Path, meta: dict[str, Any], fs_cfg: FSConfig) -> None:
    """Copy ZIP sang ``fail_data`` (giữ nguyên bản gốc)."""
    zip_name = zip_path.name
    try:
        extract_dir = meta.get("extract_dir") if meta else None
        src = (extract_dir / zip_name) if (extract_dir and (extract_dir / zip_name).exists()) else zip_path
        fail_path = fs_cfg.fail_dir / zip_name
        fail_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(fail_path))
        logger.info(f"[COPY] {zip_name}: {src} → {fail_path}")
    except Exception as move_err:
        logger.warning(f"[WARN] Could not copy ZIP to fail_data: {move_err}")
