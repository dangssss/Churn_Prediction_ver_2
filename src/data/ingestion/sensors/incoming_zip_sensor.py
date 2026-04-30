# sensors/incoming_zip_sensor.py
"""Scan ``incoming_dir`` → filter ZIPs → invoke ``ingest_zip_job``.

Convention:
  - 10 §5.1 SRP — module orchestrate scan/filter, KHÔNG biết DB schema chi tiết.
  - 04 §5.3 Application layer.

Re-ingest policy:
  Skip-decision được delegate sang :class:`IngestLogRepository.should_process`
  (md5-based). Trước đây dùng ``has_success_log`` + so sánh mtime — không
  đáng tin khi file copy qua nhiều stage.

Filter:
  - **snapshot**: chỉ lấy file có ``period_key`` lớn nhất.
  - **monthly**: top 2 tháng mới nhất + auto-fill gap (tháng cũ chưa success).
"""
from __future__ import annotations

import shutil
from collections import defaultdict
from pathlib import Path

from data.ingestion.jobs.ingest_zip_job import ingest_zip_job
from data.ingestion.ops.ingest_log_repository import IngestLogRepository
from data.ingestion.ops.naming import parse_zip_and_decide_names
from data.ingestion.resources import (
    FSConfig,
    PostgresConfig,
    get_pg_conn,
    list_zip_files,
)
from shared.logging_config import get_logger

logger = get_logger(__name__)


# ---- Constants ----------------------------------------------------------

# Số tháng mới nhất luôn refresh ở mode monthly (Kịch bản A — full reload).
_MONTHLY_REFRESH_TOP_N = 2

# None = process tất cả gap; số cụ thể = giới hạn batch.
_MONTHLY_MAX_BATCH_SIZE: int | None = None

# Sleep giữa các lần scan ở chế độ ``main()`` daemon.
_SCAN_INTERVAL_SECONDS = 30


# ---- Public API ---------------------------------------------------------

def filter_files_to_process(
    zip_paths: list[Path],
    pg_cfg: PostgresConfig,
    ingest_schema: str,
) -> list[Path]:
    """Lọc danh sách ZIP cần process.

    - Snapshot: chỉ giữ file có ``period_key`` lớn nhất per base.
    - Monthly: top ``_MONTHLY_REFRESH_TOP_N`` mới nhất + gap fill.
    """
    snapshot_files: dict[str, list[tuple]] = defaultdict(list)
    monthly_files: dict[str, list[tuple]] = defaultdict(list)

    for zip_path in zip_paths:
        try:
            meta = parse_zip_and_decide_names(zip_path)
        except Exception as e:
            logger.warning(f"Skipping {zip_path.name}: {e}")
            continue

        mode = meta.get("mode")
        base = meta.get("base")

        if mode == "snapshot":
            snapshot_files[base].append((meta.get("period_key"), zip_path))
        elif mode == "monthly":
            monthly_files[base].append((meta.get("yymm"), zip_path))

    result: list[Path] = []

    # ----- Snapshot: 1 file mới nhất per base -----
    for base, files in snapshot_files.items():
        files.sort(key=lambda x: x[0], reverse=True)
        latest_pk, latest_path = files[0]
        logger.info(f"Selected latest snapshot {base}: {latest_path.name} (period_key={latest_pk})")
        result.append(latest_path)
        if len(files) > 1:
            logger.debug(f"Skipped {len(files) - 1} older {base} files")

    # ----- Monthly: top N + gap fill -----
    if monthly_files:
        # Mở 1 connection cho toàn bộ gap-check (giảm overhead).
        result.extend(_select_monthly_files(monthly_files, pg_cfg, ingest_schema))

    return result


def run_once_scan(
    fs_cfg: FSConfig | None = None,
    pg_cfg: PostgresConfig | None = None,
    *,
    prod_schema: str = "public",
    ingest_schema: str = "ingest",
) -> dict[str, int]:
    """1 vòng scan: discover → filter → ingest từng ZIP.

    Returns:
        Counters: ``{"total", "filtered", "success", "skipped", "failed"}``.
    """
    if fs_cfg is None:
        fs_cfg = FSConfig.from_env()
    if pg_cfg is None:
        pg_cfg = PostgresConfig.from_env()

    counters = {"total": 0, "filtered": 0, "success": 0, "skipped": 0, "failed": 0}

    incoming = fs_cfg.incoming_dir
    logger.info(f"Scanning ZIP files in: {incoming}")

    zip_paths = list_zip_files(fs_cfg)
    counters["total"] = len(zip_paths)
    if not zip_paths:
        logger.info(f"No ZIP files found in {incoming}")
        return counters

    filtered_paths = filter_files_to_process(zip_paths, pg_cfg, ingest_schema)
    counters["filtered"] = len(filtered_paths)
    logger.info(f"Filtered: {len(filtered_paths)}/{len(zip_paths)} files to process")

    for zip_path in filtered_paths:
        zip_name = zip_path.name
        try:
            logger.info(f"Running ingest_zip_job for {zip_name}")
            result = ingest_zip_job(
                zip_path=zip_path,
                fs_cfg=fs_cfg,
                pg_cfg=pg_cfg,
                prod_schema=prod_schema,
                ingest_schema=ingest_schema,
            )
        except Exception as e:
            counters["failed"] += 1
            logger.error(f"Error processing {zip_name}: {e}")
            continue

        if result.get("success"):
            counters["success"] += 1
            logger.info(
                f"Successfully ingested {zip_name}: "
                f"rows_inserted={result.get('rows_inserted', 0):,}, "
                f"rows_in_csv={result.get('rows_in_csv', 0):,}, "
                f"validation_passed={result.get('validation_passed')}"
            )
            _save_processed_zip(zip_path, fs_cfg)
        elif result.get("skipped"):
            counters["skipped"] += 1
            logger.info(f"Skipped {zip_name}: {result.get('reason', 'unknown')}")
        else:
            counters["failed"] += 1
            logger.error(f"Failed {zip_name}: {result.get('error', 'unknown error')}")

    logger.info(f"Scan summary: {counters}")
    return counters


def main() -> None:
    """Daemon entry — scan loop, sleep ``_SCAN_INTERVAL_SECONDS`` giữa các lần."""
    import time

    logger.info("Starting incoming ZIP sensor...")
    while True:
        try:
            run_once_scan()
        except Exception as e:
            logger.error(f"run_once_scan crashed: {e}")
        time.sleep(_SCAN_INTERVAL_SECONDS)


# ---- Internal helpers ---------------------------------------------------

def _select_monthly_files(
    monthly_files: dict[str, list[tuple]],
    pg_cfg: PostgresConfig,
    ingest_schema: str,
) -> list[Path]:
    """Chọn file monthly: top N mới nhất + auto-fill gap.

    Mở 1 connection để check gap qua :class:`IngestLogRepository`.
    """
    selected: list[Path] = []
    conn = get_pg_conn(pg_cfg)
    try:
        repo = IngestLogRepository(conn, ingest_schema=ingest_schema)
        repo.ensure_schema()

        for base, files in monthly_files.items():
            if not files:
                continue
            files.sort(key=lambda x: x[0], reverse=True)

            top_n = files[: min(_MONTHLY_REFRESH_TOP_N, len(files))]
            to_process: set[tuple] = set(top_n)
            logger.info(
                f"Selected top {len(top_n)} month(s) for {base} (always refresh)"
            )
            for yymm, p in top_n:
                logger.info(f"  [TOP] {p.name} (yymm={yymm})")

            # Gap fill: tháng cũ hơn nhưng chưa success (theo md5).
            older = files[_MONTHLY_REFRESH_TOP_N:]
            gaps: list[tuple] = []
            for yymm, old_path in older:
                should, reason = repo.should_process(old_path)
                if should:
                    gaps.append((yymm, old_path))
                    logger.info(f"  [GAP] {old_path.name} (yymm={yymm}, reason={reason})")
                    if (
                        _MONTHLY_MAX_BATCH_SIZE is not None
                        and len(to_process) + len(gaps) >= _MONTHLY_MAX_BATCH_SIZE
                    ):
                        logger.info(
                            f"  Reached batch limit ({_MONTHLY_MAX_BATCH_SIZE}), "
                            f"remaining gaps deferred to next run"
                        )
                        break

            to_process.update(gaps)
            sorted_files = sorted(to_process, key=lambda x: x[0], reverse=True)
            selected.extend(p for _, p in sorted_files)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return selected


def _save_processed_zip(zip_path: Path, fs_cfg: FSConfig) -> None:
    """Copy ZIP đã success sang ``saved_dir`` (giữ nguyên bản gốc)."""
    zip_name = zip_path.name
    saved_path = fs_cfg.saved_dir / zip_name
    if saved_path.exists():
        logger.info(f"File {zip_name} already exists in saved_data, overwriting...")
        saved_path.unlink()
    saved_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(zip_path), str(saved_path))
    logger.info(f"Copied {zip_name} to saved_data (kept original in incoming_dir)")


if __name__ == "__main__":
    main()
