"""Tests for ``ingest_zip_job`` re-ingest decision and result shape.

Mock heavy infra (DB connection, COPY, post_ingest) — kiểm tra control
flow + result keys.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from data.ingestion.ops.copy_and_insert_to_production import IngestStats


@pytest.fixture
def fake_zip(tmp_path: Path) -> Path:
    p = tmp_path / "fake.zip"
    p.write_bytes(b"some bytes")
    return p


def test_skip_when_should_process_returns_false(fake_zip: Path) -> None:
    from data.ingestion.jobs import ingest_zip_job as job_mod

    with patch.object(
        job_mod,
        "_check_should_process",
        return_value=(True, "abc123", "already_ingested_same_md5"),
    ):
        result = job_mod.ingest_zip_job(
            zip_path=fake_zip,
            fs_cfg=object(),  # not used when skipped
            pg_cfg=object(),
        )

    assert result["skipped"] is True
    assert result["reason"] == "already_ingested_same_md5"
    assert result["md5_hash"] == "abc123"
    assert result["success"] is False


def test_success_threads_stats_into_result(fake_zip: Path) -> None:
    from data.ingestion.jobs import ingest_zip_job as job_mod

    fake_meta = {
        "base": "bccp_orderitem",
        "table_name": "bccp_orderitem_2501",
        "csv_files": [Path("dummy.csv")],
        "extract_dir": fake_zip.parent,
    }
    fake_stats = IngestStats(
        rows_inserted=1234, rows_in_csv=1234, validation_passed=True, diff_pct=0.0
    )

    with patch.object(job_mod, "_check_should_process", return_value=(False, "md5x", "new_file")), \
         patch.object(job_mod, "unzip_and_discover", return_value=fake_meta), \
         patch.object(job_mod, "_copy_with_retry", return_value=fake_stats), \
         patch.object(job_mod, "post_ingest_maintenance"):
        result = job_mod.ingest_zip_job(
            zip_path=fake_zip,
            fs_cfg=object(),
            pg_cfg=object(),
        )

    assert result["success"] is True
    assert result["rows_inserted"] == 1234
    assert result["rows_in_csv"] == 1234
    assert result["validation_passed"] is True
    assert result["md5_hash"] == "md5x"


def test_no_csv_after_unzip_short_circuits(fake_zip: Path) -> None:
    from data.ingestion.jobs import ingest_zip_job as job_mod

    with patch.object(job_mod, "_check_should_process", return_value=(False, "m", "new_file")), \
         patch.object(job_mod, "unzip_and_discover", return_value={"csv_files": []}):
        result = job_mod.ingest_zip_job(
            zip_path=fake_zip,
            fs_cfg=object(),
            pg_cfg=object(),
        )

    assert result["skipped"] is True
    assert result["reason"] == "no_csv_after_unzip"
