"""Tests for ``ingest_log_repository``.

Convention 07 §3 — unit tests dùng fake/mocks, không touch DB thật.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from data.ingestion.ops.ingest_log_repository import (
    IngestLogRepository,
    compute_zip_md5,
)


# ---- compute_zip_md5 ----------------------------------------------------

def test_compute_zip_md5_matches_hashlib(tmp_path: Path) -> None:
    payload = b"hello world" * 1000
    f = tmp_path / "x.zip"
    f.write_bytes(payload)

    assert compute_zip_md5(f) == hashlib.md5(payload).hexdigest()


def test_compute_zip_md5_handles_empty_file(tmp_path: Path) -> None:
    f = tmp_path / "empty.zip"
    f.write_bytes(b"")

    assert compute_zip_md5(f) == hashlib.md5(b"").hexdigest()


# ---- IngestLogRepository.should_process ---------------------------------

class _FakeCursor:
    def __init__(self, fetch_result):
        self._fetch_result = fetch_result
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetch_result


class _FakeConn:
    def __init__(self, fetch_result=None):
        self._fetch_result = fetch_result
        self.commits = 0

    def cursor(self):
        return _FakeCursor(self._fetch_result)

    def commit(self):
        self.commits += 1


def _make_zip(tmp_path: Path, name: str, content: bytes = b"abc") -> Path:
    p = tmp_path / name
    p.write_bytes(content)
    return p


def test_should_process_new_file(tmp_path: Path) -> None:
    conn = _FakeConn(fetch_result=None)
    repo = IngestLogRepository(conn)

    zip_path = _make_zip(tmp_path, "new.zip")
    should, reason = repo.should_process(zip_path)

    assert should is True
    assert reason == "new_file"


def test_should_process_skip_when_success_and_md5_matches(tmp_path: Path) -> None:
    zip_path = _make_zip(tmp_path, "same.zip", b"payload")
    md5 = hashlib.md5(b"payload").hexdigest()

    conn = _FakeConn(fetch_result=("success", None, md5, 100))
    repo = IngestLogRepository(conn)

    should, reason = repo.should_process(zip_path)

    assert should is False
    assert reason == "already_ingested_same_md5"


def test_should_process_when_md5_changed(tmp_path: Path) -> None:
    zip_path = _make_zip(tmp_path, "changed.zip", b"new-payload")
    old_md5 = hashlib.md5(b"old-payload").hexdigest()

    conn = _FakeConn(fetch_result=("success", None, old_md5, 100))
    repo = IngestLogRepository(conn)

    should, reason = repo.should_process(zip_path)

    assert should is True
    assert reason == "content_changed_md5_diff"


def test_should_process_retries_after_failure_with_same_md5(tmp_path: Path) -> None:
    zip_path = _make_zip(tmp_path, "retry.zip", b"payload")
    md5 = hashlib.md5(b"payload").hexdigest()

    conn = _FakeConn(fetch_result=("failed", None, md5, 0))
    repo = IngestLogRepository(conn)

    should, reason = repo.should_process(zip_path)

    assert should is True
    assert reason == "retry_previous_failure"
