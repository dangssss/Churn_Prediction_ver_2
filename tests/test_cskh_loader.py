"""Tests for CSKH label file parsing."""

from __future__ import annotations

import pandas as pd
import pytest

from data.preprocessing.dataset_prep.cskh_loader import (
    _build_crm_resolution_ctes,
    _extract_confirmed_ids,
    _normalize_source_column,
    _prepare_raw_label_rows,
    load_eval_id_cohorts_from_db,
    load_eval_ids_from_db,
    parse_cskh_filename,
    shift_yymm,
)


def test_parse_legacy_roi_bo_filename() -> None:
    assert parse_cskh_filename("Roi_bo_03_25.csv") == (3, 25)


def test_parse_current_label_filename() -> None:
    assert parse_cskh_filename("label_2503.csv") == (3, 25)


def test_extract_confirmed_ids_accepts_encoded_label_column() -> None:
    df = pd.DataFrame({"crm_code_enc": [" C001 ", "C001", "C002", ""]})

    assert _extract_confirmed_ids(df, source_name="label_2503.csv") == ["C001", "C002"]


def test_extract_confirmed_ids_accepts_single_column_file() -> None:
    df = pd.DataFrame({"whatever": ["A", "B", "A"]})

    assert _extract_confirmed_ids(df, source_name="label_2503.csv") == ["A", "B"]


def test_normalize_source_column_handles_accented_month_column() -> None:
    assert _normalize_source_column("Th\u00e1ng KS") == "thang_ks"


def test_prepare_raw_label_rows_keeps_business_columns_and_deduplicates() -> None:
    df = pd.DataFrame(
        {
            "stt": ["1", "2"],
            "ma_kh": ["MKH001", "MKH001_DUP"],
            "ma_cms": ["CMS001", "CMS001_DUP"],
            "crm_code_enc": ["CRM001", "CRM001_DUP"],
            "cms_code_enc": [" C001 ", "C001"],
            "ma_don_vi": ["DV01", "DV01_DUP"],
            "ten_don_vi": ["Don vi 1", "Don vi duplicate"],
            "tinh_trang_kh": ["Roi bo", "Roi bo duplicate"],
            "Th\u00e1ng KS": ["202503", "202503"],
        }
    )

    rows = _prepare_raw_label_rows(
        df,
        month=3,
        year=25,
        source_file="label_2503.csv",
        source_name="label_2503.zip:label_2503.csv",
        source_zip="label_2503.zip",
        source_member="label_2503.csv",
    )

    assert rows == [
        {
            "label_yymm": 2503,
            "file_month": 3,
            "file_year": 25,
            "customer_key": "C001",
            "customer_key_type": "cms_code_enc",
            "source_file": "label_2503.csv",
            "source_zip": "label_2503.zip",
            "source_member": "label_2503.csv",
            "stt": "1",
            "ma_kh": "MKH001",
            "ma_cms": "CMS001",
            "crm_code_enc": "CRM001",
            "cms_code_enc": "C001",
            "ma_don_vi": "DV01",
            "ten_don_vi": "Don vi 1",
            "tinh_trang_kh": "Roi bo",
            "thang_ks": "202503",
        }
    ]


def test_prepare_raw_label_rows_falls_back_to_crm_when_cms_code_enc_is_blank() -> None:
    df = pd.DataFrame(
        {
            "ma_kh": ["MKH001", "MKH002"],
            "cms_code_enc": ["", " C002 "],
            "crm_code_enc": [" R001 ", "R002"],
        }
    )

    rows = _prepare_raw_label_rows(
        df,
        month=1,
        year=25,
        source_file="label_2501.csv",
        source_name="label_2501.zip:label_2501.csv",
    )

    assert [(row["customer_key"], row["customer_key_type"]) for row in rows] == [
        ("R001", "crm_code_enc"),
        ("C002", "cms_code_enc"),
    ]


def test_prepare_raw_label_rows_rejects_non_encoded_keys() -> None:
    df = pd.DataFrame({"ma_kh": ["MKH001"]})

    try:
        _prepare_raw_label_rows(
            df,
            month=1,
            year=25,
            source_file="label_2501.csv",
            source_name="label_2501.zip:label_2501.csv",
        )
    except ValueError as exc:
        assert "encoded customer id column" in str(exc)
    else:
        raise AssertionError("Expected non-encoded label key to be rejected")


def test_shift_yymm_handles_year_boundary() -> None:
    assert shift_yymm(2501, -1) == 2412
    assert shift_yymm(2512, 1) == 2601


def test_build_crm_resolution_ctes_keeps_all_multi_cms_and_uses_point_in_time_bccp() -> None:
    sql = _build_crm_resolution_ctes(
        [
            ("bccp_orderitem_2501", 2501),
            ("bccp_orderitem_2502", 2502),
        ]
    )

    assert "public.bccp_orderitem_2501" in sql
    assert "public.bccp_orderitem_2502" in sql
    assert "rc.label_yymm >= 2502" in sql
    assert "MIN(" not in sql
    assert "n_cms = 1" not in sql
    assert "NOT EXISTS" not in sql
    assert "'cas_info' AS resolve_source" in sql


def test_build_crm_resolution_ctes_falls_back_to_cas_info_without_bccp() -> None:
    sql = _build_crm_resolution_ctes([])

    assert "WHERE FALSE" in sql
    assert "JOIN public.cas_info ci" in sql
    assert "'cas_info' AS resolve_source" in sql


def test_load_eval_ids_from_db_binds_historical_label_range(monkeypatch) -> None:
    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    calls = []

    def fake_read_sql(sql, conn, params):
        calls.append((str(sql), params))
        if len(calls) == 1:
            return pd.DataFrame(
                {
                    "label_yymm": [2502, 2503],
                    "cms_code_enc": ["CMS001", "CMS999"],
                }
            )
        return pd.DataFrame(
            {
                "raw_crm_keys": [1],
                "resolved_crm_keys": [1],
                "unresolved_crm_keys": [0],
                "multi_cms_crm_keys": [0],
                "bccp_resolved_crm_keys": [1],
                "cas_info_resolved_crm_keys": [0],
                "resolved_cms_ids": [1],
            }
        )

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.cskh_loader.ensure_cskh_schema",
        lambda engine: None,
    )
    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.cskh_loader._discover_bccp_mapping_tables",
        lambda engine, label_to_yymm: [("bccp_orderitem_2501", 2501)],
    )
    monkeypatch.setattr("data.preprocessing.dataset_prep.cskh_loader.pd.read_sql", fake_read_sql)

    result = load_eval_id_cohorts_from_db(
        FakeEngine(),
        {"CMS001"},
        label_to_yymm=2503,
        months_back=3,
    )

    assert result == {2502: {"CMS001"}}
    assert len(calls) == 2
    for sql, params in calls:
        assert "BETWEEN :label_from_yymm AND :label_to_yymm" in sql
        assert params == {"label_from_yymm": 2501, "label_to_yymm": 2503}


def test_load_eval_ids_from_db_rejects_invalid_month_range() -> None:
    with pytest.raises(ValueError, match="months_back"):
        load_eval_ids_from_db(object(), set(), label_to_yymm=2503, months_back=0)
