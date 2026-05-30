"""Tests for CSKH label file parsing."""

from __future__ import annotations

import pandas as pd

from data.preprocessing.dataset_prep.cskh_loader import (
    _extract_confirmed_ids,
    _normalize_source_column,
    _prepare_raw_label_rows,
    parse_cskh_filename,
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
