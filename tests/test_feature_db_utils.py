import pytest

from features.engineering.feature_gen import db_utils
from features.engineering.feature_gen.feature_source_schema import (
    BCCP_ORDERITEM_CONTRACT,
    FEATURE_SOURCE_CONTRACTS,
)


def _columns_for(contract: dict[str, tuple[str, ...]]) -> list[dict]:
    return [
        {"name": column_name, "type": accepted_types[0]}
        for column_name, accepted_types in contract.items()
    ]


class _FakeInspector:
    def __init__(self, schemas: dict[str, list[dict]]):
        self._schemas = schemas

    def get_table_names(self, schema: str) -> list[str]:
        assert schema == "public"
        return list(self._schemas)

    def get_columns(self, table_name: str, schema: str) -> list[dict]:
        assert schema == "public"
        return self._schemas[table_name]


def test_discover_bccp_tables_accepts_only_strict_yymm_suffixes():
    assert db_utils.discover_bccp_tables(
        {
            "bccp_orderitem_2502",
            "bccp_orderitem_backup",
            "bccp_orderitem_2501",
            "bccp_orderitem_25a1",
            "cas_customer",
        }
    ) == [
        "bccp_orderitem_2501",
        "bccp_orderitem_2502",
    ]


def test_build_bccp_src_rejects_missing_monthly_and_base_tables(monkeypatch):
    inspector = _FakeInspector({})
    monkeypatch.setattr(db_utils, "inspect", lambda _engine: inspector)

    with pytest.raises(RuntimeError, match="No bccp_orderitem source found"):
        db_utils.build_bccp_src(object(), "2025-01-01", "2025-01-31")


def test_build_bccp_src_uses_base_table_as_explicit_fallback(monkeypatch):
    inspector = _FakeInspector({"bccp_orderitem": []})
    monkeypatch.setattr(db_utils, "inspect", lambda _engine: inspector)

    assert (
        db_utils.build_bccp_src(object(), "2025-01-01", "2025-01-31")
        == "public.bccp_orderitem"
    )


def test_ensure_feature_source_schema_validates_monthly_bccp_tables(monkeypatch):
    schemas = {
        table_name: _columns_for(contract)
        for table_name, contract in FEATURE_SOURCE_CONTRACTS.items()
    }
    schemas["bccp_orderitem_2501"] = _columns_for(BCCP_ORDERITEM_CONTRACT)
    inspector = _FakeInspector(schemas)
    monkeypatch.setattr(db_utils, "inspect", lambda _engine: inspector)

    db_utils.ensure_feature_source_schema(object())


def test_ensure_feature_source_schema_rejects_bad_bccp_partition(monkeypatch):
    schemas = {
        table_name: _columns_for(contract)
        for table_name, contract in FEATURE_SOURCE_CONTRACTS.items()
    }
    schemas["bccp_orderitem_2501"] = [
        column
        for column in _columns_for(BCCP_ORDERITEM_CONTRACT)
        if column["name"] != "sending_time"
    ]
    inspector = _FakeInspector(schemas)
    monkeypatch.setattr(db_utils, "inspect", lambda _engine: inspector)

    with pytest.raises(
        RuntimeError,
        match="Table bccp_orderitem_2501 missing column: sending_time",
    ):
        db_utils.ensure_feature_source_schema(object())
