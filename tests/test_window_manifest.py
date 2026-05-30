import pytest

from features.engineering.feature_gen.window_manifest import (
    _validate_qualified_window_name,
    _validate_short_window_name,
    find_retry_window_tables,
)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _sql, _params):
        return _FakeResult(self._rows)


class _FakeEngine:
    def __init__(self, rows):
        self._rows = rows

    def connect(self):
        return _FakeConnection(self._rows)


@pytest.mark.parametrize(
    "table_name",
    [
        "cus_feature_3m_2501_2503",
        "cus_feature_12m_2501_2512",
    ],
)
def test_validate_short_window_name_accepts_expected_format(table_name):
    assert _validate_short_window_name(table_name) == table_name


@pytest.mark.parametrize(
    "table_name",
    [
        "cus_feature_3_2501_2503",
        "cus_feature_3m_2501",
        "cus_feature_3m_202501_2503",
        "public.cus_feature_3m_2501_2503",
        "cus_feature_3m_2501_2503;DROP TABLE public.cas_customer",
    ],
)
def test_validate_short_window_name_rejects_unexpected_format(table_name):
    with pytest.raises(ValueError, match="Invalid feature window table name"):
        _validate_short_window_name(table_name)


def test_validate_qualified_window_name_requires_data_window_schema():
    assert (
        _validate_qualified_window_name("data_window.cus_feature_3m_2501_2503")
        == "data_window.cus_feature_3m_2501_2503"
    )

    with pytest.raises(ValueError, match="Invalid data_window table name"):
        _validate_qualified_window_name("public.cus_feature_3m_2501_2503")


def test_find_retry_window_tables_returns_latest_incomplete_attempts():
    engine = _FakeEngine(
        [
            {"window_table": "data_window.cus_feature_3m_2501_2503", "status": "success"},
            {"window_table": "data_window.cus_feature_3m_2502_2504", "status": "failed"},
            {"window_table": "data_window.cus_feature_4m_2501_2504", "status": "running"},
        ]
    )

    assert find_retry_window_tables(
        engine,
        {
            "cus_feature_3m_2501_2503",
            "cus_feature_3m_2502_2504",
            "cus_feature_4m_2501_2504",
        },
    ) == {
        "cus_feature_3m_2502_2504",
        "cus_feature_4m_2501_2504",
    }
