"""Unit tests for feature generation template engine."""

import pytest
from features.engineering.feature_gen.template_engine import (
    TEMPLATE_PATHS,
    clear_cache,
    get_template,
    render_template,
)


@pytest.fixture(autouse=True)
def _clear_template_cache():
    """Ensure cache is empty before each test."""
    clear_cache()
    yield
    clear_cache()


def test_get_template_loads_from_disk():
    """Verify that get_template successfully loads the SQL file."""
    # This assumes 'lifetime_table' exists in the correctly placed database/sql folder
    content = get_template("lifetime_table")
    assert content, "Template content should not be empty."
    assert "data_static" in content or "CREATE TABLE" in content


def test_render_template_replaces_placeholders():
    """Verify placeholders like {schema} and {table_name} are correctly replaced."""
    # Usually lifetime_table has placeholders, we'll test the engine string replacement.
    
    # Let's mock the cache to inject a fake template for controlled testing
    from features.engineering.feature_gen.template_engine import _TEMPLATE_CACHE
    _TEMPLATE_CACHE["dummy_test"] = "SELECT * FROM {schema}.{table_name} WHERE id = '{id}'"
    
    rendered = render_template(
        "dummy_test", 
        schema="test_schema", 
        table_name="test_table", 
        id="123"
    )
    
    expected = "SELECT * FROM test_schema.test_table WHERE id = '123'"
    assert rendered == expected, f"Expected {expected}, got {rendered}"


def test_get_template_invalid_throws_error():
    """Verify unknown template name triggers ValueError."""
    with pytest.raises(ValueError, match="Unknown template: invalid_name"):
        get_template("invalid_name")


def test_render_template_rejects_unresolved_placeholders():
    """Verify missing replacements fail before invalid SQL reaches Postgres."""
    from features.engineering.feature_gen.template_engine import _TEMPLATE_CACHE

    _TEMPLATE_CACHE["dummy_test"] = "SELECT * FROM {schema}.{table_name}"

    with pytest.raises(ValueError, match=r"\{table_name\}"):
        render_template("dummy_test", schema="public")


def test_template_paths_use_feature_engineering_canonical_directory():
    """Verify runtime SQL ownership does not drift back into preprocessing."""
    for path in TEMPLATE_PATHS.values():
        assert "features/engineering/database/sql" in path.as_posix()
        assert "data/preprocessing/database/sql" not in path.as_posix()


def test_sliding_template_keeps_end_day_complaints_and_non_null_inactive_days():
    """Verify fixed activity and timestamp-boundary guards remain in SQL."""
    content = get_template("sliding_aggregate")

    assert "create_complaint_date < DATE '{END_DATE}' + INTERVAL '1 day'" in content
    assert "(DATE '{END_DATE}' - DATE '{START_DATE}' + 1)" in content
    assert "bs.window_days::int - COALESCE(bs.active_days, 0)" not in content


def test_sliding_template_defines_recency_as_inclusive_offset_from_window_start():
    """Verify recency is day position of the latest service date inside the window."""
    content = get_template("sliding_aggregate")

    assert "(MAX(send_date) - DATE '{START_DATE}'::date + 1)::int AS recency_days" in content
    assert "DATE '{END_DATE}'::date - MAX(send_date)" not in content


def test_lifetime_template_handles_single_bccp_order_and_refreshes_contract_classify():
    """Verify lifetime aggregation keeps corrected denominator and upsert fields."""
    content = get_template("lifetime_aggregate")

    assert "NULLIF(COUNT(*), 0)" in content
    assert "NULLIF(COUNT(*), 1)" not in content
    assert "contract_classify = EXCLUDED.contract_classify" in content


def test_lifetime_template_counts_distinct_active_report_months():
    """Verify lifetime activity counts months with orders, not source rows."""
    content = get_template("lifetime_aggregate")

    assert "COUNT(DISTINCT date_trunc('month', report_month))" in content
    assert "FILTER (WHERE item_count > 0)" in content
    assert "AND report_month <= CURRENT_DATE" in content
    assert "AND create_complaint_date < CURRENT_DATE + INTERVAL '1 day'" in content
    assert "AND sending_time < CURRENT_DATE + INTERVAL '1 day'" in content
    assert "COUNT(report_month) AS lifetime_months_active" not in content
