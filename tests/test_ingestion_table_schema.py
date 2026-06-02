from data.ingestion.config.table_schema import TABLE_SCHEMAS, get_prod_table_ddl
from data.validation.table_schema import get_prod_table_ddl as get_validation_prod_table_ddl


def test_postgres_ddl_uses_supported_timestamp_type() -> None:
    for table_base in TABLE_SCHEMAS:
        ddl = get_prod_table_ddl(table_base, table_base)

        assert "DATETIME" not in ddl


def test_timestamp_columns_are_rendered_in_postgres_ddl() -> None:
    ddl = get_prod_table_ddl("bccp_orderitem", "bccp_orderitem_2501")

    assert '"sending_time" TIMESTAMP' in ddl
    assert '"ending_time" TIMESTAMP' in ddl
    assert '"bccp_update_date" TIMESTAMP' in ddl


def test_complaint_ddl_includes_transformed_etl_date() -> None:
    ddl = get_prod_table_ddl("cms_complaint", "cms_complaint")

    assert '"etl_date" TIMESTAMP' in ddl


def test_encoded_keys_allow_current_hash_length() -> None:
    ddl = get_prod_table_ddl("bccp_orderitem", "bccp_orderitem_2501")

    assert '"crm_code_enc" VARCHAR(100)' in ddl
    assert '"cms_code_enc" VARCHAR(100)' in ddl
    assert '"item_code" VARCHAR(100)' in ddl


def test_bccp_total_fee_supports_values_above_integer_range() -> None:
    ddl = get_prod_table_ddl("bccp_orderitem", "bccp_orderitem_2501")
    validation_ddl = get_validation_prod_table_ddl(
        "bccp_orderitem",
        "bccp_orderitem_2501",
    )

    assert '"total_fee" BIGINT' in ddl
    assert '"total_fee" BIGINT' in validation_ddl
