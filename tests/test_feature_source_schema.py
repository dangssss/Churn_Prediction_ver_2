from data.ingestion.config.csv_schema import get_datetime_cols
from data.ingestion.config.table_schema import get_column_datatype
from data.validation.csv_schema import get_datetime_cols as get_validation_datetime_cols
from data.validation.table_schema import (
    get_column_datatype as get_validation_column_datatype,
)
from features.engineering.feature_gen.feature_source_schema import collect_schema_errors


def _column(name: str, data_type: str) -> dict:
    return {"name": name, "type": data_type}


def test_collect_schema_errors_accepts_compatible_type_prefixes():
    errors = collect_schema_errors(
        "cas_customer",
        [
            _column("cms_code_enc", "VARCHAR(20)"),
            _column("report_month", "DATE"),
            _column("item_count", "BIGINT"),
        ],
        {
            "cms_code_enc": ("VARCHAR", "TEXT"),
            "report_month": ("DATE", "TIMESTAMP"),
            "item_count": ("BIGINT", "NUMERIC"),
        },
    )

    assert errors == []


def test_collect_schema_errors_reports_missing_and_incompatible_columns():
    errors = collect_schema_errors(
        "cas_info",
        [
            _column("cms_code_enc", "VARCHAR(20)"),
            _column("contract_service", "TIMESTAMP WITHOUT TIME ZONE"),
        ],
        {
            "cms_code_enc": ("VARCHAR", "TEXT"),
            "contract_service": ("INT", "BIGINT", "NUMERIC"),
            "customer_update_date": ("DATE", "TIMESTAMP"),
        },
    )

    assert errors == [
        "Table cas_info column contract_service has type "
        "TIMESTAMP WITHOUT TIME ZONE; expected INT/BIGINT/NUMERIC",
        "Table cas_info missing column: customer_update_date",
    ]


def test_cas_info_ingestion_schema_keeps_contract_service_numeric():
    assert get_column_datatype("cas_info", "contract_service") == "INT"
    assert get_datetime_cols("cas_info") == {
        "contract_sig_first",
        "customer_update_date",
    }
    assert get_validation_column_datatype("cas_info", "contract_service") == "INT"
    assert get_validation_datetime_cols("cas_info") == {
        "contract_sig_first",
        "customer_update_date",
    }
