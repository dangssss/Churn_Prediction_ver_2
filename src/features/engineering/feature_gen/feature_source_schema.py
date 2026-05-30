"""Feature-generation source contracts and pure schema validation helpers."""

from __future__ import annotations

TEXT_TYPES = ("CHAR", "VARCHAR", "TEXT")
NUMERIC_TYPES = (
    "BIGINT",
    "DECIMAL",
    "DOUBLE",
    "FLOAT",
    "INT",
    "NUMERIC",
    "REAL",
    "SMALLINT",
)
DATE_TYPES = ("DATE", "DATETIME", "TIMESTAMP")

CAS_CUSTOMER_CONTRACT = {
    "cms_code_enc": TEXT_TYPES,
    "report_month": DATE_TYPES,
    "item_count": NUMERIC_TYPES,
    "weight_kg": NUMERIC_TYPES,
    "total_fee": NUMERIC_TYPES,
    "intra_province": NUMERIC_TYPES,
    "international": NUMERIC_TYPES,
    "ser_c": NUMERIC_TYPES,
    "ser_e": NUMERIC_TYPES,
    "ser_m": NUMERIC_TYPES,
    "ser_p": NUMERIC_TYPES,
    "ser_r": NUMERIC_TYPES,
    "ser_u": NUMERIC_TYPES,
    "ser_l": NUMERIC_TYPES,
    "ser_q": NUMERIC_TYPES,
    "delay_day": NUMERIC_TYPES,
    "delay_count": NUMERIC_TYPES,
    "nodone": NUMERIC_TYPES,
    "refunded": NUMERIC_TYPES,
    "noaccepted": NUMERIC_TYPES,
    "lost_order": NUMERIC_TYPES,
    "lastday": NUMERIC_TYPES,
    "order_score": NUMERIC_TYPES,
    "satisfaction_score": NUMERIC_TYPES,
    "total_complaint": NUMERIC_TYPES,
}

CMS_COMPLAINT_CONTRACT = {
    "cms_code_enc": TEXT_TYPES,
    "create_complaint_date": DATE_TYPES,
    "complaint_code": NUMERIC_TYPES,
}

CAS_INFO_CONTRACT = {
    "cms_code_enc": TEXT_TYPES,
    "cus_province": NUMERIC_TYPES,
    "contract_service": NUMERIC_TYPES,
    "contract_sig_first": DATE_TYPES,
    "tenure": NUMERIC_TYPES,
    "custype": NUMERIC_TYPES,
    "contract_classify": NUMERIC_TYPES,
    "contract_mgr_org": NUMERIC_TYPES,
    "cus_poscode": NUMERIC_TYPES,
    "customer_update_date": DATE_TYPES,
}

BCCP_ORDERITEM_CONTRACT = {
    "cms_code_enc": TEXT_TYPES,
    "rec_province_code": NUMERIC_TYPES,
    "rec_district_code": NUMERIC_TYPES,
    "rec_commune_code": NUMERIC_TYPES,
    "region": TEXT_TYPES,
    "sending_time": DATE_TYPES,
    "total_complaint": NUMERIC_TYPES,
}

FEATURE_SOURCE_CONTRACTS = {
    "cas_customer": CAS_CUSTOMER_CONTRACT,
    "cms_complaint": CMS_COMPLAINT_CONTRACT,
    "cas_info": CAS_INFO_CONTRACT,
}


def collect_schema_errors(
    table_name: str,
    columns_info: list[dict],
    contract: dict[str, tuple[str, ...]],
) -> list[str]:
    """Return missing-column and incompatible-type errors for one source table."""
    columns = {column["name"]: column for column in columns_info}
    errors = []
    for column_name, accepted_types in contract.items():
        column = columns.get(column_name)
        if column is None:
            errors.append(f"Table {table_name} missing column: {column_name}")
            continue

        actual_type = str(column["type"]).upper()
        if not actual_type.startswith(accepted_types):
            expected = "/".join(accepted_types)
            errors.append(
                f"Table {table_name} column {column_name} has type "
                f"{actual_type}; expected {expected}"
            )
    return errors
