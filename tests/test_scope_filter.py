from __future__ import annotations

import pandas as pd
import pytest

from data.preprocessing.dataset_prep.scope_filter import load_eval_ids, load_working_set


def test_load_eval_ids_rejects_future_single_file(tmp_path) -> None:
    path = tmp_path / "label_2504.csv"
    pd.DataFrame({"cms_code_enc": ["CMS001"]}).to_csv(path, index=False)

    with pytest.raises(ValueError, match="after allowed cutoff"):
        load_eval_ids(path, {"CMS001"}, label_to_yymm=2503)


def test_load_eval_ids_accepts_single_file_at_cutoff(tmp_path) -> None:
    path = tmp_path / "label_2503.csv"
    pd.DataFrame({"cms_code_enc": [" CMS001 ", "CMS999"]}).to_csv(path, index=False)

    assert load_eval_ids(path, {"CMS001"}, label_to_yymm=2503) == {"CMS001"}


def test_load_working_set_binds_point_in_time_snapshot(monkeypatch) -> None:
    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    captured = {}

    def fake_read_sql(sql, conn, params):
        captured["sql"] = str(sql)
        captured["params"] = params
        return pd.DataFrame({"cms_code_enc": ["CMS001"]})

    monkeypatch.setattr(
        "data.preprocessing.dataset_prep.scope_filter.pd.read_sql",
        fake_read_sql,
    )

    result = load_working_set(FakeEngine(), 3, 1000.0, 2, pd.Timestamp("2025-03-31"))

    assert result["cms_code_enc"].tolist() == ["CMS001"]
    assert "data_static.cus_lifetime_snapshot" in captured["sql"]
    assert "cl.lifetime_total_revenue >= :min_gmv" in captured["sql"]
    assert "cl.tenure >= :min_account_age_months" in captured["sql"]
    assert "cl.snapshot_month = :snapshot_month" in captured["sql"]
    assert captured["params"] == {
        "min_orders": 3,
        "min_gmv": 1000.0,
        "min_account_age_months": 2,
        "snapshot_month": pd.Timestamp("2025-03-01"),
    }
