from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def ensure_best_config_table(engine: Engine) -> None:
    create_sql = """
    CREATE SCHEMA IF NOT EXISTS data_static;

    CREATE TABLE IF NOT EXISTS data_static.model_best_config (
        as_of_month       INT NOT NULL,
        horizon           INT NOT NULL,

        best_k            INT NOT NULL,
        use_static        BOOLEAN NOT NULL,

        best_threshold    DOUBLE PRECISION NOT NULL,
        best_spw          DOUBLE PRECISION NOT NULL,

        metric_f05_val    DOUBLE PRECISION,
        metric_pr_auc_val DOUBLE PRECISION,
        val_month         INT,
        target_month      INT,

        created_at        TIMESTAMP DEFAULT now(),
        notes            TEXT,

        -- production gating
        is_accepted       BOOLEAN NOT NULL DEFAULT TRUE,
        prev_accepted_f05 DOUBLE PRECISION,
        accept_rule       TEXT,
        accepted_at       TIMESTAMP,

        PRIMARY KEY (as_of_month, horizon)
    );
    """
    with engine.begin() as conn:
        for stmt in create_sql.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

    # add new columns if table already existed
    alter_sql = """
    ALTER TABLE data_static.model_best_config
    ADD COLUMN IF NOT EXISTS metric_f05_val DOUBLE PRECISION;

    ALTER TABLE data_static.model_best_config
    ADD COLUMN IF NOT EXISTS metric_pr_auc_val DOUBLE PRECISION;

    ALTER TABLE data_static.model_best_config
    ADD COLUMN IF NOT EXISTS is_accepted BOOLEAN NOT NULL DEFAULT TRUE;

    ALTER TABLE data_static.model_best_config
    ADD COLUMN IF NOT EXISTS prev_accepted_f05 DOUBLE PRECISION;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS metric_f2_val;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS metric_roc_auc_val;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS prev_accepted_f1;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS prev_accepted_f2;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS main_threshold;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS main_f1_val;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS main_ap_val;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS model_type;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS xgb_best_iteration;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS xgb_best_score;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS xgb_es_rounds;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS val_prevalence;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS dummy_ap_const0;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS dummy_ap_random;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS dummy_ap_simple2;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS dummy_simple2_features;

    ALTER TABLE data_static.model_best_config
    DROP COLUMN IF EXISTS guardrail_warning;

    ALTER TABLE data_static.model_best_config
    ADD COLUMN IF NOT EXISTS accept_rule TEXT;

    ALTER TABLE data_static.model_best_config
    ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMP;
    """
    with engine.begin() as conn:
        for stmt in alter_sql.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def upsert_best_config(engine: Engine, best_config: dict) -> None:
    ensure_best_config_table(engine)
    upsert_sql = """
    INSERT INTO data_static.model_best_config (
        as_of_month, horizon,
        best_k, use_static,
        best_threshold, best_spw,
        metric_f05_val, metric_pr_auc_val, val_month, target_month,
        notes,
        is_accepted, prev_accepted_f05, accept_rule, accepted_at
    )
    VALUES (
        :as_of_month, :horizon,
        :best_k, :use_static,
        :best_threshold, :best_spw,
        :metric_f05_val, :metric_pr_auc_val, :val_month, :target_month,
        :notes,
        :is_accepted, :prev_accepted_f05, :accept_rule, :accepted_at
    )
    ON CONFLICT (as_of_month, horizon) DO UPDATE SET
        best_k=EXCLUDED.best_k,
        use_static=EXCLUDED.use_static,
        best_threshold=EXCLUDED.best_threshold,
        best_spw=EXCLUDED.best_spw,
        metric_f05_val=EXCLUDED.metric_f05_val,
        metric_pr_auc_val=EXCLUDED.metric_pr_auc_val,
        val_month=EXCLUDED.val_month,
        target_month=EXCLUDED.target_month,
        created_at=now(),
        notes=EXCLUDED.notes,
        is_accepted=EXCLUDED.is_accepted,
        prev_accepted_f05=EXCLUDED.prev_accepted_f05,
        accept_rule=EXCLUDED.accept_rule,
        accepted_at=EXCLUDED.accepted_at;
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql), best_config)


def load_latest_best_config(engine: Engine, horizon: int) -> dict:
    q = text("""
        SELECT *
        FROM data_static.model_best_config
        WHERE horizon=:h
        ORDER BY as_of_month DESC
        LIMIT 1
    """)
    df = pd.read_sql(q, engine, params={"h": horizon})
    if df.empty:
        raise ValueError(f"Không tìm thấy best_config cho horizon={horizon}")
    return df.iloc[0].to_dict()


def load_latest_accepted_best_config(engine: Engine, horizon: int) -> dict:
    q = text("""
        SELECT *
        FROM data_static.model_best_config
        WHERE horizon=:h AND is_accepted=TRUE
        ORDER BY as_of_month DESC
        LIMIT 1
    """)
    df = pd.read_sql(q, engine, params={"h": horizon})
    if df.empty:
        raise ValueError(f"Không tìm thấy accepted best_config cho horizon={horizon}")
    return df.iloc[0].to_dict()


def load_previous_accepted_best_config(engine: Engine, horizon: int) -> dict | None:
    q = text("""
        SELECT *
        FROM data_static.model_best_config
        WHERE horizon=:h AND is_accepted=TRUE
        ORDER BY as_of_month DESC
        OFFSET 1
        LIMIT 1
    """)
    df = pd.read_sql(q, engine, params={"h": horizon})
    return None if df.empty else df.iloc[0].to_dict()
