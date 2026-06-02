"""Pipeline configuration for the dataset preparation steps.

All hyperparameters and thresholds from the notebook CONFIG BLOCK
are centralized here as a typed, validated Pydantic model.

Conventions applied:
  - 02-Config §3.1: One config per subsystem.
  - 02-Config §5.1: Strong typing.
  - 02-Config §6.1: Self-validating.
  - 13-Data_ML §7.2: Hyperparameters through config, not hardcoded.
  - 13-Data_ML §9.4: Pipeline config externalized.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass
class DatasetPipelineConfig:
    """Configuration for the 7-step dataset preparation pipeline.

    Attributes:
        horizon_months: Fixed at 1 — predict inactivity in the next calendar month.
        w_min: Minimum window size to search.
        min_train_windows: Minimum folds for walk-forward validation.
        data_start: Earliest date to consider for feature computation.
        t_obs_override: Override observation date (None = auto-detect).

        min_lifetime_orders: Scope filter — minimum lifetime order count.
        min_account_age_months: Scope filter — minimum account age in months.

        recency_active: Tier boundary for 'active' (days).
        recency_at_risk: Tier boundary for 'at_risk' (days).
        alpha_ewma: EWMA smoothing parameter.
        sigma_reg: Regularization for covariance matrix inversion.

        label_smooth_eps_confirmed: Label smoothing for confirmed.
        label_smooth_eps_pseudo: Label smoothing for pseudo-labeled.

        cskh_file_path: Path to CSKH confirmed churn file (optional).
        label_months_back: Number of historical CSKH label months allowed.
    """

    # ── Window & horizon ──────────────────────────────────
    horizon_months: int = 1
    w_min: int = 3
    min_train_windows: int = 5
    data_start: date = field(default_factory=lambda: date(2025, 1, 1))
    t_obs_override: date | None = None

    # ── Scope filter ──────────────────────────────────────
    min_lifetime_orders: int = 3
    min_lifetime_gmv: float = 0.0
    min_account_age_months: int = 1

    # ── Tiering ───────────────────────────────────────────
    recency_active: int = 90
    recency_at_risk: int = 180
    reliable_neg_recency_quantile: float = 0.25

    # ── EWMA & similarity ─────────────────────────────────
    alpha_ewma: float = 0.3
    similarity_quantile: float = 0.95
    sigma_reg: float = 0.01

    # ── Label smoothing ───────────────────────────────────
    label_smooth_eps_confirmed: float = 0.00
    label_smooth_eps_pseudo: float = 0.10

    # ── Pseudo-labeling ───────────────────────────────────
    trend_down_quantile: float = 0.25

    # ── PU learning ───────────────────────────────────────
    min_aux_weight: float = 0.01
    max_aux_weight: float = 0.80

    # ── Prototype ─────────────────────────────────────────
    min_prototype_samples: int = 10

    # ── CSKH file ─────────────────────────────────────────
    cskh_file_path: Path | None = None
    cskh_dir: Path | None = None
    label_months_back: int = 6

    # ── Fallback behavior (khi không có file CSKH) ────────
    allow_prototype_fallback: bool = True
    max_prototype_age_months: int = 3

    def validate(self) -> None:
        """Validate configuration values.

        Raises:
            ValueError: If any parameter is invalid.
        """
        if self.horizon_months != 1:
            raise ValueError(
                "horizon_months must be 1 for the next-month inactivity target, "
                f"got {self.horizon_months}"
            )
        if self.w_min < 2:
            raise ValueError(f"w_min must be >= 2, got {self.w_min}")
        if self.min_train_windows < 2:
            raise ValueError(f"min_train_windows must be >= 2, got {self.min_train_windows}")
        if self.min_lifetime_orders < 0:
            raise ValueError(f"min_lifetime_orders must be >= 0, got {self.min_lifetime_orders}")
        if self.min_lifetime_gmv < 0:
            raise ValueError(f"min_lifetime_gmv must be >= 0, got {self.min_lifetime_gmv}")
        if self.min_account_age_months < 0:
            raise ValueError(f"min_account_age_months must be >= 0, got {self.min_account_age_months}")
        if not (0 < self.alpha_ewma < 1):
            raise ValueError(f"alpha_ewma must be in (0, 1), got {self.alpha_ewma}")
        for name, value in (
            ("similarity_quantile", self.similarity_quantile),
            ("reliable_neg_recency_quantile", self.reliable_neg_recency_quantile),
            ("trend_down_quantile", self.trend_down_quantile),
        ):
            if not (0 <= value <= 1):
                raise ValueError(f"{name} must be in [0, 1], got {value}")
        if not (0 < self.min_aux_weight <= self.max_aux_weight <= 1):
            raise ValueError(
                "auxiliary weights must satisfy "
                f"0 < min_aux_weight <= max_aux_weight <= 1, got "
                f"{self.min_aux_weight}, {self.max_aux_weight}"
            )
        if self.recency_active >= self.recency_at_risk:
            raise ValueError(
                f"recency_active ({self.recency_active}) must be < recency_at_risk ({self.recency_at_risk})"
            )
        if self.label_months_back < 1:
            raise ValueError(f"label_months_back must be >= 1, got {self.label_months_back}")

    def to_safe_dict(self) -> dict:
        """Return config as a logging-safe dictionary."""
        return {
            "horizon_months": self.horizon_months,
            "w_min": self.w_min,
            "data_start": str(self.data_start),
            "t_obs_override": str(self.t_obs_override) if self.t_obs_override else None,
            "min_lifetime_orders": self.min_lifetime_orders,
            "min_lifetime_gmv": self.min_lifetime_gmv,
            "min_account_age_months": self.min_account_age_months,
            "recency_active": self.recency_active,
            "recency_at_risk": self.recency_at_risk,
            "alpha_ewma": self.alpha_ewma,
            "similarity_quantile": self.similarity_quantile,
            "reliable_neg_recency_quantile": self.reliable_neg_recency_quantile,
            "trend_down_quantile": self.trend_down_quantile,
            "min_aux_weight": self.min_aux_weight,
            "max_aux_weight": self.max_aux_weight,
            "label_months_back": self.label_months_back,
        }


# ── Numeric features list (from notebook) ─────────────────
# Convention: 13-Data_ML §6.1 — snake_case, prefixed by domain.
NUMERIC_FEATURES: list[str] = [
    "item_sum",
    "item_avg",
    "item_std",
    "item_median",
    "revenue_sum",
    "revenue_avg",
    "revenue_std",
    "complaint_sum",
    "complaint_avg",
    "weight_sum",
    "weight_avg",
    "avg_revenue_per_item",
    "pct_delay",
    "pct_refund",
    "pct_noaccepted",
    "pct_lost_order",
    "pct_complaint",
    "pct_successful_item",
    "pct_intra_province",
    "pct_international",
    "avg_delayday",
    "order_score_avg",
    "satisfaction_avg",
    "active_months",
    "inactive_months",
    "active_days",
    "inactive_days",
    "avg_noservice_days",
    "avg_lastday",
    "item_slope",
    "revenue_slope",
    "satisfy_slope",
    "complaint_slope",
    "cv_item",
    "cv_revenue",
    "item_range",
    "revenue_range",
    "service_types_used",
    "dominant_service_ratio",
    "ser_c_sum",
    "ser_e_sum",
    "ser_m_sum",
    "ser_p_sum",
    "ser_r_sum",
    "ser_u_sum",
    "ser_l_sum",
    "ser_q_sum",
    "recency",
    "frequency",
    "monetary",
    # Multi-signal EWMA features
    "ewma_item",
    "delta_ewma_item",
    "ewma_revenue",
    "delta_ewma_revenue",
    "ewma_complaint",
    "delta_ewma_complaint",
    "ewma_delay",
    "delta_ewma_delay",
    "ewma_nodone",
    "delta_ewma_nodone",
    "ewma_order",
    "delta_ewma_order",
    "ewma_satisfaction",
    "delta_ewma_satisfaction",
    # Legacy aliases (backward compat)
    "ewma",
    "delta_ewma",
]
