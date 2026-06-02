"""Step 5 — Leading prototype computation.

Build the leading prototype (μ_lead, Σ_lead) from confirmed churners'
features at T-offset, then compute Mahalanobis-based similarity scores.

Conventions applied:
  - 13-Data_ML §6.2: Isolated, stateless functions.
  - 13-Data_ML §6.4: No hidden internal state.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from data.preprocessing.dataset_prep.ewma import compute_ewma
from data.preprocessing.dataset_prep.label_construction import (
    load_window_features,
)
from data.preprocessing.dataset_prep.pipeline_config import NUMERIC_FEATURES

logger = logging.getLogger(__name__)


def build_leading_prototype(
    engine: Any,
    eval_id_cohorts: dict[int, set[str]],
    window_size: int,
    alpha_ewma: float,
    sigma_reg: float,
    lead_offset: int = 1,
    min_prototype_samples: int = 10,
) -> dict[str, Any]:
    """Build the leading prototype from confirmed churners' features.

    Extracts features from each cohort's T-(lead_offset) month snapshot
    and computes the mean vector (μ) and regularized inverse covariance (Σ⁻¹).

    Args:
        engine: SQLAlchemy engine.
        eval_id_cohorts: Confirmed churn CMS codes grouped by label YYMM.
        window_size: Window size W.
        alpha_ewma: EWMA smoothing parameter.
        sigma_reg: Regularization for covariance inverse (σ_reg * I).
        lead_offset: Months before churn confirmation to sample features.
            Defaults to 1 to match the next-month inactivity target.
        min_prototype_samples: Minimum confirmed churners required.

    Returns:
        Dict with keys: mu, Sigma_inv, sigma2, feature_names, n_confirmed.
        Empty dict if insufficient data.
    """
    if not eval_id_cohorts:
        logger.warning("eval_id_cohorts is empty; prototype will be meaningless")
        return {}

    cohort_frames = []
    sampled_ids: set[str] = set()
    for label_yymm, cohort_ids in sorted(eval_id_cohorts.items()):
        new_ids = cohort_ids - sampled_ids
        if not new_ids:
            continue

        prototype_end = _parse_yymm(label_yymm) - pd.DateOffset(months=lead_offset)
        proto_df = load_window_features(engine, window_size, prototype_end)
        if proto_df.empty:
            logger.warning(
                "Cannot load prototype window at %s for label cohort %d",
                prototype_end.date(),
                label_yymm,
            )
            continue

        proto_df = compute_ewma(proto_df, window_size, alpha_ewma)
        cohort_frames.append(proto_df[proto_df["cms_code_enc"].isin(new_ids)])
        sampled_ids.update(new_ids)

    if not cohort_frames:
        logger.warning("No time-aligned prototype windows could be loaded")
        return {}

    proto_confirmed = pd.concat(cohort_frames, ignore_index=True)
    feats = [f for f in NUMERIC_FEATURES if f in proto_confirmed.columns]
    x_proto = proto_confirmed[feats].fillna(0).values

    if len(x_proto) < min_prototype_samples:
        logger.warning(
            "Prototype requires at least %d confirmed churners, "
            "but only found %d. Returning empty prototype — "
            "caller should handle fallback.",
            min_prototype_samples,
            len(x_proto),
        )
        return {}

    # Compute mean and regularized covariance inverse
    mu = x_proto.mean(axis=0)
    sigma = np.cov(x_proto.T) + sigma_reg * np.eye(len(feats))

    try:
        sigma_inv = np.linalg.inv(sigma)
    except np.linalg.LinAlgError:
        logger.warning("Singular covariance — using pseudo-inverse")
        sigma_inv = np.linalg.pinv(sigma)

    # σ² = median Mahalanobis distance within confirmed set
    diffs = x_proto - mu  # (N, D)
    mahal_sq = np.sum(diffs @ sigma_inv * diffs, axis=1)
    sigma2 = float(np.median(mahal_sq)) if len(mahal_sq) > 0 else 1.0

    logger.info(
        "Prototype built: %d confirmed churners, %d features, σ²=%.4f",
        len(x_proto),
        len(feats),
        sigma2,
    )

    return {
        "mu": mu,
        "Sigma_inv": sigma_inv,
        "sigma2": sigma2,
        "feature_names": feats,
        "n_confirmed": len(x_proto),
    }


def _parse_yymm(label_yymm: int) -> pd.Timestamp:
    """Convert a YYMM label integer into a month-start timestamp."""
    year, month = divmod(label_yymm, 100)
    if month < 1 or month > 12:
        raise ValueError(f"Invalid label_yymm month: {label_yymm}")
    return pd.Timestamp(year=2000 + year, month=month, day=1)


def compute_similarity(
    df: pd.DataFrame,
    prototype: dict[str, Any],
) -> pd.Series:
    """Compute Mahalanobis-based similarity scores against the prototype.

    sim(x) = exp(-d²(x, μ) / (2σ²))

    Args:
        df: Feature DataFrame.
        prototype: Dict from ``build_leading_prototype``.

    Returns:
        Series of similarity scores (0 to 1).
    """
    if not prototype:
        return pd.Series(0.0, index=df.index)

    feats = prototype["feature_names"]
    x = df[feats].fillna(0).values
    mu = prototype["mu"]
    s_inv = prototype["Sigma_inv"]
    sig2 = prototype["sigma2"]

    # Vectorized Mahalanobis distance
    diffs = x - mu  # (N, D)
    mahal_sq = np.maximum(0.0, np.sum(diffs @ s_inv * diffs, axis=1))

    # Secure computation to avoid exp overflow/underflow warnings
    exponent = -mahal_sq / (2 * max(sig2, 1e-9))
    exponent = np.clip(exponent, a_min=-700, a_max=0)
    scores = np.exp(exponent)

    return pd.Series(scores, index=df.index)
