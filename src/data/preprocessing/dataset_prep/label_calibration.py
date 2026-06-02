"""Data-driven calibration for pseudo-label thresholds and sample weights."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from data.preprocessing.dataset_prep.leading_prototype import compute_similarity


@dataclass(frozen=True)
class PseudoLabelThresholds:
    """Thresholds calibrated from the current scoring population."""

    sim_threshold: float
    recency_reliable_neg: int
    trend_down_ratio: float


@dataclass(frozen=True)
class LabelWeights:
    """Source-aware training weights anchored by confirmed CSKH labels."""

    confirmed: float
    pseudo_churn: float
    reliable_neg: float
    pu_unlabeled: float
    rule_based: float


def calibrate_pseudo_label_thresholds(
    active_df: pd.DataFrame,
    prototype: dict,
    *,
    similarity_quantile: float,
    reliable_neg_recency_quantile: float,
    trend_down_quantile: float,
) -> tuple[pd.Series, PseudoLabelThresholds]:
    """Calibrate pseudo-label thresholds from current feature distributions."""
    sim_scores = compute_similarity(active_df, prototype)
    recency = pd.to_numeric(active_df["recency_days"], errors="coerce").dropna()
    item_avg = pd.to_numeric(active_df["item_avg"], errors="coerce")
    if "item_last" in active_df.columns:
        item_last = pd.to_numeric(active_df["item_last"], errors="coerce")
        valid_avg = item_avg > 0
        item_ratio = (item_last[valid_avg] / item_avg[valid_avg]).dropna()
    else:
        item_ratio = pd.Series(dtype=float)

    thresholds = PseudoLabelThresholds(
        sim_threshold=_quantile_or_default(sim_scores, similarity_quantile, default=1.0),
        recency_reliable_neg=int(round(_quantile_or_default(recency, reliable_neg_recency_quantile, default=0.0))),
        trend_down_ratio=_quantile_or_default(item_ratio, trend_down_quantile, default=1.0),
    )
    return sim_scores, thresholds


def calibrate_label_weights(
    label_sources: pd.Series,
    *,
    n_confirmed_training: int,
    n_rule_based: int,
    min_aux_weight: float,
    max_aux_weight: float,
) -> LabelWeights:
    """Scale auxiliary-label weight by its volume relative to confirmed rows."""
    source_counts = label_sources.value_counts()
    anchor = max(int(n_confirmed_training), 1)

    def adaptive_weight(source_count: int) -> float:
        ratio = anchor / max(int(source_count), 1)
        return float(min(max(ratio, min_aux_weight), max_aux_weight))

    return LabelWeights(
        confirmed=1.0,
        pseudo_churn=adaptive_weight(source_counts.get("pseudo_churn", 0)),
        reliable_neg=adaptive_weight(source_counts.get("reliable_neg", 0)),
        pu_unlabeled=adaptive_weight(source_counts.get("pu_unlabeled", 0)),
        rule_based=adaptive_weight(n_rule_based),
    )


def _quantile_or_default(
    values: pd.Series,
    quantile: float,
    *,
    default: float,
) -> float:
    """Return a stable float quantile for an optional numeric series."""
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.quantile(quantile)) if not numeric.empty else default
