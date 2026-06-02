"""Build time-aligned training and holdout rows from confirmed CSKH labels."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd

from data.preprocessing.dataset_prep.ewma import compute_ewma
from data.preprocessing.dataset_prep.label_construction import (
    build_label,
    load_window_features,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConfirmedCohortSplit:
    """Temporal split of confirmed CSKH labels."""

    training_cohorts: dict[int, set[str]]
    holdout_yymm: int | None
    holdout_ids: set[str]


def split_confirmed_cohorts(
    confirmed_cohorts: dict[int, set[str]],
) -> ConfirmedCohortSplit:
    """Use the latest confirmation month as holdout and older months for train."""
    if not confirmed_cohorts:
        return ConfirmedCohortSplit({}, None, set())

    holdout_yymm = max(confirmed_cohorts)
    holdout_ids = set(confirmed_cohorts[holdout_yymm])
    sampled_ids: set[str] = set(holdout_ids)
    training_cohorts: dict[int, set[str]] = {}

    for label_yymm, cohort_ids in sorted(confirmed_cohorts.items()):
        if label_yymm == holdout_yymm:
            continue
        new_ids = set(cohort_ids) - sampled_ids
        if new_ids:
            training_cohorts[label_yymm] = new_ids
            sampled_ids.update(new_ids)

    return ConfirmedCohortSplit(training_cohorts, holdout_yymm, holdout_ids)


def build_confirmed_training_rows(
    engine: Any,
    training_cohorts: dict[int, set[str]],
    window_size: int,
    alpha_ewma: float,
    *,
    lead_offset: int = 1,
) -> pd.DataFrame:
    """Load T-1 feature rows for older confirmed cohorts."""
    rows = _load_confirmed_feature_rows(
        engine,
        training_cohorts,
        window_size,
        alpha_ewma,
        lead_offset=lead_offset,
    )
    if rows.empty:
        return rows

    rows["y_raw"] = 1
    rows["label_source"] = "confirmed"
    logger.info("Confirmed historical training rows: %d", len(rows))
    return rows


def build_confirmed_holdout_rows(
    engine: Any,
    holdout_yymm: int | None,
    holdout_ids: set[str],
    window_size: int,
    alpha_ewma: float,
    *,
    lead_offset: int = 1,
) -> pd.DataFrame:
    """Build an actual-label holdout from one CSKH cohort and active negatives."""
    if holdout_yymm is None or not holdout_ids:
        return pd.DataFrame()

    feature_end = _parse_yymm(holdout_yymm) - pd.DateOffset(months=lead_offset)
    features = load_window_features(engine, window_size, feature_end)
    if features.empty:
        return pd.DataFrame()
    features = compute_ewma(features, window_size, alpha_ewma)

    active_next_month = build_label(engine, feature_end, horizon_months=1)
    negative_ids = set(active_next_month["cms_code_enc"].astype(str)) - holdout_ids
    eval_ids = holdout_ids | negative_ids
    holdout = features[features["cms_code_enc"].astype(str).isin(eval_ids)].copy()
    holdout["y_label"] = holdout["cms_code_enc"].astype(str).isin(holdout_ids).astype(float)
    holdout["label_source"] = "confirmed"
    holdout.loc[holdout["y_label"] == 0, "label_source"] = "actual_active"
    holdout["_confirmed_label_yymm"] = holdout_yymm

    logger.info(
        "Confirmed holdout %d: %d rows (%d positive / %d actual-active negative)",
        holdout_yymm,
        len(holdout),
        int((holdout["y_label"] == 1).sum()),
        int((holdout["y_label"] == 0).sum()),
    )
    return holdout


def _load_confirmed_feature_rows(
    engine: Any,
    confirmed_cohorts: dict[int, set[str]],
    window_size: int,
    alpha_ewma: float,
    *,
    lead_offset: int,
) -> pd.DataFrame:
    frames = []
    for label_yymm, cohort_ids in sorted(confirmed_cohorts.items()):
        feature_end = _parse_yymm(label_yymm) - pd.DateOffset(months=lead_offset)
        features = load_window_features(engine, window_size, feature_end)
        if features.empty:
            logger.warning("Confirmed feature window missing for label cohort %d", label_yymm)
            continue
        features = compute_ewma(features, window_size, alpha_ewma)
        cohort_rows = features[features["cms_code_enc"].astype(str).isin(cohort_ids)].copy()
        cohort_rows["_confirmed_label_yymm"] = label_yymm
        frames.append(cohort_rows)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _parse_yymm(label_yymm: int) -> pd.Timestamp:
    """Convert YYMM integer into a month-start timestamp."""
    year, month = divmod(label_yymm, 100)
    if month < 1 or month > 12:
        raise ValueError(f"Invalid label_yymm month: {label_yymm}")
    return pd.Timestamp(year=2000 + year, month=month, day=1)
