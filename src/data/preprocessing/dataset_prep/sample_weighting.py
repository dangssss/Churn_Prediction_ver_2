"""Step 7 — Sample weighting and label smoothing.

Apply sample weights and label smoothing to produce the final
training dataset (X_train, y_train, w_train, X_eval, y_eval).

Conventions applied:
  - 13-Data_ML §6.3: Scaler fitted on train only (data leakage prevention).
  - 13-Data_ML §6.4: Scaler explicitly instantiated and returned as artifact.
  - 13-Data_ML §9.3: Returns new data, no in-place modification.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd
from sklearn.preprocessing import StandardScaler

from data.preprocessing.dataset_prep.label_calibration import LabelWeights

logger = logging.getLogger(__name__)


@dataclass
class DatasetResult:
    """Final dataset output from the preparation pipeline.

    Attributes:
        x_train: Scaled training features.
        y_train: Smoothed training labels.
        w_train: Sample weights.
        x_eval: Scaled evaluation features (confirmed set).
        y_eval: Ground truth labels for evaluation.
        x_predict: Scaled features for all active accounts.
        scaler: Fitted StandardScaler (for inference reuse).
        feature_names: List of feature column names.
        active_df: Full active DataFrame with all metadata.
        calibration: Data-driven pseudo-label thresholds and source weights.
    """

    x_train: pd.DataFrame
    y_train: pd.Series
    w_train: pd.Series
    x_eval: pd.DataFrame
    y_eval: pd.Series
    x_predict: pd.DataFrame
    scaler: StandardScaler
    feature_names: list[str]
    active_df: pd.DataFrame
    calibration: dict[str, float] = field(default_factory=dict)
    eval_metadata: pd.DataFrame = field(default_factory=pd.DataFrame)


def apply_weights_and_smoothing(
    df: pd.DataFrame,
    label_weights: LabelWeights,
    label_smooth_eps_confirmed: float,
    label_smooth_eps_pseudo: float,
) -> pd.DataFrame:
    """Apply sample weights and label smoothing.

    Args:
        df: DataFrame with ``label_source`` column.
        label_weights: Calibrated source-aware sample weights.
        label_smooth_eps_confirmed: Smoothing epsilon for confirmed.
        label_smooth_eps_pseudo: Smoothing epsilon for pseudo/unlabeled.

    Returns:
        DataFrame with ``y_label``, ``sample_weight``, ``y_smooth`` added.
    """
    result = df.copy()

    # Raw binary labels
    label_map = {
        "confirmed": 1.0,
        "pseudo_churn": 1.0,
        "reliable_neg": 0.0,
        "pu_unlabeled": 0.0,
    }
    result["y_label"] = result["label_source"].map(label_map)

    # Sample weights
    weight_map = {
        "confirmed": label_weights.confirmed,
        "pseudo_churn": label_weights.pseudo_churn,
        "reliable_neg": label_weights.reliable_neg,
        "pu_unlabeled": label_weights.pu_unlabeled,
    }
    result["sample_weight"] = result["label_source"].map(weight_map)

    # Label smoothing: y_smooth = (1 - ε) * y + ε * 0.5
    eps_map = {
        "confirmed": label_smooth_eps_confirmed,
        "pseudo_churn": label_smooth_eps_pseudo,
        "reliable_neg": label_smooth_eps_pseudo,
        "pu_unlabeled": label_smooth_eps_pseudo,
    }
    eps_series = result["label_source"].map(eps_map)
    result["y_smooth"] = (1 - eps_series) * result["y_label"] + eps_series * 0.5

    return result


def build_final_dataset(
    active_df: pd.DataFrame,
    training_history_df: pd.DataFrame,
    holdout_df: pd.DataFrame,
    eval_ids: set[str],
    feature_names: list[str],
    label_weights: LabelWeights,
    calibration: dict[str, float] | None = None,
) -> DatasetResult:
    """Build the final train/eval/predict datasets with scaling.

    Scaler is fit ONLY on the training set (convention 13-Data_ML §6.3).

    Args:
        active_df: DataFrame with ``y_smooth``, ``y_label``,
            ``sample_weight``, ``cms_code_enc`` columns.
        holdout_df: Actual-label temporal holdout rows.
        eval_ids: Confirmed churn IDs reserved from training.
        feature_names: List of numeric feature columns.
        label_weights: Calibrated source-aware sample weights.
        calibration: Data-driven thresholds and weights for audit metadata.

    Returns:
        DatasetResult containing all splits and the fitted scaler.
    """
    feats = [f for f in feature_names if f in active_df.columns]

    # Latest confirmed cohort is a strict temporal holdout.
    train_mask = ~active_df["cms_code_enc"].isin(eval_ids)

    x_train_active = active_df.loc[train_mask, feats].fillna(0)
    y_train_active = active_df.loc[train_mask, "y_smooth"]
    w_train_active = active_df.loc[train_mask, "sample_weight"]

    # Historical labels are computed from each window's own next-month outcome.
    # Current CSKH confirmations must not overwrite past labels.
    if not training_history_df.empty:
        history_df = training_history_df[
            ~training_history_df["cms_code_enc"].isin(eval_ids)
        ].copy()
        history_sources = history_df.get(
            "label_source",
            pd.Series("rule_based", index=history_df.index),
        )
        y_train_hist = (history_df["y_raw"] == 1).astype(float)
        w_train_hist = history_sources.map(
            {
                "confirmed": label_weights.confirmed,
                "rule_based": label_weights.rule_based,
            }
        ).fillna(label_weights.rule_based)

        # Ensure only available features are used and missing filled with 0
        missing_feats = [f for f in feats if f not in history_df.columns]
        for f in missing_feats:
            history_df[f] = 0.0

        x_train_hist = history_df[feats].fillna(0)

        x_train_raw = pd.concat([x_train_active, x_train_hist], ignore_index=True)
        y_train = pd.concat([y_train_active, y_train_hist], ignore_index=True)
        w_train = pd.concat([w_train_active, w_train_hist], ignore_index=True)

        logger.info(
            "Merged %d historical training samples (true labels: sum(y)=%d) into Training Set",
            len(x_train_hist),
            int(y_train_hist.sum()),
        )
    else:
        x_train_raw = x_train_active
        y_train = y_train_active
        w_train = w_train_active

    x_eval_raw = holdout_df.reindex(columns=feats, fill_value=0).fillna(0)
    y_eval = holdout_df.get("y_label", pd.Series(dtype=float))
    eval_metadata = holdout_df.reindex(
        columns=["cms_code_enc", "_confirmed_label_yymm", "label_source"],
    ).reset_index(drop=True)

    # Fit scaler on train set ONLY (prevent data leakage)
    scaler = StandardScaler()
    x_train = pd.DataFrame(scaler.fit_transform(x_train_raw), columns=feats, index=x_train_raw.index)
    if x_eval_raw.empty:
        x_eval = pd.DataFrame(columns=feats, index=x_eval_raw.index)
    else:
        x_eval = pd.DataFrame(scaler.transform(x_eval_raw), columns=feats, index=x_eval_raw.index)
    x_predict = pd.DataFrame(
        scaler.transform(active_df[feats].fillna(0)),
        columns=feats,
        index=active_df.index,
    )

    logger.info(
        "Final dataset: X_train=%s, X_eval=%s, X_predict=%s",
        x_train.shape,
        x_eval.shape,
        x_predict.shape,
    )
    logger.info(
        "Train churn rate: %.2f%%, weight range: [%.4f, %.4f]",
        (y_train > 0.5).mean() * 100,
        w_train.min(),
        w_train.max(),
    )

    return DatasetResult(
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_eval=x_eval,
        y_eval=y_eval,
        x_predict=x_predict,
        scaler=scaler,
        feature_names=feats,
        active_df=active_df,
        calibration=calibration or {},
        eval_metadata=eval_metadata,
    )
