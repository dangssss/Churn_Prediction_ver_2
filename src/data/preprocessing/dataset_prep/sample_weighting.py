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
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

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


def apply_weights_and_smoothing(
    df: pd.DataFrame,
    pu_weight_c: float,
    label_smooth_eps_confirmed: float,
    label_smooth_eps_pseudo: float,
) -> pd.DataFrame:
    """Apply sample weights and label smoothing.

    Args:
        df: DataFrame with ``label_source`` column.
        pu_weight_c: PU learning weight for unlabeled samples.
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
        "confirmed": 1.0,
        "pseudo_churn": 0.50,
        "reliable_neg": 0.80,
        "pu_unlabeled": pu_weight_c,
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
    eval_ids: set[str],
    feature_names: list[str],
) -> DatasetResult:
    """Build the final train/eval/predict datasets with scaling.

    Scaler is fit ONLY on the training set (convention 13-Data_ML §6.3).

    Args:
        active_df: DataFrame with ``y_smooth``, ``y_label``,
            ``sample_weight``, ``cms_code_enc`` columns.
        eval_ids: Confirmed churn IDs (used for eval split).
        feature_names: List of numeric feature columns.

    Returns:
        DatasetResult containing all splits and the fitted scaler.
    """
    feats = [f for f in feature_names if f in active_df.columns]

    train_mask = ~active_df["cms_code_enc"].isin(eval_ids)
    eval_mask = active_df["cms_code_enc"].isin(eval_ids)

    x_train_raw = active_df.loc[train_mask, feats].fillna(0)
    y_train = active_df.loc[train_mask, "y_smooth"]
    w_train = active_df.loc[train_mask, "sample_weight"]

    x_eval_raw = active_df.loc[eval_mask, feats].fillna(0)
    y_eval = active_df.loc[eval_mask, "y_label"]  # No smoothing for GT

    # Fit scaler on train set ONLY (prevent data leakage)
    scaler = StandardScaler()
    x_train = pd.DataFrame(
        scaler.fit_transform(x_train_raw), columns=feats, index=x_train_raw.index
    )
    x_eval = pd.DataFrame(
        scaler.transform(x_eval_raw), columns=feats, index=x_eval_raw.index
    )
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
    )
