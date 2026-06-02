"""Tests for sample_weighting — label smoothing and final dataset.

Convention: 13-Data_ML §6.3 — scaler fitted on train only.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from data.preprocessing.dataset_prep.label_calibration import LabelWeights
from data.preprocessing.dataset_prep.sample_weighting import (
    apply_weights_and_smoothing,
    build_final_dataset,
)

LABEL_WEIGHTS = LabelWeights(
    confirmed=1.0,
    pseudo_churn=0.2,
    reliable_neg=0.3,
    pu_unlabeled=0.1,
    rule_based=0.05,
)


class TestApplyWeightsAndSmoothing:
    """Tests for apply_weights_and_smoothing."""

    @staticmethod
    def _make_df() -> pd.DataFrame:
        return pd.DataFrame({
            "cms_code_enc": ["A", "B", "C", "D"],
            "label_source": [
                "confirmed", "pseudo_churn", "reliable_neg", "pu_unlabeled"
            ],
        })

    def test_label_mapping(self):
        """y_label should be 1 for confirmed/pseudo, 0 for neg/unlabeled."""
        df = self._make_df()
        result = apply_weights_and_smoothing(
            df, label_weights=LABEL_WEIGHTS,
            label_smooth_eps_confirmed=0.0,
            label_smooth_eps_pseudo=0.1,
        )
        assert result.loc[0, "y_label"] == 1.0  # confirmed
        assert result.loc[1, "y_label"] == 1.0  # pseudo_churn
        assert result.loc[2, "y_label"] == 0.0  # reliable_neg
        assert result.loc[3, "y_label"] == 0.0  # pu_unlabeled

    def test_weight_values(self):
        """Weights should match predefined mapping."""
        df = self._make_df()
        result = apply_weights_and_smoothing(
            df, label_weights=LABEL_WEIGHTS,
            label_smooth_eps_confirmed=0.0,
            label_smooth_eps_pseudo=0.1,
        )
        assert result.loc[0, "sample_weight"] == 1.0   # confirmed
        assert result.loc[1, "sample_weight"] == 0.20   # pseudo
        assert result.loc[2, "sample_weight"] == 0.30   # reliable_neg
        assert result.loc[3, "sample_weight"] == 0.10   # pu_unlabeled

    def test_label_smoothing_confirmed_no_smooth(self):
        """Confirmed with eps=0 should have y_smooth = y_label."""
        df = self._make_df()
        result = apply_weights_and_smoothing(
            df, label_weights=LABEL_WEIGHTS,
            label_smooth_eps_confirmed=0.0,
            label_smooth_eps_pseudo=0.1,
        )
        # Confirmed: y_smooth = (1-0)*1 + 0*0.5 = 1.0
        assert result.loc[0, "y_smooth"] == 1.0

    def test_label_smoothing_pseudo(self):
        """Pseudo with eps=0.1 should have y_smooth = 0.9*1 + 0.1*0.5."""
        df = self._make_df()
        result = apply_weights_and_smoothing(
            df, label_weights=LABEL_WEIGHTS,
            label_smooth_eps_confirmed=0.0,
            label_smooth_eps_pseudo=0.10,
        )
        # pseudo_churn: y_smooth = (1 - 0.10)*1 + 0.10*0.5 = 0.95
        np.testing.assert_almost_equal(result.loc[1, "y_smooth"], 0.95)


class TestBuildFinalDataset:
    """Tests for build_final_dataset."""

    def test_scaler_fitted_on_train_only(self):
        """Scaler must be fitted on train set, not eval."""
        df = pd.DataFrame({
            "cms_code_enc": [f"T{i}" for i in range(20)] + ["E0", "E1"],
            "feat_0": np.random.randn(22),
            "feat_1": np.random.randn(22),
            "y_smooth": np.random.rand(22),
            "y_label": np.random.choice([0, 1], 22).astype(float),
            "sample_weight": np.ones(22),
        })
        eval_ids = {"E0", "E1"}

        holdout_df = df[df["cms_code_enc"].isin(eval_ids)].copy()
        result = build_final_dataset(
            df,
            pd.DataFrame(),
            holdout_df,
            eval_ids,
            ["feat_0", "feat_1"],
            LABEL_WEIGHTS,
        )

        # Scaler mean should match the actual train partition, NOT full dataset mean.
        train_only = df[~df["cms_code_enc"].isin(eval_ids)]
        expected_mean = train_only[["feat_0", "feat_1"]].fillna(0).mean().values
        np.testing.assert_array_almost_equal(result.scaler.mean_, expected_mean)

    def test_no_train_eval_overlap(self):
        """X_train and X_eval should have no row overlap."""
        df = pd.DataFrame({
            "cms_code_enc": ["A", "B", "C", "D"],
            "feat_0": [1, 2, 3, 4],
            "y_smooth": [0, 0, 1, 1],
            "y_label": [0, 0, 1, 1],
            "sample_weight": [1, 1, 1, 1],
        })
        eval_ids = {"C", "D"}
        holdout_df = df[df["cms_code_enc"].isin(eval_ids)].copy()
        result = build_final_dataset(
            df,
            pd.DataFrame(),
            holdout_df,
            eval_ids,
            ["feat_0"],
            LABEL_WEIGHTS,
        )
        assert len(result.x_train) == 2
        assert len(result.x_eval) == 2

    def test_current_confirmed_ids_do_not_overwrite_historical_labels(self):
        """Historical rows must retain labels computed from their own next month."""
        df = pd.DataFrame({
            "cms_code_enc": [f"C{i}" for i in range(10)],
            "feat_0": list(range(10)),
            "y_smooth": [0.0, 1.0] * 5,
            "y_label": [0.0, 1.0] * 5,
            "sample_weight": [1.0] * 10,
        })
        training_history_df = pd.DataFrame({
            "cms_code_enc": ["C0"],
            "feat_0": [99],
            "y_raw": [0],
        })

        holdout_df = df[df["cms_code_enc"] == "C0"].copy()
        result = build_final_dataset(
            df,
            training_history_df,
            holdout_df,
            {"C0"},
            ["feat_0"],
            LABEL_WEIGHTS,
        )

        assert len(result.y_train) == 9

    def test_confirmed_history_has_more_weight_than_rule_based_history(self):
        df = pd.DataFrame({
            "cms_code_enc": [f"C{i}" for i in range(4)],
            "feat_0": list(range(4)),
            "y_smooth": [0.0, 1.0, 0.0, 1.0],
            "y_label": [0.0, 1.0, 0.0, 1.0],
            "sample_weight": [0.1] * 4,
        })
        history_df = pd.DataFrame({
            "cms_code_enc": ["H_CONFIRMED", "H_RULE"],
            "feat_0": [10, 11],
            "y_raw": [1, 1],
            "label_source": ["confirmed", "rule_based"],
        })

        result = build_final_dataset(
            df,
            history_df,
            pd.DataFrame(),
            set(),
            ["feat_0"],
            LABEL_WEIGHTS,
        )

        assert result.w_train.iloc[-2:].tolist() == [1.0, 0.05]
