"""Tests for evaluator — best_threshold_by_fbeta and metric computation.

Convention: 13-Data_ML §8.1 — metrics at threshold chosen by F-beta.
"""

from __future__ import annotations

import numpy as np

from modeling.train.evaluator import best_threshold_by_fbeta, evaluate_predictions


class TestBestThresholdByFbeta:
    """Tests for best_threshold_by_fbeta (pure numpy)."""

    def test_perfect_separation(self):
        """When classes perfectly separable, threshold should be between them."""
        y_true = np.array([0, 0, 0, 1, 1, 1])
        y_prob = np.array([0.1, 0.2, 0.3, 0.7, 0.8, 0.9])
        threshold = best_threshold_by_fbeta(y_true, y_prob)
        assert 0.3 <= threshold <= 0.7

    def test_all_positive(self):
        """All positive labels — any threshold that captures all is ok."""
        y_true = np.array([1, 1, 1, 1])
        y_prob = np.array([0.6, 0.7, 0.8, 0.9])
        threshold = best_threshold_by_fbeta(y_true, y_prob)
        assert 0.0 <= threshold <= 1.0

    def test_all_negative(self):
        """All negative labels — should get a threshold (fallback)."""
        y_true = np.array([0, 0, 0, 0])
        y_prob = np.array([0.1, 0.2, 0.3, 0.4])
        threshold = best_threshold_by_fbeta(y_true, y_prob)
        assert isinstance(threshold, float)

    def test_threshold_maximizes_f1(self):
        """Threshold should give reasonable F1 on imbalanced data."""
        rng = np.random.RandomState(42)
        y_true = np.array([0]*80 + [1]*20)
        y_prob = np.where(y_true == 1, rng.uniform(0.4, 0.9, 100), rng.uniform(0.0, 0.5, 100))
        threshold = best_threshold_by_fbeta(y_true, y_prob)
        assert 0.1 <= threshold <= 0.9, f"Threshold {threshold} seems unreasonable"

    def test_two_samples(self):
        """Edge case: minimum viable input."""
        y_true = np.array([0, 1])
        y_prob = np.array([0.3, 0.7])
        threshold = best_threshold_by_fbeta(y_true, y_prob)
        assert isinstance(threshold, float)
        assert 0.0 <= threshold <= 1.0

    def test_returns_float(self):
        """Return type should always be float."""
        y_true = np.array([0, 1, 0, 1])
        y_prob = np.array([0.1, 0.9, 0.2, 0.8])
        result = best_threshold_by_fbeta(y_true, y_prob)
        assert isinstance(result, float)


def test_should_include_confusion_matrix_when_evaluating_predictions() -> None:
    metrics = evaluate_predictions(
        np.array([0, 0, 1, 1]),
        np.array([0.1, 0.8, 0.7, 0.9]),
        threshold=0.5,
    )

    assert metrics["confusion_matrix"] == {
        "tn": 1,
        "fp": 1,
        "fn": 0,
        "tp": 2,
    }
