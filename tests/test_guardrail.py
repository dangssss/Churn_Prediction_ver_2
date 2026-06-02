"""Tests for guardrail checks — quality gates and accept/reject logic.

Convention: 13-Data_ML §8.2 — minimum quality gates before deployment.
"""

from __future__ import annotations

from modeling.train.guardrail import check_accept_reject, check_guardrail


class TestCheckGuardrail:
    """Tests for check_guardrail (quality gate)."""

    def test_passes_when_above_thresholds(self):
        metrics = {"f05": 0.50, "pr_auc": 0.30, "recall": 0.20}
        passed, msg = check_guardrail(metrics)
        assert passed is True
        assert "passed" in msg.lower()

    def test_fails_when_f05_below_threshold(self):
        metrics = {"f05": 0.05, "pr_auc": 0.30, "recall": 0.20}
        passed, msg = check_guardrail(metrics, min_f05=0.10)
        assert passed is False
        assert "F0.5" in msg

    def test_fails_when_pr_auc_below_threshold(self):
        metrics = {"f05": 0.50, "pr_auc": 0.01, "recall": 0.20}
        passed, msg = check_guardrail(metrics, min_pr_auc=0.05)
        assert passed is False
        assert "PR-AUC" in msg

    def test_fails_when_both_below(self):
        metrics = {"f05": 0.01, "pr_auc": 0.01, "recall": 0.20}
        passed, msg = check_guardrail(metrics, min_f05=0.10, min_pr_auc=0.05)
        assert passed is False
        assert "F0.5" in msg
        assert "PR-AUC" in msg

    def test_fails_when_recall_below_threshold(self):
        metrics = {"f05": 0.50, "pr_auc": 0.30, "recall": 0.01}
        passed, msg = check_guardrail(metrics, min_recall=0.10)
        assert passed is False
        assert "Recall" in msg

    def test_passes_at_exact_boundary(self):
        """Boundary: metrics == thresholds should pass."""
        metrics = {"f05": 0.10, "pr_auc": 0.05, "recall": 0.10}
        passed, _ = check_guardrail(metrics, min_f05=0.10, min_pr_auc=0.05, min_recall=0.10)
        assert passed is True

    def test_missing_keys_treated_as_zero(self):
        """Missing metrics should default to 0.0."""
        metrics = {}
        passed, msg = check_guardrail(metrics, min_f05=0.10, min_pr_auc=0.05)
        assert passed is False

    def test_custom_thresholds(self):
        metrics = {"f05": 0.30, "pr_auc": 0.20, "recall": 0.20}
        passed, _ = check_guardrail(metrics, min_f05=0.25, min_pr_auc=0.15)
        assert passed is True

    def test_returns_tuple(self):
        metrics = {"f05": 0.50, "pr_auc": 0.30, "recall": 0.20}
        result = check_guardrail(metrics)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bool)
        assert isinstance(result[1], str)


class TestCheckAcceptReject:
    """Tests for check_accept_reject (F0.5 improvement decision)."""

    def test_first_model_always_accepted(self):
        accepted, rule = check_accept_reject(0.50, prev_f05=None)
        assert accepted is True
        assert "no_previous" in rule

    def test_improved_model_accepted(self):
        accepted, rule = check_accept_reject(0.55, prev_f05=0.50)
        assert accepted is True
        assert "improved" in rule

    def test_same_f05_rejected(self):
        """Same F0.5 (within eps) should be rejected."""
        accepted, rule = check_accept_reject(0.50, prev_f05=0.50, eps=1e-6)
        assert accepted is False
        assert "not_improved" in rule

    def test_worse_model_rejected(self):
        accepted, rule = check_accept_reject(0.40, prev_f05=0.50)
        assert accepted is False
        assert "not_improved" in rule

    def test_marginal_improvement_below_eps(self):
        """Improvement less than eps should be rejected."""
        accepted, _ = check_accept_reject(0.5001, prev_f05=0.50, eps=0.001)
        assert accepted is False

    def test_improvement_above_eps(self):
        """Improvement greater than eps should be accepted."""
        accepted, _ = check_accept_reject(0.502, prev_f05=0.50, eps=0.001)
        assert accepted is True

    def test_zero_f05_first_model(self):
        """Even F0.5=0 should be accepted if first model."""
        accepted, _ = check_accept_reject(0.0, prev_f05=None)
        assert accepted is True

    def test_zero_eps_strict_improvement(self):
        """With eps=0, any improvement should be accepted."""
        accepted, _ = check_accept_reject(0.50001, prev_f05=0.50, eps=0)
        assert accepted is True

    def test_returns_tuple(self):
        result = check_accept_reject(0.50, prev_f05=0.40)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bool)
        assert isinstance(result[1], str)
