"""Post-training guardrail checks.

Convention: 13-Data_ML §8.2 — minimum quality gates before deployment.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def check_guardrail(
    metrics: dict,
    *,
    min_f1: float = 0.10,
    min_pr_auc: float = 0.05,
) -> tuple[bool, str]:
    """Check if model metrics meet minimum quality thresholds.

    Args:
        metrics: Dict from evaluate_model (must contain 'f1', 'pr_auc').
        min_f1: Minimum acceptable F1 score.
        min_pr_auc: Minimum acceptable PR-AUC.

    Returns:
        Tuple of (passed: bool, reason: str).
    """
    f1 = metrics.get("f1", 0.0)
    pr_auc = metrics.get("pr_auc", 0.0)

    reasons = []
    if f1 < min_f1:
        reasons.append(f"F1={f1:.4f} < min_f1={min_f1}")
    if pr_auc < min_pr_auc:
        reasons.append(f"PR-AUC={pr_auc:.4f} < min_pr_auc={min_pr_auc}")

    if reasons:
        msg = "GUARDRAIL FAILED: " + "; ".join(reasons)
        logger.warning(msg)
        return False, msg

    msg = f"Guardrail passed: F1={f1:.4f} >= {min_f1}, PR-AUC={pr_auc:.4f} >= {min_pr_auc}"
    logger.info(msg)
    return True, msg


def check_accept_reject(
    new_f1: float,
    prev_f1: float | None,
    *,
    eps: float = 1e-6,
) -> tuple[bool, str]:
    """Decide whether to accept the new model over the previous.

    Args:
        new_f1: F1 score of the new candidate model.
        prev_f1: F1 score of the previously accepted model (None = first run).
        eps: Minimum improvement required.

    Returns:
        Tuple of (accepted: bool, rule: str).
    """
    if prev_f1 is None:
        rule = "accepted_no_previous"
        logger.info("Accept decision: %s (first model)", rule)
        return True, rule

    improved = new_f1 > (prev_f1 + eps)
    if improved:
        rule = f"accepted_f1_improved ({prev_f1:.4f} → {new_f1:.4f})"
    else:
        rule = f"rejected_f1_not_improved ({prev_f1:.4f} → {new_f1:.4f}, eps={eps})"

    logger.info("Accept decision: %s", rule)
    return improved, rule
