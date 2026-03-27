"""Post-training guardrail checks.

Convention: 13-Data_ML §8.2 — minimum quality gates before deployment.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def check_guardrail(
    metrics: dict,
    *,
    min_f2: float = 0.10,
    min_roc_auc: float = 0.05,
) -> tuple[bool, str]:
    """Check if model metrics meet minimum quality thresholds.

    Args:
        metrics: Dict from evaluate_model (must contain 'f2', 'roc_auc').
        min_f2: Minimum acceptable F2 score.
        min_roc_auc: Minimum acceptable ROC-AUC.

    Returns:
        Tuple of (passed: bool, reason: str).
    """
    f2 = metrics.get("f2", 0.0)
    roc_auc = metrics.get("roc_auc", 0.0)

    reasons = []
    if f2 < min_f2:
        reasons.append(f"F2={f2:.4f} < min_f2={min_f2}")
    if roc_auc < min_roc_auc:
        reasons.append(f"ROC-AUC={roc_auc:.4f} < min_roc_auc={min_roc_auc}")

    if reasons:
        msg = "GUARDRAIL FAILED: " + "; ".join(reasons)
        logger.warning(msg)
        return False, msg

    msg = f"Guardrail passed: F2={f2:.4f} >= {min_f2}, ROC-AUC={roc_auc:.4f} >= {min_roc_auc}"
    logger.info(msg)
    return True, msg


def check_accept_reject(
    new_f2: float,
    prev_f2: float | None,
    *,
    eps: float = 1e-6,
) -> tuple[bool, str]:
    """Decide whether to accept the new model over the previous.

    Args:
        new_f2: F2 score of the new candidate model.
        prev_f2: F2 score of the previously accepted model (None = first run).
        eps: Minimum improvement required.

    Returns:
        Tuple of (accepted: bool, rule: str).
    """
    if prev_f2 is None:
        rule = "accepted_no_previous"
        logger.info("Accept decision: %s (first model)", rule)
        return True, rule

    improved = new_f2 > (prev_f2 + eps)
    if improved:
        rule = f"accepted_f2_improved ({prev_f2:.4f} → {new_f2:.4f})"
    else:
        rule = f"rejected_f2_not_improved ({prev_f2:.4f} → {new_f2:.4f}, eps={eps})"

    logger.info("Accept decision: %s", rule)
    return improved, rule
