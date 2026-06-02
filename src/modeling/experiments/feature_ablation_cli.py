"""CLI boundary for baseline-versus-candidate feature ablation."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("feature_ablation_cli")


def main() -> int:
    """Run one feature ablation experiment."""
    try:
        from dotenv import load_dotenv

        load_dotenv()

        from config.db_config import PostgresConfig
        from data.preprocessing.dataset_prep.pipeline_config import (
            DatasetPipelineConfig,
        )
        from modeling.experiments.feature_ablation import (
            FeatureAblationConfig,
            run_feature_ablation,
        )
        from shared.db import get_engine

        experiment_config = FeatureAblationConfig(
            candidate_feature=os.environ.get(
                "FEATURE_ABLATION_CANDIDATE",
                "max_consecutive_inactive",
            ),
            report_dir=Path(
                os.environ.get(
                    "FEATURE_ABLATION_REPORT_DIR",
                    "/data/reports/model_experiments",
                )
            ),
        )
        summary = run_feature_ablation(
            get_engine(PostgresConfig.from_env()),
            pipeline_config=DatasetPipelineConfig(cskh_dir=_resolve_cskh_dir()),
            experiment_config=experiment_config,
        )
        logger.info("Summary: %s", json.dumps(summary, default=str))
        return 0
    except Exception as exc:
        logger.exception("Feature ablation crashed: %s", exc)
        return 1


def _resolve_cskh_dir() -> Path | None:
    configured = os.environ.get("CSKH_DIR")
    candidates = [
        Path(configured) if configured else None,
        Path("/data/incoming/cskh"),
        Path("/data/cskh"),
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    return None


if __name__ == "__main__":
    sys.exit(main())
