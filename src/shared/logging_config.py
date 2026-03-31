"""Unified logging configuration.

Merges:
  - Ingestion/Data_pull/logging_config.py
  - Preprocess/logging_config.py

Conventions applied:
  - 06-Logging §4.7: Log once at boundary.
  - 08-Security §7.1: Never log secrets.
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

_CONFIGURED = False


def configure_logging(
    logs_dir: str | Path = "./logs",
    app_name: str = "ds-churn",
    level: int = logging.INFO,
) -> None:
    """Configure logging for the entire application.

    Call once at the application entrypoint. Subsequent calls are no-ops
    to prevent duplicate handlers.

    Args:
        logs_dir: Directory for log files (will be created if missing).
        app_name: Application name used in log filenames.
        level: Root logging level.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    logs_path = Path(logs_dir)
    logs_path.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates on re-import
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    log_file = logs_path / f"{app_name}.log"

    # ── File handler — all levels, rotating ────────────────
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)

    # ── Console handler — INFO and above ───────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # ── Formatters ─────────────────────────────────────────
    file_formatter = logging.Formatter(
        fmt=("%(asctime)s | %(levelname)-8s | %(name)-30s | %(funcName)-20s | %(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_formatter = logging.Formatter(
        fmt="%(levelname)-8s | %(name)-20s | %(message)s",
    )

    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a named logger instance.

    Args:
        name: Logger name (typically ``__name__``).

    Returns:
        ``logging.Logger`` instance.
    """
    return logging.getLogger(name)
