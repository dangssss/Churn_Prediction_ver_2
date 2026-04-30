# data/ingestion/cli.py
"""CLI entrypoint cho Airflow ``KubernetesPodOperator``.

Convention:
  - 14 §4 Operational entrypoint phải có exit code rõ ràng (0=ok, 1=fail).
  - 18 §3 K8s pod stdout/stderr là log channel duy nhất → log rõ ràng.

Usage:
    python -m data.ingestion.cli scan      # full scan + ingest
    python -m data.ingestion.cli scan --dry-run

Exit codes:
    0 — Scan hoàn tất (kể cả khi 0 file). ``failed`` count > 0 → exit 1.
    1 — Crash hoặc có file fail.
    2 — Bad CLI args.
"""
from __future__ import annotations

import argparse
import json
import sys

from data.ingestion.resources import FSConfig, PostgresConfig
from data.ingestion.sensors.incoming_zip_sensor import run_once_scan
from shared.logging_config import configure_logging, get_logger


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="data.ingestion.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    scan = sub.add_parser("scan", help="Scan incoming ZIPs and ingest to production")
    scan.add_argument("--prod-schema", default="public")
    scan.add_argument("--ingest-schema", default="ingest")
    scan.add_argument(
        "--xcom-out",
        default=None,
        help="Path để ghi summary JSON (Airflow do_xcom_push=True đọc /airflow/xcom/return.json)",
    )
    return parser


def cmd_scan(args: argparse.Namespace, logger) -> int:
    fs_cfg = FSConfig.from_env()
    pg_cfg = PostgresConfig.from_env()

    counters = run_once_scan(
        fs_cfg=fs_cfg,
        pg_cfg=pg_cfg,
        prod_schema=args.prod_schema,
        ingest_schema=args.ingest_schema,
    )

    if args.xcom_out:
        try:
            with open(args.xcom_out, "w", encoding="utf-8") as f:
                json.dump(counters, f)
            logger.info(f"Wrote XCom summary to {args.xcom_out}")
        except Exception as e:
            logger.warning(f"Could not write XCom summary: {e}")

    return 1 if counters.get("failed", 0) > 0 else 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    logger = get_logger("data.ingestion.cli")

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "scan":
        return cmd_scan(args, logger)

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
