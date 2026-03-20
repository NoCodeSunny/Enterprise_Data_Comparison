# -*- coding: utf-8 -*-
"""
data_compare.main
─────────────────
CLI entry point.

Usage
-----
    python -m data_compare.main
    python -m data_compare.main --config config/config.yaml
    python -m data_compare.main --config config/config.yaml --capabilities comparison,data_quality
    python -m data_compare.main --list-capabilities

Flags
-----
--config / -c       Path to YAML config file (optional; uses hardcoded defaults if omitted)
--capabilities      Comma-separated list of capabilities to enable (overrides config.yaml)
--list-capabilities Print all registered capability names and exit
--fail-on-error     Exit with code 1 if any batch has failures or alerts (default: False)
"""

from __future__ import annotations

import argparse
import sys

from data_compare.registry.capability_registry import default_registry
from data_compare.utils.logger import configure_logging, get_logger

logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="edcp",
        description="DataComparePro (edcp) v3.0.0 — Enterprise Data Comparison Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config", "-c",
        default=None,
        metavar="PATH",
        help="Path to YAML config file (default: use hardcoded values in config_loader.py)",
    )
    parser.add_argument(
        "--capabilities",
        default=None,
        metavar="LIST",
        help=(
            "Comma-separated capability names to enable; all others disabled. "
            "Example: --capabilities comparison,data_quality,audit"
        ),
    )
    parser.add_argument(
        "--list-capabilities",
        action="store_true",
        default=False,
        help="List all registered capabilities in execution order and exit",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        default=False,
        help="Exit with code 1 if any batch has failures, errors, or triggered alerts",
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = _parse_args()

    # ── --list-capabilities ───────────────────────────────────────────────────
    if args.list_capabilities:
        caps = default_registry.list_capabilities()
        print("\nRegistered capabilities (in execution order):")
        for name in caps:
            print(f"  • {name}")
        print()
        sys.exit(0)

    # ── capability override from CLI ──────────────────────────────────────────
    cli_caps_override: dict = {}
    if args.capabilities:
        enabled_set = {c.strip() for c in args.capabilities.split(",") if c.strip()}
        all_caps    = default_registry.list_capabilities()
        cli_caps_override = {name: (name in enabled_set) for name in all_caps}
        logger.info(f"CLI capability override: {cli_caps_override}")

    # ── run ───────────────────────────────────────────────────────────────────
    from data_compare.orchestrator import run_comparison
    from data_compare.config.config_loader import load_config

    cfg = load_config(args.config)

    # Apply CLI capability override on top of config
    if cli_caps_override:
        cfg["capabilities"].update(cli_caps_override)

    try:
        # Pass the merged cfg (with CLI overrides applied) to run_comparison
        summaries = run_comparison(
            config_path=args.config,
            registry=default_registry,
            _override_cfg=cfg if cli_caps_override else None,
        )

        if args.fail_on_error:
            has_failures = any(
                s.get("MatchedFailed", 0) > 0
                or s.get("Error")
                or s.get("AlertsTriggered", 0) > 0
                for s in summaries
            )
            if has_failures:
                logger.error(
                    "Exiting with code 1: one or more batches have failures, "
                    "errors, or triggered alerts (--fail-on-error is set)"
                )
                sys.exit(1)
        sys.exit(0)

    except FileNotFoundError as exc:
        logger.error(f"FATAL: {exc}")
        sys.exit(2)
    except Exception as exc:
        logger.error(f"FATAL unexpected error: {exc}", exc_info=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
