# -*- coding: utf-8 -*-
"""
edcp.cli
─────────────────
Full CLI interface for the data_compare framework.

Subcommands
────────────
run            Execute comparisons from a YAML config file
validate       Validate a YAML config file without running
list-caps      List all registered capabilities
version        Print the framework version

Usage examples
──────────────
    # Run with YAML config
    python -m data_compare run config/config.yaml

    # Run only specific capabilities
    python -m data_compare run config/config.yaml --capabilities comparison,schema

    # Run with parallel workers
    python -m data_compare run config/config.yaml --workers 4

    # Fail with exit code 1 on any batch failure
    python -m data_compare run config/config.yaml --fail-on-error

    # Validate config only (no comparisons run)
    python -m data_compare validate config/config.yaml

    # List all capabilities
    python -m data_compare list-caps

    # Version
    python -m data_compare version

This module is the entry point for:
    python -m edcp  (via __main__.py)
    edcp                    (via pyproject.toml console_scripts)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional


# ── version ───────────────────────────────────────────────────────────────────

def _get_version() -> str:
    try:
        import edcp
        return getattr(edcp, "__version__", "3.0.0")
    except Exception:
        return "3.0.0"


# ── subcommand handlers ───────────────────────────────────────────────────────

def cmd_version(args: argparse.Namespace) -> int:
    print(f"DataComparePro (edcp) v{_get_version()}")
    return 0


def cmd_list_caps(args: argparse.Namespace) -> int:
    from data_compare.registry.capability_registry import default_registry
    caps = default_registry.list_capabilities()
    print("\nRegistered capabilities (execution order):")
    for i, name in enumerate(caps, 1):
        print(f"  {i:2d}.  {name}")
    print()
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate a config YAML without running any comparisons."""
    from data_compare.config.config_loader import load_config
    from data_compare.config.config_schema import validate_config, ConfigError

    cfg_path = args.config
    print(f"Validating: {cfg_path}")

    try:
        raw = load_config(cfg_path)
    except Exception as exc:
        print(f"❌ Failed to load config: {exc}", file=sys.stderr)
        return 2

    try:
        validated = validate_config(raw)
        print("✅ Configuration is valid")
        print(f"   input_sheet : {validated.get('input_sheet', '(not set)')}")
        print(f"   report_root : {validated.get('report_root', '(not set)')}")
        print(f"   capabilities: {validated.get('capabilities', {})}")
        return 0
    except ConfigError as exc:
        print(f"❌ Configuration invalid:\n{exc}", file=sys.stderr)
        return 1


def cmd_run(args: argparse.Namespace) -> int:
    """Execute comparisons from a YAML config."""
    from data_compare.config.config_loader import load_config
    from data_compare.config.config_schema import validate_config, ConfigError
    from data_compare.utils.logger import configure_logging, get_logger
    import logging

    cfg_path = getattr(args, "config", None)

    # Load and validate config
    try:
        raw = load_config(cfg_path)
    except Exception as exc:
        print(f"❌ Failed to load config: {exc}", file=sys.stderr)
        return 2

    try:
        cfg = validate_config(raw)
    except ConfigError as exc:
        print(f"❌ Config validation failed:\n{exc}", file=sys.stderr)
        return 1

    # Apply CLI overrides to config
    if getattr(args, "capabilities", None):
        enabled_set = {c.strip() for c in args.capabilities.split(",") if c.strip()}
        all_caps = ["comparison","tolerance","duplicate","schema",
                    "data_quality","audit","alerts","plugins"]
        cfg["capabilities"] = {name: (name in enabled_set) for name in all_caps}

    if getattr(args, "workers", None):
        cfg["max_workers"] = int(args.workers)

    if getattr(args, "use_spark", False):
        cfg["use_spark"] = True

    if getattr(args, "log_file", None):
        cfg["log_file"] = args.log_file

    # Configure logging
    level_str = str(cfg.get("log_level", "INFO")).upper()
    level_map = {"DEBUG": logging.DEBUG, "INFO": logging.INFO,
                 "WARNING": logging.WARNING, "ERROR": logging.ERROR}
    level = level_map.get(level_str, logging.INFO)

    log_dir = None
    if cfg.get("log_file"):
        log_dir = Path(cfg["log_file"]).parent
    elif cfg.get("report_root"):
        log_dir = Path(cfg["report_root"]) / "logs"

    configure_logging(level=level, log_dir=log_dir)
    logger = get_logger(__name__)

    # Run
    from data_compare.orchestrator import run_comparison
    try:
        summaries = run_comparison(
            config_path=cfg_path,
            _override_cfg=cfg,
        )
    except FileNotFoundError as exc:
        logger.error(f"FATAL: {exc}")
        return 2
    except Exception as exc:
        logger.error(f"FATAL unexpected error: {exc}", exc_info=True)
        return 2

    # Exit code
    if getattr(args, "fail_on_error", False):
        has_failures = any(
            s.get("MatchedFailed", 0) > 0
            or s.get("Error")
            or s.get("AlertsTriggered", 0) > 0
            for s in summaries
        )
        if has_failures:
            logger.error(
                "Exiting with code 1: one or more batches have failures "
                "(--fail-on-error is set)"
            )
            return 1

    return 0


# ── argument parser ───────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edcp",
        description="DataComparePro (edcp) v3.0.0 — Enterprise Data Comparison Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m edcp run config/config.yaml
  python -m edcp run config/config.yaml --capabilities comparison,schema
  python -m edcp run config/config.yaml --workers 4 --fail-on-error
  python -m edcp validate config/config.yaml
  python -m data_compare list-caps
  python -m data_compare version
        """,
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # ── run ───────────────────────────────────────────────────────────────────
    run_p = sub.add_parser("run", help="Execute comparisons from a YAML config")
    run_p.add_argument(
        "config",
        nargs="?",
        default=None,
        metavar="CONFIG",
        help="Path to YAML config file (omit to use hardcoded defaults)",
    )
    run_p.add_argument(
        "--capabilities", "-c",
        default=None,
        metavar="LIST",
        help="Comma-separated capability names to enable (all others disabled)",
    )
    run_p.add_argument(
        "--workers", "-w",
        type=int,
        default=None,
        metavar="N",
        help="Number of parallel comparison workers (default from config)",
    )
    run_p.add_argument(
        "--fail-on-error",
        action="store_true",
        default=False,
        help="Exit with code 1 if any batch has failures or triggered alerts",
    )
    run_p.add_argument(
        "--use-spark",
        action="store_true",
        default=False,
        help="Force SparkEngine for all batches",
    )
    run_p.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="Write logs to this file (JSON + plain text)",
    )
    run_p.set_defaults(func=cmd_run)

    # ── validate ──────────────────────────────────────────────────────────────
    val_p = sub.add_parser("validate", help="Validate a config file without running")
    val_p.add_argument("config", metavar="CONFIG", help="Path to YAML config file")
    val_p.set_defaults(func=cmd_validate)

    # ── list-caps ─────────────────────────────────────────────────────────────
    lc_p = sub.add_parser("list-caps", help="List all registered capabilities")
    lc_p.set_defaults(func=cmd_list_caps)

    # ── version ───────────────────────────────────────────────────────────────
    ver_p = sub.add_parser("version", help="Print the framework version")
    ver_p.set_defaults(func=cmd_version)

    return parser


# ── main entry point ──────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> None:
    parser  = build_parser()
    args    = parser.parse_args(argv)

    if not args.command:
        # Default: 'run' with remaining args
        parser.print_help()
        sys.exit(0)

    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        sys.exit(0)

    exit_code = func(args)
    sys.exit(exit_code or 0)


if __name__ == "__main__":
    main()
