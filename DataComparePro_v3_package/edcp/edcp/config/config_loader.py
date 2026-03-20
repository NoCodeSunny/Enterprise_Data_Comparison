# -*- coding: utf-8 -*-
"""
edcp.config.config_loader
──────────────────────────
Loads run configuration from a YAML file, merging over portable defaults.

All defaults are resolved from environment variables with safe fallbacks
using Path.home() — no hardcoded user-specific paths or email addresses.

Environment variables
---------------------
EDCP_REPORT_ROOT   Override default report output directory
EDCP_INPUT_SHEET   Override default InputSheet path
EDCP_EMAIL_TO      Set email recipient for alerts (default: empty)
"""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, Dict, Optional

from edcp.utils.logger import get_logger

logger = get_logger(__name__)


def _make_defaults() -> Dict[str, Any]:
    """Build defaults dict fresh — reads env vars at call time (not at import)."""
    home = Path.home()
    return {
        # Core paths — resolved from env vars with portable fallbacks
        "input_sheet": os.environ.get(
            "EDCP_INPUT_SHEET",
            str(home / "DataComparePro" / "InputSheet_Data_Comparison.xlsx"),
        ),
        "report_root": os.environ.get(
            "EDCP_REPORT_ROOT",
            str(home / "DataComparePro" / "Reports"),
        ),
        "email_to":             os.environ.get("EDCP_EMAIL_TO", ""),
        "email_subject_prefix": "Data Comparison Results",
        "log_level":            "DEBUG",

        # SMTP fallback (used when pywin32/Outlook is unavailable)
        "smtp_host": "smtp.yourdomain.com",
        "smtp_port": 587,
        "smtp_from": "noreply@yourdomain.com",
        "smtp_user": "",
        "smtp_pass": "",

        # Capability enable/disable map
        "capabilities": {
            "comparison":   True,
            "tolerance":    True,
            "duplicate":    True,
            "schema":       True,
            "data_quality": True,
            "audit":        True,
            "alerts":       False,
            "plugins":      False,
        },

        # Alert rules
        "alert_rules": [],

        # Data quality checks
        "data_quality_checks": {
            "null_rate":      True,
            "distinct_count": True,
            "type_inference": True,
            "numeric_stats":  True,
        },

        # Plugin config
        "plugins": {
            "enabled": False,
            "modules": [],
        },
    }


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from *config_path* (YAML), merging over portable defaults.

    Parameters
    ----------
    config_path : str | None
        Path to a YAML file.  None → return defaults as-is.
    """
    cfg = _make_defaults()

    if not config_path:
        logger.debug("  [config] No config file specified – using portable defaults")
        return cfg

    p = Path(config_path)
    if not p.exists():
        logger.warning(f"  [config] Config file not found: {config_path} — using defaults")
        return cfg

    try:
        import yaml
        with open(p, encoding="utf-8") as f:
            user_cfg = yaml.safe_load(f) or {}
        logger.debug(f"  [config] Loaded from: {config_path}")
    except Exception as exc:
        logger.warning(f"  [config] Failed to parse {config_path}: {exc} — using defaults")
        return cfg

    # Deep-merge nested dicts
    for key, val in user_cfg.items():
        if isinstance(val, dict) and isinstance(cfg.get(key), dict):
            cfg[key] = {**cfg[key], **val}
        else:
            cfg[key] = val

    # Warn about deprecated paths that look like Windows user directories
    for field in ("input_sheet", "report_root"):
        if "Users" in str(cfg.get(field, "")) and "mtyagi" in str(cfg.get(field, "")):
            logger.warning(
                f"  [config] {field!r} contains a user-specific path: {cfg[field]!r}. "
                "Consider using EDCP_REPORT_ROOT / EDCP_INPUT_SHEET env vars instead."
            )

    return cfg
