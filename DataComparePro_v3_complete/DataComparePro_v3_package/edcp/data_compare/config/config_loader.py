# -*- coding: utf-8 -*-
"""
data_compare.config.config_loader
───────────────────────────────────
Loads run configuration from a YAML file, merging over hardcoded defaults.

Hardcoded defaults are the source of truth when no config file is provided
or when a key is absent from the YAML.  This preserves backward compatibility
with the original monolithic script.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# ── defaults resolved dynamically from environment variables each call ───────────
import os as _os

def _make_defaults() -> Dict[str, Any]:
    """Build defaults dict fresh — reads env vars at call time (not at import)."""
    return {
    # Core paths — resolved from env vars with safe fallbacks
    "input_sheet":           _os.environ.get("EDCP_INPUT_SHEET",
                                 str(Path.home() / "DataComparePro" / "InputSheet_Data_Comparison.xlsx")),
    "report_root":           _os.environ.get("EDCP_REPORT_ROOT",
                                 str(Path.home() / "DataComparePro" / "Reports")),
    "email_to":              _os.environ.get("EDCP_EMAIL_TO", ""),
    "email_subject_prefix":  "Data Comparison Results",
    "log_level":             "DEBUG",

    # SMTP fallback (used when pywin32/Outlook is unavailable)
    "smtp_host": "smtp.yourdomain.com",
    "smtp_port": 587,
    "smtp_from": "noreply@yourdomain.com",
    "smtp_user": "",
    "smtp_pass": "",

    # Capability enable/disable map
    # Each key maps to a capability NAME in the registry.
    # Missing keys default to True (capability runs unless explicitly disabled).
    "capabilities": {
        "comparison":   True,
        "tolerance":    True,
        "duplicate":    True,
        "schema":       True,
        "data_quality": True,
        "audit":        True,
        "alerts":       False,   # disabled by default; enable + add rules to activate
        "plugins":      False,   # disabled by default; enable + list modules to activate
    },

    # Alert rules evaluated by AlertsCapability when alerts.enabled = true
    # Each rule: {metric, operator, threshold, level, message}
    "alert_rules": [],

    # Data quality checks toggled by DataQualityCapability
    "data_quality_checks": {
        "null_rate":      True,
        "distinct_count": True,
        "type_inference": True,
        "numeric_stats":  True,
    },

    # Plugin config evaluated by PluginCapability when plugins.enabled = true
    "plugins": {
        "enabled": False,
        "modules": [],
    },
    }


def _make_defaults_compat():
    return _make_defaults()


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from *config_path* (YAML), merging over hardcoded defaults.

    Nested dicts (capabilities, data_quality_checks, plugins, alert_rules) are
    merged at the top level of each sub-dict so partial YAML overrides work.

    Parameters
    ----------
    config_path : str | None
        Path to a YAML file.  None → return hardcoded defaults as-is.

    Returns
    -------
    dict
        Merged configuration dict.
    """
    import copy
    cfg = _make_defaults()

    if not config_path:
        logger.debug("  [config] No config file specified – using hardcoded defaults")
        return cfg

    p = Path(config_path)
    if not p.exists():
        logger.warning(
            f"  [config] Config file not found: {p}  – using hardcoded defaults"
        )
        return cfg

    try:
        import yaml  # type: ignore
    except ImportError:
        logger.warning(
            "  [config] PyYAML not installed (pip install PyYAML).  "
            "Using hardcoded defaults."
        )
        return cfg

    try:
        with open(p, "r", encoding="utf-8") as fh:
            overrides: Dict[str, Any] = yaml.safe_load(fh) or {}
    except Exception as exc:
        logger.warning(
            f"  [config] Failed to parse {p}: {exc}  – using hardcoded defaults"
        )
        return cfg

    # ── flat key merge ────────────────────────────────────────────────────────
    for key, value in overrides.items():
        if key in ("capabilities", "data_quality_checks", "plugins") and isinstance(value, dict):
            # Deep-merge sub-dicts so partial YAML overrides don't wipe defaults
            cfg[key].update(value)
        elif key == "alert_rules" and isinstance(value, list):
            cfg["alert_rules"] = value
        else:
            cfg[key] = value

    logger.info(f"  [config] Loaded from: {p}")
    logger.debug(f"  [config] Capabilities: {cfg.get('capabilities')}")
    logger.debug(f"  [config] Alert rules:  {len(cfg.get('alert_rules', []))} rule(s)")
    return cfg
