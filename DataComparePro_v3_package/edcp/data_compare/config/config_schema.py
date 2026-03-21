# -*- coding: utf-8 -*-
"""
data_compare.config.config_schema
───────────────────────────────────
Configuration validation layer.

Uses Python dataclasses + manual validation to provide Pydantic-style
type checking and error messages without any external dependencies.

Validates:
  - Required top-level keys
  - Type correctness for all fields
  - Capability map structure
  - Alert rule structure
  - Engine settings
  - Parallelism settings
  - Log settings

Usage
─────
    from data_compare.config.config_schema import validate_config, ConfigError

    try:
        validated = validate_config(raw_dict)
    except ConfigError as exc:
        print(f"Config invalid: {exc}")
        sys.exit(1)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


class ConfigError(ValueError):
    """Raised when the run configuration fails validation."""


# ── field-level validators ────────────────────────────────────────────────────

def _str(val: Any, key: str) -> str:
    if not isinstance(val, str):
        raise ConfigError(f"'{key}' must be a string, got {type(val).__name__}")
    return val


def _int_pos(val: Any, key: str) -> int:
    try:
        v = int(val)
    except (TypeError, ValueError):
        raise ConfigError(f"'{key}' must be an integer, got {type(val).__name__}")
    if v <= 0:
        raise ConfigError(f"'{key}' must be a positive integer, got {v}")
    return v


def _int_nonneg(val: Any, key: str) -> int:
    try:
        v = int(val)
    except (TypeError, ValueError):
        raise ConfigError(f"'{key}' must be a non-negative integer, got {type(val).__name__}")
    if v < 0:
        raise ConfigError(f"'{key}' must be >= 0, got {v}")
    return v


def _bool(val: Any, key: str) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        if val.lower() in ("true", "1", "yes"):
            return True
        if val.lower() in ("false", "0", "no"):
            return False
    raise ConfigError(f"'{key}' must be a boolean, got {type(val).__name__}: {val!r}")


def _list_of_str(val: Any, key: str) -> List[str]:
    if not isinstance(val, list):
        raise ConfigError(f"'{key}' must be a list, got {type(val).__name__}")
    for i, item in enumerate(val):
        if not isinstance(item, str):
            raise ConfigError(f"'{key}[{i}]' must be a string, got {type(item).__name__}")
    return val


def _dict(val: Any, key: str) -> Dict[str, Any]:
    if not isinstance(val, dict):
        raise ConfigError(f"'{key}' must be a dict, got {type(val).__name__}")
    return val


def _log_level(val: Any, key: str) -> str:
    allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    v = _str(val, key).upper()
    if v not in allowed:
        raise ConfigError(f"'{key}' must be one of {sorted(allowed)}, got {v!r}")
    return v


def _validate_capabilities(caps: Any) -> Dict[str, bool]:
    _dict(caps, "capabilities")
    allowed_names = {
        "comparison", "tolerance", "duplicate", "schema",
        "data_quality", "audit", "alerts", "plugins",
    }
    validated: Dict[str, bool] = {}
    for k, v in caps.items():
        if k not in allowed_names:
            # Allow custom capability names without error
            pass
        validated[k] = _bool(v, f"capabilities.{k}")
    return validated


def _validate_alert_rules(rules: Any) -> List[Dict[str, Any]]:
    if not isinstance(rules, list):
        raise ConfigError("'alert_rules' must be a list")
    _VALID_OPS    = {">", "<", ">=", "<=", "==", "!="}
    _VALID_LEVELS = {"ERROR", "WARNING", "INFO"}
    validated = []
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise ConfigError(f"alert_rules[{i}] must be a dict")
        for req in ("metric", "operator", "threshold"):
            if req not in rule:
                raise ConfigError(f"alert_rules[{i}] missing required key '{req}'")
        op = str(rule["operator"]).strip()
        if op not in _VALID_OPS:
            raise ConfigError(
                f"alert_rules[{i}].operator must be one of {sorted(_VALID_OPS)}, got {op!r}"
            )
        lvl = str(rule.get("level", "WARNING")).upper()
        if lvl not in _VALID_LEVELS:
            raise ConfigError(
                f"alert_rules[{i}].level must be one of {sorted(_VALID_LEVELS)}, got {lvl!r}"
            )
        try:
            float(rule["threshold"])
        except (TypeError, ValueError):
            raise ConfigError(
                f"alert_rules[{i}].threshold must be numeric, got {rule['threshold']!r}"
            )
        validated.append(rule)
    return validated


def _validate_dq_checks(checks: Any) -> Dict[str, bool]:
    _dict(checks, "data_quality_checks")
    allowed = {"null_rate", "distinct_count", "type_inference", "numeric_stats"}
    validated: Dict[str, bool] = {}
    for k, v in checks.items():
        if k not in allowed:
            raise ConfigError(
                f"data_quality_checks.{k!r} is not a recognised check. "
                f"Allowed: {sorted(allowed)}"
            )
        validated[k] = _bool(v, f"data_quality_checks.{k}")
    return validated


# ── main validation function ──────────────────────────────────────────────────

def validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and normalise *cfg*.

    All known fields are type-checked.  Unknown keys are passed through
    unchanged so user extensions are not broken.

    Returns the validated (and normalised) config dict.
    Raises ConfigError with a descriptive message on the first failure.
    """
    errors: List[str] = []

    def _check(fn, *args):
        try:
            return fn(*args)
        except ConfigError as exc:
            errors.append(str(exc))
            return args[0] if args else None   # return raw value so we can collect more errors

    validated: Dict[str, Any] = dict(cfg)  # start with a copy

    # ── required string fields ────────────────────────────────────────────────
    for key in ("input_sheet", "report_root", "email_to"):
        if key not in validated:
            errors.append(f"Missing required config key: '{key}'")
        else:
            validated[key] = _check(_str, validated[key], key) or validated[key]

    # ── optional string fields ────────────────────────────────────────────────
    for key in ("email_subject_prefix", "smtp_host", "smtp_from", "smtp_user", "smtp_pass"):
        if key in validated:
            validated[key] = _check(_str, validated[key], key) or validated[key]

    # ── log level ─────────────────────────────────────────────────────────────
    if "log_level" in validated:
        try:
            validated["log_level"] = _log_level(validated["log_level"], "log_level")
        except ConfigError as exc:
            errors.append(str(exc))

    # ── smtp port ─────────────────────────────────────────────────────────────
    if "smtp_port" in validated:
        try:
            validated["smtp_port"] = _int_pos(validated["smtp_port"], "smtp_port")
        except ConfigError as exc:
            errors.append(str(exc))

    # ── capabilities ──────────────────────────────────────────────────────────
    if "capabilities" in validated:
        try:
            validated["capabilities"] = _validate_capabilities(validated["capabilities"])
        except ConfigError as exc:
            errors.append(str(exc))

    # ── alert rules ───────────────────────────────────────────────────────────
    if "alert_rules" in validated:
        try:
            validated["alert_rules"] = _validate_alert_rules(validated["alert_rules"])
        except ConfigError as exc:
            errors.append(str(exc))

    # ── data quality checks ───────────────────────────────────────────────────
    if "data_quality_checks" in validated:
        try:
            validated["data_quality_checks"] = _validate_dq_checks(validated["data_quality_checks"])
        except ConfigError as exc:
            errors.append(str(exc))

    # ── engine settings ───────────────────────────────────────────────────────
    if "use_spark" in validated:
        try:
            validated["use_spark"] = _bool(validated["use_spark"], "use_spark")
        except ConfigError as exc:
            errors.append(str(exc))

    if "spark_threshold_bytes" in validated:
        try:
            validated["spark_threshold_bytes"] = _int_pos(
                validated["spark_threshold_bytes"], "spark_threshold_bytes"
            )
        except ConfigError as exc:
            errors.append(str(exc))

    if "pandas_chunk_size" in validated:
        try:
            validated["pandas_chunk_size"] = _int_pos(
                validated["pandas_chunk_size"], "pandas_chunk_size"
            )
        except ConfigError as exc:
            errors.append(str(exc))

    # ── parallelism ───────────────────────────────────────────────────────────
    if "max_workers" in validated:
        try:
            validated["max_workers"] = _int_pos(validated["max_workers"], "max_workers")
        except ConfigError as exc:
            errors.append(str(exc))

    # ── path existence check (warning only – file may be on remote mount) ──────
    if "input_sheet" in validated and validated.get("input_sheet"):
        # Only warn about missing InputSheet when not running in YAML-batches mode
        # (batches: section bypasses InputSheet entirely)
        if not validated.get("batches"):
            p = Path(str(validated["input_sheet"]))
            if not p.exists():
                import logging
                _warn_logger = logging.getLogger("DataComparator.config")
                _warn_logger.warning(
                    f"input_sheet path does not exist: {p}  "
                    f"(will fail at runtime if still absent)"
                )
                # NOT added to errors – path may be on a network mount not yet accessible

    # ── collect all errors ────────────────────────────────────────────────────
    if errors:
        bullet = "\n  • "
        raise ConfigError(
            f"Configuration validation failed ({len(errors)} error(s)):"
            + bullet
            + bullet.join(errors)
        )

    return validated


def validate_batch_row(
    base_path: str,
    prod_name: str,
    dev_name: str,
    result_name: str,
    keys: List[str],
    batch_num: int = 0,
) -> None:
    """
    Validate a single batch row extracted from the InputSheet.

    Raises ConfigError with batch-specific context on failure.
    """
    errors: List[str] = []

    if not base_path or not base_path.strip():
        errors.append(f"Batch {batch_num}: 'Files Path' is empty")
    if not prod_name or not prod_name.strip():
        errors.append(f"Batch {batch_num}: 'Prod File Name' is empty")
    if not dev_name or not dev_name.strip():
        errors.append(f"Batch {batch_num}: 'Dev File Name' is empty")
    if not result_name or not result_name.strip():
        errors.append(f"Batch {batch_num}: 'Result Report Name' is empty")

    if base_path and prod_name:
        prod_path = Path(base_path) / prod_name
        if not prod_path.exists():
            errors.append(
                f"Batch {batch_num}: PROD file not found: {prod_path}"
            )
    if base_path and dev_name:
        dev_path = Path(base_path) / dev_name
        if not dev_path.exists():
            errors.append(
                f"Batch {batch_num}: DEV file not found: {dev_path}"
            )

    if errors:
        raise ConfigError("\n".join(errors))
