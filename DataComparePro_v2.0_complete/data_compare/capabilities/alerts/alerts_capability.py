# -*- coding: utf-8 -*-
"""
data_compare.capabilities.alerts.alerts_capability
────────────────────────────────────────────────────
AlertsCapability

Evaluates user-defined threshold rules against comparison metrics and
writes triggered alerts into context['alerts'].

Alert rules are defined in config.alert_rules as a list of dicts:

    alert_rules:
      - metric: matched_failed
        operator: ">"
        threshold: 0
        level: ERROR
        message: "Validation failures detected"

      - metric: missing_dev_pct
        operator: ">"
        threshold: 5.0
        level: WARNING
        message: "More than 5% records missing in Dev"

      - metric: dup_count_prod
        operator: ">"
        threshold: 0
        level: INFO
        message: "Duplicate records found in PROD"

Supported metrics (resolved from context):
    matched_failed      – results.matched_failed
    matched_passed      – results.matched_passed
    total_diffs         – results.total_diffs
    missing_dev         – count of only_in_prod records
    extra_dev           – count of only_in_dev records
    missing_dev_pct     – missing_dev / total_prod * 100
    extra_dev_pct       – extra_dev / total_dev * 100
    dup_count_prod      – dup_count_prod
    dup_count_dev       – dup_count_dev
    schema_missing      – schema_diff Missing-in-Dev count
    schema_extra        – schema_diff Extra-in-Dev count
    type_mismatch_count – len(metrics.type_mismatch_cols)

Supported operators: > < >= <= == !=

Requires context keys
---------------------
    results, dup_count_prod, dup_count_dev, schema_diff_df, metrics,
    only_in_prod, only_in_dev, prod_df, dev_df, config.alert_rules

Writes context keys
-------------------
    alerts.triggered    : list of triggered alert dicts
    alerts.alert_report : pd.DataFrame

Independent – does NOT depend on any reporting capability.
"""

from __future__ import annotations

import operator as _op
from typing import Any, Dict, List, Optional

import pandas as pd

from data_compare.capabilities.base import BaseCapability
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

_OPERATORS = {
    ">":  _op.gt,
    "<":  _op.lt,
    ">=": _op.ge,
    "<=": _op.le,
    "==": _op.eq,
    "!=": _op.ne,
}


def _safe_len(obj) -> int:
    """Return len(obj) or 0 if obj is None."""
    try:
        return len(obj) if obj is not None else 0
    except TypeError:
        return 0


def _resolve_metric(metric_name: str, context: Dict[str, Any]) -> Optional[float]:
    """Resolve a named metric string to a numeric value from context."""
    results     = context.get("results", {})
    prod_df     = context.get("prod_df")
    dev_df      = context.get("dev_df")
    schema_diff = context.get("schema_diff_df")
    metrics     = context.get("metrics", {})
    only_prod   = context.get("only_in_prod")
    only_dev    = context.get("only_in_dev")

    total_prod = _safe_len(prod_df)
    total_dev  = _safe_len(dev_df)
    missing_dev = _safe_len(only_prod)
    extra_dev   = _safe_len(only_dev)

    lookup: Dict[str, float] = {
        "matched_failed":     float(results.get("matched_failed", 0)),
        "matched_passed":     float(results.get("matched_passed", 0)),
        "total_diffs":        float(results.get("total_diffs",    0)),
        "missing_dev":        float(missing_dev),
        "extra_dev":          float(extra_dev),
        "missing_dev_pct":    (
            round(missing_dev / total_prod * 100, 4)
            if total_prod > 0 else 0.0
        ),
        "extra_dev_pct":      (
            round(extra_dev / total_dev * 100, 4)
            if total_dev > 0 else 0.0
        ),
        "dup_count_prod":     float(context.get("dup_count_prod", 0)),
        "dup_count_dev":      float(context.get("dup_count_dev",  0)),
        "schema_missing":     float(
            (schema_diff["Side"] == "Missing in Dev").sum()
            if schema_diff is not None and not schema_diff.empty else 0
        ),
        "schema_extra":       float(
            (schema_diff["Side"] == "Extra in Dev").sum()
            if schema_diff is not None and not schema_diff.empty else 0
        ),
        "type_mismatch_count": float(
            len(metrics.get("type_mismatch_cols", []))
        ),
    }
    return lookup.get(metric_name)


class AlertsCapability(BaseCapability):
    NAME = "alerts"

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        alert_rules: List[Dict] = context.get("config", {}).get("alert_rules", [])

        if not alert_rules:
            logger.debug("  [alerts] No alert rules configured – skipping")
            context["alerts"]["triggered"]    = []
            context["alerts"]["alert_report"] = pd.DataFrame(columns=[
                "Level", "Metric", "Operator", "Threshold",
                "ActualValue", "Message",
            ])
            return context

        triggered: List[Dict[str, Any]] = []

        for rule in alert_rules:
            metric_name = str(rule.get("metric", "")).strip()
            op_str      = str(rule.get("operator", ">")).strip()
            threshold   = float(rule.get("threshold", 0))
            level       = str(rule.get("level", "WARNING")).upper()
            message     = str(rule.get("message", f"Alert: {metric_name}"))

            if op_str not in _OPERATORS:
                logger.warning(
                    f"  [alerts] Unknown operator '{op_str}' in rule "
                    f"for metric '{metric_name}' – skipped"
                )
                continue

            actual = _resolve_metric(metric_name, context)
            if actual is None:
                logger.warning(
                    f"  [alerts] Cannot resolve metric '{metric_name}' – "
                    f"skipped"
                )
                continue

            fn = _OPERATORS[op_str]
            if fn(actual, threshold):
                alert = {
                    "Level":       level,
                    "Metric":      metric_name,
                    "Operator":    op_str,
                    "Threshold":   threshold,
                    "ActualValue": actual,
                    "Message":     message,
                }
                triggered.append(alert)
                log_fn = (
                    logger.error   if level == "ERROR"   else
                    logger.warning if level == "WARNING" else
                    logger.info
                )
                log_fn(
                    f"  [alerts] 🚨 {level}: {message}  "
                    f"({metric_name}={actual} {op_str} {threshold})"
                )

        logger.info(
            f"  [alerts] {len(triggered)} alert(s) triggered "
            f"out of {len(alert_rules)} rule(s)"
        )

        alert_report = (
            pd.DataFrame(triggered)
            if triggered
            else pd.DataFrame(columns=[
                "Level", "Metric", "Operator", "Threshold",
                "ActualValue", "Message",
            ])
        )

        context["alerts"]["triggered"]    = triggered
        context["alerts"]["alert_report"] = alert_report
        return context
