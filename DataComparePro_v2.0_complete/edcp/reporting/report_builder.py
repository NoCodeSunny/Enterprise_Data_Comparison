# -*- coding: utf-8 -*-
"""
data_compare.reporting.report_builder
───────────────────────────────────────
ReportBuilder

Reads the fully-populated context after all capabilities have run and
assembles the final Excel workbook.  This is NOT a capability itself —
it is called by the orchestrator as a post-pipeline step so that its
presence or absence does not affect any capability.

All sheets are determined by what is available in context:
  Always written (if data present):
    Grouped Differences, Count Differences, Pass Comparison,
    Duplicates Records, Schema Differences, Settings

  Written if DataQualityCapability ran:
    Data Quality Report

  Written if AuditCapability ran:
    Audit Trail, Capability Timings

  Written if AlertsCapability ran:
    Alerts

The Settings sheet is always built from context metadata.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from data_compare.reporting.excel_report import write_workbook_once
from data_compare.utils.helpers import ensure_dir, safe_output_path
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


def build_settings_df(context: Dict[str, Any]) -> pd.DataFrame:
    """Assemble the Settings sheet DataFrame from context."""
    cfg           = context.get("config", {})
    results       = context.get("results", {})
    schema_diff   = context.get("schema_diff_df")
    existing_keys = context.get("existing_keys", [])
    missing_keys  = context.get("missing_keys",  [])
    ignore_set    = context.get("ignore_set", set())
    tol_for_pair  = context.get("tol_for_pair", {})
    using_seq     = context.get("using_seq", False)
    prod_df       = context.get("prod_df")
    dev_df        = context.get("dev_df")
    common_idx    = context.get("prod_common")
    only_in_prod  = context.get("only_in_prod")
    only_in_dev   = context.get("only_in_dev")

    def _schema_fields(side: str) -> str:
        if schema_diff is None or schema_diff.empty:
            return "—"
        vals = sorted(schema_diff.loc[schema_diff["Side"] == side, "Field"])
        return ", ".join(vals) or "—"

    settings = {
        "Used Keys":                         ", ".join(existing_keys) or "(row-order fallback)",
        "Missing Key Columns":               ", ".join(missing_keys) or "—",
        "Ignored Fields":                    ", ".join(sorted(ignore_set)) or "—",
        "Tolerance Applied":                 ", ".join(f"{c}({tol_for_pair[c]}dp)" for c in sorted(tol_for_pair)) or "—",
        "Duplicates in Prod":                str(context.get("dup_count_prod", 0)),
        "Duplicates in Dev":                 str(context.get("dup_count_dev",  0)),
        "Using Sequence Index (_SEQ_)":      str(using_seq),
        "Extra Fields in Dev":               _schema_fields("Extra in Dev"),
        "Missing Fields in Dev (Prod only)": _schema_fields("Missing in Dev"),
        "Total Prod Rows":                   str(len(prod_df) if prod_df is not None else 0),
        "Total Dev Rows":                    str(len(dev_df)  if dev_df  is not None else 0),
        "Matched Rows":                      str(len(common_idx) if common_idx is not None else 0),
        "Only in Prod":                      str(len(only_in_prod) if only_in_prod is not None else 0),
        "Only in Dev":                       str(len(only_in_dev)  if only_in_dev  is not None else 0),
        "Total Cell Differences":            str(results.get("total_diffs", 0)),
        "Matched Passed":                    str(results.get("matched_passed", 0)),
        "Matched Failed":                    str(results.get("matched_failed", 0)),
        "Run ID":                            context.get("audit", {}).get("run_id", "—"),
        "Result Name":                       cfg.get("result_name", "—"),
    }
    return pd.DataFrame(list(settings.items()), columns=["Setting", "Value"])


def _safe_df(obj, fallback_cols=("Info",)) -> pd.DataFrame:
    """Return obj if it is a non-None DataFrame, else a one-row info DF."""
    if isinstance(obj, pd.DataFrame) and not obj.empty:
        return obj
    return pd.DataFrame({"Info": ["No data"]})


def build_report(context: Dict[str, Any]) -> Optional[Path]:
    """
    Assemble and write the comparison workbook from context.

    Returns
    -------
    Path | None
        Path to the written workbook, or None if report_root is not configured.
    """
    cfg         = context.get("config", {})
    report_root = cfg.get("report_root")
    result_name = cfg.get("result_name", "CompareRecords")
    results     = context.get("results", {})
    metrics     = context.get("metrics", {})
    audit       = context.get("audit",   {})
    alerts      = context.get("alerts",  {})

    if not report_root:
        logger.warning("  [report_builder] report_root not set – skipping workbook")
        return None

    report_root = Path(report_root)
    ensure_dir(report_root)
    out_xlsx = safe_output_path(report_root, result_name)

    # ── core sheets ───────────────────────────────────────────────────────────
    sheets: Dict[str, pd.DataFrame] = {
        "Grouped Differences": _safe_df(results.get("grouped_differences")),
        "Count Differences":   _safe_df(results.get("missing_extra")),
        "Pass Comparison":     _safe_df(results.get("pass_report")),
        "Duplicates Records":  _safe_df(context.get("duplicates_summary")),
        "Schema Differences":  _safe_df(context.get("schema_diff_df")),
        "Settings":            build_settings_df(context),
    }

    # ── optional: data quality ────────────────────────────────────────────────
    dq_report = metrics.get("data_quality_report")
    if isinstance(dq_report, pd.DataFrame) and not dq_report.empty:
        sheets["Data Quality Report"] = dq_report
        logger.debug("  [report_builder] Adding 'Data Quality Report' sheet")

    # ── optional: audit trail ─────────────────────────────────────────────────
    events_df  = audit.get("events_df")
    timing_df  = audit.get("timing_df")
    summary_df = audit.get("summary_df")
    if isinstance(events_df, pd.DataFrame) and not events_df.empty:
        sheets["Audit Trail"]    = events_df
        sheets["Audit Summary"]  = summary_df if isinstance(summary_df, pd.DataFrame) else pd.DataFrame()
        sheets["Cap Timings"]    = timing_df  if isinstance(timing_df,  pd.DataFrame) else pd.DataFrame()
        logger.debug("  [report_builder] Adding Audit sheets")

    # ── optional: alerts ──────────────────────────────────────────────────────
    alert_report = alerts.get("alert_report")
    if isinstance(alert_report, pd.DataFrame) and not alert_report.empty:
        sheets["Alerts"] = alert_report
        logger.debug(
            f"  [report_builder] Adding 'Alerts' sheet "
            f"({len(alert_report)} triggered)"
        )

    write_workbook_once(sheets, out_xlsx)
    logger.info(f"  [report_builder] Workbook written: {out_xlsx.name}")
    return out_xlsx
