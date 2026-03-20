# -*- coding: utf-8 -*-
"""
data_compare.context.run_context
──────────────────────────────────
Shared context object passed through the capability pipeline.

Every capability reads from this context and writes its results back into it.
No capability holds global state or references another capability directly.
The context is the single shared data bus for the entire pipeline.

Schema
------
{
    # ── INPUT ──────────────────────────────────────────────────────────────
    "prod_df":          pd.DataFrame | None,   # raw PROD data (after loading)
    "dev_df":           pd.DataFrame | None,   # raw DEV  data (after loading)
    "prod_csv_path":    Path | None,           # path to PROD CSV on disk
    "dev_csv_path":     Path | None,           # path to DEV  CSV on disk
    "prod_name":        str,                   # original PROD filename (for tol lookup)
    "dev_name":         str,                   # original DEV  filename (for tol lookup)

    # ── COMPARISON STATE (set by ComparisonCapability) ──────────────────────
    "prod_common":      pd.DataFrame | None,   # PROD rows matched to DEV
    "dev_common":       pd.DataFrame | None,   # DEV  rows matched to PROD
    "prod_vals":        pd.DataFrame | None,   # string-cast prod_common[common_cols]
    "dev_vals":         pd.DataFrame | None,   # string-cast dev_common[common_cols]
    "common_cols":      list,                  # column names compared
    "existing_keys":    list,                  # key columns found in both files
    "missing_keys":     list,                  # key columns defined but absent
    "align_keys":       list,                  # keys used to build the index
    "using_seq":        bool,                  # True when _SEQ_ duplicate alignment used
    "only_in_prod":     pd.Index | None,       # index values only in PROD
    "only_in_dev":      pd.Index | None,       # index values only in DEV
    "ignore_set":       set,                   # field names excluded from comparison
    "prod_aligned":     pd.DataFrame | None,   # PROD with index applied
    "dev_aligned":      pd.DataFrame | None,   # DEV  with index applied

    # ── TOLERANCE (set by ToleranceCapability) ───────────────────────────────
    "tol_map":          dict,                  # full tolerance map from InputSheet
    "tol_for_pair":     dict,                  # {col: decimals} active for this pair

    # ── DUPLICATE (set by DuplicateCapability) ───────────────────────────────
    "dup_prod_df":      pd.DataFrame | None,
    "dup_dev_df":       pd.DataFrame | None,
    "duplicates_summary": pd.DataFrame | None,
    "dup_count_prod":   int,
    "dup_count_dev":    int,

    # ── SCHEMA (set by SchemaCapability) ─────────────────────────────────────
    "schema_diff_df":   pd.DataFrame | None,

    # ── RESULTS (set by ComparisonCapability + others) ───────────────────────
    "results": {
        "grouped_differences":  pd.DataFrame | None,
        "missing_extra":        pd.DataFrame | None,
        "pass_report":          pd.DataFrame | None,
        "value_differences":    pd.DataFrame | None,
        "matched_passed":       int,
        "matched_failed":       int,
        "total_diffs":          int,
    },

    # ── METRICS (set by DataQualityCapability) ───────────────────────────────
    "metrics": {
        "null_rate_prod":       dict,  # {col: float}
        "null_rate_dev":        dict,
        "distinct_count_prod":  dict,
        "distinct_count_dev":   dict,
        "type_mismatch_cols":   list,
        "numeric_stats_prod":   dict,
        "numeric_stats_dev":    dict,
        "data_quality_report":  pd.DataFrame | None,
    },

    # ── AUDIT (set by AuditCapability) ───────────────────────────────────────
    "audit": {
        "events":           list,   # list of {timestamp, event, detail}
        "capability_times": dict,   # {capability_name: elapsed_seconds}
        "run_id":           str,
    },

    # ── ALERTS (set by AlertsCapability) ─────────────────────────────────────
    "alerts": {
        "triggered":    list,   # list of {level, rule, message, value, threshold}
        "alert_report": pd.DataFrame | None,
    },

    # ── PLUGINS output ───────────────────────────────────────────────────────
    "plugin_outputs":   dict,   # {plugin_name: any}

    # ── RUN CONFIG ───────────────────────────────────────────────────────────
    "config": {
        "result_name":          str,
        "report_root":          Path,
        "include_pass_report":  bool,
        "keys":                 list,
        "ignore_fields":        list,
        "capabilities":         dict,   # {name: bool}
        "alert_rules":          list,
        "data_quality_checks":  dict,
    },

    # ── OUTPUT ───────────────────────────────────────────────────────────────
    "report_path":      str | None,
    "elapsed_seconds":  float,
}
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def make_context(
    prod_csv_path: Optional[Path] = None,
    dev_csv_path: Optional[Path] = None,
    prod_name: str = "",
    dev_name: str = "",
    result_name: str = "CompareRecords",
    report_root: Optional[Path] = None,
    include_pass_report: bool = True,
    keys: Optional[List[str]] = None,
    ignore_fields: Optional[List[str]] = None,
    tol_map: Optional[Dict] = None,
    capabilities_cfg: Optional[Dict[str, bool]] = None,
    alert_rules: Optional[List[Dict]] = None,
    data_quality_checks: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Factory function that initialises a clean, fully-typed run context.

    Every key the pipeline may read or write is pre-declared here with a
    safe default.  Capabilities should never add undeclared keys; they should
    update existing ones.

    Parameters
    ----------
    prod_csv_path   Path to PROD CSV (UTF-8) on disk.
    dev_csv_path    Path to DEV  CSV (UTF-8) on disk.
    prod_name       Original PROD filename (used as key in tolerance map).
    dev_name        Original DEV  filename (used as key in tolerance map).
    result_name     Base name for the output workbook.
    report_root     Directory where reports are written.
    include_pass_report
                    True → write all matched rows to Pass Comparison;
                    False → write only failed rows.
    keys            List of key column names.  Empty → row-order fallback.
    ignore_fields   Column names excluded from all comparisons.
    tol_map         Pre-built tolerance map {(prod, dev, field): decimals}.
    capabilities_cfg
                    Dict of {capability_name: bool} controlling which
                    capabilities are active for this run.
    alert_rules     List of alert rule dicts (used by AlertsCapability).
    data_quality_checks
                    Dict of check config (used by DataQualityCapability).

    Returns
    -------
    dict
        Fully initialised run context.
    """
    return {
        # ── INPUT ─────────────────────────────────────────────────────────────
        "prod_df":            None,
        "dev_df":             None,
        "prod_csv_path":      prod_csv_path,
        "dev_csv_path":       dev_csv_path,
        "prod_name":          prod_name,
        "dev_name":           dev_name,

        # ── COMPARISON STATE ──────────────────────────────────────────────────
        "prod_common":        None,
        "dev_common":         None,
        "prod_vals":          None,
        "dev_vals":           None,
        "common_cols":        [],
        "existing_keys":      [],
        "missing_keys":       [],
        "align_keys":         [],
        "using_seq":          False,
        "only_in_prod":       None,
        "only_in_dev":        None,
        "ignore_set":         set(),
        "prod_aligned":       None,
        "dev_aligned":        None,

        # ── TOLERANCE ─────────────────────────────────────────────────────────
        "tol_map":            tol_map or {},
        "tol_for_pair":       {},

        # ── DUPLICATE ─────────────────────────────────────────────────────────
        "dup_prod_df":        None,
        "dup_dev_df":         None,
        "duplicates_summary": None,
        "dup_count_prod":     0,
        "dup_count_dev":      0,

        # ── SCHEMA ────────────────────────────────────────────────────────────
        "schema_diff_df":     None,

        # ── RESULTS ───────────────────────────────────────────────────────────
        "results": {
            "grouped_differences": None,
            "missing_extra":       None,
            "pass_report":         None,
            "value_differences":   None,
            "matched_passed":      0,
            "matched_failed":      0,
            "total_diffs":         0,
        },

        # ── METRICS ───────────────────────────────────────────────────────────
        "metrics": {
            "null_rate_prod":      {},
            "null_rate_dev":       {},
            "distinct_count_prod": {},
            "distinct_count_dev":  {},
            "type_mismatch_cols":  [],
            "numeric_stats_prod":  {},
            "numeric_stats_dev":   {},
            "data_quality_report": None,
        },

        # ── AUDIT ─────────────────────────────────────────────────────────────
        "audit": {
            "events":           [],
            "capability_times": {},
            "run_id":           uuid.uuid4().hex[:12],
        },

        # ── ALERTS ────────────────────────────────────────────────────────────
        "alerts": {
            "triggered":    [],
            "alert_report": None,
        },

        # ── PLUGINS ───────────────────────────────────────────────────────────
        "plugin_outputs": {},

        # ── CONFIG ────────────────────────────────────────────────────────────
        "config": {
            "result_name":         result_name,
            "report_root":         report_root or Path("."),
            "include_pass_report": include_pass_report,
            "keys":                keys or [],
            "ignore_fields":       ignore_fields or [],
            "capabilities":        capabilities_cfg or _default_capabilities(),
            "alert_rules":         alert_rules or [],
            "data_quality_checks": data_quality_checks or _default_dq_checks(),
        },

        # ── OUTPUT ────────────────────────────────────────────────────────────
        "report_path":    None,
        "elapsed_seconds": 0.0,
    }


def _default_capabilities() -> Dict[str, bool]:
    """Return the default capability enable/disable map."""
    return {
        "comparison":    True,
        "tolerance":     True,
        "duplicate":     True,
        "schema":        True,
        "data_quality":  True,
        "audit":         True,
        "alerts":        False,
        "plugins":       False,
    }


def _default_dq_checks() -> Dict[str, Any]:
    """Return default data-quality check configuration."""
    return {
        "null_rate":     True,
        "distinct_count": True,
        "type_inference": True,
        "numeric_stats":  True,
    }
