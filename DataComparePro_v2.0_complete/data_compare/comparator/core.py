# -*- coding: utf-8 -*-
"""
data_compare.comparator.core
──────────────────────────────
Core comparison engine.

Public API
----------
compare_records(...)  → Dict   Full comparison; writes Excel workbook.
_handle_empty_files(...)        Internal handler for zero-row files.

Features retained (no business logic changed):
  [F-25]  Case- and format-sensitive string comparison by default
  [F-26]  Tolerance fields compared after ROUND_HALF_UP rounding
  [F-27]  Vectorised column-wise comparison (no scalar .loc loops)
  [F-28]  MultiIndex sorted before .loc (lexsort safe, no PerformanceWarning)
  [F-29]  Common index intersection for matched records
  [F-30]  Unmatched records → Missing-in-Dev / Extra-in-Dev
  [F-31]  Grouped Differences sheet
  [F-32]  Count Differences sheet
  [F-33]  Pass Comparison sheet (wide)
  [F-34]  include_pass_report flag
  [F-35]  Duplicates Records sheet
  [F-36]  Schema Differences sheet (with Count)
  [F-37]  Settings sheet
  [F-51]  Empty-file early exit
  [F-53]  Collision-safe output filename (uuid4)
  [F-54]  Path traversal guard via safe_output_path
"""

import time
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from data_compare.comparator.duplicate import (
    detect_duplicates,
    add_sequence_for_duplicates,
    build_duplicates_summary,
)
from data_compare.comparator.schema import (
    build_schema_diff_dataframe,
    annotate_schema_counts,
)
from data_compare.comparator.tolerance import (
    ToleranceMap,
    normalize_numeric_with_tolerance,
    resolve_pair_tolerance,
)
from data_compare.loaders.encoding import read_csv_robust
from data_compare.reporting.excel_report import write_workbook_once
from data_compare.utils.helpers import (
    ensure_dir,
    normalize_keys,
    safe_output_path,
    trim_df,
)
from data_compare.utils.logger import get_logger, banner, progress

logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def compare_records(
    prod_csv: Path,
    dev_csv: Path,
    keys: List[str],
    result_name: str,
    report_root: Path,
    include_pass_report: bool,
    tol_map: ToleranceMap,
    prod_name_for_tol: str,
    dev_name_for_tol: str,
    ignore_fields: List[str],
) -> Dict:
    """
    Full comparison engine.  All output sheets are written to a single Excel
    workbook in one openpyxl pass (no double-open).

    Parameters
    ----------
    prod_csv : Path            Path to the PROD CSV (UTF-8).
    dev_csv : Path             Path to the DEV CSV (UTF-8).
    keys : list[str]           Key column names.  Empty list → row-order fallback.
    result_name : str          Base name used for the output workbook filename.
    report_root : Path         Directory where the workbook is written.
    include_pass_report : bool True → all matched rows; False → only failing rows.
    tol_map : ToleranceMap     Full tolerance map from the Tolerance sheet.
    prod_name_for_tol : str    Original PROD filename used as key in tol_map.
    dev_name_for_tol : str     Original DEV filename used as key in tol_map.
    ignore_fields : list[str]  Fields excluded from all comparisons.

    Returns
    -------
    dict
        Summary dictionary with counts and report path; consumed by the
        orchestrator to build the HTML summary and JSON audit log.
    """
    t0 = time.perf_counter()
    banner(f"COMPARING  {result_name}", "·")

    # ── load ──────────────────────────────────────────────────────────────────
    logger.info(f"  Loading PROD: {prod_csv.name}")
    prod_df = read_csv_robust(prod_csv, dtype=str, low_memory=False)
    prod_df = trim_df(prod_df)
    logger.info(f"  PROD loaded : {len(prod_df):>10,} rows × {len(prod_df.columns)} cols")

    logger.info(f"  Loading DEV : {dev_csv.name}")
    dev_df = read_csv_robust(dev_csv, dtype=str, low_memory=False)
    dev_df = trim_df(dev_df)
    logger.info(f"  DEV  loaded : {len(dev_df):>10,} rows × {len(dev_df.columns)} cols")

    # ── ignore fields ─────────────────────────────────────────────────────────
    ignore_set = {s.strip() for s in ignore_fields if str(s).strip()}
    if ignore_set:
        dropped_p = [c for c in prod_df.columns if c in ignore_set]
        dropped_d = [c for c in dev_df.columns  if c in ignore_set]
        prod_df.drop(columns=dropped_p, errors="ignore", inplace=True)
        dev_df.drop(columns=dropped_d,  errors="ignore", inplace=True)
        logger.info(f"  Ignored fields: {sorted(ignore_set)}")
        logger.debug(
            f"  Dropped from PROD: {dropped_p} | from DEV: {dropped_d}"
        )

    # ── tolerance for this file pair ──────────────────────────────────────────
    tol_for_pair = resolve_pair_tolerance(
        tol_map,
        prod_name_for_tol,
        dev_name_for_tol,
        list(prod_df.columns),
        list(dev_df.columns),
        ignore_set,
    )

    # ── keys ──────────────────────────────────────────────────────────────────
    keys_clean        = [k for k in keys if k]
    exclude_for_schema = set(keys_clean) | ignore_set

    # ── schema diff (single unified pass) ─────────────────────────────────────
    schema_diff_df = build_schema_diff_dataframe(
        list(prod_df.columns), list(dev_df.columns), exclude_for_schema
    )

    # ── empty-file early exit ─────────────────────────────────────────────────
    if len(prod_df) == 0 or len(dev_df) == 0:
        logger.warning(
            f"  ⚠ Empty file(s): PROD={len(prod_df):,} rows  "
            f"DEV={len(dev_df):,} rows"
        )
        return _handle_empty_files(
            prod_df, dev_df, keys_clean, result_name, report_root,
            schema_diff_df, ignore_set, tol_for_pair, prod_csv, dev_csv,
        )

    # ── key normalisation ─────────────────────────────────────────────────────
    prod_df = normalize_keys(prod_df, keys_clean)
    dev_df  = normalize_keys(dev_df,  keys_clean)

    missing_keys  = [
        k for k in keys_clean
        if k not in prod_df.columns or k not in dev_df.columns
    ]
    existing_keys = [
        k for k in keys_clean
        if k in prod_df.columns and k in dev_df.columns
    ]
    if missing_keys:
        logger.warning(
            f"  Key columns not found in both files: {missing_keys}"
        )
    if existing_keys:
        logger.info(f"  Keys in use: {existing_keys}")
    else:
        logger.warning("  No valid keys – falling back to row-position alignment")

    # ── duplicate detection ───────────────────────────────────────────────────
    dup_prod_df = detect_duplicates(prod_df, existing_keys)
    dup_dev_df  = detect_duplicates(dev_df,  existing_keys)

    if not existing_keys:
        duplicates_summary = pd.DataFrame(columns=["_NO_KEYS_"])
        dup_count_prod = dup_count_dev = 0
    else:
        dup_count_prod = len(dup_prod_df)
        dup_count_dev  = len(dup_dev_df)
        logger.info(
            f"  Duplicate key groups – PROD: {dup_count_prod:,}  "
            f"DEV: {dup_count_dev:,}"
        )
        duplicates_summary = build_duplicates_summary(
            dup_prod_df, dup_dev_df, existing_keys
        )

    # ── alignment ─────────────────────────────────────────────────────────────
    using_seq  = False
    align_keys = existing_keys[:]

    if existing_keys:
        if dup_count_prod > 0 or dup_count_dev > 0:
            using_seq = True
            logger.info("  Duplicate alignment: adding _SEQ_ sequence index")
            prod_aligned = add_sequence_for_duplicates(prod_df, existing_keys)
            dev_aligned  = add_sequence_for_duplicates(dev_df,  existing_keys)
            align_keys   = existing_keys + ["_SEQ_"]
        else:
            prod_aligned = prod_df.copy()
            dev_aligned  = dev_df.copy()
    else:
        prod_aligned = (
            prod_df.reset_index(drop=True)
            .reset_index()
            .rename(columns={"index": "_ROW_ID_"})
        )
        dev_aligned = (
            dev_df.reset_index(drop=True)
            .reset_index()
            .rename(columns={"index": "_ROW_ID_"})
        )
        align_keys = ["_ROW_ID_"]

    prod_aligned = prod_aligned.set_index(align_keys, drop=False).sort_index()
    dev_aligned  = dev_aligned.set_index(align_keys,  drop=False).sort_index()

    common_idx   = prod_aligned.index.intersection(dev_aligned.index)
    only_in_prod = prod_aligned.index.difference(dev_aligned.index)
    only_in_dev  = dev_aligned.index.difference(prod_aligned.index)

    logger.info(
        f"  Alignment: matched={len(common_idx):,}  "
        f"only-in-prod={len(only_in_prod):,}  "
        f"only-in-dev={len(only_in_dev):,}"
    )

    try:
        common_idx = common_idx.sort_values()
    except Exception:
        pass

    prod_common = prod_aligned.loc[common_idx]
    dev_common  = dev_aligned.loc[common_idx]

    # ── columns to compare ────────────────────────────────────────────────────
    exclude_cols = set(align_keys)
    common_cols  = [
        c for c in prod_common.columns
        if c in dev_common.columns
        and c not in exclude_cols
        and c not in ignore_set
    ]
    logger.info(f"  Columns to compare: {len(common_cols)}")

    # Annotate schema diff with non-empty cell counts
    schema_diff_df = annotate_schema_counts(
        schema_diff_df, prod_common, dev_common
    )

    # ── vectorised value comparison ───────────────────────────────────────────
    logger.info("  Running value comparison …")
    prod_vals = prod_common[common_cols].astype(str)
    dev_vals  = dev_common[common_cols].astype(str)

    comparison_rows: List[pd.DataFrame] = []
    total_diffs = 0

    for idx_col, col in enumerate(common_cols, start=1):
        pvals = prod_vals[col]
        dvals = dev_vals[col]

        if col in tol_for_pair:
            dec  = tol_for_pair[col]
            pcmp = pvals.map(
                lambda x, d=dec: normalize_numeric_with_tolerance(x, d)
            )
            dcmp = dvals.map(
                lambda x, d=dec: normalize_numeric_with_tolerance(x, d)
            )
            diffs = pcmp != dcmp
        else:
            diffs = pvals != dvals

        n_diffs = int(diffs.sum())
        if n_diffs:
            total_diffs += n_diffs
            diff_idx    = diffs[diffs].index
            tmp = pd.DataFrame({
                "Column":     col,
                "Prod Value": pvals.loc[diff_idx].values,
                "Dev Value":  dvals.loc[diff_idx].values,
            })
            for k in existing_keys:
                tmp.insert(
                    tmp.columns.get_loc("Column"),
                    k,
                    prod_common.loc[diff_idx, k].values,
                )
            if using_seq and "_SEQ_" in prod_common.columns:
                tmp["_SEQ_"] = prod_common.loc[diff_idx, "_SEQ_"].values
            comparison_rows.append(tmp)
            logger.debug(f"  Col '{col}': {n_diffs:,} difference(s)")

        if idx_col % 20 == 0 or idx_col == len(common_cols):
            progress(
                idx_col,
                len(common_cols),
                f"columns compared ({n_diffs} diffs this col)",
            )

    logger.info(
        f"  Value comparison done: {total_diffs:,} total cell difference(s) "
        f"across {len(comparison_rows)} column(s)"
    )

    # ── grouped differences ───────────────────────────────────────────────────
    grouped_cols_list = (
        existing_keys
        + (["_SEQ_"] if using_seq and existing_keys else [])
        + ["Column", "Prod Value", "Dev Value"]
    )

    if comparison_rows:
        value_differences = pd.concat(comparison_rows, ignore_index=True)
        grouped_differences = value_differences[
            [c for c in grouped_cols_list if c in value_differences.columns]
        ].copy()
    else:
        value_differences   = pd.DataFrame(columns=grouped_cols_list)
        grouped_differences = pd.DataFrame(columns=grouped_cols_list)

    # ── count differences (missing / extra records) ───────────────────────────
    mx_rows: List[pd.DataFrame] = []
    if len(only_in_prod) > 0:
        base = (
            prod_aligned.loc[only_in_prod, existing_keys].copy()
            if existing_keys
            else pd.DataFrame({"_ROW_ID_": list(only_in_prod)})
        )
        if using_seq and "_SEQ_" in prod_aligned.columns and existing_keys:
            base["_SEQ_"] = prod_aligned.loc[only_in_prod, "_SEQ_"].values
        base["Side"] = "Missing in Dev"
        mx_rows.append(base)

    if len(only_in_dev) > 0:
        base = (
            dev_aligned.loc[only_in_dev, existing_keys].copy()
            if existing_keys
            else pd.DataFrame({"_ROW_ID_": list(only_in_dev)})
        )
        if using_seq and "_SEQ_" in dev_aligned.columns and existing_keys:
            base["_SEQ_"] = dev_aligned.loc[only_in_dev, "_SEQ_"].values
        base["Side"] = "Extra in Dev"
        mx_rows.append(base)

    count_diff_cols = (
        existing_keys
        + (["_SEQ_"] if using_seq and existing_keys else [])
        + ["Side"]
        if existing_keys
        else ["_ROW_ID_", "Side"]
    )
    missing_extra = (
        pd.concat(mx_rows, ignore_index=True)
        if mx_rows
        else pd.DataFrame(columns=count_diff_cols)
    )

    # ── pass comparison (wide) ────────────────────────────────────────────────
    logger.info("  Building Pass Comparison sheet …")
    if existing_keys:
        pass_report_full = prod_common[existing_keys].copy()
        if using_seq and "_SEQ_" in prod_common.columns:
            pass_report_full["_SEQ_"] = prod_common["_SEQ_"].values
    else:
        pass_report_full = pd.DataFrame(index=prod_common.index)
        pass_report_full["_ROW_ID_"] = [
            (i[0] if isinstance(i, tuple) else i)
            for i in prod_common.index
        ]

    for col in common_cols:
        pass_report_full[f"Prod.{col}"] = prod_vals[col].values
        pass_report_full[f"Dev.{col}"]  = dev_vals[col].values
        if col in tol_for_pair:
            dec  = tol_for_pair[col]
            pcmp = prod_vals[col].map(
                lambda x, d=dec: normalize_numeric_with_tolerance(x, d)
            )
            dcmp = dev_vals[col].map(
                lambda x, d=dec: normalize_numeric_with_tolerance(x, d)
            )
            eq = pcmp.values == dcmp.values
        else:
            eq = prod_vals[col].values == dev_vals[col].values
        pass_report_full[f"CompareResult_{col}"] = [
            "Pass" if b else "Fail" for b in eq
        ]

    compare_result_cols = [
        c for c in pass_report_full.columns
        if c.startswith("CompareResult_")
    ]
    if compare_result_cols:
        row_fail       = (pass_report_full[compare_result_cols] == "Fail").any(axis=1)
        matched_failed = int(row_fail.sum())
        matched_passed = int((~row_fail).sum())
    else:
        matched_passed = len(pass_report_full)
        matched_failed = 0

    logger.info(
        f"  Pass Comparison: passed={matched_passed:,}  "
        f"failed={matched_failed:,}"
    )

    if include_pass_report:
        pass_report = pass_report_full
    else:
        if compare_result_cols:
            fail_rows = pass_report_full.loc[row_fail].copy()
        else:
            fail_rows = pd.DataFrame()
        pass_report = (
            fail_rows
            if not fail_rows.empty
            else pd.DataFrame([{
                "Info": (
                    "All matched rows passed. "
                    "Full pass-report suppressed (include_pass_report=False)."
                )
            }])
        )

    # ── settings sheet ────────────────────────────────────────────────────────
    def _schema_fields(side: str) -> str:
        if schema_diff_df.empty:
            return "—"
        vals = sorted(
            schema_diff_df.loc[schema_diff_df["Side"] == side, "Field"]
        )
        return ", ".join(vals) or "—"

    settings_dict = {
        "Used Keys":                         ", ".join(existing_keys) if existing_keys else "(row-order fallback)",
        "Missing Key Columns":               ", ".join(missing_keys) if missing_keys else "—",
        "Ignored Fields":                    ", ".join(sorted(ignore_set)) if ignore_set else "—",
        "Tolerance Applied":                 ", ".join(f"{c}({tol_for_pair[c]}dp)" for c in sorted(tol_for_pair)) or "—",
        "Duplicates in Prod":                str(dup_count_prod),
        "Duplicates in Dev":                 str(dup_count_dev),
        "Using Sequence Index (_SEQ_)":      str(using_seq),
        "Extra Fields in Dev":               _schema_fields("Extra in Dev"),
        "Missing Fields in Dev (Prod only)": _schema_fields("Missing in Dev"),
        "Total Prod Rows":                   str(len(prod_df)),
        "Total Dev Rows":                    str(len(dev_df)),
        "Matched Rows":                      str(len(common_idx)),
        "Only in Prod":                      str(len(only_in_prod)),
        "Only in Dev":                       str(len(only_in_dev)),
        "Total Cell Differences":            str(total_diffs),
    }
    settings_df = pd.DataFrame(
        list(settings_dict.items()), columns=["Setting", "Value"]
    )

    # ── write workbook (single atomic pass) ───────────────────────────────────
    ensure_dir(report_root)
    out_xlsx = safe_output_path(report_root, result_name)

    sheets = {
        "Grouped Differences": grouped_differences,
        "Count Differences":   missing_extra,
        "Pass Comparison":     pass_report,
        "Duplicates Records":  duplicates_summary,
        "Schema Differences":  schema_diff_df,
        "Settings":            settings_df,
    }
    write_workbook_once(sheets, out_xlsx)

    elapsed = time.perf_counter() - t0
    logger.info(f"  ✅ Report written: {out_xlsx.name}  [{elapsed:.1f}s]")
    banner(f"DONE  {result_name}", "·")

    return {
        "ResultName":        result_name,
        "ProdFile":          prod_csv.name,
        "DevFile":           dev_csv.name,
        "TotalProd":         len(prod_df),
        "TotalDev":          len(dev_df),
        "MatchedPassed":     matched_passed,
        "MatchedFailed":     matched_failed,
        "ExtraDev":          len(only_in_dev),
        "MissingDev":        len(only_in_prod),
        "DuplicateProd":     dup_count_prod,
        "DuplicateDev":      dup_count_dev,
        "UsedKeys":          existing_keys,
        "MissingKeyColumns": missing_keys,
        "IgnoredFields":     sorted(ignore_set),
        "ToleranceUsed":     tol_for_pair,
        "ReportPath":        str(out_xlsx),
        "ElapsedSeconds":    round(elapsed, 2),
        "SchemaExtraCount":  int((schema_diff_df["Side"] == "Extra in Dev").sum())
                             if not schema_diff_df.empty else 0,
        "SchemaMissingCount":int((schema_diff_df["Side"] == "Missing in Dev").sum())
                             if not schema_diff_df.empty else 0,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  EMPTY-FILE HANDLER
# ══════════════════════════════════════════════════════════════════════════════

def _handle_empty_files(
    prod_df: pd.DataFrame,
    dev_df: pd.DataFrame,
    keys_clean: List[str],
    result_name: str,
    report_root: Path,
    schema_diff_df: pd.DataFrame,
    ignore_set: set,
    tol_for_pair: Dict[str, int],
    prod_csv: Path,
    dev_csv: Path,
) -> Dict:
    """
    Write a minimal workbook when one or both files contain zero data rows
    and return a summary dict with zero-counts.
    """
    if len(prod_df) == 0 and len(dev_df) > 0:
        kp   = [k for k in keys_clean if k in dev_df.columns]
        base = (
            dev_df[kp].copy()
            if kp
            else pd.DataFrame({"Info": ["Dev has data; keys absent"]})
        )
        base["Side"] = "Extra in Dev"
        missing_extra     = base
        extra_dev_count   = len(dev_df)
        missing_dev_count = 0

    elif len(dev_df) == 0 and len(prod_df) > 0:
        kp   = [k for k in keys_clean if k in prod_df.columns]
        base = (
            prod_df[kp].copy()
            if kp
            else pd.DataFrame({"Info": ["Prod has data; keys absent"]})
        )
        base["Side"] = "Missing in Dev"
        missing_extra     = base
        extra_dev_count   = 0
        missing_dev_count = len(prod_df)

    else:
        missing_extra     = pd.DataFrame([{"Info": "Both files empty"}])
        extra_dev_count   = 0
        missing_dev_count = 0

    ensure_dir(report_root)
    out_xlsx = safe_output_path(report_root, result_name)

    info_df = pd.DataFrame([{
        "Info": "No data in one of the files – see Count Differences"
    }])
    settings_df = pd.DataFrame([
        {"Setting": "Status",         "Value": "Empty file encountered"},
        {"Setting": "Prod Rows",      "Value": str(len(prod_df))},
        {"Setting": "Dev Rows",       "Value": str(len(dev_df))},
        {"Setting": "Keys Requested", "Value": ", ".join(keys_clean) or "—"},
    ])
    sheets = {
        "Grouped Differences": info_df,
        "Count Differences":   missing_extra,
        "Pass Comparison":     info_df,
        "Duplicates Records":  info_df,
        "Schema Differences":  schema_diff_df,
        "Settings":            settings_df,
    }
    write_workbook_once(sheets, out_xlsx)
    logger.info(f"  Empty-file report written: {out_xlsx.name}")

    return {
        "ResultName":        result_name,
        "ProdFile":          prod_csv.name,
        "DevFile":           dev_csv.name,
        "TotalProd":         len(prod_df),
        "TotalDev":          len(dev_df),
        "MatchedPassed":     0,
        "MatchedFailed":     0,
        "ExtraDev":          extra_dev_count,
        "MissingDev":        missing_dev_count,
        "DuplicateProd":     0,
        "DuplicateDev":      0,
        "UsedKeys":          [],
        "MissingKeyColumns": [],
        "IgnoredFields":     sorted(ignore_set),
        "ToleranceUsed":     tol_for_pair,
        "ReportPath":        str(out_xlsx),
        "ElapsedSeconds":    0.0,
        "SchemaExtraCount":  int((schema_diff_df["Side"] == "Extra in Dev").sum())
                             if not schema_diff_df.empty else 0,
        "SchemaMissingCount":int((schema_diff_df["Side"] == "Missing in Dev").sum())
                             if not schema_diff_df.empty else 0,
    }
