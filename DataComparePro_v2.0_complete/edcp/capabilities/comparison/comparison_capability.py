# -*- coding: utf-8 -*-
"""
data_compare.capabilities.comparison.comparison_capability
───────────────────────────────────────────────────────────
ComparisonCapability

The core value-comparison engine wrapped as a capability.

This capability:
  1. Loads PROD and DEV CSVs from paths in context.
  2. Applies ignore fields.
  3. Normalises key columns (string-only, no numeric coercion).
  4. Resolves existing vs missing keys.
  5. Reads duplicate alignment state from context (set by DuplicateCapability).
  6. Performs vectorised column-wise comparison (with tolerance from context).
  7. Builds Grouped Differences, Count Differences, and Pass Comparison tables.
  8. Writes all results back into context['results'].

Requires context keys
---------------------
    prod_csv_path       : Path
    dev_csv_path        : Path
    config.keys         : list
    config.ignore_fields: list
    config.include_pass_report : bool
    tol_for_pair        : dict   (populated by ToleranceCapability; defaults to {})
    prod_aligned        : pd.DataFrame | None  (set by DuplicateCapability)
    dev_aligned         : pd.DataFrame | None
    align_keys          : list
    using_seq           : bool
    existing_keys       : list   (set by this capability if not already present)

Writes context keys
-------------------
    prod_df, dev_df
    prod_common, dev_common
    prod_vals, dev_vals
    common_cols, existing_keys, missing_keys
    ignore_set
    only_in_prod, only_in_dev
    results.grouped_differences
    results.missing_extra
    results.pass_report
    results.value_differences
    results.matched_passed
    results.matched_failed
    results.total_diffs
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from data_compare.capabilities.base import BaseCapability
from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
from data_compare.loaders.encoding import read_csv_robust
from data_compare.utils.helpers import (
    normalize_keys,
    trim_df,
)
from data_compare.utils.logger import get_logger, progress

logger = get_logger(__name__)


class ComparisonCapability(BaseCapability):
    NAME = "comparison"

    # ── helpers ───────────────────────────────────────────────────────────────

    def _load_data(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Load PROD and DEV CSVs; apply ignore_fields; set ignore_set."""
        prod_path  = context.get("prod_csv_path")
        dev_path   = context.get("dev_csv_path")
        cfg        = context.get("config", {})
        ignore_raw = cfg.get("ignore_fields", [])
        # Merge with any ignore_set already in context (e.g. from a prior run step)
        ignore_set = context.get("ignore_set", set()) | {s.strip() for s in ignore_raw if str(s).strip()}

        logger.info(f"  [comparison] Loading PROD: {Path(prod_path).name}")
        prod_df = read_csv_robust(prod_path, dtype=str, low_memory=False)
        prod_df = trim_df(prod_df)
        logger.info(
            f"  [comparison] PROD: {len(prod_df):>10,} rows × "
            f"{len(prod_df.columns)} cols"
        )

        logger.info(f"  [comparison] Loading DEV : {Path(dev_path).name}")
        dev_df = read_csv_robust(dev_path, dtype=str, low_memory=False)
        dev_df = trim_df(dev_df)
        logger.info(
            f"  [comparison] DEV : {len(dev_df):>10,} rows × "
            f"{len(dev_df.columns)} cols"
        )

        if ignore_set:
            dropped_p = [c for c in prod_df.columns if c in ignore_set]
            dropped_d = [c for c in dev_df.columns  if c in ignore_set]
            prod_df.drop(columns=dropped_p, errors="ignore", inplace=True)
            dev_df.drop(columns=dropped_d,  errors="ignore", inplace=True)
            logger.info(f"  [comparison] Ignored fields: {sorted(ignore_set)}")

        context["prod_df"]    = prod_df
        context["dev_df"]     = dev_df
        context["ignore_set"] = ignore_set
        return context

    def _resolve_keys(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Normalise keys and split into existing vs missing."""
        cfg        = context.get("config", {})
        keys       = cfg.get("keys", [])
        keys_clean = [k for k in keys if k]
        prod_df    = context["prod_df"]
        dev_df     = context["dev_df"]

        prod_df = normalize_keys(prod_df, keys_clean)
        dev_df  = normalize_keys(dev_df,  keys_clean)
        context["prod_df"] = prod_df
        context["dev_df"]  = dev_df

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
                f"  [comparison] Key columns not found in both files: {missing_keys}"
            )
        if existing_keys:
            logger.info(f"  [comparison] Keys in use: {existing_keys}")
        else:
            logger.warning(
                "  [comparison] No valid keys – row-position alignment"
            )

        context["existing_keys"] = existing_keys
        context["missing_keys"]  = missing_keys
        return context

    def _build_alignment(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build aligned DataFrames for comparison.

        Always runs full duplicate detection and _SEQ_ alignment internally.
        This ensures correct row-pairing even when DuplicateCapability has
        not yet run (it runs after ComparisonCapability in the default order).

        DuplicateCapability later updates dup_count_prod/dev and
        duplicates_summary for the report without re-doing alignment.
        """
        existing_keys = context.get("existing_keys", [])
        prod_df = context["prod_df"]
        dev_df  = context["dev_df"]

        if not existing_keys:
            # Row-order fallback
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
            using_seq  = False
        else:
            # Check for duplicate key groups in either file
            from data_compare.comparator.duplicate import (
                detect_duplicates,
                add_sequence_for_duplicates,
            )
            dup_prod = detect_duplicates(prod_df, existing_keys)
            dup_dev  = detect_duplicates(dev_df,  existing_keys)
            has_dups = len(dup_prod) > 0 or len(dup_dev) > 0

            if has_dups:
                using_seq  = True
                align_keys = existing_keys + ["_SEQ_"]
                logger.info(
                    f"  [comparison] Duplicates detected "
                    f"(prod={len(dup_prod)}, dev={len(dup_dev)}) "
                    f"– applying _SEQ_ alignment"
                )
                prod_aligned = add_sequence_for_duplicates(prod_df, existing_keys)
                dev_aligned  = add_sequence_for_duplicates(dev_df,  existing_keys)
                # Store counts so DuplicateCapability can update summary without re-detecting
                context["dup_count_prod"] = len(dup_prod)
                context["dup_count_dev"]  = len(dup_dev)
            else:
                using_seq  = False
                align_keys = existing_keys[:]
                prod_aligned = prod_df.copy()
                dev_aligned  = dev_df.copy()
                context["dup_count_prod"] = 0
                context["dup_count_dev"]  = 0

        prod_aligned = prod_aligned.set_index(align_keys, drop=False).sort_index()
        dev_aligned  = dev_aligned.set_index(align_keys,  drop=False).sort_index()

        context["prod_aligned"] = prod_aligned
        context["dev_aligned"]  = dev_aligned
        context["align_keys"]   = align_keys
        context["using_seq"]    = using_seq
        return context

    def _run_value_comparison(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Vectorised column-by-column comparison. Writes results into context."""
        prod_aligned  = context["prod_aligned"]
        dev_aligned   = context["dev_aligned"]
        align_keys    = context.get("align_keys", [])
        existing_keys = context.get("existing_keys", [])
        using_seq     = context.get("using_seq", False)
        ignore_set    = context.get("ignore_set", set())

        # Validate tol_for_pair against actual loaded columns.
        # ToleranceCapability may have stored rules before data was loaded;
        # here we filter to only columns present in both files and not ignored.
        raw_tol = context.get("tol_for_pair", {})
        prod_cols_set = set(prod_aligned.columns)
        dev_cols_set  = set(dev_aligned.columns)
        tol_for_pair: Dict[str, int] = {}
        for col, dec in raw_tol.items():
            if col in prod_cols_set and col in dev_cols_set and col not in ignore_set:
                tol_for_pair[col] = dec
                logger.debug(f"  [comparison] Tolerance active: '{col}' → {dec} decimal(s)")
            else:
                reason = "ignored" if col in ignore_set else "not in both files"
                logger.warning(f"  [comparison] Tolerance for '{col}' skipped ({reason})")
        # Write validated tolerance back so audit/alerts can inspect it
        context["tol_for_pair"] = tol_for_pair

        common_idx   = prod_aligned.index.intersection(dev_aligned.index)
        only_in_prod = prod_aligned.index.difference(dev_aligned.index)
        only_in_dev  = dev_aligned.index.difference(prod_aligned.index)

        logger.info(
            f"  [comparison] Alignment: matched={len(common_idx):,}  "
            f"only-prod={len(only_in_prod):,}  "
            f"only-dev={len(only_in_dev):,}"
        )

        try:
            common_idx = common_idx.sort_values()
        except Exception:
            pass

        prod_common = prod_aligned.loc[common_idx]
        dev_common  = dev_aligned.loc[common_idx]

        exclude_cols = set(align_keys)
        common_cols  = [
            c for c in prod_common.columns
            if c in dev_common.columns
            and c not in exclude_cols
            and c not in ignore_set
        ]
        logger.info(f"  [comparison] Columns to compare: {len(common_cols)}")

        prod_vals = prod_common[common_cols].astype(str)
        dev_vals  = dev_common[common_cols].astype(str)

        context["prod_common"] = prod_common
        context["dev_common"]  = dev_common
        context["prod_vals"]   = prod_vals
        context["dev_vals"]    = dev_vals
        context["common_cols"] = common_cols
        context["only_in_prod"] = only_in_prod
        context["only_in_dev"]  = only_in_dev

        # ── vectorised comparison ─────────────────────────────────────────────
        logger.info("  [comparison] Running value comparison …")
        comparison_rows: List[pd.DataFrame] = []
        total_diffs = 0

        for idx_col, col in enumerate(common_cols, start=1):
            pvals = prod_vals[col]
            dvals = dev_vals[col]

            # Always compare via numpy object arrays to avoid pandas MultiIndex
            # label-alignment errors (pandas 3.x raises ValueError when Series
            # indices differ, even after .loc[common_idx]).
            import numpy as _np
            if col in tol_for_pair:
                dec = tol_for_pair[col]
                pcmp_arr = _np.array([
                    normalize_numeric_with_tolerance(x, dec)
                    for x in pvals.tolist()
                ], dtype=object)
                dcmp_arr = _np.array([
                    normalize_numeric_with_tolerance(x, dec)
                    for x in dvals.tolist()
                ], dtype=object)
                diffs = pd.Series(pcmp_arr != dcmp_arr, index=pvals.index)
            else:
                p_arr = _np.array(pvals.tolist(), dtype=object)
                d_arr = _np.array(dvals.tolist(), dtype=object)
                diffs = pd.Series(p_arr != d_arr, index=pvals.index)

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
                logger.debug(f"  [comparison] Col '{col}': {n_diffs:,} diff(s)")

            if idx_col % 20 == 0 or idx_col == len(common_cols):
                progress(idx_col, len(common_cols),
                         f"cols compared ({n_diffs} diffs this col)")

        logger.info(
            f"  [comparison] Done: {total_diffs:,} total cell diff(s) "
            f"across {len(comparison_rows)} col(s)"
        )

        # ── grouped differences ───────────────────────────────────────────────
        grouped_cols_list = (
            existing_keys
            + (["_SEQ_"] if using_seq and existing_keys else [])
            + ["Column", "Prod Value", "Dev Value"]
        )
        if comparison_rows:
            value_differences   = pd.concat(comparison_rows, ignore_index=True)
            grouped_differences = value_differences[
                [c for c in grouped_cols_list if c in value_differences.columns]
            ].copy()
        else:
            value_differences   = pd.DataFrame(columns=grouped_cols_list)
            grouped_differences = pd.DataFrame(columns=grouped_cols_list)

        # ── count differences ─────────────────────────────────────────────────
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

        # ── pass comparison ───────────────────────────────────────────────────
        cfg              = context.get("config", {})
        include_pass     = cfg.get("include_pass_report", True)
        logger.info("  [comparison] Building Pass Comparison …")

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
            row_fail       = (
                pass_report_full[compare_result_cols] == "Fail"
            ).any(axis=1)
            matched_failed = int(row_fail.sum())
            matched_passed = int((~row_fail).sum())
        else:
            matched_passed = len(pass_report_full)
            matched_failed = 0

        logger.info(
            f"  [comparison] Passed={matched_passed:,}  "
            f"Failed={matched_failed:,}"
        )

        if include_pass:
            pass_report = pass_report_full
        else:
            fail_rows = (
                pass_report_full.loc[row_fail].copy()
                if compare_result_cols else pd.DataFrame()
            )
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

        # ── write results into context ────────────────────────────────────────
        context["results"]["grouped_differences"] = grouped_differences
        context["results"]["missing_extra"]       = missing_extra
        context["results"]["pass_report"]         = pass_report
        context["results"]["value_differences"]   = value_differences
        context["results"]["matched_passed"]      = matched_passed
        context["results"]["matched_failed"]      = matched_failed
        context["results"]["total_diffs"]         = total_diffs

        return context

    # ── main execute ──────────────────────────────────────────────────────────

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        context = self._load_data(context)
        context = self._resolve_keys(context)
        context = self._build_alignment(context)
        context = self._run_value_comparison(context)
        return context
