# -*- coding: utf-8 -*-
"""
data_compare.capabilities.data_quality.data_quality_capability
────────────────────────────────────────────────────────────────
DataQualityCapability

Computes column-level data quality metrics for PROD and DEV independently.
Runs entirely from prod_df / dev_df; does NOT depend on comparison results.

Metrics computed (each individually toggled via config.data_quality_checks):
  null_rate       – fraction of empty/"" cells per column
  distinct_count  – number of distinct non-empty values per column
  type_inference  – columns where pandas infers different dtypes in prod vs dev
  numeric_stats   – min, max, mean, std for columns that are numeric in both

Requires context keys
---------------------
    prod_df                  : pd.DataFrame
    dev_df                   : pd.DataFrame
    config.data_quality_checks : dict

Writes context keys
-------------------
    metrics.null_rate_prod
    metrics.null_rate_dev
    metrics.distinct_count_prod
    metrics.distinct_count_dev
    metrics.type_mismatch_cols
    metrics.numeric_stats_prod
    metrics.numeric_stats_dev
    metrics.data_quality_report  (pd.DataFrame summary)

Fully independent – runs without any other capability having executed.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from data_compare.capabilities.base import BaseCapability
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityCapability(BaseCapability):
    NAME = "data_quality"

    # ── individual checks ─────────────────────────────────────────────────────

    @staticmethod
    def _null_rates(df: pd.DataFrame) -> Dict[str, float]:
        """Fraction of empty-string ("") cells per column."""
        if df is None or df.empty:
            return {}
        total = len(df)
        if total == 0:
            return {}
        return {
            col: round(float((df[col].astype(str) == "").sum()) / total, 6)
            for col in df.columns
        }

    @staticmethod
    def _distinct_counts(df: pd.DataFrame) -> Dict[str, int]:
        """Number of distinct non-empty values per column."""
        if df is None or df.empty:
            return {}
        return {
            col: int(df[col].astype(str).replace("", pd.NA).dropna().nunique())
            for col in df.columns
        }

    @staticmethod
    def _type_mismatches(prod_df: pd.DataFrame, dev_df: pd.DataFrame) -> List[str]:
        """
        Columns present in both files where pandas infers different dtypes when
        we attempt numeric coercion.  Detects columns that look numeric in one
        file but not the other.
        """
        mismatches: List[str] = []
        common = set(prod_df.columns) & set(dev_df.columns)
        for col in sorted(common):
            p_numeric = pd.to_numeric(
                prod_df[col].astype(str), errors="coerce"
            ).notna().mean() > 0.9
            d_numeric = pd.to_numeric(
                dev_df[col].astype(str), errors="coerce"
            ).notna().mean() > 0.9
            if p_numeric != d_numeric:
                mismatches.append(col)
        return mismatches

    @staticmethod
    def _numeric_stats(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """
        For each column where numeric coercion succeeds for > 90% of rows,
        compute min, max, mean, std.
        """
        if df is None or df.empty:
            return {}
        stats: Dict[str, Dict[str, float]] = {}
        for col in df.columns:
            series = pd.to_numeric(df[col].astype(str), errors="coerce")
            if series.notna().mean() > 0.9 and series.notna().sum() > 0:
                stats[col] = {
                    "min":  round(float(series.min()),  6),
                    "max":  round(float(series.max()),  6),
                    "mean": round(float(series.mean()), 6),
                    "std":  round(float(series.std()),  6) if len(series) > 1 else 0.0,
                    "count": int(series.notna().sum()),
                }
        return stats

    def _build_quality_report(self, context: Dict[str, Any]) -> pd.DataFrame:
        """
        Assemble a wide summary DataFrame comparing PROD vs DEV per column.
        Columns present in only one side are included with NaN on the absent side.
        """
        metrics = context.get("metrics", {})
        nr_p  = metrics.get("null_rate_prod",      {})
        nr_d  = metrics.get("null_rate_dev",        {})
        dc_p  = metrics.get("distinct_count_prod",  {})
        dc_d  = metrics.get("distinct_count_dev",   {})
        ns_p  = metrics.get("numeric_stats_prod",   {})
        ns_d  = metrics.get("numeric_stats_dev",    {})
        mismatches = set(metrics.get("type_mismatch_cols", []))

        all_cols = sorted(set(nr_p.keys()) | set(nr_d.keys()))
        rows = []
        for col in all_cols:
            row: Dict[str, Any] = {
                "Column":              col,
                "NullRate_Prod":       nr_p.get(col, ""),
                "NullRate_Dev":        nr_d.get(col, ""),
                "DistinctCount_Prod":  dc_p.get(col, ""),
                "DistinctCount_Dev":   dc_d.get(col, ""),
                "TypeMismatch":        "Yes" if col in mismatches else "No",
            }
            if col in ns_p:
                row["Min_Prod"]  = ns_p[col].get("min", "")
                row["Max_Prod"]  = ns_p[col].get("max", "")
                row["Mean_Prod"] = ns_p[col].get("mean", "")
                row["Std_Prod"]  = ns_p[col].get("std", "")
            else:
                row["Min_Prod"] = row["Max_Prod"] = row["Mean_Prod"] = row["Std_Prod"] = ""
            if col in ns_d:
                row["Min_Dev"]  = ns_d[col].get("min", "")
                row["Max_Dev"]  = ns_d[col].get("max", "")
                row["Mean_Dev"] = ns_d[col].get("mean", "")
                row["Std_Dev"]  = ns_d[col].get("std", "")
            else:
                row["Min_Dev"] = row["Max_Dev"] = row["Mean_Dev"] = row["Std_Dev"] = ""
            rows.append(row)

        if not rows:
            return pd.DataFrame(columns=[
                "Column", "NullRate_Prod", "NullRate_Dev",
                "DistinctCount_Prod", "DistinctCount_Dev",
                "TypeMismatch",
                "Min_Prod", "Max_Prod", "Mean_Prod", "Std_Prod",
                "Min_Dev",  "Max_Dev",  "Mean_Dev",  "Std_Dev",
            ])
        return pd.DataFrame(rows)

    # ── main execute ──────────────────────────────────────────────────────────

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        prod_df = context.get("prod_df")
        dev_df  = context.get("dev_df")

        if prod_df is None or dev_df is None:
            logger.warning(
                "  [data_quality] prod_df or dev_df not loaded – skipping"
            )
            return context

        dq_cfg = context.get("config", {}).get("data_quality_checks", {})

        if dq_cfg.get("null_rate", True):
            context["metrics"]["null_rate_prod"] = self._null_rates(prod_df)
            context["metrics"]["null_rate_dev"]  = self._null_rates(dev_df)
            logger.debug(
                f"  [data_quality] null_rate computed for "
                f"{len(context['metrics']['null_rate_prod'])} PROD cols"
            )

        if dq_cfg.get("distinct_count", True):
            context["metrics"]["distinct_count_prod"] = self._distinct_counts(prod_df)
            context["metrics"]["distinct_count_dev"]  = self._distinct_counts(dev_df)
            logger.debug("  [data_quality] distinct_count computed")

        if dq_cfg.get("type_inference", True):
            context["metrics"]["type_mismatch_cols"] = self._type_mismatches(
                prod_df, dev_df
            )
            n_mm = len(context["metrics"]["type_mismatch_cols"])
            logger.info(f"  [data_quality] Type mismatches detected: {n_mm} col(s)")
            if n_mm:
                logger.debug(
                    f"  [data_quality] Mismatched cols: "
                    f"{context['metrics']['type_mismatch_cols']}"
                )

        if dq_cfg.get("numeric_stats", True):
            context["metrics"]["numeric_stats_prod"] = self._numeric_stats(prod_df)
            context["metrics"]["numeric_stats_dev"]  = self._numeric_stats(dev_df)
            logger.debug(
                f"  [data_quality] numeric_stats: "
                f"{len(context['metrics']['numeric_stats_prod'])} PROD numeric cols"
            )

        context["metrics"]["data_quality_report"] = self._build_quality_report(context)
        logger.info(
            f"  [data_quality] Report built: "
            f"{len(context['metrics']['data_quality_report'])} col rows"
        )
        return context
