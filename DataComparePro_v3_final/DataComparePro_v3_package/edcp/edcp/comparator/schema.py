# -*- coding: utf-8 -*-
"""
data_compare.comparator.schema
────────────────────────────────
Schema difference detection between PROD and DEV column sets,
and HTML rendering helpers for the summary email.

Features:
  [F-36]  Field + Side + Count (non-empty cells) in Schema Differences sheet
  [F-44]  Schema difference mini-table for HTML email
"""

import html as _html_module
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


def build_schema_diff_dataframe(
    prod_cols: List[str],
    dev_cols: List[str],
    exclude: set,
) -> pd.DataFrame:
    """
    Compute schema differences between PROD and DEV column sets.

    Columns present only in PROD  → Side = "Missing in Dev"
    Columns present only in DEV   → Side = "Extra in Dev"
    Columns in *exclude* are ignored (keys, ignored fields, alignment helpers).

    Returns a DataFrame with columns ["Field", "Side"].
    Returns an empty DataFrame with those columns when no differences exist.
    """
    pset = {c for c in prod_cols if c not in exclude}
    dset = {c for c in dev_cols  if c not in exclude}

    only_prod = sorted(pset - dset)
    only_dev  = sorted(dset - pset)

    rows  = [{"Field": c, "Side": "Missing in Dev"} for c in only_prod]
    rows += [{"Field": c, "Side": "Extra in Dev"}   for c in only_dev]

    df = (
        pd.DataFrame(rows, columns=["Field", "Side"])
        if rows
        else pd.DataFrame(columns=["Field", "Side"])
    )

    if only_prod or only_dev:
        logger.info(
            f"  Schema: Missing-in-Dev={len(only_prod)}, "
            f"Extra-in-Dev={len(only_dev)}"
        )
        if only_prod:
            logger.debug(f"  Missing in Dev: {only_prod}")
        if only_dev:
            logger.debug(f"  Extra in Dev:   {only_dev}")
    else:
        logger.info("  Schema: identical (no field differences)")

    return df


def annotate_schema_counts(
    schema_diff_df: pd.DataFrame,
    prod_common: pd.DataFrame,
    dev_common: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add a "Count" column to *schema_diff_df* representing the number of
    non-empty cells in the side where each field exists.

    "Extra in Dev"    → counts non-empty values in dev_common[field]
    "Missing in Dev"  → counts non-empty values in prod_common[field]
    """
    if schema_diff_df.empty:
        return schema_diff_df

    count_vals = []
    for _, r in schema_diff_df.iterrows():
        field = r["Field"]
        side  = r["Side"]
        src   = dev_common if side == "Extra in Dev" else prod_common
        if field in src.columns:
            cnt = int(
                src[field]
                .replace("", pd.NA)
                .dropna()
                .shape[0]
            )
        else:
            cnt = 0
        count_vals.append(cnt)

    result = schema_diff_df.copy()
    result["Count"] = count_vals
    return result


# ── HTML helpers ──────────────────────────────────────────────────────────────

def read_schema_items_from_report(
    xlsx_path: Path,
) -> List[Tuple[str, str, Optional[int]]]:
    """
    Read schema difference items from the "Schema Differences" sheet of a
    previously written comparison workbook.

    Returns a list of (field, side, count_or_None) tuples.
    Returns an empty list if the file does not exist or the sheet is absent.
    """
    items: List[Tuple[str, str, Optional[int]]] = []
    try:
        if not xlsx_path or not Path(xlsx_path).exists():
            return items
        with pd.ExcelFile(xlsx_path, engine="openpyxl") as xl:
            sheet = next(
                (
                    n for n in xl.sheet_names
                    if n.strip().lower() in (
                        "schema differences", "schema_differences"
                    )
                ),
                None,
            )
            if not sheet:
                return items
            df = xl.parse(sheet)

        if df is None or df.empty:
            return items

        cols = {str(c).strip().lower(): c for c in df.columns}
        fld  = cols.get("field")
        sid  = cols.get("side")
        cnt  = cols.get("count") or cols.get("counts") or cols.get("cnt")

        if not fld or not sid:
            return items

        for _, r in df.iterrows():
            f = str(r[fld]).strip()
            s = str(r[sid]).strip()
            n: Optional[int] = None
            if cnt and not pd.isna(r.get(cnt, None)):
                try:
                    n = int(float(str(r[cnt]).replace(",", "")))
                except Exception:
                    pass
            items.append((f, s, n))
    except Exception:
        pass
    return items


def render_schema_items_html(
    items: List[Tuple[str, str, Optional[int]]],
    limit: int = 8,
) -> str:
    """
    Render schema difference items as an Outlook-safe inline HTML mini-table.

    Shows up to *limit* items; appends an overflow indicator if more exist.
    Returns a plain escaped string when *items* is empty.
    """
    if not items:
        return _html_module.escape("None — schema matches")

    rows = []
    for field, side, count in items[:limit]:
        right = _html_module.escape(side) + (
            f" ({count:,})" if isinstance(count, int) else ""
        )
        rows.append(
            f"<tr>"
            f"<td style='padding:0 6px 0 0;white-space:nowrap;"
            f"font-family:Consolas,monospace'>"
            f"{_html_module.escape(field)}</td>"
            f"<td style='padding:0'>&mdash; {right}</td>"
            f"</tr>"
        )
    if len(items) > limit:
        rows.append(
            f"<tr><td colspan='2' style='color:#666'>"
            f"… +{len(items) - limit} more</td></tr>"
        )

    return (
        "<table role='presentation' "
        "style='border-collapse:collapse;width:100%'>"
        + "".join(rows)
        + "</table>"
    )
