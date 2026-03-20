# -*- coding: utf-8 -*-
"""
data_compare.comparator.tolerance
───────────────────────────────────
Numerical tolerance: parsing the Tolerance sheet and applying decimal-place
rounding before equality comparison.

Features:
  [F-21]  Per (ProdFile, DevFile, Field) tolerance as DECIMAL PLACES
  [F-22]  Only listed fields use rounding; all others use strict equality
  [F-23]  Misconfigurations surfaced as warnings
  [F-24]  Every accepted/skipped tolerance row is logged explicitly
"""

import math
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, Optional, Tuple

import pandas as pd

from data_compare.utils.helpers import normalized_colnames_mapping
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# Type alias for the tolerance map
ToleranceMap = Dict[Tuple[str, str, str], int]


def normalize_numeric_with_tolerance(val: str, decimals: int) -> str:
    """
    Round *val* to *decimals* decimal places using ROUND_HALF_UP.

    Returns the rounded value formatted as a fixed-point string,
    e.g. '0000003485.00000000' → '3485.00' when decimals=2.

    Non-numeric values (after stripping commas) are returned unchanged.
    Empty strings are returned as-is.
    """
    if isinstance(val, float) and math.isnan(val):
        return ""
    s = str(val).strip()
    if not s:
        return s
    s_clean = s.replace(",", "").strip()
    try:
        q = Decimal(s_clean)
    except InvalidOperation:
        return s
    ctx = Decimal(10) ** (-decimals)
    rounded = q.quantize(ctx, rounding=ROUND_HALF_UP)
    return f"{rounded:.{decimals}f}"


def build_tolerance_map(tol_df: pd.DataFrame) -> ToleranceMap:
    """
    Parse the Numerical Tolerance sheet into a lookup dictionary.

    Expected columns (matched case-insensitively, whitespace-collapsed):
        "Prod File Name", "Dev File Name", "Field Name",
        "Tolerance Level ( in decimal )"

    Returns
    -------
    ToleranceMap
        {(ProdFileName, DevFileName, FieldName): decimal_places}

    Every row accepted or skipped is logged.  The caller receives an empty
    dict if the sheet is absent or structurally invalid.
    """
    out: ToleranceMap = {}
    if tol_df is None or tol_df.empty:
        logger.info("  No tolerance sheet found or sheet is empty.")
        return out

    cols = normalized_colnames_mapping(tol_df)
    need = [
        "prod file name",
        "dev file name",
        "field name",
        "tolerance level ( in decimal )",
    ]
    missing = [n for n in need if n not in cols]
    if missing:
        logger.warning(
            f"  Tolerance sheet missing required columns (normalised): {missing}  "
            f"→ tolerance disabled for this run"
        )
        return out

    accepted = skipped = 0
    for idx, r in tol_df.iterrows():
        p = str(r[cols["prod file name"]]).strip()
        d = str(r[cols["dev file name"]]).strip()
        f = str(r[cols["field name"]]).strip()
        t = r[cols["tolerance level ( in decimal )"]]

        # Skip blank / null rows
        if not p or not d or not f or pd.isna(t) or str(t).strip() in ("", "nan"):
            skipped += 1
            logger.debug(f"  Tolerance row {idx}: skipped (blank or null field)")
            continue

        try:
            decimals = int(str(t).strip())
            out[(p, d, f)] = decimals
            logger.debug(
                f"  Tolerance row {idx}: ({p}, {d}, {f}) → {decimals} decimal(s)"
            )
            accepted += 1
        except (ValueError, TypeError):
            logger.warning(
                f"  Tolerance row {idx}: invalid decimal value '{t}' "
                f"for ({p},{d},{f}) – skipped"
            )
            skipped += 1

    logger.info(
        f"  Tolerance map built: {accepted} rule(s) accepted, "
        f"{skipped} row(s) skipped"
    )
    return out


def resolve_pair_tolerance(
    tol_map: ToleranceMap,
    prod_name: str,
    dev_name: str,
    prod_columns: list,
    dev_columns: list,
    ignore_set: set,
) -> Dict[str, int]:
    """
    Filter *tol_map* to only the rules that apply to this specific file pair
    and where the field exists in both files and is not in *ignore_set*.

    Logs a warning for each rule that is defined but cannot be applied.

    Returns
    -------
    dict
        {field_name: decimal_places} for active tolerance rules.
    """
    raw = {
        col: dec
        for (pf, df_name, col), dec in tol_map.items()
        if pf == prod_name and df_name == dev_name
    }
    active: Dict[str, int] = {}
    for col, dec in raw.items():
        in_prod  = col in prod_columns
        in_dev   = col in dev_columns
        ignored  = col in ignore_set
        if in_prod and in_dev and not ignored:
            active[col] = dec
            logger.debug(f"  Tolerance active: '{col}' → {dec} decimal(s)")
        else:
            if ignored:
                reason = "field is in IgnoreFields list"
            elif not in_prod:
                reason = "field absent from PROD"
            elif not in_dev:
                reason = "field absent from DEV"
            else:
                reason = "unknown"
            logger.warning(
                f"  Tolerance for '{col}' skipped: {reason}"
            )
    return active
