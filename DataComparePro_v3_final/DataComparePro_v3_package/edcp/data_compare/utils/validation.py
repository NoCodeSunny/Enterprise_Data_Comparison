# -*- coding: utf-8 -*-
"""
data_compare.utils.validation
──────────────────────────────
Validates the parsed InputSheet configuration DataFrame before the
orchestrator begins dispatching batches.

Raises ValueError with a descriptive message when required columns are absent
so the caller can surface a clean error instead of a cryptic KeyError deep in
the pipeline.
"""

import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

from data_compare.utils.helpers import normalized_colnames_mapping
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# ── required column names (normalised) ───────────────────────────────────────
_REQUIRED_COLS = [
    "run",
    "files path",
    "prod file name",
    "dev file name",
    "result report name",
]

# ── optional column names (normalised) ───────────────────────────────────────
_OPTIONAL_COLS = [
    "include pass report",
    "ignorefields",
    "sheetname",
]


def validate_config_sheet(cfg_df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    Validate that *cfg_df* (the parsed InputSheet) contains all required columns.

    Returns
    -------
    colmap : dict
        Mapping of normalised column names → actual column names in *cfg_df*,
        for all required AND optional columns found.

    Raises
    ------
    ValueError
        If any required column is missing.
    """
    colmap = normalized_colnames_mapping(cfg_df)

    missing_required = [c for c in _REQUIRED_COLS if c not in colmap]
    if missing_required:
        found = list(cfg_df.columns)
        msg = (
            f"InputSheet is missing required columns: {missing_required}\n"
            f"  Columns found in sheet: {found}\n"
            f"  Required (case-insensitive): {_REQUIRED_COLS}"
        )
        logger.error(msg)
        raise ValueError(msg)

    for col in _OPTIONAL_COLS:
        if col not in colmap:
            logger.debug(f"  Optional column '{col}' not present in InputSheet – skipped")

    logger.info(f"  Config sheet validated: {len(_REQUIRED_COLS)} required columns all present")
    return colmap


def extract_key_columns(cfg_df: pd.DataFrame) -> List[str]:
    """
    Discover all Key ColumnN columns in *cfg_df* and return them sorted by N.

    Matches: 'Key Column1', 'key column2', 'KeyColumn10', etc.
    """
    key_cols = [
        c for c in cfg_df.columns
        if re.match(r"(?i)^key\s*column\d+", str(c).strip())
    ]
    key_cols.sort(
        key=lambda x: int(re.findall(r"\d+", str(x))[0])
        if re.findall(r"\d+", str(x)) else 0
    )
    logger.info(f"  Key column headers detected: {key_cols}")
    return key_cols


def parse_batch_row(
    row: pd.Series,
    colmap: Dict[str, Optional[str]],
    key_cols: List[str],
) -> Tuple[str, str, str, str, int, List[str], bool, List[str]]:
    """
    Extract and validate all fields from a single active batch row.

    Returns
    -------
    (base_path_str, prod_name, dev_name, result_name,
     sheet_name, keys, include_pass_report, ignore_fields)
    """
    import math

    col_path   = colmap.get("files path")
    col_prod   = colmap.get("prod file name")
    col_dev    = colmap.get("dev file name")
    col_rname  = colmap.get("result report name")
    col_ipass  = colmap.get("include pass report")
    col_ignore = colmap.get("ignorefields")
    col_sheet  = colmap.get("sheetname")

    base_path_str = str(row.get(col_path, "")).strip()
    prod_name     = str(row.get(col_prod, "")).strip()
    dev_name      = str(row.get(col_dev, "")).strip()
    result_name   = str(row.get(col_rname, "")).strip() or "CompareRecords"

    # XLSX sheet name/index
    sheet_name: int = 0
    if col_sheet and pd.notna(row.get(col_sheet)):
        raw_sheet = str(row[col_sheet]).strip()
        if raw_sheet and raw_sheet.lower() not in ("nan", "none", ""):
            try:
                sheet_name = int(raw_sheet)
            except ValueError:
                sheet_name = raw_sheet  # type: ignore[assignment]

    # Keys
    keys: List[str] = []
    for kcol in key_cols:
        v = row.get(kcol)
        if isinstance(v, float) and math.isnan(v):
            continue
        val = str(v).strip()
        if val and val.lower() not in ("nan", "none", ""):
            keys.append(val)

    # Include pass report flag
    include_pass_report = bool(
        col_ipass
        and str(row.get(col_ipass, "")).strip().lower() in ("yes", "y", "true", "1")
    )

    # Ignore fields
    ignore_fields: List[str] = []
    if col_ignore:
        raw = row.get(col_ignore)
        if raw is not None and not (isinstance(raw, float) and math.isnan(raw)):
            ignore_fields = [f.strip() for f in str(raw).split(",") if f.strip()]

    return (
        base_path_str,
        prod_name,
        dev_name,
        result_name,
        sheet_name,   # type: ignore[return-value]
        keys,
        include_pass_report,
        ignore_fields,
    )
