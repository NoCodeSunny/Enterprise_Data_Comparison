# -*- coding: utf-8 -*-
"""
data_compare.utils.helpers
──────────────────────────
General-purpose utility functions used across the package:
  - Directory creation
  - DataFrame whitespace / NBSP normalisation
  - Key column string normalisation
  - Column-name lower-case mapping helper
  - Filename sanitisation and collision-safe path generation
"""

import re
import uuid
from pathlib import Path
from typing import Dict, List

import pandas as pd

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


# ── directory helpers ─────────────────────────────────────────────────────────

def ensure_dir(path: Path) -> None:
    """Create *path* (and any missing parents) if it does not already exist."""
    path.mkdir(parents=True, exist_ok=True)


# ── DataFrame normalisation ───────────────────────────────────────────────────

def trim_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of *df* where:
      - NaN values are replaced with empty string
      - Non-breaking spaces (U+00A0) are replaced with regular space
      - Leading/trailing whitespace is stripped from every cell and column name
    """
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    df = df.copy()
    df = df.fillna("")
    for c in df.columns:
        df[c] = (
            df[c]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
    return df


def normalize_keys(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    """
    Normalise key columns to clean strings ONLY.
    No numeric coercion – '001' and '1' remain distinct.

    Strips whitespace and U+00A0 from each key column.
    Columns listed in *keys* that are absent from *df* are silently skipped.
    """
    df = df.copy()
    for k in keys:
        if k not in df.columns:
            continue
        df[k] = (
            df[k]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
    return df


# ── column-name mapping ───────────────────────────────────────────────────────

def normalized_colnames_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """
    Return {normalised_name: original_name} for all columns in *df*.

    Normalisation: lowercase + collapse any run of whitespace to a single space.
    Used throughout the package for case/whitespace-insensitive column lookup.
    """
    return {
        re.sub(r"\s+", " ", str(c).strip().lower()): c
        for c in df.columns
    }


# ── filename safety ───────────────────────────────────────────────────────────

def sanitise_filename(name: str) -> str:
    """
    Remove characters that would allow path traversal or cause OS-level issues.

    Strips: < > : " / \\ | ? * and ASCII control characters 0x00-0x1f.
    Also strips leading/trailing dots and spaces.
    Returns 'CompareRecords' if the result is empty.
    """
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    safe = safe.strip(". ")
    return safe or "CompareRecords"


def safe_output_path(
    report_root: Path,
    result_name: str,
    suffix: str = "_RecordComparison.xlsx",
) -> Path:
    """
    Return a collision-free output path.

    If *report_root / sanitised(result_name) + suffix* already exists,
    a UUID4 hex fragment is appended before the suffix so the new file
    never overwrites an existing report.
    """
    safe = sanitise_filename(result_name)
    candidate = report_root / f"{safe}{suffix}"
    if candidate.exists():
        uid = uuid.uuid4().hex[:8]
        candidate = report_root / f"{safe}_{uid}{suffix}"
        logger.debug(f"  Output collision resolved → {candidate.name}")
    return candidate
