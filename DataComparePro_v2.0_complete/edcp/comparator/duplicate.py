# -*- coding: utf-8 -*-
"""
data_compare.comparator.duplicate
───────────────────────────────────
Duplicate key detection and deterministic sequence-index alignment.

Features:
  [F-16]  Per-side duplicate detection by key group
  [F-17]  Deterministic _SEQ_ counter within each key group (mergesort stable)
  [F-18]  Cross-matched duplicate summary for the Duplicates Records sheet
  [F-19]  Duplicate counts surfaced in summary dict
"""

from typing import List

import pandas as pd

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


def detect_duplicates(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    """
    Return a DataFrame of key groups that appear more than once in *df*.

    Columns in the result: *keys* + ["Count"].

    Returns an empty DataFrame with sentinel column "_NO_KEYS_" when *keys*
    is empty, and an empty DataFrame with *keys* + ["Count"] columns when any
    key is missing from *df*.
    """
    if not keys:
        return pd.DataFrame(columns=["_NO_KEYS_"])

    missing = [k for k in keys if k not in df.columns]
    if missing:
        logger.debug(f"  detect_duplicates: keys not in df: {missing}")
        return pd.DataFrame(columns=keys + ["Count"])

    grp  = df.groupby(keys, dropna=False).size().reset_index(name="Count")
    dups = grp[grp["Count"] > 1].copy()
    logger.debug(f"  Duplicates found: {len(dups)} key group(s)")
    return dups


def add_sequence_for_duplicates(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    """
    Add a deterministic _SEQ_ sequence counter within each key group.

    Algorithm:
    1. Build "_ORDER_JOIN_" by joining all non-key columns as strings with "|".
    2. Stable-sort by keys + "_ORDER_JOIN_".
    3. groupby(keys).cumcount() → _SEQ_.
    4. Drop the helper column.

    This produces a stable, deterministic ordering so that duplicate records
    at the same key in prod and dev can be aligned consistently even when the
    source file row order differs.
    """
    df = df.copy()
    if not keys:
        df["_SEQ_"] = range(len(df))
        return df

    non_keys = [c for c in df.columns if c not in keys]
    if non_keys:
        df["_ORDER_JOIN_"] = df[non_keys].astype(str).agg("|".join, axis=1)
        sort_cols = keys + ["_ORDER_JOIN_"]
    else:
        sort_cols = keys

    df = df.sort_values(sort_cols, kind="mergesort")
    df["_SEQ_"] = df.groupby(keys, sort=False).cumcount()

    if "_ORDER_JOIN_" in df.columns:
        df = df.drop(columns=["_ORDER_JOIN_"])
    return df


def build_duplicates_summary(
    dup_prod_df: pd.DataFrame,
    dup_dev_df: pd.DataFrame,
    existing_keys: List[str],
) -> pd.DataFrame:
    """
    Cross-match duplicates found in PROD and DEV into a single summary frame.

    Columns: *existing_keys* + ["Duplicate Present in Prod", "Duplicate Present in Dev"]

    Returns a sentinel DataFrame with column "_NO_KEYS_" if *existing_keys* is empty.
    """
    if not existing_keys:
        return pd.DataFrame(columns=["_NO_KEYS_"])

    left  = dup_prod_df[existing_keys].copy(); left["In_Prod"] = True
    right = dup_dev_df[existing_keys].copy();  right["In_Dev"]  = True

    merged = pd.merge(left, right, on=existing_keys, how="outer")
    merged["In_Prod"] = merged["In_Prod"].fillna(False).astype(bool)
    merged["In_Dev"]  = merged["In_Dev"].fillna(False).astype(bool)

    summary = merged[existing_keys + ["In_Prod", "In_Dev"]].rename(
        columns={
            "In_Prod": "Duplicate Present in Prod",
            "In_Dev":  "Duplicate Present in Dev",
        }
    )
    return summary
