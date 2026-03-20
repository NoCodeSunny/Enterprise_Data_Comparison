# -*- coding: utf-8 -*-
"""
data_compare.capabilities.duplicate.duplicate_capability
──────────────────────────────────────────────────────────
DuplicateCapability

Detects duplicate key groups in PROD and DEV, builds the cross-matched
summary, and decides whether _SEQ_ alignment is needed.

Requires context keys
---------------------
    prod_df       : pd.DataFrame
    dev_df        : pd.DataFrame
    existing_keys : list

Writes context keys
-------------------
    dup_prod_df        : pd.DataFrame
    dup_dev_df         : pd.DataFrame
    duplicates_summary : pd.DataFrame
    dup_count_prod     : int
    dup_count_dev      : int
    using_seq          : bool
    prod_aligned       : pd.DataFrame
    dev_aligned        : pd.DataFrame
    align_keys         : list

Can run independently of ComparisonCapability (existing_keys must be populated
first, either by ComparisonCapability or set manually in context).
"""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from data_compare.capabilities.base import BaseCapability
from data_compare.comparator.duplicate import (
    add_sequence_for_duplicates,
    build_duplicates_summary,
    detect_duplicates,
)
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class DuplicateCapability(BaseCapability):
    NAME = "duplicate"

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        When running AFTER ComparisonCapability (default order):
          - prod_df, dev_df, existing_keys are already in context.
          - prod_aligned/dev_aligned are already built.
          - We enrich duplicates_summary for the report and update counts.

        When running standalone (existing_keys must be set manually):
          - We build full alignment including _SEQ_ as before.
        """
        prod_df       = context.get("prod_df")
        dev_df        = context.get("dev_df")
        existing_keys = context.get("existing_keys", [])

        if prod_df is None or dev_df is None:
            logger.warning("  [duplicate] prod_df or dev_df not loaded – skipping")
            return context

        # ── detect duplicates ─────────────────────────────────────────────────
        dup_prod_df = detect_duplicates(prod_df, existing_keys)
        dup_dev_df  = detect_duplicates(dev_df,  existing_keys)

        if not existing_keys:
            duplicates_summary = pd.DataFrame(columns=["_NO_KEYS_"])
            dup_count_prod = dup_count_dev = 0
        else:
            dup_count_prod = len(dup_prod_df)
            dup_count_dev  = len(dup_dev_df)
            logger.info(
                f"  [duplicate] PROD duplicate groups: {dup_count_prod:,}  "
                f"DEV duplicate groups: {dup_count_dev:,}"
            )
            duplicates_summary = build_duplicates_summary(
                dup_prod_df, dup_dev_df, existing_keys
            )

        context["dup_prod_df"]        = dup_prod_df
        context["dup_dev_df"]         = dup_dev_df
        context["duplicates_summary"] = duplicates_summary
        context["dup_count_prod"]     = dup_count_prod
        context["dup_count_dev"]      = dup_count_dev

        # ── alignment (only build if not already set by ComparisonCapability) ─
        if context.get("prod_aligned") is not None:
            # ComparisonCapability already built alignment; just update counts.
            logger.debug(
                "  [duplicate] Alignment already built by ComparisonCapability – "
                "skipping re-alignment; counts updated."
            )
            return context

        # Standalone mode: build alignment from scratch
        using_seq  = False
        align_keys = existing_keys[:]

        if existing_keys:
            if dup_count_prod > 0 or dup_count_dev > 0:
                using_seq = True
                logger.info(
                    "  [duplicate] Standalone: duplicates present – activating _SEQ_"
                )
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

        context["using_seq"]    = using_seq
        context["align_keys"]   = align_keys
        context["prod_aligned"] = prod_aligned
        context["dev_aligned"]  = dev_aligned

        return context
