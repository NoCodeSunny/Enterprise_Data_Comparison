# -*- coding: utf-8 -*-
"""
data_compare.capabilities.schema.schema_capability
────────────────────────────────────────────────────
SchemaCapability

Detects schema differences between PROD and DEV column sets, then annotates
each difference with the count of non-empty cells on the side where the field
exists.

Requires context keys
---------------------
    prod_df       : pd.DataFrame
    dev_df        : pd.DataFrame
    existing_keys : list
    ignore_set    : set
    prod_common   : pd.DataFrame | None  (used for count annotation; optional)
    dev_common    : pd.DataFrame | None

Writes context keys
-------------------
    schema_diff_df : pd.DataFrame
        Columns: Field | Side | Count

Runs independently of all other capabilities.
"""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from data_compare.capabilities.base import BaseCapability
from data_compare.comparator.schema import (
    annotate_schema_counts,
    build_schema_diff_dataframe,
)
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class SchemaCapability(BaseCapability):
    NAME = "schema"

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        When running AFTER ComparisonCapability (default order):
          - prod_df, dev_df, prod_common, dev_common, existing_keys, ignore_set
            are all already populated in context.

        When running standalone:
          - prod_df and dev_df must be set manually.
          - existing_keys and ignore_set should be set (default to empty).
        """
        prod_df       = context.get("prod_df")
        dev_df        = context.get("dev_df")
        existing_keys = context.get("existing_keys", [])
        ignore_set    = context.get("ignore_set", set())

        if prod_df is None or dev_df is None:
            logger.warning("  [schema] prod_df or dev_df not loaded – skipping")
            return context

        exclude_for_schema = set(existing_keys) | ignore_set

        schema_diff_df = build_schema_diff_dataframe(
            list(prod_df.columns),
            list(dev_df.columns),
            exclude_for_schema,
        )

        # Annotate with non-empty cell counts.
        # prod_common/dev_common are set by ComparisonCapability.
        # In standalone mode they may be None; annotation is skipped gracefully.
        prod_common = context.get("prod_common")
        dev_common  = context.get("dev_common")
        if prod_common is not None and dev_common is not None:
            schema_diff_df = annotate_schema_counts(
                schema_diff_df, prod_common, dev_common
            )
            logger.debug("  [schema] Schema diff annotated with non-empty cell counts")
        else:
            logger.debug(
                "  [schema] prod_common/dev_common not available – "
                "Count column skipped (run after ComparisonCapability for full output)"
            )

        context["schema_diff_df"] = schema_diff_df

        missing_count = int(
            (schema_diff_df["Side"] == "Missing in Dev").sum()
        ) if not schema_diff_df.empty else 0
        extra_count = int(
            (schema_diff_df["Side"] == "Extra in Dev").sum()
        ) if not schema_diff_df.empty else 0

        logger.info(
            f"  [schema] Missing-in-Dev: {missing_count}  "
            f"Extra-in-Dev: {extra_count}"
        )
        return context
