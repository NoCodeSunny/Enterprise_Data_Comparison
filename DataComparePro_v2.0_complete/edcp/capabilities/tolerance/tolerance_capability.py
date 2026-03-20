# -*- coding: utf-8 -*-
"""
data_compare.capabilities.tolerance.tolerance_capability
──────────────────────────────────────────────────────────
ToleranceCapability

Reads the global tolerance map from context['tol_map'] and resolves the
per-file-pair active tolerance rules, writing them to context['tol_for_pair'].

Requires context keys
---------------------
    tol_map       : dict – full {(prod, dev, field): decimals} map
    prod_name     : str
    dev_name      : str
    prod_df       : pd.DataFrame | None  (used to check column presence)
    dev_df        : pd.DataFrame | None
    ignore_set    : set

Writes context keys
-------------------
    tol_for_pair  : dict – {field: decimals} active for this pair

Can run independently of ComparisonCapability.
"""

from __future__ import annotations

from typing import Any, Dict

from data_compare.capabilities.base import BaseCapability
from data_compare.comparator.tolerance import resolve_pair_tolerance
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class ToleranceCapability(BaseCapability):
    NAME = "tolerance"

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        tol_map    = context.get("tol_map", {})
        prod_name  = context.get("prod_name", "")
        dev_name   = context.get("dev_name",  "")
        ignore_set = context.get("ignore_set", set())

        if not tol_map:
            logger.debug("  [tolerance] No tolerance rules defined – skipping resolution")
            context["tol_for_pair"] = {}
            return context

        # If prod_df/dev_df are already loaded, validate column presence.
        # If not yet loaded (tolerance runs before comparison), store all rules
        # for this pair; ComparisonCapability will validate column presence itself.
        prod_df = context.get("prod_df")
        dev_df  = context.get("dev_df")

        if prod_df is not None and dev_df is not None:
            prod_cols = list(prod_df.columns)
            dev_cols  = list(dev_df.columns)
            tol_for_pair = resolve_pair_tolerance(
                tol_map, prod_name, dev_name, prod_cols, dev_cols, ignore_set
            )
        else:
            # Data not loaded yet – store all rules for this pair unvalidated.
            # ComparisonCapability will validate column presence when it runs.
            tol_for_pair = {
                col: dec
                for (pf, df_name, col), dec in tol_map.items()
                if pf == prod_name and df_name == dev_name
            }
            logger.debug(
                f"  [tolerance] Data not yet loaded – storing {len(tol_for_pair)} "
                f"raw rule(s) for ({prod_name}, {dev_name}); "
                f"ComparisonCapability will validate column presence."
            )

        context["tol_for_pair"] = tol_for_pair
        logger.info(
            f"  [tolerance] {len(tol_for_pair)} rule(s) resolved for "
            f"({prod_name}, {dev_name})"
        )
        return context
