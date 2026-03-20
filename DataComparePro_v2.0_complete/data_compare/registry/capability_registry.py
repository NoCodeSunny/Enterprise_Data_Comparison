# -*- coding: utf-8 -*-
"""
data_compare.registry.capability_registry
───────────────────────────────────────────
Capability Registry  (v3.1 + Parquet)

Execution order:
    parquet → tolerance → comparison → duplicate → schema →
    data_quality → audit → alerts → plugins

Parquet runs FIRST so it can:
  - detect parquet inputs
  - force use_spark=True
  - pre-load data into context (prod_df/dev_df)
  - store optimization plan for SparkEngine

All other capabilities are unchanged.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from data_compare.capabilities.base import BaseCapability
from data_compare.capabilities.alerts.alerts_capability import AlertsCapability
from data_compare.capabilities.audit.audit_capability import AuditCapability
from data_compare.capabilities.comparison.comparison_capability import ComparisonCapability
from data_compare.capabilities.data_quality.data_quality_capability import DataQualityCapability
from data_compare.capabilities.duplicate.duplicate_capability import DuplicateCapability
from data_compare.capabilities.parquet.parquet_capability import ParquetCapability
from data_compare.capabilities.plugins.plugin_capability import PluginCapability
from data_compare.capabilities.schema.schema_capability import SchemaCapability
from data_compare.capabilities.tolerance.tolerance_capability import ToleranceCapability
from data_compare.utils.logger import get_logger, banner

logger = get_logger(__name__)

# ── Execution order ────────────────────────────────────────────────────────────
# parquet  – detect + validate + pre-load parquet inputs; forces use_spark=True
# tolerance – resolve tol_for_pair (needs only prod/dev names)
# comparison – load data (skips load if parquet already populated prod_df/dev_df)
# duplicate – enrich duplicates_summary
# schema    – annotate with prod_common/dev_common counts
# data_quality – read prod_df/dev_df
# audit     – materialise event trail
# alerts    – evaluate final metrics
# plugins   – user code last
_DEFAULT_ORDER = [
    "parquet",
    "tolerance",
    "comparison",
    "duplicate",
    "schema",
    "data_quality",
    "audit",
    "alerts",
    "plugins",
]


class CapabilityRegistry:
    """
    Registry of all capabilities available to the framework.

    Built-in capabilities are registered automatically at construction.
    Third-party capabilities can be added via register().
    """

    def __init__(self) -> None:
        self._builtin: Dict[str, BaseCapability] = {}
        self._custom:  Dict[str, BaseCapability] = {}
        self._register_builtins()

    # ── registration ──────────────────────────────────────────────────────────

    def _register_builtins(self) -> None:
        builtins: List[BaseCapability] = [
            ParquetCapability(),
            ToleranceCapability(),
            SchemaCapability(),
            DuplicateCapability(),
            ComparisonCapability(),
            DataQualityCapability(),
            AuditCapability(),
            AlertsCapability(),
            PluginCapability(),
        ]
        for cap in builtins:
            self._builtin[cap.NAME] = cap
        logger.debug(
            f"  [registry] Built-in capabilities registered: "
            f"{list(self._builtin.keys())}"
        )

    def register(self, capability: BaseCapability) -> None:
        if not isinstance(capability, BaseCapability):
            raise TypeError(f"Expected BaseCapability subclass, got {type(capability)}")
        if capability.NAME in self._builtin:
            logger.warning(f"  [registry] Replacing built-in '{capability.NAME}'")
            self._builtin[capability.NAME] = capability
        else:
            self._custom[capability.NAME] = capability
            logger.info(f"  [registry] Custom capability '{capability.NAME}' registered")

    def unregister(self, name: str) -> None:
        removed = False
        for store in (self._builtin, self._custom):
            if name in store:
                del store[name]; removed = True
        if removed:
            logger.info(f"  [registry] Capability '{name}' unregistered")
        else:
            logger.warning(f"  [registry] Capability '{name}' not found")

    def list_capabilities(self) -> List[str]:
        ordered  = [n for n in _DEFAULT_ORDER if n in self._builtin]
        extras   = [n for n in self._builtin   if n not in _DEFAULT_ORDER]
        customs  = list(self._custom.keys())
        return ordered + extras + customs

    def get(self, name: str) -> Optional[BaseCapability]:
        return self._builtin.get(name) or self._custom.get(name)

    # ── pipeline execution ────────────────────────────────────────────────────

    def run_pipeline(
        self,
        context: Dict[str, Any],
        override_order: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        execution_order = override_order if override_order else self.list_capabilities()

        enabled_caps  = [
            n for n in execution_order
            if context.get("config", {}).get("capabilities", {}).get(n, True)
        ]
        disabled_caps = [n for n in execution_order if n not in enabled_caps]

        # parquet is opt-in (default disabled) – only enable via auto-detect or config
        # The is_enabled() override on ParquetCapability handles auto-detect.

        logger.info(
            f"  [registry] Pipeline: {len(enabled_caps)} enabled, "
            f"{len(disabled_caps)} disabled"
        )
        if disabled_caps:
            logger.info(f"  [registry] Disabled: {disabled_caps}")

        for name in execution_order:
            cap = self.get(name)
            if cap is None:
                logger.warning(f"  [registry] '{name}' listed but not registered – skipped")
                continue
            context = cap.run(context)

        return context


# ── module-level singleton ────────────────────────────────────────────────────
default_registry = CapabilityRegistry()
