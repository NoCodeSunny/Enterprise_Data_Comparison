# -*- coding: utf-8 -*-
"""
data_compare.capabilities.base
────────────────────────────────
Abstract base class for all capabilities.

Every capability in the framework must:
  1. Subclass BaseCapability.
  2. Implement execute(context) → context.
  3. Declare its name via the NAME class attribute.
  4. Be stateless across calls (all state lives in context).

Capabilities are instantiated once and reused across batches.  They must
never store batch-specific state as instance attributes.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Dict

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class BaseCapability(ABC):
    """
    Abstract base class for all pipeline capabilities.

    Subclasses must define:
        NAME : str   – unique identifier used in config and registry.

    The execute() method receives the shared context dict, performs its work,
    updates the relevant keys, and returns the (mutated) context.  It must
    not raise exceptions silently; it should let them propagate so the
    orchestrator can catch and log them per-batch.
    """

    #: Unique string identifier.  Override in every subclass.
    NAME: str = "base"

    def __repr__(self) -> str:
        return f"<Capability: {self.NAME}>"

    def is_enabled(self, context: Dict[str, Any]) -> bool:
        """
        Check whether this capability is enabled in the current context config.

        Returns True if the capability name is not present in the config
        (safe default: unknown capabilities run unless explicitly disabled).
        """
        caps = context.get("config", {}).get("capabilities", {})
        return caps.get(self.NAME, True)

    def _record_timing(self, context: Dict[str, Any], elapsed: float) -> None:
        """Write elapsed time into the audit section of context."""
        try:
            context["audit"]["capability_times"][self.NAME] = round(elapsed, 4)
        except (KeyError, TypeError):
            pass

    def _emit_audit_event(
        self,
        context: Dict[str, Any],
        event: str,
        detail: str = "",
    ) -> None:
        """Append an audit event to context['audit']['events']."""
        from datetime import datetime
        try:
            context["audit"]["events"].append({
                "timestamp": datetime.now().isoformat(),
                "capability": self.NAME,
                "event":      event,
                "detail":     detail,
            })
        except (KeyError, TypeError):
            pass

    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute this capability against the shared context.

        Parameters
        ----------
        context : dict
            The shared run context (see data_compare.context.run_context).

        Returns
        -------
        dict
            The same context dict with this capability's outputs written in.
        """

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Public entry point called by the registry / orchestrator.

        Checks enabled flag, times the call, records audit event, delegates
        to execute().  Subclasses should NOT override this method.
        """
        if not self.is_enabled(context):
            logger.debug(f"  [{self.NAME}] SKIPPED (disabled in config)")
            self._emit_audit_event(context, "SKIPPED", "disabled in config")
            return context

        logger.info(f"  [{self.NAME}] Starting …")
        self._emit_audit_event(context, "START")
        t0 = time.perf_counter()
        try:
            context = self.execute(context)
            elapsed = time.perf_counter() - t0
            self._record_timing(context, elapsed)
            logger.info(f"  [{self.NAME}] Done  [{elapsed:.2f}s]")
            self._emit_audit_event(context, "DONE", f"elapsed={elapsed:.2f}s")
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            self._record_timing(context, elapsed)
            logger.error(f"  [{self.NAME}] FAILED: {exc}", exc_info=True)
            self._emit_audit_event(context, "FAILED", str(exc))
            raise
        return context
