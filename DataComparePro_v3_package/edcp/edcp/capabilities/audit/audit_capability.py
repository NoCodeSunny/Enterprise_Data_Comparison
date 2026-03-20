# -*- coding: utf-8 -*-
"""
data_compare.capabilities.audit.audit_capability
──────────────────────────────────────────────────
AuditCapability

Collates everything written into context['audit'] during the pipeline run
and produces a structured audit trail DataFrame that is written to the
'Audit Trail' sheet of the Excel workbook.

Because audit events are appended by BaseCapability.run() automatically,
this capability needs no other capability to have run before it.  It
simply reads what is already in context['audit']['events'] and
context['audit']['capability_times'] and materialises them into DataFrames.

Requires context keys
---------------------
    audit.events             : list of event dicts
    audit.capability_times   : dict {name: elapsed_seconds}
    audit.run_id             : str

Writes context keys
-------------------
    audit.events_df          : pd.DataFrame
    audit.timing_df          : pd.DataFrame

Fully independent – does not depend on any other capability.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import pandas as pd

from data_compare.capabilities.base import BaseCapability
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class AuditCapability(BaseCapability):
    NAME = "audit"

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        audit = context.get("audit", {})
        events         = audit.get("events", [])
        cap_times      = audit.get("capability_times", {})
        run_id         = audit.get("run_id", "unknown")
        result_name    = context.get("config", {}).get("result_name", "")

        logger.info(
            f"  [audit] Collating {len(events)} event(s) "
            f"for run_id={run_id}"
        )

        # ── events table ──────────────────────────────────────────────────────
        if events:
            events_df = pd.DataFrame(events)
        else:
            events_df = pd.DataFrame(columns=[
                "timestamp", "capability", "event", "detail"
            ])

        # ── timing table ──────────────────────────────────────────────────────
        timing_rows = [
            {
                "Capability":       name,
                "Elapsed_Seconds":  round(elapsed, 4),
                "Status":           "Completed",
            }
            for name, elapsed in sorted(cap_times.items())
        ]
        timing_df = (
            pd.DataFrame(timing_rows)
            if timing_rows
            else pd.DataFrame(columns=["Capability", "Elapsed_Seconds", "Status"])
        )

        # ── summary row ───────────────────────────────────────────────────────
        total_elapsed = sum(cap_times.values())
        summary_rows = [
            {"Setting": "Run ID",          "Value": run_id},
            {"Setting": "Result Name",     "Value": result_name},
            {"Setting": "Run Timestamp",   "Value": datetime.now().isoformat()},
            {"Setting": "Total Elapsed s", "Value": str(round(total_elapsed, 3))},
            {"Setting": "Events Count",    "Value": str(len(events))},
            {"Setting": "Capabilities Run","Value": str(len(cap_times))},
        ]
        summary_df = pd.DataFrame(summary_rows)

        # Write back into audit section of context
        context["audit"]["events_df"]  = events_df
        context["audit"]["timing_df"]  = timing_df
        context["audit"]["summary_df"] = summary_df

        logger.info(
            f"  [audit] Trail ready: {len(events_df)} events, "
            f"{len(timing_df)} capability timings"
        )
        return context
