# -*- coding: utf-8 -*-
"""
data_compare.debugger.debugger
────────────────────────────────
Debugger

Central façade for intelligent error diagnosis in data_compare.

Usage (automatic — integrated into ComparisonJob):
    ComparisonJob already calls Debugger.diagnose() on every exception.
    The result is stored in JobResult.debug_report and printed to the log.

Usage (manual):
    from data_compare.debugger import Debugger

    try:
        ... # some data_compare operation
    except Exception as exc:
        report = Debugger().diagnose(exc, context=ctx)
        print(report.full_report())
        # OR just log it:
        report.log()

Usage (standalone diagnosis of an existing error dict):
    report = Debugger().diagnose_from_summary(summary_dict)

DebugReport combines an ErrorRecord (what + why + fix) from ErrorMapper
with an AnalysisReport (where exactly) from ErrorAnalyzer into a single,
printable, loggable, JSON-serialisable object.
"""

from __future__ import annotations

import json
import sys
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from data_compare.debugger.error_analyzer import AnalysisReport, ErrorAnalyzer
from data_compare.debugger.error_mapper import ErrorMapper, ErrorRecord
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DebugReport:
    """
    Combined diagnostic report for one exception.

    Contains:
    - error_record  : structured error info (code, category, human message, fix)
    - analysis      : code location, stack frames, context snapshot
    - raw_traceback : the original traceback as a string
    """
    error_record:  ErrorRecord
    analysis:      AnalysisReport
    raw_traceback: str = ""
    context_hint:  str = ""

    # ── human-readable output ─────────────────────────────────────────────────

    def full_report(self) -> str:
        """Return a complete human-readable diagnostic report."""
        sep = "═" * 72
        thin = "─" * 72
        lines = [
            "",
            sep,
            f"  DATA COMPARE DIAGNOSTIC REPORT",
            sep,
            str(self.error_record),
            thin,
            str(self.analysis),
        ]
        if self.raw_traceback:
            lines.append(thin)
            lines.append("  Stack Trace:")
            for tb_line in self.raw_traceback.strip().splitlines():
                lines.append(f"    {tb_line}")
        lines.append(sep)
        return "\n".join(lines)

    def summary(self) -> str:
        """One-line summary for console output."""
        return (
            f"[{self.error_record.severity}] {self.error_record.error_code}: "
            f"{self.error_record.human_message[:120]} "
            f"→ {self.analysis.format_origin()}"
        )

    def log(self) -> None:
        """Write the full report to the DataComparator logger."""
        severity = self.error_record.severity
        msg = self.full_report()
        if severity == "CRITICAL":
            logger.critical(msg)
        elif severity == "WARNING":
            logger.warning(msg)
        else:
            logger.error(msg)

    # ── serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a JSON-compatible dict."""
        return {
            "error_record": self.error_record.to_dict(),
            "origin": self.analysis.format_origin(),
            "context_snapshot": self.analysis.context_snapshot,
            "suggestions": (
                self.analysis.suggestions
                + [self.error_record.recommended_fix]
            ),
            "raw_traceback": self.raw_traceback,
        }

    def to_json(self) -> str:
        """Return JSON string of the full report."""
        return json.dumps(self.to_dict(), indent=2, default=str)


class Debugger:
    """
    Intelligent error diagnostic façade.

    Combines ErrorMapper (what/why/fix) with ErrorAnalyzer (where)
    to produce a DebugReport for any exception raised during a comparison run.

    Parameters
    ----------
    framework_root : str
        Package name used to identify framework-internal stack frames.
    """

    def __init__(self, framework_root: str = "data_compare") -> None:
        self._mapper   = ErrorMapper()
        self._analyzer = ErrorAnalyzer(framework_root=framework_root)

    # ── public API ────────────────────────────────────────────────────────────

    def diagnose(
        self,
        exc: Exception,
        context: Optional[Dict[str, Any]] = None,
        context_hint: str = "",
        tb: Optional[Any] = None,
    ) -> DebugReport:
        """
        Produce a full DebugReport for *exc*.

        Parameters
        ----------
        exc : Exception
        context : dict | None
            Pipeline context dict at the time of failure.
        context_hint : str
            Short label (e.g. batch name, capability name) for context.
        tb : traceback | None
            If None, captured from sys.exc_info().

        Returns
        -------
        DebugReport
        """
        if tb is None:
            _, _, tb = sys.exc_info()

        error_record = self._mapper.map(exc, context_hint=context_hint)
        analysis     = self._analyzer.analyze(exc, tb=tb, context=context)
        raw_tb       = "".join(traceback.format_exception(type(exc), exc, tb))

        return DebugReport(
            error_record=error_record,
            analysis=analysis,
            raw_traceback=raw_tb,
            context_hint=context_hint,
        )

    def diagnose_and_log(
        self,
        exc: Exception,
        context: Optional[Dict[str, Any]] = None,
        context_hint: str = "",
    ) -> DebugReport:
        """
        Diagnose and immediately log the report.
        Returns the DebugReport for further use.
        """
        report = self.diagnose(exc, context=context, context_hint=context_hint)
        report.log()
        return report

    def diagnose_from_summary(
        self, summary: Dict[str, Any]
    ) -> Optional[DebugReport]:
        """
        Diagnose from a job summary dict (produced by ComparisonJob).

        Returns None if the summary contains no error information.
        """
        error_msg = summary.get("Error")
        if not error_msg:
            return None

        exc = Exception(error_msg)
        error_record = self._mapper.map(exc, context_hint=summary.get("ResultName", ""))
        analysis = AnalysisReport(
            suggestions=["Re-run with DEBUG logging to get a full stack trace."]
        )
        return DebugReport(
            error_record=error_record,
            analysis=analysis,
            raw_traceback=summary.get("Traceback", ""),
            context_hint=summary.get("ResultName", ""),
        )

    @staticmethod
    def format_for_cli(report: DebugReport) -> str:
        """
        Compact version of the report suitable for CLI output.
        """
        lines = [
            f"\n{'─'*60}",
            f"  ERROR DIAGNOSIS",
            f"{'─'*60}",
            f"  Code    : {report.error_record.error_code}",
            f"  Category: {report.error_record.category}",
            f"  Message : {report.error_record.human_message}",
            f"  Origin  : {report.analysis.format_origin()}",
            f"{'─'*60}",
            f"  Root Cause:",
            f"    {report.error_record.root_cause}",
            f"  Recommended Fix:",
        ]
        for line in report.error_record.recommended_fix.splitlines():
            lines.append(f"    {line}")
        if report.analysis.suggestions:
            lines.append("  Location-Specific Tips:")
            for s in report.analysis.suggestions[:3]:
                lines.append(f"    • {s}")
        lines.append(f"{'─'*60}\n")
        return "\n".join(lines)
