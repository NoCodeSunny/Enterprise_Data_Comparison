# -*- coding: utf-8 -*-
"""
data_compare.debugger.error_analyzer
──────────────────────────────────────
ErrorAnalyzer

Performs deep analysis of exceptions by examining:
  - the full Python traceback
  - the specific line of code that raised
  - the data_compare module that is the origin
  - runtime context from the pipeline

Produces an AnalysisReport that enriches an ErrorRecord with precise
code-location information and a context snapshot.

This module is designed to be called AFTER ErrorMapper has produced the
base ErrorRecord. It adds the "where exactly" to the "what and why".
"""

from __future__ import annotations

import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class FrameInfo:
    """A single frame from a Python traceback."""
    filename:   str
    lineno:     int
    function:   str
    code_line:  str
    is_framework: bool = False   # True when the frame is inside data_compare

    def __str__(self) -> str:
        prefix = "→ " if self.is_framework else "  "
        return (
            f"{prefix}File '{self.filename}', line {self.lineno}, "
            f"in {self.function}\n"
            f"      {self.code_line}"
        )


@dataclass
class AnalysisReport:
    """
    Full analysis of a single exception.

    Attributes
    ----------
    origin_file    : The data_compare source file where the error originated.
    origin_line    : Line number in that file.
    origin_function: Function/method name at the origin.
    framework_frames: All stack frames inside data_compare, innermost first.
    context_snapshot: Relevant keys from the pipeline context at the time of failure.
    suggestions    : Ordered list of actionable suggestions specific to this location.
    """
    origin_file:     str             = "unknown"
    origin_line:     int             = 0
    origin_function: str             = "unknown"
    framework_frames: List[FrameInfo] = field(default_factory=list)
    context_snapshot: Dict[str, Any]  = field(default_factory=dict)
    suggestions:     List[str]        = field(default_factory=list)

    def format_origin(self) -> str:
        if self.origin_file == "unknown":
            return "(origin unknown)"
        return f"{self.origin_file}:{self.origin_line} in {self.origin_function}()"

    def format_frames(self) -> str:
        if not self.framework_frames:
            return "  (no framework frames found)"
        return "\n".join(str(f) for f in self.framework_frames)

    def __str__(self) -> str:
        parts = [
            f"  Origin        : {self.format_origin()}",
        ]
        if self.framework_frames:
            parts.append("  Framework trace (innermost first):")
            for frame in self.framework_frames[:5]:   # top 5 only in string form
                parts.append(f"    {frame}")
        if self.context_snapshot:
            parts.append("  Context snapshot:")
            for k, v in self.context_snapshot.items():
                parts.append(f"    {k}: {v}")
        if self.suggestions:
            parts.append("  Location-specific suggestions:")
            for i, s in enumerate(self.suggestions, 1):
                parts.append(f"    {i}. {s}")
        return "\n".join(parts)


class ErrorAnalyzer:
    """
    Analyses Python exceptions to produce structured AnalysisReports.

    Parameters
    ----------
    framework_root : str
        The top-level package name of the framework (default: "data_compare").
        Used to identify which stack frames belong to the framework.
    """

    def __init__(self, framework_root: str = "data_compare") -> None:
        self._root = framework_root

    def analyze(
        self,
        exc: Exception,
        tb: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> AnalysisReport:
        """
        Analyse an exception and return a structured AnalysisReport.

        Parameters
        ----------
        exc : Exception
            The exception to analyse.
        tb : traceback object | None
            The traceback; if None, uses sys.exc_info()[2].
        context : dict | None
            The pipeline context dict at the time of failure.

        Returns
        -------
        AnalysisReport
        """
        report = AnalysisReport()

        # ── extract frames ────────────────────────────────────────────────────
        if tb is None:
            _, _, tb = sys.exc_info()

        all_frames = self._extract_frames(exc, tb)
        framework_frames = [f for f in all_frames if f.is_framework]
        report.framework_frames = list(reversed(framework_frames))  # innermost first

        # ── identify origin (innermost framework frame) ───────────────────────
        if report.framework_frames:
            origin = report.framework_frames[0]
            report.origin_file     = self._relative_path(origin.filename)
            report.origin_line     = origin.lineno
            report.origin_function = origin.function
        elif all_frames:
            # Fall back to the innermost frame from anywhere
            inner = all_frames[-1]
            report.origin_file     = self._relative_path(inner.filename)
            report.origin_line     = inner.lineno
            report.origin_function = inner.function

        # ── context snapshot ──────────────────────────────────────────────────
        if context:
            report.context_snapshot = self._snapshot_context(context)

        # ── location-specific suggestions ─────────────────────────────────────
        report.suggestions = self._location_suggestions(
            report.origin_file, report.origin_function, exc
        )

        return report

    # ── private helpers ───────────────────────────────────────────────────────

    def _extract_frames(
        self, exc: Exception, tb: Optional[Any]
    ) -> List[FrameInfo]:
        """Extract all stack frames from a traceback."""
        frames: List[FrameInfo] = []

        if tb is not None:
            for frame_summary in traceback.extract_tb(tb):
                frames.append(FrameInfo(
                    filename=frame_summary.filename,
                    lineno=frame_summary.lineno,
                    function=frame_summary.name,
                    code_line=(frame_summary.line or "").strip(),
                    is_framework=self._root in frame_summary.filename,
                ))
        else:
            # Try to get frames from the exception's __traceback__
            exc_tb = getattr(exc, "__traceback__", None)
            if exc_tb:
                for frame_summary in traceback.extract_tb(exc_tb):
                    frames.append(FrameInfo(
                        filename=frame_summary.filename,
                        lineno=frame_summary.lineno,
                        function=frame_summary.name,
                        code_line=(frame_summary.line or "").strip(),
                        is_framework=self._root in frame_summary.filename,
                    ))

        return frames

    @staticmethod
    def _relative_path(full_path: str) -> str:
        """Convert an absolute path to a relative path under data_compare/."""
        try:
            p = Path(full_path)
            # Find the 'data_compare' component and return from there
            parts = p.parts
            for i, part in enumerate(parts):
                if part == "data_compare":
                    return str(Path(*parts[i:]))
        except Exception:
            pass
        return full_path

    @staticmethod
    def _snapshot_context(context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract a safe, non-sensitive snapshot of the pipeline context."""
        safe_keys = {
            "prod_name", "dev_name", "result_name",
            "existing_keys", "missing_keys", "ignore_set",
            "using_seq", "dup_count_prod", "dup_count_dev",
            "align_keys",
        }
        snapshot: Dict[str, Any] = {}

        for k in safe_keys:
            v = context.get(k)
            if v is not None:
                # Convert sets to sorted lists for readability
                if isinstance(v, set):
                    snapshot[k] = sorted(v)
                else:
                    snapshot[k] = v

        # Config summary (non-sensitive subset)
        cfg = context.get("config", {})
        if cfg:
            snapshot["config.capabilities"] = cfg.get("capabilities", {})
            snapshot["config.use_spark"]     = cfg.get("use_spark", False)

        # Results summary
        results = context.get("results", {})
        if results:
            snapshot["results.matched_passed"] = results.get("matched_passed", 0)
            snapshot["results.matched_failed"] = results.get("matched_failed", 0)

        return snapshot

    def _location_suggestions(
        self,
        origin_file: str,
        origin_function: str,
        exc: Exception,
    ) -> List[str]:
        """Return location-specific suggestions based on where the error occurred."""
        suggestions: List[str] = []
        f = origin_file.lower()
        fn = origin_function.lower()

        if "comparison_capability" in f:
            suggestions.append(
                "Check that the comparison capability can find prod_df/dev_df in context."
            )
            suggestions.append(
                "Ensure keys are present in both files (run with --capabilities schema first)."
            )

        if "file_loader" in f or "encoding" in f:
            suggestions.append(
                "Try saving the file as CSV UTF-8 from Excel (File → Save As → CSV UTF-8)."
            )
            suggestions.append(
                "Check that the file path has no extra spaces or special characters."
            )

        if "parquet" in f:
            suggestions.append(
                "Verify pyarrow is installed: python -c \"import pyarrow; print(pyarrow.__version__)\""
            )
            suggestions.append(
                "Check that the file has .parquet extension and valid PAR1 magic bytes."
            )

        if "config" in f:
            suggestions.append(
                "Run: python -m data_compare validate config/config.yaml"
            )

        if "orchestrator" in f:
            suggestions.append(
                "Check the InputSheet for blank or misaligned rows."
            )
            suggestions.append(
                "Enable one batch at a time (set Run=YES for only one row) to isolate."
            )

        if "tolerance" in f:
            suggestions.append(
                "Ensure the Numerical Tolerance sheet has the correct column names."
            )

        if isinstance(exc, MemoryError):
            suggestions.append(
                "Consider enabling Spark: add use_spark: true to config.yaml"
            )

        if not suggestions:
            suggestions.append(
                "Enable DEBUG logging (log_level: DEBUG in config.yaml) and check "
                "reports/logs/data_compare.log for the full trace."
            )

        return suggestions
