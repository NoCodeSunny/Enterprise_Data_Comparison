# -*- coding: utf-8 -*-
"""
data_compare.jobs.comparison_job
──────────────────────────────────
ComparisonJob – encapsulates one complete comparison run for a single batch.

Responsibilities:
  - Hold all input parameters for one batch
  - Execute the full capability pipeline
  - Handle retries (configurable max_retries + retry_delay_s)
  - Emit structured job-level events (START / RETRY / DONE / FAILED)
  - Return a JobResult with full context and summary dict
  - Isolate failures so parallel batches are not affected

Usage (direct)
──────────────
    from data_compare.jobs.comparison_job import ComparisonJob
    from data_compare.registry.capability_registry import CapabilityRegistry

    job = ComparisonJob(
        prod_path=Path("prod.csv"),
        dev_path=Path("dev.csv"),
        prod_name="prod.csv",
        dev_name="dev.csv",
        result_name="MyComparison",
        report_root=Path("reports/"),
        keys=["TradeID"],
        max_retries=2,
    )
    result = job.run(registry=CapabilityRegistry())
    print(result.summary)

Usage (orchestrator)
─────────────────────
ComparisonJob instances are built by the orchestrator and dispatched via
ThreadPoolExecutor.  Each job runs in its own thread.
"""

from __future__ import annotations

import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from data_compare.context.run_context import make_context
from data_compare.engines import select_engine
from data_compare.reporting.report_builder import build_report
from data_compare.utils.logger import get_logger, set_job_name
from data_compare.debugger import Debugger

logger = get_logger(__name__)


# ── result dataclass ──────────────────────────────────────────────────────────

@dataclass
class JobResult:
    """Outcome of a single ComparisonJob run."""
    job_id:     str
    result_name: str
    prod_name:  str
    dev_name:   str
    status:     str           # "SUCCESS" | "FAILED" | "SKIPPED"
    summary:    Dict[str, Any] = field(default_factory=dict)
    context:    Optional[Dict[str, Any]] = field(default=None, repr=False)
    error:      Optional[str] = None
    traceback:  Optional[str] = None
    debug_report: Optional[Any] = field(default=None, repr=False)
    attempts:   int           = 1
    elapsed_s:  float         = 0.0
    report_path: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.status == "SUCCESS"

    def to_summary_dict(self) -> Dict[str, Any]:
        """Return a flat dict compatible with write_final_html() and write_json_audit()."""
        if self.summary:
            d = dict(self.summary)
            d["JobID"]    = self.job_id
            d["Attempts"] = self.attempts
            return d
        # Minimal error dict
        return {
            "ResultName":   self.result_name,
            "ProdFile":     self.prod_name,
            "DevFile":      self.dev_name,
            "TotalProd":    0, "TotalDev":    0,
            "MatchedPassed": 0, "MatchedFailed": 0,
            "ExtraDev":     0, "MissingDev":  0,
            "DuplicateProd": 0, "DuplicateDev": 0,
            "UsedKeys":     [], "MissingKeyColumns": [],
            "IgnoredFields": [], "ToleranceUsed": {},
            "ReportPath":   self.report_path or "",
            "ElapsedSeconds": self.elapsed_s,
            "AlertsTriggered": 0,
            "SchemaExtraCount": 0, "SchemaMissingCount": 0,
            "Error":        self.error,
            "JobID":        self.job_id,
            "Attempts":     self.attempts,
        }


# ── job class ─────────────────────────────────────────────────────────────────

class ComparisonJob:
    """
    Encapsulates one full comparison batch with retry logic.

    Parameters
    ----------
    prod_path : Path
        Path to the PROD CSV (after file-loader normalisation).
    dev_path : Path
        Path to the DEV CSV.
    prod_name : str
        Original PROD filename (for tolerance map lookup).
    dev_name : str
        Original DEV filename.
    result_name : str
        Base name for the output Excel workbook.
    report_root : Path
        Directory where reports are written.
    keys : list[str]
        Key column names.
    ignore_fields : list[str]
        Column names to exclude.
    tol_map : dict
        Full tolerance map.
    capabilities_cfg : dict[str, bool]
        Capability enable/disable map.
    alert_rules : list[dict]
        Alert rule definitions.
    data_quality_checks : dict
        DQ check toggles.
    plugins_cfg : dict
        Plugin config.
    include_pass_report : bool
        Include all matched rows in Pass Comparison sheet.
    max_retries : int
        Number of retry attempts (0 = no retries).
    retry_delay_s : float
        Seconds to wait between retry attempts.
    config : dict
        Full config dict for engine selection.
    job_id : str | None
        Unique job identifier.  Auto-generated if None.
    """

    def __init__(
        self,
        prod_path: Path,
        dev_path: Path,
        prod_name: str,
        dev_name: str,
        result_name: str,
        report_root: Path,
        keys: Optional[List[str]] = None,
        ignore_fields: Optional[List[str]] = None,
        tol_map: Optional[Dict] = None,
        capabilities_cfg: Optional[Dict[str, bool]] = None,
        alert_rules: Optional[List[Dict]] = None,
        data_quality_checks: Optional[Dict] = None,
        plugins_cfg: Optional[Dict] = None,
        include_pass_report: bool = True,
        max_retries: int = 0,
        retry_delay_s: float = 5.0,
        config: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
    ) -> None:
        self.prod_path          = Path(prod_path)
        self.dev_path           = Path(dev_path)
        self.prod_name          = prod_name
        self.dev_name           = dev_name
        self.result_name        = result_name
        self.report_root        = Path(report_root)
        self.keys               = keys or []
        self.ignore_fields      = ignore_fields or []
        self.tol_map            = tol_map or {}
        self.capabilities_cfg   = capabilities_cfg or {}
        self.alert_rules        = alert_rules or []
        self.data_quality_checks= data_quality_checks or {}
        self.plugins_cfg        = plugins_cfg or {}
        self.include_pass_report= include_pass_report
        self.max_retries        = max_retries
        self.retry_delay_s      = retry_delay_s
        self.config             = config or {}
        self.job_id             = job_id or uuid.uuid4().hex[:12]

    # ── public run method ─────────────────────────────────────────────────────

    def run(self, registry: Any = None) -> JobResult:
        """
        Execute this job, retrying up to max_retries times on failure.

        Parameters
        ----------
        registry : CapabilityRegistry | None
            Registry to use.  If None, a fresh default_registry is used.

        Returns
        -------
        JobResult
        """
        from data_compare.registry.capability_registry import (
            CapabilityRegistry, default_registry,
        )

        if registry is None:
            registry = default_registry

        t0 = time.perf_counter()
        last_exc:  Optional[Exception] = None
        last_tb:   str = ""
        attempts   = 0

        set_job_name(self.result_name)
        logger.info(
            f"  [job:{self.job_id}] Starting '{self.result_name}'  "
            f"PROD={self.prod_name}  DEV={self.dev_name}"
        )

        for attempt in range(1, self.max_retries + 2):   # +2: initial + retries
            attempts = attempt
            if attempt > 1:
                logger.warning(
                    f"  [job:{self.job_id}] Retry {attempt - 1}/{self.max_retries} "
                    f"for '{self.result_name}' after {self.retry_delay_s}s …"
                )
                time.sleep(self.retry_delay_s)

            try:
                context = self._execute_once(registry)
                elapsed = time.perf_counter() - t0
                context["elapsed_seconds"] = elapsed
                out_xlsx = build_report(context)
                report_path = str(out_xlsx) if out_xlsx else None

                summary = self._build_summary(context, elapsed, report_path)
                logger.info(
                    f"  [job:{self.job_id}] ✅ '{self.result_name}' "
                    f"complete [{elapsed:.1f}s] attempt={attempt}"
                )
                return JobResult(
                    job_id=self.job_id,
                    result_name=self.result_name,
                    prod_name=self.prod_name,
                    dev_name=self.dev_name,
                    status="SUCCESS",
                    summary=summary,
                    context=context,
                    attempts=attempt,
                    elapsed_s=elapsed,
                    report_path=report_path,
                )

            except Exception as exc:
                import sys as _sys
                last_exc = exc
                last_tb  = traceback.format_exc()
                logger.error(
                    f"  [job:{self.job_id}] ❌ Attempt {attempt} FAILED: {exc}",
                    exc_info=True,
                )
                # Intelligent diagnosis
                try:
                    _dbg = Debugger()
                    _report = _dbg.diagnose(exc, context_hint=self.result_name)
                    logger.error(_report.summary())
                except Exception:
                    pass  # never let debugger failures mask the real error

        # All attempts exhausted
        elapsed = time.perf_counter() - t0
        logger.error(
            f"  [job:{self.job_id}] All {attempts} attempt(s) failed for "
            f"'{self.result_name}'"
        )
        # Build final debug report for the last exception
        _final_debug = None
        if last_exc:
            try:
                _final_debug = Debugger().diagnose(last_exc, context_hint=self.result_name)
            except Exception:
                pass
        return JobResult(
            job_id=self.job_id,
            result_name=self.result_name,
            prod_name=self.prod_name,
            dev_name=self.dev_name,
            status="FAILED",
            error=str(last_exc),
            traceback=last_tb,
            attempts=attempts,
            elapsed_s=elapsed,
            debug_report=_final_debug,
        )

    # ── private helpers ───────────────────────────────────────────────────────

    def _execute_once(self, registry: Any) -> Dict[str, Any]:
        """Build context, select engine, run pipeline. Returns updated context."""
        context = make_context(
            prod_csv_path=self.prod_path,
            dev_csv_path=self.dev_path,
            prod_name=self.prod_name,
            dev_name=self.dev_name,
            result_name=self.result_name,
            report_root=self.report_root,
            include_pass_report=self.include_pass_report,
            keys=self.keys,
            ignore_fields=self.ignore_fields,
            tol_map=self.tol_map,
            capabilities_cfg=self.capabilities_cfg,
            alert_rules=self.alert_rules,
            data_quality_checks=self.data_quality_checks,
        )
        if self.plugins_cfg:
            context["config"]["plugins"] = self.plugins_cfg

        # Inject engine config so capabilities can read it
        context["config"]["use_spark"]             = self.config.get("use_spark", False)
        context["config"]["spark_threshold_bytes"] = self.config.get("spark_threshold_bytes", 500*1024*1024)
        context["config"]["pandas_chunk_size"]     = self.config.get("pandas_chunk_size", 200_000)
        context["job_id"] = self.job_id

        # Set run context for JSON logging
        from data_compare.utils.logger import set_run_id, set_job_name
        set_run_id(context["audit"]["run_id"])
        set_job_name(self.result_name)

        context = registry.run_pipeline(context)
        return context

    def _build_summary(
        self,
        context: Dict[str, Any],
        elapsed: float,
        report_path: Optional[str],
    ) -> Dict[str, Any]:
        """Extract flat summary dict from completed context."""
        r           = context.get("results", {})
        sd          = context.get("schema_diff_df")
        only_prod   = context.get("only_in_prod")
        only_dev    = context.get("only_in_dev")
        prod_df     = context.get("prod_df")
        dev_df      = context.get("dev_df")
        alerts      = context.get("alerts", {})

        return {
            "ResultName":        self.result_name,
            "ProdFile":          self.prod_name,
            "DevFile":           self.dev_name,
            "TotalProd":         len(prod_df)   if prod_df   is not None else 0,
            "TotalDev":          len(dev_df)    if dev_df    is not None else 0,
            "MatchedPassed":     r.get("matched_passed",  0),
            "MatchedFailed":     r.get("matched_failed",  0),
            "ExtraDev":          len(only_dev)  if only_dev  is not None else 0,
            "MissingDev":        len(only_prod) if only_prod is not None else 0,
            "DuplicateProd":     context.get("dup_count_prod", 0),
            "DuplicateDev":      context.get("dup_count_dev",  0),
            "UsedKeys":          context.get("existing_keys", []),
            "MissingKeyColumns": context.get("missing_keys",  []),
            "IgnoredFields":     sorted(context.get("ignore_set", set())),
            "ToleranceUsed":     context.get("tol_for_pair", {}),
            "ReportPath":        report_path or "",
            "ElapsedSeconds":    round(elapsed, 2),
            "AlertsTriggered":   len(alerts.get("triggered", [])),
            "SchemaExtraCount":  int((sd["Side"] == "Extra in Dev").sum())
                                 if sd is not None and not sd.empty else 0,
            "SchemaMissingCount":int((sd["Side"] == "Missing in Dev").sum())
                                 if sd is not None and not sd.empty else 0,
        }

    def __repr__(self) -> str:
        return (
            f"ComparisonJob(id={self.job_id!r}, "
            f"result={self.result_name!r}, "
            f"retries={self.max_retries})"
        )
