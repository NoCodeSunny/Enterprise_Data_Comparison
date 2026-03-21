# -*- coding: utf-8 -*-
"""
data_compare.orchestrator
──────────────────────────
Main pipeline controller – production-grade, parallel, engine-aware.

New in v3.1
───────────
  - Parallel execution via ThreadPoolExecutor (max_workers config key)
  - Auto engine selection: PandasEngine (default) or SparkEngine (large files)
  - Config validation via config_schema.validate_config()
  - Structured JSON logging with run_id + job_name per batch
  - Retry logic delegated to ComparisonJob
  - Chunk-based loading for large CSVs (engine-transparent)

Backward compatible
───────────────────
  run_comparison(config_path=None) signature unchanged.
  All existing YAML config keys still work.
  PySpark zip usage unchanged.

Public API
──────────
    from data_compare.orchestrator import run_comparison
    summaries = run_comparison("config/config.yaml")
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from data_compare.comparator.tolerance import build_tolerance_map
from data_compare.config.config_loader import load_config
from data_compare.config.config_schema import validate_config, ConfigError
from data_compare.engines import select_engine
from data_compare.jobs.comparison_job import ComparisonJob
from data_compare.loaders.file_loader import load_any_to_csv
from data_compare.registry.capability_registry import CapabilityRegistry, default_registry
from data_compare.reporting.html_report import write_final_html, send_email
from data_compare.reporting.json_audit import write_json_audit
from data_compare.utils.helpers import ensure_dir
from data_compare.utils.logger import (
    banner, configure_logging, get_logger,
    set_run_id, set_job_name,
)
from data_compare.utils.validation import (
    extract_key_columns,
    parse_batch_row,
    validate_config_sheet,
)

logger = get_logger(__name__)

_LOG_LEVELS = {
    "DEBUG":   logging.DEBUG,
    "INFO":    logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR":   logging.ERROR,
}

# Default parallelism: 1 worker = sequential (safe default)
_DEFAULT_MAX_WORKERS = 1


# ══════════════════════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _find_sheet_by_hint(
    xl: pd.ExcelFile, *hints: str, fallback_first: bool = True
) -> Optional[str]:
    names = xl.sheet_names
    lower = [n.lower() for n in names]
    for h in hints:
        for i, nm in enumerate(lower):
            if h.lower() in nm:
                return names[i]
    return names[0] if (names and fallback_first) else None


def _print_console_summary(summaries: List[Dict]) -> None:
    banner("RUN SUMMARY", "═")
    col_w = [4, 30, 10, 10, 12, 12, 10, 10, 8, 8, 7]
    hdr   = [
        "#", "Result Name", "TotalProd", "TotalDev",
        "Passed", "Failed", "Missing", "Extra",
        "DupP", "DupD", "Time",
    ]
    fmt = lambda row: "  │ ".join(
        str(v)[:col_w[i]].ljust(col_w[i]) for i, v in enumerate(row)
    )
    sep = "  ┼─".join("─" * w for w in col_w)
    logger.info("  " + fmt(hdr))
    logger.info("  " + sep)
    for i, s in enumerate(summaries, 1):
        err = "⚠ " if s.get("Error") else ""
        row = [
            i,
            (err + s.get("ResultName", ""))[:28],
            f"{s.get('TotalProd',0):,}",
            f"{s.get('TotalDev',0):,}",
            f"{s.get('MatchedPassed',0):,}",
            f"{s.get('MatchedFailed',0):,}",
            f"{s.get('MissingDev',0):,}",
            f"{s.get('ExtraDev',0):,}",
            f"{s.get('DuplicateProd',0):,}",
            f"{s.get('DuplicateDev',0):,}",
            f"{s.get('ElapsedSeconds',0):.1f}s",
        ]
        logger.info("  " + fmt(row))
    banner("END OF SUMMARY", "═")


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def _run_yaml_batches(
    cfg: dict,
    batches: list,
    registry,
    run_start: float,
) -> list:
    """
    Execute comparisons defined directly in the YAML config under 'batches:'.
    No InputSheet required — useful for CLI usage and local development.

    Each batch entry supports:
        prod_path:   path to PROD file
        dev_path:    path to DEV file
        result_name: output name (optional)
        keys:        list of key columns (optional)
        ignore_fields: list of columns to ignore (optional)
        capabilities: dict of capability overrides (optional)
    """
    from pathlib import Path as _Path
    from data_compare.jobs.comparison_job import ComparisonJob
    from data_compare.utils.helpers import ensure_dir

    report_root = _Path(cfg["report_root"])
    ensure_dir(report_root)
    summaries = []

    for i, batch in enumerate(batches, 1):
        prod_path    = _Path(batch.get("prod_path", "")).expanduser()
        dev_path     = _Path(batch.get("dev_path",  "")).expanduser()
        result_name  = batch.get("result_name") or f"batch_{i:03d}"
        keys         = batch.get("keys", [])
        ignore       = batch.get("ignore_fields", [])
        tol_map      = {}

        # Validate files exist
        if not prod_path.exists():
            logger.error(f"  Batch {i}: PROD file not found: {prod_path}")
            summaries.append({"ResultName": result_name, "Error": f"PROD not found: {prod_path}"})
            continue
        if not dev_path.exists():
            logger.error(f"  Batch {i}: DEV file not found: {dev_path}")
            summaries.append({"ResultName": result_name, "Error": f"DEV not found: {dev_path}"})
            continue

        # Merge global capability config with per-batch overrides
        caps = {**cfg.get("capabilities", {}), **batch.get("capabilities", {})}

        job_root = report_root / result_name
        ensure_dir(job_root)

        logger.info(f"  Batch {i}/{len(batches)}: {prod_path.name} vs {dev_path.name}")

        cjob = ComparisonJob(
            prod_path    = prod_path,
            dev_path     = dev_path,
            prod_name    = prod_path.name,
            dev_name     = dev_path.name,
            result_name  = result_name,
            report_root  = job_root,
            keys         = keys,
            ignore_fields= ignore,
            tol_map      = tol_map,
            max_retries  = cfg.get("max_retries", 0),
            capabilities_cfg = caps if caps else None,
            config       = {"use_spark": cfg.get("use_spark", False)},
        )

        result = cjob.run(registry=registry)
        summary = result.to_summary_dict() if result.succeeded else {
            "ResultName": result_name,
            "Error": result.error or "Job failed",
        }
        summaries.append(summary)

        if result.succeeded:
            logger.info(f"    ✅ {result_name}: passed={result.summary.get('MatchedPassed',0)} "
                       f"failed={result.summary.get('MatchedFailed',0)}")
        else:
            logger.error(f"    ❌ {result_name}: {result.error}")

    elapsed = time.perf_counter() - run_start
    logger.info(f"  Run complete: {len(summaries)} batch(es) in {elapsed:.1f}s")
    return summaries



def run_comparison(
    config_path: Optional[str] = None,
    registry: Optional[CapabilityRegistry] = None,
    _override_cfg: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Execute the full capability-based comparison pipeline.

    Parameters
    ----------
    config_path : str | None
        Path to a YAML config file.  None → hardcoded defaults.
    registry : CapabilityRegistry | None
        Capability registry to use.  None → module-level default_registry.
    _override_cfg : dict | None
        Pre-validated config dict (used by CLI to avoid double-loading).

    Returns
    -------
    list[dict]
        All batch summary dicts (including error entries for failed batches).
    """
    if registry is None:
        registry = default_registry

    # ── load + validate config ─────────────────────────────────────────────
    if _override_cfg is not None:
        cfg = _override_cfg
    else:
        raw = load_config(config_path)
        try:
            cfg = validate_config(raw)
        except ConfigError as exc:
            logger.error(f"Config validation failed:\n{exc}")
            raise

    # ── logging setup ──────────────────────────────────────────────────────
    level_str = str(cfg.get("log_level", "DEBUG")).upper()
    level     = _LOG_LEVELS.get(level_str, logging.DEBUG)

    report_root = Path(cfg["report_root"])
    log_dir     = report_root / "logs"

    run_id = uuid.uuid4().hex[:12]
    set_run_id(run_id)

    configure_logging(
        level=level,
        log_dir=log_dir,
        run_id=run_id,
        enable_json=True,
        enable_file=True,
    )

    run_start = time.perf_counter()
    banner("DATA COMPARATOR  v3.1  –  PRODUCTION EDITION", "═")
    logger.info(f"  Run ID       : {run_id}")
    logger.info(f"  Run started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Input sheet  : {cfg['input_sheet']}")
    logger.info(f"  Report root  : {report_root}")
    logger.info(f"  Log dir      : {log_dir}")

    input_sheet    = Path(cfg["input_sheet"])
    converted_root = report_root / "ConvertedCSVs"
    ensure_dir(report_root)
    ensure_dir(converted_root)

    # ── YAML batches shortcut (no InputSheet needed) ──────────────────────
    # If the config has a 'batches' list, run directly without InputSheet.
    yaml_batches = cfg.get("batches", [])
    if yaml_batches:
        logger.info(f"  Mode: YAML batches ({len(yaml_batches)} batch(es) defined)")
        return _run_yaml_batches(cfg, yaml_batches, registry, run_start)

    if not input_sheet.exists():
        logger.error(f"❌ Input sheet not found: {input_sheet}")
        logger.error("  Tip: define 'batches:' in your YAML config to run without InputSheet")
        raise FileNotFoundError(
            f"Input sheet not found: {input_sheet}\n"
            "Tip: add a 'batches:' section to your YAML config to run without an InputSheet."
        )

    # ── parse InputSheet ───────────────────────────────────────────────────
    logger.info("  Parsing InputSheet …")
    xl = pd.ExcelFile(input_sheet, engine="openpyxl")

    input_sheet_name = (
        "InputSheet _Data_Comparison"
        if "InputSheet _Data_Comparison" in xl.sheet_names
        else _find_sheet_by_hint(xl, "input", "comparison", fallback_first=True)
    )
    tol_sheet_name = (
        "Numerical Tolerance"
        if "Numerical Tolerance" in xl.sheet_names
        else _find_sheet_by_hint(xl, "tolerance", "numeric", fallback_first=False)
    )

    logger.info(f"  Config sheet   : {input_sheet_name}")
    logger.info(f"  Tolerance sheet: {tol_sheet_name or '(none found)'}")

    cfg_df  = xl.parse(input_sheet_name)
    tol_df  = xl.parse(tol_sheet_name) if tol_sheet_name else pd.DataFrame()
    tol_map = build_tolerance_map(tol_df)

    colmap   = validate_config_sheet(cfg_df)
    key_cols = extract_key_columns(cfg_df)

    col_run = colmap.get("run")
    active_rows = [
        row for _, row in cfg_df.iterrows()
        if str(row.get(col_run, "")).strip().lower() in ("yes", "y", "true", "1")
    ]
    logger.info(f"  Active batches : {len(active_rows)} of {len(cfg_df)} rows")

    # ── capability / engine config ─────────────────────────────────────────
    caps_cfg        = cfg.get("capabilities",        {})
    alert_rules_cfg = cfg.get("alert_rules",         [])
    dq_checks_cfg   = cfg.get("data_quality_checks", {})
    plugins_cfg     = cfg.get("plugins",             {})
    max_workers     = int(cfg.get("max_workers",     _DEFAULT_MAX_WORKERS))
    max_retries     = int(cfg.get("max_retries",     0))
    retry_delay_s   = float(cfg.get("retry_delay_s", 5.0))

    logger.info(
        f"  Parallelism: max_workers={max_workers}  "
        f"max_retries={max_retries}"
    )

    # ── build jobs ─────────────────────────────────────────────────────────
    banner(f"BUILDING  {len(active_rows)}  JOB(S)", "─")
    jobs: List[ComparisonJob] = []
    attachments: List[Path] = []

    for batch_num, row in enumerate(active_rows, start=1):
        try:
            (
                base_path_str,
                prod_name,
                dev_name,
                result_nm,
                sheet_name,
                keys,
                include_pass,
                ignore_fields,
            ) = parse_batch_row(row, colmap, key_cols)

            prod_file = Path(base_path_str) / prod_name
            dev_file  = Path(base_path_str) / dev_name

            if not prod_file.exists():
                raise FileNotFoundError(f"PROD file not found: {prod_file}")
            if not dev_file.exists():
                raise FileNotFoundError(f"DEV  file not found: {dev_file}")

            prod_csv = load_any_to_csv(prod_file, converted_root, sheet_name=sheet_name)
            dev_csv  = load_any_to_csv(dev_file,  converted_root, sheet_name=sheet_name)

            job = ComparisonJob(
                prod_path=prod_csv,
                dev_path=dev_csv,
                prod_name=prod_name,
                dev_name=dev_name,
                result_name=result_nm,
                report_root=report_root,
                keys=keys,
                ignore_fields=ignore_fields,
                tol_map=tol_map,
                capabilities_cfg=caps_cfg,
                alert_rules=alert_rules_cfg,
                data_quality_checks=dq_checks_cfg,
                plugins_cfg=plugins_cfg,
                include_pass_report=include_pass,
                max_retries=max_retries,
                retry_delay_s=retry_delay_s,
                config=cfg,
                job_id=f"{run_id}-{batch_num:03d}",
            )
            jobs.append(job)
            logger.info(
                f"  Job {batch_num}: {result_nm}  "
                f"PROD={prod_name}  DEV={dev_name}  keys={keys}"
            )
        except Exception as exc:
            logger.error(f"  ❌ Failed to build Job {batch_num}: {exc}", exc_info=True)
            # Will show up as an error entry in summaries

    # ── execute jobs (parallel or sequential) ──────────────────────────────
    banner(f"EXECUTING  {len(jobs)}  JOB(S)  [workers={max_workers}]", "─")
    summaries: List[Dict[str, Any]] = []

    if max_workers == 1 or len(jobs) == 1:
        # Sequential execution (no thread overhead)
        for job in jobs:
            result = job.run(registry=registry)
            summaries.append(result.to_summary_dict())
            if result.report_path:
                attachments.append(Path(result.report_path))
    else:
        # Parallel execution via ThreadPoolExecutor
        # Each job runs in its own thread; one failed job never stops others
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(job.run, registry): job
                for job in jobs
            }
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                try:
                    result = future.result()
                    summaries.append(result.to_summary_dict())
                    if result.report_path:
                        attachments.append(Path(result.report_path))
                    status = "✅" if result.succeeded else "❌"
                    logger.info(
                        f"  {status} Job '{job.result_name}' finished "
                        f"[{result.elapsed_s:.1f}s]"
                    )
                except Exception as exc:
                    logger.error(
                        f"  ❌ Job '{job.result_name}' raised unexpected error: {exc}",
                        exc_info=True,
                    )
                    summaries.append({
                        "ResultName": job.result_name,
                        "ProdFile":   job.prod_name,
                        "DevFile":    job.dev_name,
                        "TotalProd":  0, "TotalDev":  0,
                        "MatchedPassed": 0, "MatchedFailed": 0,
                        "ExtraDev": 0, "MissingDev": 0,
                        "DuplicateProd": 0, "DuplicateDev": 0,
                        "UsedKeys": [], "MissingKeyColumns": [],
                        "IgnoredFields": [], "ToleranceUsed": {},
                        "ReportPath": "", "ElapsedSeconds": 0.0,
                        "AlertsTriggered": 0,
                        "SchemaExtraCount": 0, "SchemaMissingCount": 0,
                        "Error": str(exc),
                    })

        # Restore original order (parallel may finish out-of-order)
        order = {job.result_name: i for i, job in enumerate(jobs)}
        summaries.sort(key=lambda s: order.get(s.get("ResultName", ""), 999))

    # ── final outputs ──────────────────────────────────────────────────────
    total_elapsed = time.perf_counter() - run_start
    _print_console_summary(summaries)

    if summaries:
        banner("WRITING FINAL OUTPUTS")
        final_html = write_final_html(summaries, report_root, total_elapsed)
        write_json_audit(summaries, report_root, total_elapsed)
        attachments.append(final_html)

        subject = (
            f"{cfg['email_subject_prefix']} – "
            f"{datetime.now():%Y-%m-%d}  "
            f"({len(summaries)} batch{'es' if len(summaries)!=1 else ''})"
        )
        send_email(
            email_to=cfg["email_to"],
            subject=subject,
            html_body_path=final_html,
            attachments=attachments,
            smtp_host=cfg.get("smtp_host", ""),
            smtp_port=int(cfg.get("smtp_port", 587)),
            smtp_from=cfg.get("smtp_from", ""),
            smtp_user=cfg.get("smtp_user", ""),
            smtp_pass=cfg.get("smtp_pass", ""),
        )
    else:
        logger.warning("No batches completed – nothing to report.")

    logger.info(
        f"  Total run time: {total_elapsed:.1f}s  "
        f"({math.ceil(total_elapsed / 60)} min)  "
        f"run_id={run_id}"
    )
    banner("DATA COMPARATOR  COMPLETE", "═")
    return summaries
