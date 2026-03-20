# -*- coding: utf-8 -*-
"""
data_compare.reporting.json_audit
───────────────────────────────────
Writes a machine-readable JSON audit sidecar alongside the HTML summary.

Features:
  [F-65]  JSON sidecar (run_audit.json) written to report root
  [F-66]  Machine-readable; suitable for CI/CD gating
          overall_pass = True only when ALL batches have 0 failures and 0 errors
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


def write_json_audit(
    summaries: List[Dict],
    report_root: Path,
    total_elapsed: float,
) -> Path:
    """
    Write run_audit.json to *report_root*.

    Parameters
    ----------
    summaries : list[dict]
        All batch summary dicts returned by the orchestrator.
    report_root : Path
        Directory where the file is written.
    total_elapsed : float
        Wall-clock seconds for the full run.

    Returns
    -------
    Path
        Path to the written JSON file.
    """
    overall_pass = all(
        s.get("MatchedFailed", 0) == 0
        and s.get("Error") is None
        and s.get("AlertsTriggered", 0) == 0
        for s in summaries
    )

    payload = {
        "run_timestamp":    datetime.now().isoformat(),
        "total_elapsed_s":  round(total_elapsed, 2),
        "batch_count":      len(summaries),
        "overall_pass":     overall_pass,
        "framework_version": "3.0.0",
        "batches":          summaries,
    }

    out = Path(report_root) / "run_audit.json"
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    logger.info(f"  JSON audit log written: {out.name}  (overall_pass={overall_pass})")
    return out
