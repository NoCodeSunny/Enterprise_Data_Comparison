# -*- coding: utf-8 -*-
"""
edcp.audit.enterprise_audit_logger
────────────────────────────────────
EnterpriseAuditLogger — writes immutable JSON audit records to filesystem.

Requirements: F-AUDIT-001 to F-AUDIT-006

Storage: <audit_root>/YYYY/MM/DD/<batch_id>_audit.jsonl
Format: One JSON object per line (JSON Lines format).
Retention: Files are never auto-deleted (F-AUDIT-005).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import threading


class EnterpriseAuditLogger:
    """
    Writes structured audit log entries to the filesystem.

    Each entry is one JSON line appended to:
      <audit_root>/YYYY/MM/DD/<batch_id>_audit.jsonl

    Files are opened in append mode — never truncated or deleted.
    """

    def __init__(self, audit_root: Path) -> None:
        self._root  = audit_root
        self._lock  = threading.Lock()

    # ── public logging methods ────────────────────────────────────────────────

    def log_batch_created(self, batch: Any) -> None:
        self._write(batch.batch_id, {
            "action":         "BATCH_CREATED",
            "batch_id":       batch.batch_id,
            "execution_mode": batch.execution_mode,
            "max_workers":    batch.max_workers,
            "total_jobs":     batch.total_jobs,
            "jobs": [j.job_id for j in batch.jobs],
        })

    def log_batch_completed(self, batch: Any) -> None:
        self._write(batch.batch_id, {
            "action":          "BATCH_COMPLETED",
            "batch_id":        batch.batch_id,
            "final_status":    batch.status.value,
            "total_jobs":      batch.total_jobs,
            "successful_jobs": batch.successful_jobs,
            "failed_jobs":     batch.failed_jobs,
            "started_at":      batch.started_at.isoformat() if batch.started_at else None,
            "completed_at":    batch.completed_at.isoformat() if batch.completed_at else None,
        })

    def log_batch_cancelled(self, batch: Any, reason: str = "") -> None:
        """F-AUDIT-006: Cancellation events must be audited."""
        self._write(batch.batch_id, {
            "action":           "BATCH_CANCELLED",
            "batch_id":         batch.batch_id,
            "cancel_reason":    reason or batch.cancel_reason,
            "jobs_running":     len(batch.running_jobs),
            "jobs_queued":      len(batch.queued_jobs),
            "jobs_completed":   batch.successful_jobs,
        })

    def log_job_completed(self, batch: Any, job: Any) -> None:
        """F-AUDIT-001 to F-AUDIT-003: Full job audit record."""
        self._write(batch.batch_id, {
            "action":           "JOB_COMPLETED",
            "batch_id":         batch.batch_id,
            "job_id":           job.job_id,
            "final_status":     job.status.value,
            "files": {
                "prod":         job.prod_path,
                "dev":          job.dev_path,
                "prod_size_mb": round(job.file_size_mb, 2),
            },
            "execution": {
                "engine":           job.engine_used,
                "engine_reason":    self._engine_reason(job),
                "duration_sec":     round(job.duration_sec, 2),
                "records_compared": job.records_compared,
                "differences":      job.differences_found,
                "retry_count":      job.retry_count,
            },
            "config": {
                "keys":             job.keys,
                "ignore_columns":   job.ignore_columns,
                "tolerance":        job.tolerance,
            },
            "error":            job.error_message or None,
            "error_code":       job.error_code or None,
        })

    def log_validation_failure(self, batch_id: str, errors: list) -> None:
        self._write(batch_id, {
            "action":     "VALIDATION_FAILED",
            "batch_id":   batch_id,
            "error_count":len(errors),
            "errors":     [e.to_dict() for e in errors],
        })

    # ── internal ──────────────────────────────────────────────────────────────

    def _write(self, batch_id: str, payload: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        entry = {
            "timestamp":  now.isoformat(),
            "user":       "system",
            **payload,
        }
        log_path = self._log_path(batch_id, now)
        with self._lock:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, default=str) + "\n")
            # Make immutable (best-effort) — F-AUDIT-005
            try:
                os.chmod(log_path, 0o444)
            except OSError:
                pass

    def _log_path(self, batch_id: str, dt: datetime) -> Path:
        return (self._root / dt.strftime("%Y") / dt.strftime("%m")
                / dt.strftime("%d") / f"{batch_id}_audit.jsonl")

    @staticmethod
    def _engine_reason(job: Any) -> str:
        from edcp.batch.engine_selector import engine_selection_reason
        return engine_selection_reason(job.prod_path, job.dev_path, job.engine_used)
