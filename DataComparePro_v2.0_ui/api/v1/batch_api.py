# -*- coding: utf-8 -*-
"""
edcp_ui.api.v1.batch_api
─────────────────────────
Flask Blueprint implementing all /api/v1/ endpoints.

Endpoints
---------
POST   /api/v1/batch                     Create + submit batch
GET    /api/v1/batch/<id>                Get batch status
POST   /api/v1/batch/<id>/cancel         Cancel batch
GET    /api/v1/batch/<id>/cancel-preview Preview cancellation
POST   /api/v1/batch/<id>/pause          Pause batch
POST   /api/v1/batch/<id>/resume         Resume batch
POST   /api/v1/batch/<id>/retry          Retry failed jobs
GET    /api/v1/batch                     List all batches
GET    /api/v1/job/<id>                  Get job details
GET    /api/v1/job/<id>/logs             Get job logs (paginated)

Standard response format (Section 8.1):
{
  "success": true|false,
  "data":     {...},
  "error":    {"code", "message", "details", "suggestion", "request_id"},
  "metadata": {"timestamp", "processing_time_ms"}
}
"""
from __future__ import annotations

import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from flask import Blueprint, jsonify, request

# ── path setup ────────────────────────────────────────────────────────────────
import os
_FW = Path(__file__).parent.parent.parent.parent / "edcp"
_UI = Path(__file__).parent.parent.parent
for _pp in (str(_FW), str(_UI)):
    if _pp not in sys.path:
        sys.path.insert(0, _pp)

from edcp.batch.batch_manager import BatchManager, BatchStatus, JobStatus
from edcp.validation.pre_flight import PreFlightValidator
from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger

# ── singleton instances (shared with app.py) ──────────────────────────────────
_REPORT_ROOT  = Path(os.environ.get("EDCP_REPORT_ROOT", "/tmp/edcp_reports"))
_AUDIT_ROOT   = _REPORT_ROOT / "audit"
_audit_logger = EnterpriseAuditLogger(_AUDIT_ROOT)
_batch_manager = BatchManager(
    report_root  = _REPORT_ROOT,
    audit_logger = _audit_logger,
)
_validator = PreFlightValidator()

# ── blueprint ─────────────────────────────────────────────────────────────────
v1 = Blueprint("v1", __name__, url_prefix="/api/v1")


# ── helpers ───────────────────────────────────────────────────────────────────

def _ok(data: Any, status: int = 200, t0: float = None) -> tuple:
    ms = int((time.time() - (t0 or time.time())) * 1000)
    return jsonify({
        "success":  True,
        "data":     data,
        "metadata": {
            "timestamp":         datetime.now(timezone.utc).isoformat(),
            "processing_time_ms": ms,
        },
    }), status


def _err(code: str, message: str, details=None, suggestion: str = "", status: int = 400) -> tuple:
    return jsonify({
        "success": False,
        "error": {
            "code":       code,
            "message":    message,
            "details":    details or [],
            "suggestion": suggestion,
            "request_id": f"req_{uuid.uuid4().hex[:8]}",
        },
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "processing_time_ms": 0,
        },
    }), status


def _get_batch_or_404(batch_id: str):
    try:
        return _batch_manager.get_batch(batch_id), None
    except KeyError:
        return None, _err("BATCH_NOT_FOUND", f"Batch '{batch_id}' not found.", status=404)


# ─────────────────────────────────────────────────────────────────────────────
#  BATCH ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@v1.route("/batch", methods=["POST"])
def create_batch():
    """POST /api/v1/batch — Create and submit a new comparison batch."""
    t0 = time.time()
    body = request.get_json(force=True, silent=True) or {}

    comparisons = body.get("comparisons", [])
    config      = body.get("config", {})

    if not comparisons:
        return _err("INVALID_REQUEST", "comparisons list is required and must not be empty.",
                    suggestion="Provide at least 1 comparison spec with prod_path and dev_path.")

    # Pre-flight validation (F-VALID-001 to F-VALID-006)
    errors = _validator.validate_batch(comparisons)
    if errors:
        return _err(
            "VALIDATION_FAILED",
            f"{len(errors)} validation error(s) found. Batch not started.",
            details=[e.to_dict() for e in errors],
            suggestion="Fix all validation errors and resubmit.",
        )

    try:
        batch = _batch_manager.create_batch(comparisons, config)
    except ValueError as e:
        return _err("INVALID_REQUEST", str(e))
    except RuntimeError as e:
        return _err("CAPACITY_EXCEEDED", str(e),
                    suggestion="Wait for a running batch to complete.",
                    status=429)

    # Start asynchronously
    _batch_manager.start_batch(batch.batch_id)

    # Estimated completion (rough)
    avg_job_seconds = 30
    jobs_count = batch.total_jobs
    if config.get("execution_mode") == "parallel":
        est_s = (jobs_count / max(config.get("max_workers", 4), 1)) * avg_job_seconds
    else:
        est_s = jobs_count * avg_job_seconds
    from datetime import timedelta
    eta = (datetime.now(timezone.utc) + timedelta(seconds=est_s)).isoformat()

    return _ok({
        "batch_id":             batch.batch_id,
        "status":               batch.status.value,
        "jobs_queued":          batch.total_jobs,
        "estimated_completion": eta,
        "check_status_url":     f"/api/v1/batch/{batch.batch_id}",
    }, status=202, t0=t0)


@v1.route("/batch", methods=["GET"])
def list_batches():
    """GET /api/v1/batch — List all batches (paginated, filterable)."""
    t0     = time.time()
    status = request.args.get("status")
    limit  = min(int(request.args.get("limit", 50)), 100)
    offset = int(request.args.get("offset", 0))

    batches, total = _batch_manager.list_batches(status=status, limit=limit, offset=offset)
    return _ok({
        "batches":    [b.to_dict() for b in batches],
        "total":      total,
        "limit":      limit,
        "offset":     offset,
        "next_offset": offset + limit if offset + limit < total else None,
    }, t0=t0)


@v1.route("/batch/<batch_id>", methods=["GET"])
def get_batch(batch_id: str):
    """GET /api/v1/batch/<id> — Get batch status and all jobs."""
    t0 = time.time()
    batch, err = _get_batch_or_404(batch_id)
    if err: return err
    return _ok(batch.to_dict(), t0=t0)


@v1.route("/batch/<batch_id>/cancel-preview", methods=["GET"])
def cancel_preview(batch_id: str):
    """GET /api/v1/batch/<id>/cancel-preview — Preview for confirmation popup."""
    t0 = time.time()
    batch, err = _get_batch_or_404(batch_id)
    if err: return err
    try:
        preview = _batch_manager.cancel_preview(batch_id)
        return _ok(preview, t0=t0)
    except Exception as e:
        return _err("PREVIEW_FAILED", str(e))


@v1.route("/batch/<batch_id>/cancel", methods=["POST"])
def cancel_batch(batch_id: str):
    """POST /api/v1/batch/<id>/cancel — Cancel a running batch."""
    t0   = time.time()
    body = request.get_json(force=True, silent=True) or {}
    reason = body.get("reason", "User requested cancellation")

    batch, err = _get_batch_or_404(batch_id)
    if err: return err

    try:
        batch = _batch_manager.cancel_batch(batch_id, reason)
    except ValueError as e:
        return _err("INVALID_STATE", str(e),
                    suggestion="Only RUNNING or QUEUED batches can be cancelled.")

    return _ok({
        "batch_id":                  batch_id,
        "status":                    "CANCELLING",
        "jobs_stopped":              len(batch.running_jobs),
        "jobs_completed_preserved":  batch.successful_jobs,
        "cancel_reason":             reason,
        "estimated_completion":      "CANCELLED",
    }, t0=t0)


@v1.route("/batch/<batch_id>/pause", methods=["POST"])
def pause_batch(batch_id: str):
    """POST /api/v1/batch/<id>/pause — Pause a running batch."""
    t0 = time.time()
    batch, err = _get_batch_or_404(batch_id)
    if err: return err
    try:
        batch = _batch_manager.pause_batch(batch_id)
    except ValueError as e:
        return _err("INVALID_STATE", str(e), status=409)
    return _ok({
        "batch_id":  batch_id,
        "status":    "PAUSED",
        "paused_at": batch.paused_at.isoformat() if batch.paused_at else None,
    }, t0=t0)


@v1.route("/batch/<batch_id>/resume", methods=["POST"])
def resume_batch(batch_id: str):
    """POST /api/v1/batch/<id>/resume — Resume a paused batch."""
    t0   = time.time()
    body = request.get_json(force=True, silent=True) or {}
    retry_failed  = bool(body.get("retry_failed", False))
    batch, err = _get_batch_or_404(batch_id)
    if err: return err
    try:
        batch = _batch_manager.resume_batch(batch_id, retry_failed=retry_failed)
    except ValueError as e:
        return _err("INVALID_STATE", str(e), status=409)
    return _ok({
        "batch_id":       batch_id,
        "status":         batch.status.value,
        "jobs_remaining": len(batch.queued_jobs) + len(batch.running_jobs),
    }, t0=t0)


@v1.route("/batch/<batch_id>/retry", methods=["POST"])
def retry_batch(batch_id: str):
    """POST /api/v1/batch/<id>/retry — Retry specific failed jobs."""
    t0   = time.time()
    body = request.get_json(force=True, silent=True) or {}
    job_ids      = body.get("job_ids", [])
    max_attempts = int(body.get("max_attempts", 3))

    batch, err = _get_batch_or_404(batch_id)
    if err: return err

    if not job_ids:
        # Retry all failed jobs
        job_ids = [j.job_id for j in batch.jobs if j.status == JobStatus.FAILED]

    if not job_ids:
        return _err("NO_FAILED_JOBS", "No failed jobs found to retry.")

    try:
        batch = _batch_manager.retry_jobs(batch_id, job_ids, max_attempts)
    except ValueError as e:
        return _err("RETRY_FAILED", str(e))

    return _ok({
        "batch_id":    batch_id,
        "jobs_retried": len(job_ids),
        "status_url":  f"/api/v1/batch/{batch_id}",
    }, t0=t0)


# ─────────────────────────────────────────────────────────────────────────────
#  JOB ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@v1.route("/job/<job_id>", methods=["GET"])
def get_job(job_id: str):
    """GET /api/v1/job/<id> — Get full job details."""
    t0 = time.time()
    # Search all batches for this job
    batches, _ = _batch_manager.list_batches(limit=1000)
    for batch in batches:
        for job in batch.jobs:
            if job.job_id == job_id:
                d = job.to_dict()
                d["logs_url"] = f"/api/v1/job/{job_id}/logs"
                return _ok(d, t0=t0)
    return _err("JOB_NOT_FOUND", f"Job '{job_id}' not found.", status=404)


@v1.route("/job/<job_id>/logs", methods=["GET"])
def get_job_logs(job_id: str):
    """GET /api/v1/job/<id>/logs — Get paginated job logs."""
    t0     = time.time()
    limit  = min(int(request.args.get("limit", 50)), 500)
    offset = int(request.args.get("offset", 0))
    level  = request.args.get("level", "").upper()

    batches, _ = _batch_manager.list_batches(limit=1000)
    for batch in batches:
        for job in batch.jobs:
            if job.job_id == job_id:
                logs = job.logs
                if level:
                    logs = [l for l in logs if l.get("level","").upper() == level]
                total = len(logs)
                page  = logs[offset: offset + limit]
                return _ok({
                    "job_id":      job_id,
                    "logs":        page,
                    "total":       total,
                    "limit":       limit,
                    "offset":      offset,
                    "next_offset": offset + limit if offset + limit < total else None,
                }, t0=t0)
    return _err("JOB_NOT_FOUND", f"Job '{job_id}' not found.", status=404)


@v1.route("/job/<job_id>/skip", methods=["POST"])
def skip_job(job_id: str):
    """POST /api/v1/job/<id>/skip — Mark a failed job as skipped."""
    t0 = time.time()
    batches, _ = _batch_manager.list_batches(limit=1000)
    for batch in batches:
        for job in batch.jobs:
            if job.job_id == job_id:
                if job.status not in (JobStatus.FAILED, JobStatus.QUEUED):
                    return _err("INVALID_STATE",
                                f"Job {job_id} cannot be skipped (status={job.status})", status=409)
                job.status = JobStatus.SKIPPED
                return _ok({"job_id": job_id, "status": "SKIPPED"}, t0=t0)
    return _err("JOB_NOT_FOUND", f"Job '{job_id}' not found.", status=404)


# ─────────────────────────────────────────────────────────────────────────────
#  SYSTEM ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@v1.route("/health", methods=["GET"])
def health():
    """GET /api/v1/health — Health check."""
    return _ok({
        "status":  "ok",
        "version": "2.0.0",
        "platform":"DataComparePro",
    })


@v1.route("/config/schema", methods=["GET"])
def config_schema():
    """GET /api/v1/config/schema — Return the supported YAML config schema."""
    return _ok({
        "version": "1.0",
        "schema": {
            "comparisons": "list of comparison specs",
            "execution": {
                "mode":             "sequential | parallel",
                "max_workers":      "int 1-10 (parallel mode)",
                "timeout_seconds":  "int",
            },
            "retry": {
                "max_attempts":     "int (default 3)",
                "backoff_strategy": "exponential",
            },
        },
        "example_yaml": """
version: "1.0"
name: "Monthly Validation"
comparisons:
  - name: "Customer Data"
    prod_path: "/data/prod/customers.parquet"
    dev_path:  "/data/dev/customers.parquet"
    keys: ["customer_id"]
    ignore_columns: ["last_updated"]
    tolerance:
      account_balance: 0.01
execution:
  mode: "parallel"
  max_workers: 3
  timeout_seconds: 1800
retry:
  max_attempts: 3
  backoff_strategy: "exponential"
""",
    })


# Export the singleton batch manager for use in app.py SSE
def get_batch_manager() -> BatchManager:
    return _batch_manager
