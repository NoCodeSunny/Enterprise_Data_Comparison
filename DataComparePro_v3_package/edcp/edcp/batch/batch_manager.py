# -*- coding: utf-8 -*-
"""
edcp.batch.batch_manager
─────────────────────────
BatchManager — orchestrates multi-job comparison batches.

Responsibilities
----------------
- Create and track batches (1-20 jobs each)
- Sort jobs by file size (small → large per F-BATCH-002)
- Support sequential and parallel execution modes
- Cancel running batches with graceful shutdown
- Pause and resume batches
- Preserve completed job results on cancel
- Enforce max 5 concurrent batches (NFR-PERF-006)
- Log all actions to EnterpriseAuditLogger

Data model
----------
Batch 1..* Job
Each Job wraps one ComparisonJob from the core engine.
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import json
import sqlite3

from edcp.utils.logger import get_logger

logger = get_logger(__name__)

MAX_BATCHES_CONCURRENT = 5
MAX_JOBS_PER_BATCH     = 20
MIN_JOBS_PER_BATCH     = 1


# ── Status enumerations ───────────────────────────────────────────────────────

class BatchStatus(str, Enum):
    PENDING     = "PENDING"
    VALIDATING  = "VALIDATING"
    QUEUED      = "QUEUED"
    RUNNING     = "RUNNING"
    PAUSED      = "PAUSED"
    CANCELLING  = "CANCELLING"
    CANCELLED   = "CANCELLED"
    SUCCESS     = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED      = "FAILED"

class JobStatus(str, Enum):
    PENDING     = "PENDING"
    VALIDATING  = "VALIDATING"
    QUEUED      = "QUEUED"
    RUNNING     = "RUNNING"
    SUCCESS     = "SUCCESS"
    FAILED      = "FAILED"
    CANCELLED   = "CANCELLED"
    SKIPPED     = "SKIPPED"
    RETRYING    = "RETRYING"


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class JobRecord:
    """Represents one comparison job within a batch."""
    job_id:          str
    batch_id:        str
    prod_path:       str
    dev_path:        str
    keys:            List[str]            = field(default_factory=list)
    ignore_columns:  List[str]            = field(default_factory=list)
    tolerance:       Dict[str, float]     = field(default_factory=dict)
    status:          JobStatus            = JobStatus.PENDING
    progress:        float                = 0.0
    engine_used:     str                  = ""
    file_size_mb:    float                = 0.0
    priority:        str                  = "MEDIUM"   # HIGH / MEDIUM / LOW
    started_at:      Optional[datetime]   = None
    completed_at:    Optional[datetime]   = None
    duration_sec:    Optional[float]       = None
    records_compared: int                 = 0
    differences_found: int                = 0
    error_message:   str                  = ""
    error_code:      str                  = ""
    retry_count:     int                  = 0
    max_retries:     int                  = 3
    result_path:     str                  = ""
    logs:            List[Dict[str, Any]] = field(default_factory=list)
    _cancel_flag:    bool                 = field(default=False, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id":           self.job_id,
            "batch_id":         self.batch_id,
            "status":           self.status.value,
            "progress":         round(self.progress, 1),
            "priority":         self.priority,
            "engine_used":      self.engine_used,
            "file_size_mb":     round(self.file_size_mb, 2),
            "files":            {"prod": self.prod_path, "dev": self.dev_path},
            "keys":             self.keys,
            "ignore_columns":   self.ignore_columns,
            "tolerance":        self.tolerance,
            "started_at":       self.started_at.isoformat() if self.started_at else None,
            "completed_at":     self.completed_at.isoformat() if self.completed_at else None,
            "duration_sec":     round(self.duration_sec, 2) if self.duration_sec is not None else None,
            "records_compared": self.records_compared,
            "differences_found":self.differences_found,
            "error_message":    self.error_message,
            "error_code":       self.error_code,
            "retry_count":      self.retry_count,
            "result_path":      self.result_path,
        }


@dataclass
class BatchRecord:
    """Represents a batch of 1-20 comparison jobs."""
    batch_id:       str
    created_at:     datetime
    status:         BatchStatus           = BatchStatus.PENDING
    execution_mode: str                   = "sequential"   # sequential | parallel
    max_workers:    int                   = 4
    timeout_seconds: int                  = 3600
    jobs:           List[JobRecord]       = field(default_factory=list)
    cancel_reason:  str                   = ""
    paused_at:      Optional[datetime]    = None
    started_at:     Optional[datetime]    = None
    completed_at:   Optional[datetime]    = None
    _cancel_event:  threading.Event       = field(default_factory=threading.Event, repr=False)
    _pause_event:   threading.Event       = field(default_factory=threading.Event, repr=False)

    def __post_init__(self):
        self._cancel_event = threading.Event()
        self._pause_event  = threading.Event()
        self._pause_event.set()   # unset = paused; set = running

    @property
    def total_jobs(self) -> int:
        return len(self.jobs)

    @property
    def completed_jobs(self) -> int:
        return sum(1 for j in self.jobs if j.status in (
            JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.SKIPPED))

    @property
    def successful_jobs(self) -> int:
        return sum(1 for j in self.jobs if j.status == JobStatus.SUCCESS)

    @property
    def failed_jobs(self) -> int:
        return sum(1 for j in self.jobs if j.status == JobStatus.FAILED)

    @property
    def running_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if j.status == JobStatus.RUNNING]

    @property
    def queued_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if j.status in (JobStatus.QUEUED, JobStatus.PENDING)]

    @property
    def progress(self) -> float:
        if not self.jobs: return 0.0
        done = sum(j.progress for j in self.jobs)
        return round(done / len(self.jobs), 1)

    @property
    def estimated_remaining(self) -> str:
        completed = self.completed_jobs
        if completed == 0: return "calculating..."
        if not self.started_at: return "—"
        elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        rate = completed / elapsed if elapsed > 0 else 0
        if rate == 0: return "—"
        remaining = (self.total_jobs - completed) / rate
        mins, secs = divmod(int(remaining), 60)
        return f"{mins}m {secs}s" if mins else f"{secs}s"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id":         self.batch_id,
            "status":           self.status.value,
            "execution_mode":   self.execution_mode,
            "max_workers":      self.max_workers,
            "created_at":       self.created_at.isoformat(),
            "started_at":       self.started_at.isoformat() if self.started_at else None,
            "completed_at":     self.completed_at.isoformat() if self.completed_at else None,
            "cancel_reason":    self.cancel_reason,
            "progress":         self.progress,
            "estimated_remaining": self.estimated_remaining,
            "jobs": {
                "total":     self.total_jobs,
                "completed": self.completed_jobs,
                "successful":self.successful_jobs,
                "failed":    self.failed_jobs,
                "running":   len(self.running_jobs),
                "queued":    len(self.queued_jobs),
            },
            "jobs_detail": [j.to_dict() for j in self.jobs],
        }


# ── Batch Manager ─────────────────────────────────────────────────────────────

class BatchManager:
    """
    Central orchestrator for multi-job batches.

    Thread-safe: all state mutations are protected by _lock.
    """

    def __init__(
        self,
        report_root: Path,
        audit_logger: Optional[Any] = None,
        on_job_update: Optional[Callable] = None,
        db_path: Optional[Path] = None,
    ):
        self._report_root  = report_root
        self._audit        = audit_logger
        self._on_job_update= on_job_update   # callback for SSE push
        self._batches: Dict[str, BatchRecord] = {}
        self._lock         = threading.Lock()
        self._executors: Dict[str, ThreadPoolExecutor] = {}
        # SQLite persistence — survives server restarts
        self._db_path = db_path or (report_root / "batch_state.db")
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._load_persisted_batches()

    # ── SQLite persistence ───────────────────────────────────────────────────

    def _init_db(self) -> None:
        """Create the batch_state table if it doesn't exist."""
        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batch_state (
                    batch_id   TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    status     TEXT NOT NULL,
                    snapshot   TEXT NOT NULL
                )
            """)
            conn.commit()

    def _persist_batch(self, batch: BatchRecord) -> None:
        """Persist batch snapshot to SQLite (best-effort, non-blocking)."""
        try:
            snapshot = json.dumps(batch.to_dict(), default=str)
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO batch_state (batch_id, created_at, status, snapshot)
                    VALUES (?, ?, ?, ?)
                """, (batch.batch_id, batch.created_at.isoformat(), batch.status.value, snapshot))
                conn.commit()
        except Exception as exc:
            logger.warning(f"[BatchManager] Could not persist batch {batch.batch_id}: {exc}")

    def _load_persisted_batches(self) -> None:
        """Load completed batch summaries from SQLite on startup (for history queries)."""
        try:
            with sqlite3.connect(str(self._db_path)) as conn:
                rows = conn.execute(
                    "SELECT batch_id, status FROM batch_state ORDER BY created_at DESC LIMIT 200"
                ).fetchall()
            logger.info(f"[BatchManager] Found {len(rows)} persisted batch records in DB")
        except Exception as exc:
            logger.warning(f"[BatchManager] Could not load persisted batches: {exc}")

    def get_batch_history(self, limit: int = 50, offset: int = 0) -> list:
        """Return persisted batch summaries from SQLite (includes restarts)."""
        try:
            with sqlite3.connect(str(self._db_path)) as conn:
                rows = conn.execute(
                    "SELECT snapshot FROM batch_state ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (limit, offset)
                ).fetchall()
            return [json.loads(r[0]) for r in rows]
        except Exception:
            return []

    # ── public API ────────────────────────────────────────────────────────────

    def create_batch(
        self,
        comparisons: List[Dict[str, Any]],
        config:      Dict[str, Any],
    ) -> BatchRecord:
        """
        Create a new batch from a list of comparison specs.

        comparisons: list of dicts with prod_path, dev_path, keys, etc.
        config:      execution_mode, max_workers, timeout_seconds, retry.max_attempts
        """
        with self._lock:
            if sum(1 for b in self._batches.values()
                   if b.status in (BatchStatus.RUNNING, BatchStatus.QUEUED,
                                   BatchStatus.PENDING)) >= MAX_BATCHES_CONCURRENT:
                raise RuntimeError(
                    f"Maximum {MAX_BATCHES_CONCURRENT} concurrent batches reached. "
                    "Please wait for a batch to complete before submitting a new one."
                )

        n = len(comparisons)
        if not (MIN_JOBS_PER_BATCH <= n <= MAX_JOBS_PER_BATCH):
            raise ValueError(
                f"Batch must contain {MIN_JOBS_PER_BATCH}–{MAX_JOBS_PER_BATCH} jobs. Got {n}."
            )

        ts   = datetime.now(timezone.utc)
        bid  = f"BATCH_{ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6].upper()}"

        batch = BatchRecord(
            batch_id       = bid,
            created_at     = ts,
            status         = BatchStatus.PENDING,
            execution_mode = config.get("execution_mode", "sequential"),
            max_workers    = min(int(config.get("max_workers", 4)), 10),
            timeout_seconds= int(config.get("timeout_seconds", 3600)),
        )

        max_attempts = config.get("retry", {}).get("max_attempts", 3)

        # Build jobs, sorted by file size small → large (F-BATCH-002)
        jobs = []
        for i, spec in enumerate(comparisons):
            prod_p = spec.get("prod_path", "")
            dev_p  = spec.get("dev_path",  "")
            size   = self._get_file_size_mb(prod_p)
            priority = self._size_to_priority(size)
            jid    = f"{bid}_JOB_{i+1:03d}"
            job    = JobRecord(
                job_id        = jid,
                batch_id      = bid,
                prod_path     = prod_p,
                dev_path      = dev_p,
                keys          = spec.get("keys", []),
                ignore_columns= spec.get("ignore_columns", []),
                tolerance     = spec.get("tolerance", {}),
                file_size_mb  = size,
                priority      = priority,
                max_retries   = max_attempts,
                status        = JobStatus.QUEUED,
            )
            jobs.append(job)

        # Sort: small files first (F-BATCH-002), then by priority tier
        jobs.sort(key=lambda j: j.file_size_mb)
        batch.jobs = jobs

        with self._lock:
            self._batches[bid] = batch

        if self._audit:
            self._audit.log_batch_created(batch)

        logger.info(f"[BatchManager] Created {bid} with {n} jobs")
        self._persist_batch(batch)
        return batch

    def start_batch(self, batch_id: str) -> None:
        """Start executing a batch in a background thread."""
        batch = self._get_batch(batch_id)
        batch.status     = BatchStatus.RUNNING
        batch.started_at = datetime.now(timezone.utc)

        t = threading.Thread(
            target=self._execute_batch,
            args=(batch,),
            daemon=True,
            name=f"batch-{batch_id}",
        )
        t.start()

    def cancel_batch(self, batch_id: str, reason: str = "") -> BatchRecord:
        """
        Cancel a running/queued batch.
        - Signals running jobs to stop
        - Removes queued jobs
        - Preserves completed job results
        """
        batch = self._get_batch(batch_id)

        if batch.status in (BatchStatus.CANCELLED, BatchStatus.SUCCESS,
                             BatchStatus.FAILED, BatchStatus.PARTIAL_SUCCESS):
            raise ValueError(f"Batch {batch_id} is already {batch.status} — cannot cancel.")

        batch.cancel_reason = reason or "User requested cancellation"
        batch.status = BatchStatus.CANCELLING
        batch._cancel_event.set()   # signal all workers

        # Mark queued jobs as cancelled immediately
        for job in batch.queued_jobs:
            job.status = JobStatus.CANCELLED

        logger.info(f"[BatchManager] Cancellation signalled for {batch_id}")

        if self._audit:
            self._audit.log_batch_cancelled(batch, reason)

        return batch

    def cancel_preview(self, batch_id: str) -> Dict[str, Any]:
        """Return what would be cancelled — for the confirmation popup."""
        batch = self._get_batch(batch_id)
        return {
            "batch_id":    batch_id,
            "running_jobs": [
                {"job_id": j.job_id, "file": Path(j.prod_path).name,
                 "progress": j.progress}
                for j in batch.running_jobs
            ],
            "queued_count": len(batch.queued_jobs),
            "completed_jobs": [
                {"job_id": j.job_id, "file": Path(j.prod_path).name}
                for j in batch.jobs
                if j.status in (JobStatus.SUCCESS,)
            ],
        }

    def pause_batch(self, batch_id: str) -> BatchRecord:
        """Pause a running batch — queued jobs wait, running jobs finish."""
        batch = self._get_batch(batch_id)
        if batch.status != BatchStatus.RUNNING:
            raise ValueError(f"Batch {batch_id} is not RUNNING (status={batch.status})")
        batch._pause_event.clear()   # workers will block on this
        batch.status   = BatchStatus.PAUSED
        batch.paused_at= datetime.now(timezone.utc)
        logger.info(f"[BatchManager] Paused {batch_id}")
        return batch

    def resume_batch(self, batch_id: str, retry_failed: bool = False) -> BatchRecord:
        """Resume a paused batch."""
        batch = self._get_batch(batch_id)
        if batch.status != BatchStatus.PAUSED:
            raise ValueError(f"Batch {batch_id} is not PAUSED")
        if retry_failed:
            for job in batch.jobs:
                if job.status == JobStatus.FAILED:
                    job.status    = JobStatus.QUEUED
                    job.retry_count = 0
        batch._pause_event.set()
        batch.status   = BatchStatus.RUNNING
        batch.paused_at= None
        logger.info(f"[BatchManager] Resumed {batch_id}")
        return batch

    def retry_jobs(self, batch_id: str, job_ids: List[str], max_attempts: int = 3) -> BatchRecord:
        """
        Reset specified failed jobs and restart batch execution.

        Works even if the batch has already reached a terminal state
        (SUCCESS, FAILED, PARTIAL_SUCCESS, CANCELLED).

        Raises
        ------
        ValueError  — if none of the given IDs correspond to FAILED jobs.
        """
        batch = self._get_batch(batch_id)
        id_set = set(job_ids)
        to_retry = [j for j in batch.jobs if j.job_id in id_set and j.status == JobStatus.FAILED]
        if not to_retry:
            raise ValueError(
                f"No FAILED jobs found matching the given IDs in batch {batch_id}. "
                f"Available job statuses: "
                f"{[(j.job_id, j.status.value) for j in batch.jobs if j.job_id in id_set]}"
            )
        # Reset each failed job cleanly
        from datetime import datetime, timezone
        for job in to_retry:
            job.status        = JobStatus.QUEUED
            job.retry_count   = 0
            job.max_retries   = max_attempts
            job.progress      = 0.0
            job.error_message = ""
            job.started_at    = None
            job.completed_at  = None
            job.duration_sec  = None
            job.differences_found = 0
            job.records_compared  = 0

        # Reset batch to allow re-execution even from a terminal state
        batch.status       = BatchStatus.PENDING
        batch.completed_at = None
        batch._cancel_event.clear()

        # Persist the reset state
        self._persist_batch(batch)

        # Re-launch execution thread
        import threading as _threading
        t = _threading.Thread(target=self._execute_batch, args=(batch,), daemon=True)
        t.start()

        logger.info(
            f"[BatchManager] Retry started for {len(to_retry)} job(s) "
            f"in batch {batch_id}"
        )
        return batch

    def get_batch(self, batch_id: str) -> BatchRecord:
        return self._get_batch(batch_id)

    def list_batches(
        self, status: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> Tuple[List[BatchRecord], int]:
        with self._lock:
            all_batches = sorted(
                self._batches.values(),
                key=lambda b: b.created_at,
                reverse=True,
            )
        if status:
            all_batches = [b for b in all_batches if b.status.value == status.upper()]
        total = len(all_batches)
        return all_batches[offset: offset + limit], total

    # ── internal execution ────────────────────────────────────────────────────

    def _execute_batch(self, batch: BatchRecord) -> None:
        """Main execution loop — runs in a dedicated thread."""
        try:
            if batch.execution_mode == "parallel":
                self._run_parallel(batch)
            else:
                self._run_sequential(batch)
        except Exception as exc:
            logger.exception(f"[BatchManager] Unhandled error in {batch.batch_id}: {exc}")
            batch.status = BatchStatus.FAILED
        finally:
            batch.completed_at = datetime.now(timezone.utc)
            # Determine final status — cancel_event takes precedence
            if batch._cancel_event.is_set():
                batch.status = BatchStatus.CANCELLED
            elif batch.status not in (BatchStatus.CANCELLED, BatchStatus.FAILED):
                if batch.failed_jobs == 0:
                    batch.status = BatchStatus.SUCCESS
                elif batch.successful_jobs > 0:
                    batch.status = BatchStatus.PARTIAL_SUCCESS
                else:
                    batch.status = BatchStatus.FAILED

            if self._audit:
                self._audit.log_batch_completed(batch)

            self._persist_batch(batch)
            # Finalize audit log (make immutable) on terminal state
            if self._audit and batch.status in (
                BatchStatus.SUCCESS, BatchStatus.FAILED,
                BatchStatus.PARTIAL_SUCCESS, BatchStatus.CANCELLED,
            ):
                try:
                    self._audit.finalize_batch(batch.batch_id)
                except Exception:
                    pass   # best-effort
            logger.info(
                f"[BatchManager] {batch.batch_id} → {batch.status} "
                f"({batch.successful_jobs}/{batch.total_jobs} succeeded)"
            )

    def _run_sequential(self, batch: BatchRecord) -> None:
        for job in batch.jobs:
            if batch._cancel_event.is_set():
                if job.status == JobStatus.QUEUED:
                    job.status = JobStatus.CANCELLED
                continue
            # Pause support
            batch._pause_event.wait()
            self._execute_job(batch, job)

    def _run_parallel(self, batch: BatchRecord) -> None:
        from edcp.batch.queue_controller import QueueController
        qc = QueueController(batch.jobs)
        futures: Dict[Future, JobRecord] = {}

        with ThreadPoolExecutor(max_workers=batch.max_workers,
                                thread_name_prefix=f"job-{batch.batch_id}") as ex:
            # Submit in priority order
            for job in qc.drain():
                if batch._cancel_event.is_set():
                    job.status = JobStatus.CANCELLED
                    continue
                fut = ex.submit(self._execute_job, batch, job)
                futures[fut] = job

            for fut in as_completed(futures):
                if batch._cancel_event.is_set():
                    break

    def _execute_job(self, batch: BatchRecord, job: JobRecord) -> None:
        """Execute a single job using the core ComparisonJob engine."""
        from edcp.batch.recovery_manager import RecoveryManager
        from edcp.jobs.comparison_job import ComparisonJob
        from edcp.loaders.file_loader import load_any_to_csv

        recovery = RecoveryManager()

        for attempt in range(job.max_retries + 1):
            if batch._cancel_event.is_set():
                job.status = JobStatus.CANCELLED
                return

            if attempt > 0:
                job.status      = JobStatus.RETRYING
                delay           = recovery.backoff_seconds(attempt)
                job.retry_count = attempt
                logger.info(f"[{job.job_id}] Retry {attempt}/{job.max_retries} in {delay}s")
                time.sleep(delay)

            # Pause check
            batch._pause_event.wait()

            job.status     = JobStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)
            job.progress   = 0.0
            self._notify(batch, job)

            try:
                report_root = self._report_root / batch.batch_id / job.job_id
                report_root.mkdir(parents=True, exist_ok=True)

                conv_dir = report_root / "converted"
                conv_dir.mkdir(exist_ok=True)

                # Determine engine
                from edcp.batch.engine_selector import select_job_engine
                engine_name = select_job_engine(job.prod_path, job.dev_path)
                job.engine_used = engine_name

                # Build tolerance map
                tol_map = {}
                prod_name = Path(job.prod_path).name
                dev_name  = Path(job.dev_path).name
                for col, val in job.tolerance.items():
                    tol_map[(prod_name, dev_name, col)] = val

                prod_csv = load_any_to_csv(Path(job.prod_path), conv_dir)
                dev_csv  = load_any_to_csv(Path(job.dev_path),  conv_dir)

                job.progress = 10.0
                self._notify(batch, job)

                cjob = ComparisonJob(
                    prod_path     = prod_csv,
                    dev_path      = dev_csv,
                    prod_name     = prod_name,
                    dev_name      = dev_name,
                    result_name   = job.job_id,
                    report_root   = report_root,
                    keys          = job.keys,
                    ignore_fields = job.ignore_columns,
                    tol_map       = tol_map,
                    max_retries   = 0,   # retry handled at batch level
                    config        = {"use_spark": engine_name == "spark"},
                )

                result = cjob.run()

                job.progress          = 100.0
                job.completed_at      = datetime.now(timezone.utc)
                job.duration_sec      = (job.completed_at - job.started_at).total_seconds()
                job.records_compared  = (result.summary.get("TotalProd", 0)
                                         if result.summary else 0)
                job.differences_found = (result.summary.get("MatchedFailed", 0)
                                         if result.summary else 0)
                job.result_path       = str(result.report_path or "")

                if result.succeeded:
                    job.status = JobStatus.SUCCESS
                    if self._audit:
                        self._audit.log_job_completed(batch, job)
                    self._notify(batch, job)
                    return   # success — no retry needed
                else:
                    err = result.error or "Unknown failure"
                    # Classify retry-ability
                    if recovery.is_retryable(err) and attempt < job.max_retries:
                        job.error_message = err
                        continue   # retry
                    job.status        = JobStatus.FAILED
                    job.error_message = err
                    if result.debug_report:
                        job.error_code = result.debug_report.error_record.error_code
                    break

            except Exception as exc:
                err = str(exc)
                if recovery.is_retryable(err) and attempt < job.max_retries:
                    job.error_message = err
                    continue
                job.status        = JobStatus.FAILED
                job.error_message = err
                job.completed_at  = datetime.now(timezone.utc)
                if job.started_at:
                    job.duration_sec = (job.completed_at - job.started_at).total_seconds()
                break

        # If we exit the loop without returning, it's a failure
        if job.status not in (JobStatus.SUCCESS, JobStatus.CANCELLED):
            job.status = JobStatus.FAILED

        if self._audit:
            self._audit.log_job_completed(batch, job)
        self._notify(batch, job)

    def _notify(self, batch: BatchRecord, job: JobRecord) -> None:
        if self._on_job_update:
            try:
                self._on_job_update(batch.batch_id, job.job_id, job.to_dict())
            except Exception:
                pass

    def _get_batch(self, batch_id: str) -> BatchRecord:
        with self._lock:
            batch = self._batches.get(batch_id)
        if not batch:
            raise KeyError(f"Batch '{batch_id}' not found.")
        return batch

    @staticmethod
    def _get_file_size_mb(path: str) -> float:
        try:
            return Path(path).stat().st_size / (1024 * 1024)
        except OSError:
            return 0.0

    @staticmethod
    def _size_to_priority(size_mb: float) -> str:
        if size_mb < 100:    return "HIGH"
        if size_mb < 500:    return "MEDIUM"
        return "LOW"
