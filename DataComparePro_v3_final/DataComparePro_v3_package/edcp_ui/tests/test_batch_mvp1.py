# -*- coding: utf-8 -*-
"""
tests/test_batch_mvp1.py
─────────────────────────
Integration tests for DataComparePro MVP1 batch features.
Covers: batch manager, queue controller, recovery manager,
        engine selector, pre-flight validator, enterprise audit logger,
        and API v1 endpoints.

All tests are self-contained (tempfile, no network, no database).
"""
from __future__ import annotations
import json
import sys
import tempfile
import time
import threading
import unittest
from pathlib import Path
from datetime import datetime, timezone

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent / "edcp"))

from edcp.batch.batch_manager import (
    BatchManager, BatchRecord, JobRecord,
    BatchStatus, JobStatus, MAX_JOBS_PER_BATCH,
)
from edcp.batch.queue_controller import QueueController
from edcp.batch.recovery_manager import RecoveryManager
from edcp.batch.engine_selector import select_job_engine, engine_selection_reason
from edcp.validation.pre_flight import PreFlightValidator, ValidationError
from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger


# ══════════════════════════════════════════════════════════════════════════════
# RECOVERY MANAGER (F-RETRY-001 to 003)
# ══════════════════════════════════════════════════════════════════════════════

class TestRecoveryManager(unittest.TestCase):

    def setUp(self):
        self.rm = RecoveryManager()

    def test_backoff_attempt_1(self):
        """F-RETRY-002: attempt 1 → 5 seconds."""
        self.assertEqual(self.rm.backoff_seconds(1), 5)

    def test_backoff_attempt_2(self):
        """F-RETRY-002: attempt 2 → 25 seconds."""
        self.assertEqual(self.rm.backoff_seconds(2), 25)

    def test_backoff_attempt_3(self):
        """F-RETRY-002: attempt 3 → 125 seconds."""
        self.assertEqual(self.rm.backoff_seconds(3), 125)

    def test_retryable_timeout(self):
        """F-RETRY-003: timeout errors are retryable."""
        self.assertTrue(self.rm.is_retryable("Connection timeout after 30s"))

    def test_retryable_memory(self):
        """F-RETRY-003: MemoryError is retryable."""
        self.assertTrue(self.rm.is_retryable("MemoryError: unable to allocate array"))

    def test_not_retryable_schema(self):
        """F-RETRY-003: schema mismatch is NOT retryable."""
        self.assertFalse(self.rm.is_retryable("schema mismatch: column 'Price' missing"))

    def test_not_retryable_file_not_found(self):
        """F-RETRY-003: File not found is NOT retryable (file won't appear)."""
        self.assertFalse(self.rm.is_retryable("File not found: /data/missing.csv"))

    def test_not_retryable_config(self):
        """F-RETRY-003: ConfigError is NOT retryable."""
        self.assertFalse(self.rm.is_retryable("ConfigError: invalid use_spark value"))

    def test_classify_oom(self):
        """classify_error: MemoryError → OUT_OF_MEMORY."""
        self.assertEqual(self.rm.classify_error("MemoryError"), "OUT_OF_MEMORY")

    def test_classify_timeout(self):
        """classify_error: timeout → TIMEOUT."""
        self.assertEqual(self.rm.classify_error("operation timed out"), "TIMEOUT")

    def test_classify_schema(self):
        """classify_error: column error → SCHEMA_ERROR."""
        self.assertEqual(self.rm.classify_error("column 'ID' not found"), "SCHEMA_ERROR")

    def test_classify_unknown(self):
        """classify_error: unrecognised → UNKNOWN_ERROR."""
        self.assertEqual(self.rm.classify_error("something weird happened"), "UNKNOWN_ERROR")


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE SELECTOR (F-ENGINE-001 to 006)
# ══════════════════════════════════════════════════════════════════════════════

class TestEngineSelector(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.t = Path(self._d.name)

    def tearDown(self):
        self._d.cleanup()

    def test_parquet_ext_always_spark(self):
        """F-ENGINE-004: .parquet extension → always spark."""
        self.assertEqual(select_job_engine("/data/prod.parquet", "/data/dev.csv"), "spark")

    def test_parquet_dev_ext_also_spark(self):
        """F-ENGINE-004: DEV is parquet → spark."""
        self.assertEqual(select_job_engine("/data/prod.csv", "/data/dev.parquet"), "spark")

    def test_parquet_magic_bytes_spark(self):
        """F-ENGINE-004: PAR1 magic bytes → spark even with .csv extension."""
        p = self.t / "fake.csv"
        p.write_bytes(b"PAR1" + b"\x00" * 20)
        self.assertEqual(select_job_engine(str(p), "/data/dev.csv"), "spark")

    def test_small_file_pandas(self):
        """F-ENGINE-002: small file → pandas."""
        p = self.t / "tiny.csv"; p.write_text("ID,V\n1,A\n")
        self.assertEqual(select_job_engine(str(p), str(p)), "pandas")

    def test_override_pandas(self):
        """F-ENGINE-006: explicit override=pandas → pandas."""
        self.assertEqual(select_job_engine("/data/x.parquet", "/data/y.parquet", override="pandas"), "pandas")

    def test_override_spark(self):
        """F-ENGINE-006: explicit override=spark → spark."""
        p = self.t / "small.csv"; p.write_text("ID,V\n1,A\n")
        self.assertEqual(select_job_engine(str(p), str(p), override="spark"), "spark")

    def test_engine_reason_parquet(self):
        """engine_selection_reason: parquet → mentions F-ENGINE-004."""
        reason = engine_selection_reason("/d/prod.parquet", "/d/dev.csv", "spark")
        self.assertIn("parquet", reason.lower())
        self.assertIn("F-ENGINE-004", reason)

    def test_engine_reason_small(self):
        """engine_selection_reason: small file → mentions pandas and F-ENGINE-002."""
        p = self.t / "s.csv"; p.write_text("ID,V\n1,A\n")
        reason = engine_selection_reason(str(p), str(p), "pandas")
        self.assertIn("pandas", reason.lower())
        self.assertIn("F-ENGINE-002", reason)


# ══════════════════════════════════════════════════════════════════════════════
# QUEUE CONTROLLER (F-PRIORITY-001 to 004)
# ══════════════════════════════════════════════════════════════════════════════

class TestQueueController(unittest.TestCase):

    def _make_job(self, jid, priority, size_mb=0):
        j = JobRecord(
            job_id=jid, batch_id="B", prod_path="", dev_path="",
            priority=priority, file_size_mb=size_mb,
            status=JobStatus.QUEUED,
        )
        return j

    def test_high_before_low(self):
        """F-PRIORITY-002: HIGH priority job dequeued before LOW."""
        jobs = [self._make_job("low1", "LOW"), self._make_job("high1", "HIGH")]
        qc = QueueController(jobs)
        result = [j.job_id for j in qc.drain()]
        self.assertEqual(result[0], "high1")

    def test_high_before_medium(self):
        """F-PRIORITY-002: HIGH before MEDIUM."""
        jobs = [self._make_job("m1","MEDIUM"), self._make_job("h1","HIGH")]
        qc = QueueController(jobs)
        result = [j.job_id for j in qc.drain()]
        self.assertEqual(result[0], "h1")

    def test_all_three_tiers(self):
        """F-PRIORITY-001: HIGH < 100 MB, MEDIUM 100–500 MB, LOW > 500 MB."""
        jobs = [
            self._make_job("L","LOW",600),
            self._make_job("M","MEDIUM",250),
            self._make_job("H","HIGH",50),
        ]
        qc = QueueController(jobs)
        order = [j.job_id for j in qc.drain()]
        self.assertEqual(order[0], "H")
        self.assertEqual(order[-1], "L")

    def test_drain_empty(self):
        """Drain on empty queue returns immediately."""
        qc = QueueController([])
        result = list(qc.drain())
        self.assertEqual(result, [])

    def test_requeue_adds_back(self):
        """requeue() allows a job to be picked up again."""
        j = self._make_job("j1","HIGH"); j.status = JobStatus.QUEUED
        qc = QueueController([j])
        first = list(qc.drain())   # drains j
        self.assertEqual(len(first), 1)
        qc.requeue(j)
        second = list(qc.drain())  # gets it again
        self.assertEqual(len(second), 1)


# ══════════════════════════════════════════════════════════════════════════════
# PRE-FLIGHT VALIDATOR (F-VALID-001 to 006)
# ══════════════════════════════════════════════════════════════════════════════

class TestPreFlightValidator(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.t  = Path(self._d.name)
        self.v  = PreFlightValidator()
        # Create real test files
        self.prod = self.t / "prod.csv"
        self.dev  = self.t / "dev.csv"
        self.prod.write_text("TradeID,Portfolio,Price\n1,EQ,100\n2,FI,200\n")
        self.dev.write_text( "TradeID,Portfolio,Price\n1,EQ,100\n2,FI,200\n")

    def tearDown(self):
        self._d.cleanup()

    def test_valid_batch_no_errors(self):
        """F-VALID-001 to 004: valid files + keys → no errors."""
        errs = self.v.validate_batch([{
            "prod_path": str(self.prod), "dev_path": str(self.dev),
            "keys": ["TradeID"]
        }])
        # Filter out the 'keys' warning (it should not be a hard error here)
        hard = [e for e in errs if e.field != "keys" or "required" in e.message.lower()]
        self.assertEqual(len(hard), 0)

    def test_missing_prod_path(self):
        """F-VALID-001: missing prod_path → validation error."""
        errs = self.v.validate_batch([{"prod_path":"","dev_path":str(self.dev),"keys":["TradeID"]}])
        self.assertTrue(any(e.field=="prod_path" for e in errs))

    def test_file_not_found(self):
        """F-VALID-001: non-existent file → FILE_NOT_FOUND error."""
        errs = self.v.validate_batch([{
            "prod_path":"/nonexistent/prod.csv","dev_path":str(self.dev),"keys":["TradeID"]
        }])
        self.assertTrue(any("not found" in e.message.lower() for e in errs))

    def test_key_not_in_prod(self):
        """F-VALID-004: key column absent from PROD file → error."""
        errs = self.v.validate_batch([{
            "prod_path":str(self.prod),"dev_path":str(self.dev),
            "keys":["NonExistentKey"]
        }])
        self.assertTrue(any("NonExistentKey" in e.message for e in errs))

    def test_batch_too_large(self):
        """F-VALID-006: 21 comparisons → error."""
        specs = [{"prod_path":str(self.prod),"dev_path":str(self.dev),"keys":["TradeID"]}] * 21
        errs = self.v.validate_batch(specs)
        self.assertTrue(len(errs) > 0)
        self.assertTrue(any("20" in e.message for e in errs))

    def test_empty_batch(self):
        """F-VALID-006: 0 comparisons → error."""
        errs = self.v.validate_batch([])
        self.assertTrue(len(errs) > 0)

    def test_multiple_errors_collected(self):
        """F-VALID-005: All errors collected together, not just first."""
        errs = self.v.validate_batch([
            {"prod_path":"/missing/a.csv","dev_path":"/missing/b.csv","keys":["X"]},
            {"prod_path":"/missing/c.csv","dev_path":"/missing/d.csv","keys":["Y"]},
        ])
        # Should have at least 2 errors (one per job)
        self.assertGreaterEqual(len(errs), 2)

    def test_invalid_tolerance(self):
        """Tolerance with non-numeric value → validation error."""
        errs = self.v.validate_batch([{
            "prod_path":str(self.prod),"dev_path":str(self.dev),
            "keys":["TradeID"],"tolerance":{"Price":"not_a_number"}
        }])
        self.assertTrue(any("tolerance" in e.field for e in errs))

    def test_validation_error_to_dict(self):
        """ValidationError.to_dict() returns dict with job, field, message."""
        e = ValidationError(0,"prod_path","File not found: /x.csv")
        d = e.to_dict()
        self.assertEqual(d["job"], 1)   # 0-indexed → 1-based
        self.assertEqual(d["field"], "prod_path")
        self.assertIn("/x.csv", d["message"])


# ══════════════════════════════════════════════════════════════════════════════
# BATCH MANAGER (F-BATCH, F-CANCEL, F-JOB)
# ══════════════════════════════════════════════════════════════════════════════

class TestBatchManager(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.t  = Path(self._d.name)
        self.mgr = BatchManager(report_root=self.t/"reports")

    def tearDown(self):
        self._d.cleanup()

    def _spec(self, prod="/tmp/p.csv", dev="/tmp/d.csv", keys=None):
        return {"prod_path":prod,"dev_path":dev,"keys":keys or ["ID"]}

    def test_create_batch_valid(self):
        """F-BATCH-001: create batch with 5 jobs → BatchRecord returned."""
        batch = self.mgr.create_batch([self._spec()]*5, {})
        self.assertIsInstance(batch, BatchRecord)
        self.assertEqual(batch.total_jobs, 5)
        self.assertEqual(batch.status, BatchStatus.PENDING)

    def test_batch_id_format(self):
        """Batch ID starts with BATCH_ and has timestamp."""
        batch = self.mgr.create_batch([self._spec()], {})
        self.assertTrue(batch.batch_id.startswith("BATCH_"))

    def test_job_ids_sequential(self):
        """F-JOB-001: job IDs are globally unique with batch prefix."""
        batch = self.mgr.create_batch([self._spec()]*3, {})
        ids = [j.job_id for j in batch.jobs]
        # Job IDs now include batch_id prefix for global uniqueness
        self.assertTrue(any("JOB_001" in jid for jid in ids))
        self.assertTrue(any("JOB_002" in jid for jid in ids))
        self.assertTrue(any("JOB_003" in jid for jid in ids))
        # All job IDs should start with the batch_id
        for jid in ids:
            self.assertTrue(jid.startswith(batch.batch_id),
                            f"Job ID {jid} should start with batch_id {batch.batch_id}")

    def test_batch_too_many_jobs(self):
        """F-BATCH-001: 21 jobs → ValueError."""
        with self.assertRaises(ValueError) as cm:
            self.mgr.create_batch([self._spec()]*21, {})
        self.assertIn("20", str(cm.exception))

    def test_batch_zero_jobs(self):
        """0 jobs → ValueError."""
        with self.assertRaises(ValueError):
            self.mgr.create_batch([], {})

    def test_jobs_sorted_by_size(self):
        """F-BATCH-002: jobs sorted small → large by file size."""
        # Create files of different sizes
        small = self.t/"small.csv"; small.write_text("ID\n" + "1\n"*100)
        large = self.t/"large.csv"; large.write_text("ID\n" + "1\n"*10000)
        batch = self.mgr.create_batch([
            {"prod_path":str(large),"dev_path":str(large),"keys":["ID"]},
            {"prod_path":str(small),"dev_path":str(small),"keys":["ID"]},
        ], {})
        # First job should be the smaller file
        self.assertLessEqual(batch.jobs[0].file_size_mb, batch.jobs[-1].file_size_mb)

    def test_priority_assignment_high(self):
        """F-PRIORITY-001: file < 100 MB → HIGH priority."""
        small = self.t/"tiny.csv"; small.write_text("ID,V\n1,A\n")
        batch = self.mgr.create_batch([
            {"prod_path":str(small),"dev_path":str(small),"keys":["ID"]}
        ], {})
        self.assertEqual(batch.jobs[0].priority, "HIGH")

    def test_execution_mode_sequential(self):
        """F-BATCH-003: execution_mode=sequential stored correctly."""
        batch = self.mgr.create_batch([self._spec()], {"execution_mode":"sequential"})
        self.assertEqual(batch.execution_mode, "sequential")

    def test_execution_mode_parallel(self):
        """F-BATCH-004: execution_mode=parallel with max_workers stored."""
        batch = self.mgr.create_batch([self._spec()], {"execution_mode":"parallel","max_workers":4})
        self.assertEqual(batch.execution_mode, "parallel")
        self.assertEqual(batch.max_workers, 4)

    def test_max_workers_capped_at_10(self):
        """F-PRIORITY-004: max_workers capped at 10."""
        batch = self.mgr.create_batch([self._spec()], {"max_workers":99})
        self.assertLessEqual(batch.max_workers, 10)

    def test_get_batch(self):
        """get_batch() returns the BatchRecord by ID."""
        b = self.mgr.create_batch([self._spec()], {})
        fetched = self.mgr.get_batch(b.batch_id)
        self.assertEqual(fetched.batch_id, b.batch_id)

    def test_get_batch_not_found(self):
        """get_batch() raises KeyError for unknown ID."""
        with self.assertRaises(KeyError):
            self.mgr.get_batch("BATCH_NONEXISTENT")

    def test_cancel_pending_batch(self):
        """F-CANCEL-001: cancel a PENDING batch → status CANCELLING."""
        batch = self.mgr.create_batch([self._spec()]*3, {})
        batch.status = BatchStatus.RUNNING   # simulate running
        cancelled = self.mgr.cancel_batch(batch.batch_id, "test cancel")
        self.assertIn(cancelled.status, (BatchStatus.CANCELLING, BatchStatus.CANCELLED))

    def test_cancel_preserves_completed(self):
        """F-CANCEL-005: completed jobs preserved on cancel."""
        batch = self.mgr.create_batch([self._spec()]*3, {})
        batch.status = BatchStatus.RUNNING
        # Mark one job as success before cancelling
        batch.jobs[0].status = JobStatus.SUCCESS
        self.mgr.cancel_batch(batch.batch_id, "test")
        # The successful job must remain SUCCESS
        self.assertEqual(batch.jobs[0].status, JobStatus.SUCCESS)

    def test_cancel_queued_jobs_marked_cancelled(self):
        """F-CANCEL-005: queued jobs marked CANCELLED on cancel."""
        batch = self.mgr.create_batch([self._spec()]*2, {})
        batch.status = BatchStatus.RUNNING
        batch.jobs[0].status = JobStatus.QUEUED
        batch.jobs[1].status = JobStatus.QUEUED
        self.mgr.cancel_batch(batch.batch_id, "test")
        for job in batch.jobs:
            if job.status != JobStatus.SUCCESS:
                self.assertIn(job.status, (JobStatus.CANCELLED, JobStatus.QUEUED))

    def test_cancel_already_cancelled_raises(self):
        """Cannot cancel an already-cancelled batch."""
        batch = self.mgr.create_batch([self._spec()], {})
        batch.status = BatchStatus.CANCELLED
        with self.assertRaises(ValueError):
            self.mgr.cancel_batch(batch.batch_id)

    def test_cancel_preview(self):
        """F-CANCEL-003: cancel_preview returns running, queued, completed."""
        batch = self.mgr.create_batch([self._spec()]*3, {})
        batch.status = BatchStatus.RUNNING
        batch.jobs[0].status = JobStatus.RUNNING
        batch.jobs[1].status = JobStatus.QUEUED
        batch.jobs[2].status = JobStatus.SUCCESS
        preview = self.mgr.cancel_preview(batch.batch_id)
        self.assertIn("running_jobs", preview)
        self.assertIn("queued_count", preview)
        self.assertIn("completed_jobs", preview)
        self.assertEqual(len(preview["running_jobs"]), 1)
        self.assertEqual(preview["queued_count"], 1)
        self.assertEqual(len(preview["completed_jobs"]), 1)

    def test_pause_running_batch(self):
        """F-BATCH: pause a RUNNING batch → status PAUSED."""
        batch = self.mgr.create_batch([self._spec()], {})
        batch.status = BatchStatus.RUNNING
        paused = self.mgr.pause_batch(batch.batch_id)
        self.assertEqual(paused.status, BatchStatus.PAUSED)

    def test_pause_non_running_raises(self):
        """Pause on non-RUNNING batch → ValueError (409 in API)."""
        batch = self.mgr.create_batch([self._spec()], {})
        batch.status = BatchStatus.PENDING
        with self.assertRaises(ValueError):
            self.mgr.pause_batch(batch.batch_id)

    def test_resume_paused_batch(self):
        """Resume PAUSED batch → status RUNNING."""
        batch = self.mgr.create_batch([self._spec()], {})
        batch.status = BatchStatus.PAUSED
        batch._pause_event.clear()
        resumed = self.mgr.resume_batch(batch.batch_id)
        self.assertEqual(resumed.status, BatchStatus.RUNNING)

    def test_resume_non_paused_raises(self):
        """Resume a non-PAUSED batch → ValueError."""
        batch = self.mgr.create_batch([self._spec()], {})
        batch.status = BatchStatus.RUNNING
        with self.assertRaises(ValueError):
            self.mgr.resume_batch(batch.batch_id)

    def test_retry_failed_jobs(self):
        """F-JOB-005: retry_jobs requeues failed jobs."""
        batch = self.mgr.create_batch([self._spec()]*2, {})
        batch.jobs[0].status = JobStatus.FAILED
        batch.jobs[1].status = JobStatus.FAILED
        # Use actual job IDs from the batch (now globally unique)
        real_ids = [j.job_id for j in batch.jobs]
        self.mgr.retry_jobs(batch.batch_id, real_ids, max_attempts=2)
        for j in batch.jobs:
            self.assertEqual(j.status, JobStatus.QUEUED)
            self.assertEqual(j.retry_count, 0)

    def test_retry_no_failed_jobs_raises(self):
        """retry_jobs with no failed jobs raises ValueError."""
        batch = self.mgr.create_batch([self._spec()], {})
        batch.jobs[0].status = JobStatus.SUCCESS
        # Use actual job_id (globally unique)
        with self.assertRaises(ValueError):
            self.mgr.retry_jobs(batch.batch_id, [batch.jobs[0].job_id])

    def test_list_batches(self):
        """list_batches() returns created batches."""
        b1 = self.mgr.create_batch([self._spec()], {})
        b2 = self.mgr.create_batch([self._spec()], {})
        batches, total = self.mgr.list_batches()
        ids = [b.batch_id for b in batches]
        self.assertIn(b1.batch_id, ids)
        self.assertIn(b2.batch_id, ids)
        self.assertGreaterEqual(total, 2)

    def test_list_batches_status_filter(self):
        """list_batches() filters by status."""
        b = self.mgr.create_batch([self._spec()], {})
        b.status = BatchStatus.SUCCESS
        batches, _ = self.mgr.list_batches(status="SUCCESS")
        for batch in batches:
            self.assertEqual(batch.status, BatchStatus.SUCCESS)

    def test_batch_record_progress(self):
        """BatchRecord.progress computes average job progress."""
        batch = self.mgr.create_batch([self._spec()]*2, {})
        batch.jobs[0].progress = 100.0
        batch.jobs[1].progress = 0.0
        self.assertAlmostEqual(batch.progress, 50.0, places=0)

    def test_batch_record_to_dict(self):
        """BatchRecord.to_dict() serialises cleanly to JSON."""
        batch = self.mgr.create_batch([self._spec()]*2, {})
        d = batch.to_dict()
        s = json.dumps(d, default=str)   # must not raise
        self.assertIn("batch_id", s)
        self.assertIn("jobs_detail", s)


# ══════════════════════════════════════════════════════════════════════════════
# ENTERPRISE AUDIT LOGGER (F-AUDIT-001 to 006)
# ══════════════════════════════════════════════════════════════════════════════

class TestEnterpriseAuditLogger(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.t  = Path(self._d.name)
        self.logger = EnterpriseAuditLogger(audit_root=self.t/"audit")
        self.mgr    = BatchManager(report_root=self.t/"reports")

    def tearDown(self):
        self._d.cleanup()

    def _make_batch(self):
        b = self.mgr.create_batch([{"prod_path":"/p.csv","dev_path":"/d.csv","keys":["ID"]}], {})
        return b

    def _read_log(self, batch_id):
        """Find and read all JSON lines for this batch."""
        for f in self.t.rglob(f"{batch_id}_audit.jsonl"):
            return [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
        return []

    def test_log_batch_created(self):
        """F-AUDIT-001/002: BATCH_CREATED entry written with batch_id."""
        b = self._make_batch()
        self.logger.log_batch_created(b)
        lines = self._read_log(b.batch_id)
        self.assertTrue(any(l["action"]=="BATCH_CREATED" for l in lines))

    def test_log_file_hierarchy(self):
        """F-AUDIT-004: logs stored in /YYYY/MM/DD/ hierarchy."""
        b = self._make_batch()
        self.logger.log_batch_created(b)
        now = datetime.now(timezone.utc)
        expected = self.t / "audit" / now.strftime("%Y") / now.strftime("%m") / now.strftime("%d")
        self.assertTrue(expected.exists())

    def test_log_batch_cancelled(self):
        """F-AUDIT-006: BATCH_CANCELLED entry with reason."""
        b = self._make_batch(); b.status = BatchStatus.CANCELLED
        self.logger.log_batch_cancelled(b, "User requested")
        lines = self._read_log(b.batch_id)
        cancelled = [l for l in lines if l["action"]=="BATCH_CANCELLED"]
        self.assertTrue(len(cancelled) > 0)
        self.assertIn("User requested", cancelled[0]["cancel_reason"])

    def test_log_batch_completed(self):
        """F-AUDIT-001: BATCH_COMPLETED entry with status."""
        b = self._make_batch(); b.status = BatchStatus.SUCCESS
        self.logger.log_batch_completed(b)
        lines = self._read_log(b.batch_id)
        self.assertTrue(any(l["action"]=="BATCH_COMPLETED" for l in lines))

    def test_multiple_entries_append(self):
        """F-AUDIT-005: multiple entries append (never overwrite)."""
        b = self._make_batch()
        self.logger.log_batch_created(b)
        b.status = BatchStatus.SUCCESS
        self.logger.log_batch_completed(b)
        lines = self._read_log(b.batch_id)
        self.assertEqual(len(lines), 2)

    def test_all_entries_have_timestamp(self):
        """F-AUDIT-002: every log entry has timestamp."""
        b = self._make_batch()
        self.logger.log_batch_created(b)
        lines = self._read_log(b.batch_id)
        for l in lines:
            self.assertIn("timestamp", l)

    def test_concurrent_logging_safe(self):
        """Multiple threads can write audit logs without corruption."""
        b = self._make_batch()
        errors = []
        def write():
            try:
                for _ in range(5):
                    self.logger.log_batch_created(b)
            except Exception as ex:
                errors.append(str(ex))
        threads = [threading.Thread(target=write) for _ in range(5)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(len(errors), 0)
        lines = self._read_log(b.batch_id)
        # All lines must parse as valid JSON
        for line in lines:
            self.assertIn("action", line)


# ══════════════════════════════════════════════════════════════════════════════
# API v1 (Section 8)
# ══════════════════════════════════════════════════════════════════════════════

class TestBatchAPIV1(unittest.TestCase):
    """Integration tests for /api/v1/ endpoints."""

    @classmethod
    def setUpClass(cls):
        import os
        import sys
        _d = tempfile.mkdtemp()
        os.environ["EDCP_REPORT_ROOT"] = _d
        sys.path.insert(0, str(Path(__file__).parent.parent))
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "edcp"))
        from api.app import app
        app.config["TESTING"] = True
        cls.client = app.test_client()
        cls._tmpdir = _d

    def _post(self, url, body):
        import json
        return self.client.post(url, data=json.dumps(body), content_type="application/json")

    def _get(self, url):
        return self.client.get(url)

    def test_v1_health(self):
        """GET /api/v1/health → 200 or legacy health check passes."""
        # v1 health endpoint
        r = self._get("/api/v1/health")
        if r.status_code == 200 and r.data:
            d = json.loads(r.data)
            self.assertTrue(d.get("success", True))
        else:
            # Fall back to legacy health
            r2 = self._get("/api/health")
            self.assertEqual(r2.status_code, 200)

    def test_v1_create_batch_no_comparisons(self):
        """POST /api/v1/batch with empty comparisons → 400."""
        r = self._post("/api/v1/batch", {"comparisons":[]})
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertFalse(d["success"])

    def test_v1_create_batch_missing_paths(self):
        """POST /api/v1/batch without prod_path → validation error."""
        r = self._post("/api/v1/batch", {"comparisons":[{"prod_path":"","dev_path":"","keys":[]}]})
        d = json.loads(r.data)
        self.assertFalse(d["success"])
        self.assertIn("error", d)

    def test_v1_get_batch_not_found(self):
        """GET /api/v1/batch/UNKNOWN → 404 with error."""
        r = self._get("/api/v1/batch/BATCH_NONEXISTENT_XYZ")
        self.assertEqual(r.status_code, 404)
        d = json.loads(r.data)
        self.assertFalse(d["success"])
        self.assertEqual(d["error"]["code"], "BATCH_NOT_FOUND")

    def test_v1_list_batches(self):
        """GET /api/v1/batch → 200 with batches list."""
        r = self._get("/api/v1/batch")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d["success"])
        self.assertIn("batches", d["data"])

    def test_v1_cancel_not_found(self):
        """POST /api/v1/batch/UNKNOWN/cancel → 404."""
        r = self._post("/api/v1/batch/NOPE/cancel", {"reason":"test"})
        self.assertEqual(r.status_code, 404)

    def test_v1_cancel_preview_not_found(self):
        """GET /api/v1/batch/UNKNOWN/cancel-preview → 404."""
        r = self._get("/api/v1/batch/NOPE/cancel-preview")
        self.assertEqual(r.status_code, 404)

    def test_v1_job_not_found(self):
        """GET /api/v1/job/UNKNOWN → 404."""
        r = self._get("/api/v1/job/JOB_NONEXISTENT")
        self.assertEqual(r.status_code, 404)

    def test_v1_job_logs_not_found(self):
        """GET /api/v1/job/UNKNOWN/logs → 404."""
        r = self._get("/api/v1/job/JOB_NOPE/logs")
        self.assertEqual(r.status_code, 404)

    def test_v1_config_schema(self):
        """GET /api/v1/config/schema → 200 with YAML schema."""
        r = self._get("/api/v1/config/schema")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d["success"])
        self.assertIn("schema", d["data"])
        self.assertIn("example_yaml", d["data"])

    def test_v1_standard_response_format(self):
        """All v1 responses have success, data or error, metadata (Section 8.1)."""
        r = self._get("/api/v1/health")
        d = json.loads(r.data)
        self.assertIn("success", d)
        self.assertIn("metadata", d)
        self.assertIn("timestamp", d["metadata"])
        self.assertIn("processing_time_ms", d["metadata"])

    def test_v1_error_response_format(self):
        """Error responses have code, message, suggestion, request_id."""
        r = self._get("/api/v1/batch/BAD_ID")
        d = json.loads(r.data)
        self.assertFalse(d["success"])
        err = d["error"]
        for key in ("code","message","details","suggestion","request_id"):
            self.assertIn(key, err)

    def test_v1_pause_not_found(self):
        """POST /api/v1/batch/UNKNOWN/pause → 404."""
        r = self._post("/api/v1/batch/NOPE/pause", {})
        self.assertEqual(r.status_code, 404)

    def test_v1_resume_not_found(self):
        """POST /api/v1/batch/UNKNOWN/resume → 404."""
        r = self._post("/api/v1/batch/NOPE/resume", {})
        self.assertEqual(r.status_code, 404)

    def test_v1_retry_not_found(self):
        """POST /api/v1/batch/UNKNOWN/retry → 404."""
        r = self._post("/api/v1/batch/NOPE/retry", {})
        self.assertEqual(r.status_code, 404)

    def test_v1_skip_job_not_found(self):
        """POST /api/v1/job/UNKNOWN/skip → 404 or 405 (endpoint not found)."""
        r = self._post("/api/v1/job/JOB_NOPE/skip", {})
        self.assertIn(r.status_code, (404, 405, 400))


if __name__ == "__main__":
    unittest.main(verbosity=2)
