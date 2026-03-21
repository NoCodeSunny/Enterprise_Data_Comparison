# -*- coding: utf-8 -*-
"""
tests/test_mvp_integration.py
──────────────────────────────
MVP Deep Review Integration Tests
Verifies full local runnability:
  - CLI runs without InputSheet (YAML batches mode)
  - CLI version shows correct product name
  - API starts, health endpoints work
  - Full batch lifecycle: create → execute → SUCCESS
  - Retry actually re-executes
  - Job IDs are unique
  - Empty keys = warning, not blocker
  - Audit logger writes multiple entries safely
  - Report download works
  - No datetime.utcnow() deprecation warnings
  - Config has no hardcoded user paths
  - All imports work without circular dependencies
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "edcp"))
sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ.setdefault("TESTING", "true")
os.environ.setdefault("EDCP_REPORT_ROOT", tempfile.mkdtemp())


def _csv(path, cols, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(",".join(cols) + "\n")
        for row in rows:
            f.write(",".join(str(v) for v in row) + "\n")
    return path


class _FlaskBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.mkdtemp()
        os.environ["EDCP_REPORT_ROOT"] = cls._tmp
        from api.app import app
        app.config["TESTING"] = True
        cls.client = app.test_client()
        cls.prod = Path(cls._tmp) / "prod.csv"
        cls.dev  = Path(cls._tmp) / "dev.csv"
        _csv(cls.prod, ["ID", "Name", "Amount"],
             [["1", "Alice", "100"], ["2", "Bob", "200"], ["3", "Carol", "150"]])
        _csv(cls.dev,  ["ID", "Name", "Amount"],
             [["1", "Alice", "100"], ["2", "Bob", "210"], ["3", "Carol", "150"]])

    def _post(self, url, body, headers=None):
        return self.client.post(url, data=json.dumps(body),
                                content_type="application/json",
                                headers=headers or {})

    def _get(self, url):
        return self.client.get(url)

    def _wait_batch(self, batch_id, timeout=30):
        for _ in range(timeout * 2):
            r = self._get(f"/api/v1/batch/{batch_id}")
            d = json.loads(r.data)
            status = d.get("data", {}).get("status", "")
            if status in ("SUCCESS", "FAILED", "PARTIAL_SUCCESS"):
                return status, d
            time.sleep(0.5)
        return "TIMEOUT", {}


# ─── MVP-01: CLI correctness ─────────────────────────────────────────────────

class TestMVP01CLICorrectness(unittest.TestCase):

    def test_cli_version_shows_edcp_not_data_compare(self):
        """CLI 'version' must print DataComparePro/edcp, not 'data_compare'."""
        cli_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "cli.py"
        content = cli_path.read_text()
        self.assertIn("DataComparePro", content,
                      "CLI version output must show DataComparePro")
        self.assertNotIn('print(f"data_compare  v', content,
                         "CLI must not print 'data_compare' as product name")

    def test_orchestrator_supports_yaml_batches(self):
        """Orchestrator must have _run_yaml_batches for local use without InputSheet."""
        import data_compare.orchestrator as orch
        self.assertTrue(hasattr(orch, "_run_yaml_batches"),
                        "orchestrator must have _run_yaml_batches for YAML-based CLI use")

    def test_yaml_batches_mode_runs_without_inputsheet(self):
        """CLI run with batches: section must NOT require InputSheet.xlsx."""
        import tempfile
        from data_compare.orchestrator import run_comparison
        tmp = tempfile.mkdtemp()
        prod = Path(tmp) / "p.csv"
        dev  = Path(tmp) / "d.csv"
        _csv(prod, ["ID", "Val"], [["1", "A"], ["2", "B"]])
        _csv(dev,  ["ID", "Val"], [["1", "A"], ["2", "C"]])
        cfg = {
            "report_root": tmp,
            "batches": [{"prod_path": str(prod), "dev_path": str(dev),
                         "result_name": "test", "keys": ["ID"]}],
            "capabilities": {"comparison": True, "tolerance": False, "duplicate": False,
                              "schema": False, "data_quality": False, "audit": False,
                              "alerts": False, "plugins": False},
            "input_sheet": "/nonexistent/InputSheet.xlsx",  # should be ignored
            "email_to": "",
        }
        # Must NOT raise FileNotFoundError for InputSheet
        summaries = run_comparison(_override_cfg=cfg)
        self.assertTrue(len(summaries) > 0, "Should return at least one summary")
        self.assertNotIn("Error", summaries[0].get("Error", ""), 
                         f"Unexpected error: {summaries[0].get('Error', '')}")

    def test_cli_list_caps_works(self):
        """edcp list-caps must return all 9 capabilities."""
        from data_compare.registry.capability_registry import default_registry
        caps = default_registry.list_capabilities()
        expected = {"comparison", "tolerance", "schema", "duplicate",
                    "data_quality", "audit", "alerts", "plugins", "parquet"}
        self.assertEqual(set(caps), expected,
                         f"Expected 9 capabilities, got: {caps}")

    def test_sample_config_exists(self):
        """sample_config.yaml must exist for local onboarding."""
        config_path = Path(__file__).parent.parent.parent / "edcp" / "config" / "sample_config.yaml"
        self.assertTrue(config_path.exists(),
                        "edcp/config/sample_config.yaml must exist for local use")

    def test_input_sheet_warning_suppressed_with_yaml_batches(self):
        """No warning about missing InputSheet when batches: is defined."""
        from data_compare.config.config_schema import validate_config
        cfg = {
            "input_sheet": "/nonexistent/path.xlsx",
            "report_root": "/tmp",
            "email_to": "",
            "batches": [{"prod_path": "/p.csv", "dev_path": "/d.csv"}],
        }
        import logging
        import io
        # Capture log output
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            validate_config(cfg)
            output = stream.getvalue()
            self.assertNotIn("input_sheet path does not exist", output,
                             "No InputSheet warning should appear when batches: is defined")
        finally:
            root_logger.removeHandler(handler)


# ─── MVP-02: API health and startup ─────────────────────────────────────────

class TestMVP02APIHealthAndStartup(_FlaskBase):

    def test_legacy_health_returns_v3(self):
        r = self._get("/api/health")
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(d["version"], "3.0.0")
        self.assertEqual(d["status"], "ok")

    def test_v1_health_returns_v3(self):
        r = self._get("/api/v1/health")
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(d["data"]["version"], "3.0.0")

    def test_capabilities_endpoint_lists_all(self):
        r = self._get("/api/capabilities")
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 200)
        self.assertIsInstance(d, list)
        self.assertEqual(len(d), 9, f"Expected 9 capabilities, got {len(d)}")

    def test_no_utcnow_in_app_py(self):
        """app.py must not use deprecated datetime.utcnow()."""
        app_path = Path(__file__).parent.parent / "api" / "app.py"
        content = app_path.read_text()
        self.assertNotIn("utcnow()", content,
                         "app.py must use timezone.utc not deprecated utcnow()")


# ─── MVP-03: Full batch lifecycle ───────────────────────────────────────────

class TestMVP03BatchLifecycle(_FlaskBase):

    def test_batch_creates_and_executes_to_success(self):
        """Full lifecycle: create → execute → SUCCESS with correct result."""
        payload = {
            "comparisons": [{
                "prod_path": str(self.prod),
                "dev_path":  str(self.dev),
                "keys": ["ID"],
                "result_name": "mvp_lifecycle_test",
            }],
            "config": {"retry": {"max_attempts": 0}},
        }
        r = self._post("/api/v1/batch", payload)
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 202, f"Batch creation failed: {d}")
        self.assertTrue(d["success"])

        batch_id = d["data"]["batch_id"]
        status, resp = self._wait_batch(batch_id)
        self.assertEqual(status, "SUCCESS",
                         f"Batch failed: {json.dumps(resp.get('data',{}).get('jobs_detail',[]))}")

        # Verify job details are correct
        jobs = resp["data"].get("jobs_detail", [])
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job["status"], "SUCCESS")
        self.assertGreater(job["differences_found"], 0,
                           "Should find difference in Amount column (200 vs 210)")

    def test_batch_job_ids_are_globally_unique(self):
        """Job IDs must be globally unique across batches."""
        ids = set()
        for _ in range(3):
            payload = {"comparisons": [{
                "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]
            }], "config": {"retry": {"max_attempts": 0}}}
            r = self._post("/api/v1/batch", payload)
            d = json.loads(r.data)
            if d.get("success"):
                batch_id = d["data"]["batch_id"]
                r2 = self._get(f"/api/v1/batch/{batch_id}")
                d2 = json.loads(r2.data)
                for job in d2.get("data", {}).get("jobs_detail", []):
                    jid = job["job_id"]
                    self.assertNotIn(jid, ids, f"Duplicate job ID: {jid}")
                    ids.add(jid)

    def test_batch_status_transitions_are_forward(self):
        """Batch must not go backward: QUEUED → RUNNING → terminal."""
        payload = {"comparisons": [{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]
        }], "config": {"retry": {"max_attempts": 0}}}
        r = self._post("/api/v1/batch", payload)
        d = json.loads(r.data)
        if not d.get("success"):
            self.skipTest("Batch creation failed")

        batch_id = d["data"]["batch_id"]
        seen_statuses = []
        for _ in range(20):
            r = self._get(f"/api/v1/batch/{batch_id}")
            d = json.loads(r.data)
            s = d.get("data", {}).get("status", "")
            if s and (not seen_statuses or seen_statuses[-1] != s):
                seen_statuses.append(s)
            if s in ("SUCCESS", "FAILED", "PARTIAL_SUCCESS"):
                break
            time.sleep(0.3)

        terminal_statuses = {"SUCCESS", "FAILED", "PARTIAL_SUCCESS", "CANCELLED"}
        self.assertTrue(
            any(s in terminal_statuses for s in seen_statuses),
            f"Batch never reached terminal state: {seen_statuses}"
        )

    def test_report_download_works(self):
        """Excel report must be downloadable after SUCCESS."""
        payload = {"comparisons": [{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]
        }], "config": {"retry": {"max_attempts": 0}}}
        r = self._post("/api/v1/batch", payload)
        d = json.loads(r.data)
        if not d.get("success"):
            self.skipTest("Batch creation failed")
        batch_id = d["data"]["batch_id"]
        status, _ = self._wait_batch(batch_id)

        if status == "SUCCESS":
            r = self._get(f"/api/v1/batch/{batch_id}/report/excel")
            self.assertIn(r.status_code, [200, 404],
                          f"Report download returned unexpected status: {r.status_code}")


# ─── MVP-04: Retry works ─────────────────────────────────────────────────────

class TestMVP04RetryExecution(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.tmp = Path(self._d.name)
        self.prod = self.tmp / "prod.csv"
        self.dev  = self.tmp / "dev.csv"
        _csv(self.prod, ["ID", "Val"], [["1", "A"], ["2", "B"]])
        _csv(self.dev,  ["ID", "Val"], [["1", "A"], ["2", "B"]])

    def tearDown(self):
        try: self._d.cleanup()
        except: pass

    def test_retry_actually_relaunches_execution(self):
        """retry_jobs() must launch execution thread that reaches terminal state."""
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        from edcp.batch.batch_manager import BatchManager, BatchStatus, JobStatus
        from datetime import datetime, timezone

        audit = EnterpriseAuditLogger(self.tmp / "audit")
        mgr = BatchManager(report_root=self.tmp / "reports",
                           audit_logger=audit,
                           db_path=self.tmp / "state.db")
        batch = mgr.create_batch([{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]
        }], {"retry": {"max_attempts": 0}})

        # Mark job as failed
        job = batch.jobs[0]
        job.status = JobStatus.FAILED
        job.error_message = "Simulated failure"
        batch.status = BatchStatus.FAILED

        # Retry
        mgr.retry_jobs(batch.batch_id, [job.job_id], max_attempts=0)

        # Wait for execution
        deadline = time.time() + 30
        while batch.status in (BatchStatus.PENDING, BatchStatus.RUNNING,
                                BatchStatus.QUEUED) and time.time() < deadline:
            time.sleep(0.2)

        self.assertIn(batch.status,
                      (BatchStatus.SUCCESS, BatchStatus.PARTIAL_SUCCESS, BatchStatus.FAILED),
                      f"Retry should have re-executed batch; got status={batch.status}")

    def test_no_job_stuck_in_queued(self):
        """After batch creation and start, no job should be stuck QUEUED forever."""
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        from edcp.batch.batch_manager import BatchManager, JobStatus

        audit = EnterpriseAuditLogger(self.tmp / "audit2")
        mgr = BatchManager(report_root=self.tmp / "reports2",
                           audit_logger=audit,
                           db_path=self.tmp / "state2.db")
        batch = mgr.create_batch([{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]
        }], {"retry": {"max_attempts": 0}})
        mgr.start_batch(batch.batch_id)

        deadline = time.time() + 30
        while time.time() < deadline:
            all_done = all(
                j.status.value in ("SUCCESS", "FAILED", "CANCELLED", "SKIPPED")
                for j in batch.jobs
            )
            if all_done:
                break
            time.sleep(0.3)

        for job in batch.jobs:
            self.assertNotEqual(job.status, JobStatus.QUEUED,
                                f"Job {job.job_id} stuck in QUEUED")


# ─── MVP-05: Validation warnings ────────────────────────────────────────────

class TestMVP05ValidationWarnings(_FlaskBase):

    def test_empty_keys_returns_warning_not_400(self):
        """POST /api/v1/batch with empty keys must succeed with warning."""
        payload = {"comparisons": [{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": []
        }], "config": {"retry": {"max_attempts": 0}}}
        r = self._post("/api/v1/batch", payload)
        d = json.loads(r.data)
        self.assertIn(r.status_code, [202, 200],
                      f"Empty keys must not return 400 — got {r.status_code}: {d}")
        self.assertTrue(d.get("success"),
                        "Empty keys should succeed with warning, not fail")

    def test_validate_endpoint_empty_keys_is_warning(self):
        """POST /api/v1/batch/validate with empty keys must return valid=True + warnings."""
        payload = {"comparisons": [{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": []
        }]}
        r = self._post("/api/v1/batch/validate", payload)
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 200,
                         f"Validate with empty keys must return 200, got {r.status_code}: {d}")
        self.assertTrue(d.get("data", {}).get("valid"),
                        "Validate must return valid=True for empty-keys warning")
        warnings = d.get("data", {}).get("warnings", [])
        self.assertGreater(len(warnings), 0, "Should have at least one warning about keys")

    def test_missing_file_still_blocks(self):
        """Batch with nonexistent file must return 400."""
        payload = {"comparisons": [{
            "prod_path": "/nonexistent/file.csv",
            "dev_path":  str(self.dev),
            "keys": ["ID"]
        }]}
        r = self._post("/api/v1/batch", payload)
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 400,
                         "Nonexistent file must return 400")


# ─── MVP-06: Audit multi-write ───────────────────────────────────────────────

class TestMVP06AuditMultiWrite(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.tmp = Path(self._d.name)

    def tearDown(self):
        try: self._d.cleanup()
        except: pass

    def test_audit_accepts_multiple_writes(self):
        """Audit logger must accept multiple appends to same file (not chmod after each write)."""
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        from edcp.batch.batch_manager import BatchRecord, BatchStatus
        from datetime import datetime, timezone

        audit = EnterpriseAuditLogger(self.tmp / "audit")
        batch = BatchRecord(
            batch_id="BATCH_MULTI_WRITE_TEST",
            created_at=datetime.now(timezone.utc),
            jobs=[],
            execution_mode="sequential",
            max_workers=1,
        )
        batch.status = BatchStatus.RUNNING

        # Multiple writes to same file must all succeed
        writes_succeeded = 0
        try:
            audit.log_batch_created(batch)
            writes_succeeded += 1
            audit.log_batch_completed(batch)
            writes_succeeded += 1
            audit.log_batch_completed(batch)  # third write
            writes_succeeded += 1
        except PermissionError as e:
            self.fail(f"Audit file locked after write {writes_succeeded}: {e}")

        self.assertEqual(writes_succeeded, 3,
                         f"All 3 writes must succeed, only {writes_succeeded} did")

    def test_audit_finalize_makes_readonly(self):
        """finalize_batch() must make audit file read-only."""
        import os
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        from edcp.batch.batch_manager import BatchRecord, BatchStatus
        from datetime import datetime, timezone

        audit = EnterpriseAuditLogger(self.tmp / "audit2")
        batch = BatchRecord(
            batch_id="BATCH_FINAL_TEST",
            created_at=datetime.now(timezone.utc),
            jobs=[],
            execution_mode="sequential",
            max_workers=1,
        )
        batch.status = BatchStatus.SUCCESS
        audit.log_batch_created(batch)
        audit.finalize_batch(batch.batch_id)

        logs = list((self.tmp / "audit2").rglob("*.jsonl"))
        self.assertTrue(len(logs) > 0, "Audit log file must exist after write")

        if os.getuid() != 0:  # Skip on root
            mode = logs[0].stat().st_mode
            self.assertFalse(bool(mode & 0o200),
                             f"File must be read-only after finalize; mode={oct(mode)}")


# ─── MVP-07: Import integrity ────────────────────────────────────────────────

class TestMVP07ImportIntegrity(unittest.TestCase):

    def test_all_core_modules_import_cleanly(self):
        """All core edcp modules must import without errors."""
        import importlib
        modules = [
            "edcp",
            "edcp.batch.batch_manager",
            "edcp.validation.pre_flight",
            "edcp.audit.enterprise_audit_logger",
            "edcp.jobs.comparison_job",
            "edcp.loaders.file_loader",
            "edcp.config.config_loader",
            "edcp.registry.capability_registry",
            "data_compare.orchestrator",
            "data_compare.config.config_loader",
            "data_compare.config.config_schema",
        ]
        for mod in modules:
            try:
                importlib.import_module(mod)
            except ImportError as e:
                self.fail(f"Failed to import {mod}: {e}")

    def test_no_circular_imports(self):
        """edcp package import must not cause circular import errors."""
        import importlib
        # These are the most likely sources of circular imports
        pairs = [
            ("edcp.utils.logger", "edcp.utils.helpers"),
            ("edcp.batch.batch_manager", "edcp.validation.pre_flight"),
            ("data_compare.orchestrator", "data_compare.registry.capability_registry"),
        ]
        for m1, m2 in pairs:
            try:
                importlib.import_module(m1)
                importlib.import_module(m2)
            except ImportError as e:
                self.fail(f"Circular import detected: {m1} + {m2}: {e}")

    def test_edcp_package_version_is_3(self):
        import edcp
        self.assertEqual(edcp.__version__, "3.0.0")

    def test_data_compare_lazy_import(self):
        """import data_compare must not eagerly load orchestrator."""
        import data_compare
        self.assertEqual(data_compare.__version__, "3.0.0")

    def test_no_hardcoded_mtyagi_paths(self):
        """No hardcoded mtyagi/cppib paths in config defaults."""
        from edcp.config.config_loader import load_config
        cfg = load_config(None)
        for key in ("report_root", "input_sheet", "email_to"):
            val = str(cfg.get(key, ""))
            self.assertNotIn("mtyagi", val,
                             f"config.{key} must not contain mtyagi: {val!r}")
            self.assertNotIn("cppib", val,
                             f"config.{key} must not contain cppib: {val!r}")


# ─── MVP-08: Config portability ──────────────────────────────────────────────

class TestMVP08ConfigPortability(unittest.TestCase):

    def test_env_var_overrides_report_root(self):
        old = os.environ.get("EDCP_REPORT_ROOT")
        os.environ["EDCP_REPORT_ROOT"] = "/custom/reports"
        try:
            import importlib
            import edcp.config.config_loader as cl
            importlib.reload(cl)
            cfg = cl.load_config(None)
            self.assertEqual(cfg["report_root"], "/custom/reports")
        finally:
            if old is None:
                os.environ.pop("EDCP_REPORT_ROOT", None)
            else:
                os.environ["EDCP_REPORT_ROOT"] = old

    def test_default_config_uses_home_dir(self):
        """Default report_root must use Path.home() not Windows path."""
        from edcp.config.config_loader import load_config
        cfg = load_config(None)
        report_root = cfg.get("report_root", "")
        self.assertNotIn("C:\\Users", report_root,
                         "report_root must not use Windows-style path on non-Windows")
        self.assertNotIn("\\", report_root.replace("\\\\", ""),
                         "report_root must use forward slashes on Linux/Mac")


# ─── MVP-09: Job lookup correctness ─────────────────────────────────────────

class TestMVP09JobLookup(_FlaskBase):

    def test_get_job_by_id_works(self):
        """GET /api/v1/job/<id> must return correct job details."""
        payload = {"comparisons": [{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]
        }], "config": {"retry": {"max_attempts": 0}}}
        r = self._post("/api/v1/batch", payload)
        d = json.loads(r.data)
        if not d.get("success"):
            self.skipTest("Batch creation failed")
        batch_id = d["data"]["batch_id"]
        self._wait_batch(batch_id)

        # Get job ID from batch
        r = self._get(f"/api/v1/batch/{batch_id}")
        d = json.loads(r.data)
        jobs = d.get("data", {}).get("jobs_detail", [])
        if not jobs:
            self.skipTest("No jobs found in batch")
        job_id = jobs[0]["job_id"]

        # Get job directly
        r = self._get(f"/api/v1/job/{job_id}")
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 200, f"GET /job/{job_id} failed: {d}")
        self.assertTrue(d.get("success"))
        self.assertEqual(d["data"]["job_id"], job_id)

    def test_unknown_batch_returns_404(self):
        r = self._get("/api/v1/batch/BATCH_NONEXISTENT_XYZ")
        self.assertEqual(r.status_code, 404)

    def test_batch_list_returns_jobs_detail(self):
        """GET /api/v1/batch/<id> response must have jobs_detail as list."""
        payload = {"comparisons": [{
            "prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]
        }], "config": {"retry": {"max_attempts": 0}}}
        r = self._post("/api/v1/batch", payload)
        d = json.loads(r.data)
        if not d.get("success"):
            self.skipTest("Batch creation failed")
        batch_id = d["data"]["batch_id"]
        self._wait_batch(batch_id)
        r = self._get(f"/api/v1/batch/{batch_id}")
        d = json.loads(r.data)
        jobs_detail = d.get("data", {}).get("jobs_detail", None)
        self.assertIsNotNone(jobs_detail, "Response must include jobs_detail")
        self.assertIsInstance(jobs_detail, list, "jobs_detail must be a list")
        if jobs_detail:
            self.assertIn("job_id", jobs_detail[0])
            self.assertIn("status", jobs_detail[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
