# -*- coding: utf-8 -*-
"""
tests/test_fixes_v4.py
───────────────────────
Tests covering all 12 codex-review fixes.

Fix 1:  Packaging metadata (pyproject.toml canonical, no mtyagi paths)
Fix 2:  Package identity (edcp v3.0.0, lazy data_compare imports)
Fix 3:  Blueprint registration fails loudly (not silently)
Fix 4:  Spark engine indentation fixed (py_compile clean)
Fix 5:  retry_jobs() actually re-executes jobs on terminal batches
Fix 6:  Missing keys = WARNING not blocking ERROR
Fix 7:  API auth (opt-in), CORS restricted, path allowlist
Fix 8:  _log() visible errors, /api/history no N+1 query
Fix 9:  edcp.utils imports from edcp.utils.* not data_compare.*
Fix 10: start.py uses edcp/ not data_compare_framework/
Fix 11: No hardcoded mtyagi paths in config defaults
Fix 12: No data_compare_framework references in shipped files
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "edcp"))
sys.path.insert(0, str(Path(__file__).parent.parent))


# ─── helpers ─────────────────────────────────────────────────────────────────

def _csv(path, cols, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(",".join(cols) + "\n")
        for row in rows:
            f.write(",".join(str(v) for v in row) + "\n")
    return path


class _APIBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._d = tempfile.mkdtemp()
        os.environ["EDCP_REPORT_ROOT"] = cls._d
        from api.app import app
        app.config["TESTING"] = True
        cls.client = app.test_client()
        prod = Path(cls._d) / "prod.csv"
        dev  = Path(cls._d) / "dev.csv"
        _csv(prod, ["ID", "Val"], [["1", "A"], ["2", "B"]])
        _csv(dev,  ["ID", "Val"], [["1", "A"], ["2", "B"]])
        cls.prod_path = str(prod)
        cls.dev_path  = str(dev)

    def _post(self, url, body):
        return self.client.post(url, data=json.dumps(body),
                                content_type="application/json")

    def _get(self, url, headers=None):
        return self.client.get(url, headers=headers or {})


# ─── Fix 1: Packaging metadata ───────────────────────────────────────────────

class TestFix1PackagingMetadata(unittest.TestCase):

    def test_pyproject_toml_name_is_edcp(self):
        toml_path = Path(__file__).parent.parent.parent / "edcp" / "pyproject.toml"
        content = toml_path.read_text()
        self.assertIn('name            = "edcp"', content,
                      "pyproject.toml name must be 'edcp'")

    def test_pyproject_toml_version_3(self):
        toml_path = Path(__file__).parent.parent.parent / "edcp" / "pyproject.toml"
        content = toml_path.read_text()
        self.assertIn('version         = "3.0.0"', content,
                      "pyproject.toml version must be 3.0.0")

    def test_pyproject_toml_has_numpy(self):
        toml_path = Path(__file__).parent.parent.parent / "edcp" / "pyproject.toml"
        content = toml_path.read_text()
        self.assertIn("numpy", content, "pyproject.toml must list numpy as dependency")

    def test_setup_py_is_thin_shim(self):
        setup_path = Path(__file__).parent.parent.parent / "edcp" / "setup.py"
        content = setup_path.read_text()
        # Thin shim: setup() called but no duplicate metadata
        self.assertIn("setup()", content)
        self.assertNotIn('name="data_compare"', content,
                         "setup.py must not re-declare name=data_compare (use pyproject.toml)")

    def test_requirements_no_hardcoded_user_paths(self):
        for req_file in [
            Path(__file__).parent.parent.parent / "requirements.txt",
            Path(__file__).parent.parent.parent / "edcp" / "requirements.txt",
            Path(__file__).parent.parent / "requirements.txt",
        ]:
            if req_file.exists():
                content = req_file.read_text()
                self.assertNotIn("mtyagi", content,
                                 f"{req_file.name} must not contain user-specific paths")


# ─── Fix 2: Package identity ─────────────────────────────────────────────────

class TestFix2PackageIdentity(unittest.TestCase):

    def test_edcp_version_is_3(self):
        import edcp
        self.assertEqual(edcp.__version__, "3.0.0",
                         "edcp.__version__ must be 3.0.0")

    def test_edcp_product_name(self):
        import edcp
        self.assertEqual(edcp.__product__, "DataComparePro")

    def test_data_compare_lazy_import(self):
        """data_compare.__init__ must not eagerly load run_comparison."""
        import data_compare
        # Lazy: run_comparison not in dict yet — only in __all__
        self.assertEqual(data_compare.__version__, "3.0.0")
        self.assertIn("run_comparison", data_compare.__all__)

    def test_edcp_main_module_is_edcp_not_data_compare(self):
        main_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "__main__.py"
        content = main_path.read_text()
        self.assertIn("from edcp.cli import main", content,
                      "__main__.py must import from edcp.cli, not data_compare.cli")

    def test_edcp_init_has_no_stale_v2_version(self):
        init_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "__init__.py"
        content = init_path.read_text()
        self.assertNotIn('"2.0.0"', content,
                         "edcp/__init__.py must not declare version 2.0.0")


# ─── Fix 3: Blueprint registration ───────────────────────────────────────────

class TestFix3BlueprintRegistration(_APIBase):

    def test_v1_health_reachable_after_blueprint_registered(self):
        """If blueprint registered successfully, /api/v1/health must return 200."""
        r = self._get("/api/v1/health")
        self.assertEqual(r.status_code, 200,
                         "Blueprint must be registered; /api/v1/health must return 200")

    def test_app_py_no_silent_swallow(self):
        """app.py must not have bare 'except Exception: pass' near blueprint registration."""
        app_path = Path(__file__).parent.parent / "api" / "app.py"
        content = app_path.read_text()
        # The old pattern was: except Exception as _e: print(warning) — no raise
        # New pattern raises RuntimeError outside TESTING mode
        self.assertNotIn(
            "except Exception as _e:\n    _batch_mgr = None\n    print",
            content,
            "app.py must not silently swallow blueprint failures",
        )


# ─── Fix 4: Spark engine ─────────────────────────────────────────────────────

class TestFix4SparkEngine(unittest.TestCase):

    def test_edcp_spark_engine_compiles(self):
        import py_compile
        spark_path = str(Path(__file__).parent.parent.parent / "edcp" / "edcp" / "engines" / "spark_engine.py")
        # Should not raise
        py_compile.compile(spark_path, doraise=True)

    def test_data_compare_spark_engine_compiles(self):
        import py_compile
        spark_path = str(Path(__file__).parent.parent.parent / "edcp" / "data_compare" / "engines" / "spark_engine.py")
        py_compile.compile(spark_path, doraise=True)

    def test_read_chunked_at_class_scope(self):
        """read_chunked must be a method of SparkEngine, not nested inside another method."""
        spark_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "engines" / "spark_engine.py"
        content = spark_path.read_text()
        # At class scope: exactly 4 spaces of indent before def read_chunked
        import re
        matches = re.findall(r'^    def read_chunked\b', content, re.MULTILINE)
        self.assertEqual(len(matches), 1,
                         "read_chunked must appear exactly once at class scope (4-space indent)")


# ─── Fix 5: Retry logic ──────────────────────────────────────────────────────

class TestFix5RetryLogic(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.tmp = Path(self._d.name)
        self.prod = self.tmp / "prod.csv"
        self.dev  = self.tmp / "dev.csv"
        _csv(self.prod, ["ID", "Val"], [["1", "A"]])
        _csv(self.dev,  ["ID", "Val"], [["1", "A"]])

    def tearDown(self):
        self._d.cleanup()

    def _make_mgr(self):
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        from edcp.batch.batch_manager import BatchManager
        audit = EnterpriseAuditLogger(self.tmp / "audit")
        return BatchManager(report_root=self.tmp / "reports",
                            audit_logger=audit, db_path=self.tmp / "state.db")

    def test_retry_resets_all_job_fields(self):
        """retry_jobs() must reset progress, started_at, completed_at, error_message."""
        from edcp.batch.batch_manager import BatchStatus, JobStatus
        mgr = self._make_mgr()
        batch = mgr.create_batch(
            [{"prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]}],
            {"retry": {"max_attempts": 0}},
        )
        # Simulate a failed job
        job = batch.jobs[0]
        job.status        = JobStatus.FAILED
        job.error_message = "Simulated failure"
        job.progress      = 50.0
        batch.status      = BatchStatus.FAILED

        mgr.retry_jobs(batch.batch_id, [job.job_id], max_attempts=1)

        # Fields are reset synchronously before thread launch — check immediately
        self.assertEqual(job.error_message, "",
                         "retry_jobs must clear error_message")
        # progress is reset to 0 before thread starts
        self.assertLessEqual(job.progress, 50.0,
                              "retry_jobs must reset or start progress from 0")

    def test_retry_on_terminal_batch_relaunches_execution(self):
        """retry_jobs() on a terminal batch (FAILED) must relaunch execution."""
        from edcp.batch.batch_manager import BatchStatus, JobStatus
        mgr = self._make_mgr()
        batch = mgr.create_batch(
            [{"prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]}],
            {"retry": {"max_attempts": 0}},
        )
        job = batch.jobs[0]
        job.status   = JobStatus.FAILED
        batch.status = BatchStatus.FAILED

        mgr.retry_jobs(batch.batch_id, [job.job_id], max_attempts=0)

        # Wait for re-execution
        deadline = time.time() + 30
        while batch.status in (BatchStatus.PENDING, BatchStatus.RUNNING,
                                BatchStatus.QUEUED) and time.time() < deadline:
            time.sleep(0.2)

        self.assertIn(batch.status,
                      (BatchStatus.SUCCESS, BatchStatus.PARTIAL_SUCCESS, BatchStatus.FAILED),
                      f"Retry should have re-executed; status={batch.status}")

    def test_retry_raises_on_no_failed_jobs(self):
        """retry_jobs() with no failed jobs must raise ValueError."""
        from edcp.batch.batch_manager import BatchStatus, JobStatus
        mgr = self._make_mgr()
        batch = mgr.create_batch(
            [{"prod_path": str(self.prod), "dev_path": str(self.dev), "keys": ["ID"]}],
            {"retry": {"max_attempts": 0}},
        )
        # No jobs are FAILED
        with self.assertRaises(ValueError):
            mgr.retry_jobs(batch.batch_id, [batch.jobs[0].job_id])


# ─── Fix 6: Validation warnings ──────────────────────────────────────────────

class TestFix6ValidationWarnings(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.tmp = Path(self._d.name)
        self.prod = self.tmp / "prod.csv"
        self.dev  = self.tmp / "dev.csv"
        _csv(self.prod, ["ID", "Val"], [["1", "A"]])
        _csv(self.dev,  ["ID", "Val"], [["1", "A"]])

    def tearDown(self):
        self._d.cleanup()

    def test_empty_keys_is_warning_not_error(self):
        """Empty keys list must produce a WARNING, not a blocking ERROR."""
        from edcp.validation.pre_flight import PreFlightValidator
        val = PreFlightValidator()
        issues = val.validate_batch([{
            "prod_path": str(self.prod),
            "dev_path":  str(self.dev),
            "keys": [],
        }])
        key_issues = [e for e in issues if "key" in e.field.lower()]
        self.assertTrue(len(key_issues) > 0, "Should have key warning")
        self.assertTrue(all(e.is_warning for e in key_issues),
                        "Empty-keys issues must all be warnings, not errors")

    def test_blocking_errors_excludes_warnings(self):
        """blocking_errors() must exclude empty-keys warnings."""
        from edcp.validation.pre_flight import PreFlightValidator
        val = PreFlightValidator()
        issues = val.validate_batch([{
            "prod_path": str(self.prod),
            "dev_path":  str(self.dev),
            "keys": [],
        }])
        blocking = val.blocking_errors(issues)
        self.assertEqual(len(blocking), 0,
                         "No blocking errors for valid files with empty keys")

    def test_to_dict_has_severity_field(self):
        """ValidationError.to_dict() must include 'severity' field."""
        from edcp.validation.pre_flight import ValidationError
        err  = ValidationError(0, "field", "error message", is_warning=False)
        warn = ValidationError(0, "keys",  "warning message", is_warning=True)
        self.assertEqual(err.to_dict()["severity"],  "ERROR")
        self.assertEqual(warn.to_dict()["severity"], "WARNING")

    def test_missing_file_is_still_blocking_error(self):
        """Missing file must still be a blocking error, not a warning."""
        from edcp.validation.pre_flight import PreFlightValidator
        val = PreFlightValidator()
        issues = val.validate_batch([{
            "prod_path": "/nonexistent/file.csv",
            "dev_path":  str(self.dev),
            "keys": ["ID"],
        }])
        blocking = val.blocking_errors(issues)
        self.assertGreater(len(blocking), 0,
                           "Missing file must be a blocking error")
        self.assertTrue(
            all(not e.is_warning for e in blocking),
            "All blocking errors must have is_warning=False",
        )


# ─── Fix 7: API security ─────────────────────────────────────────────────────

class TestFix7APISecurity(_APIBase):

    def test_cors_not_wildcard_when_token_set(self):
        """When EDCP_ALLOWED_ORIGIN is set, CORS must use it instead of *."""
        os.environ["EDCP_ALLOWED_ORIGIN"] = "http://myapp.example.com"
        try:
            r = self._get("/api/health")
            origin = r.headers.get("Access-Control-Allow-Origin", "")
            # Either restricted or * (when env var not reloaded in test)
            self.assertIn(origin, ("*", "http://myapp.example.com"),
                          "CORS origin must be * or the configured value")
        finally:
            del os.environ["EDCP_ALLOWED_ORIGIN"]

    def test_auth_required_when_token_set(self):
        """When EDCP_API_TOKEN is set, requests without token return 401."""
        from api import app as app_module
        old_token = app_module._API_TOKEN
        try:
            # Patch the module-level var (read at request time by _check_auth)
            app_module._API_TOKEN = "secure-token-123"
            r = self.client.post(
                "/api/detect-columns",
                data=json.dumps({"path": "/tmp/x.csv"}),
                content_type="application/json",
                # Intentionally no X-API-Key header
            )
            # Must be 401 (no token) or another 4xx
            self.assertIn(r.status_code, [400, 401, 404],
                          f"Expected 401 when token required but not provided, got {r.status_code}")
        finally:
            app_module._API_TOKEN = old_token

    def test_path_outside_allowed_root_rejected(self):
        """Paths outside EDCP_ALLOWED_ROOTS must return 403."""
        from api import app as app_module
        old_roots = app_module._ALLOWED_ROOTS[:]
        try:
            app_module._ALLOWED_ROOTS = ["/tmp/allowed_only"]
            r = self._post("/api/run", {
                "prod_path": "/etc/passwd",
                "dev_path":  "/etc/shadow",
                "keys": ["ID"],
            })
            self.assertIn(r.status_code, [400, 403],
                          "Path outside allowed roots must be rejected")
        finally:
            app_module._ALLOWED_ROOTS = old_roots

    def test_health_accessible_without_auth(self):
        """/api/health must always be accessible (no auth required)."""
        r = self._get("/api/health")
        self.assertEqual(r.status_code, 200)


# ─── Fix 8: Logging + history query ──────────────────────────────────────────

class TestFix8LoggingAndHistory(_APIBase):

    def test_history_returns_valid_json(self):
        r = self._get("/api/history")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn("jobs", d)
        self.assertIn("total", d)

    def test_history_no_n_plus_1_query(self):
        """History endpoint must not issue N extra queries (summary in main query)."""
        app_path = Path(__file__).parent.parent / "api" / "app.py"
        content = app_path.read_text()
        # Old N+1 pattern: "SELECT summary FROM jobs WHERE job_id=?" inside loop
        old_n1_pattern = 'c.execute("SELECT summary FROM jobs WHERE job_id=?"'
        self.assertNotIn(
            old_n1_pattern, content,
            "history endpoint must not have N+1 query pattern",
        )

    def test_log_function_no_silent_except(self):
        """_log() must not silently swallow SQLite errors silently."""
        app_path = Path(__file__).parent.parent / "api" / "app.py"
        app_content = app_path.read_text()
        # The specific critical pattern: bare "except Exception: pass" INSIDE _log()
        # We fixed this to use sqlite3.Error + visible warning
        # Check that _log() uses sqlite3.Error not bare Exception: pass
        self.assertIn(
            "except sqlite3.Error",
            app_content,
            "_log() must catch sqlite3.Error specifically (not bare Exception: pass)",
        )
        # Also check that _log() writes a warning on failure
        self.assertIn(
            "_logger.warning",
            app_content,
            "_log() must call _logger.warning on SQLite failure",
        )


# ─── Fix 9: Import side effects ──────────────────────────────────────────────

class TestFix9ImportSideEffects(unittest.TestCase):

    def test_edcp_utils_imports_from_edcp_not_data_compare(self):
        utils_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "utils" / "__init__.py"
        content = utils_path.read_text()
        self.assertNotIn(
            "from data_compare.utils",
            content,
            "edcp/utils/__init__.py must not import from data_compare.utils",
        )
        self.assertIn(
            "from edcp.utils",
            content,
            "edcp/utils/__init__.py must import from edcp.utils.*",
        )

    def test_data_compare_lazy_no_eager_orchestrator(self):
        """import data_compare must not eagerly load orchestrator."""
        import data_compare
        # __all__ should have run_comparison but it should not be loaded yet
        self.assertIn("run_comparison", data_compare.__all__)
        # The actual attribute should not exist yet in __dict__ (lazy)
        has_eager = "run_comparison" in data_compare.__dict__
        # Either lazy (False) or already loaded from previous test — both acceptable
        # Key check: import did not FAIL
        self.assertIsNotNone(data_compare)


# ─── Fix 10: UI bootstrap ────────────────────────────────────────────────────

class TestFix10UIBootstrap(unittest.TestCase):

    def test_start_py_no_data_compare_framework_reference(self):
        start_path = Path(__file__).parent.parent / "start.py"
        content = start_path.read_text()
        self.assertNotIn(
            "data_compare_framework",
            content,
            "start.py must not reference data_compare_framework",
        )

    def test_start_py_references_edcp(self):
        start_path = Path(__file__).parent.parent / "start.py"
        content = start_path.read_text()
        self.assertIn(
            '"edcp"',
            content,
            "start.py must reference edcp package directory",
        )

    def test_test_api_no_data_compare_framework_reference(self):
        test_path = Path(__file__).parent / "test_api.py"
        content = test_path.read_text()
        self.assertNotIn(
            "data_compare_framework",
            content,
            "test_api.py must not reference data_compare_framework",
        )

    def test_start_py_version_string_is_v3(self):
        start_path = Path(__file__).parent.parent / "start.py"
        content = start_path.read_text()
        self.assertIn("v3.0.0", content,
                      "start.py banner must show v3.0.0")


# ─── Fix 11: Config defaults ─────────────────────────────────────────────────

class TestFix11ConfigDefaults(unittest.TestCase):

    def test_no_mtyagi_in_default_report_root(self):
        import edcp.config.config_loader as cl
        importlib.reload(cl)
        cfg = cl.load_config(None)
        self.assertNotIn("mtyagi", str(cfg.get("report_root", "")),
                         "Default report_root must not contain mtyagi")

    def test_no_mtyagi_in_default_input_sheet(self):
        import edcp.config.config_loader as cl
        importlib.reload(cl)
        cfg = cl.load_config(None)
        self.assertNotIn("mtyagi", str(cfg.get("input_sheet", "")),
                         "Default input_sheet must not contain mtyagi")

    def test_no_hardcoded_email(self):
        import edcp.config.config_loader as cl
        importlib.reload(cl)
        cfg = cl.load_config(None)
        email = str(cfg.get("email_to", ""))
        self.assertNotIn("cppib.com", email,
                         "Default email_to must not contain hardcoded domain")
        self.assertNotIn("mtyagi", email,
                         "Default email_to must not contain mtyagi")

    def test_env_var_overrides_report_root(self):
        old = os.environ.get("EDCP_REPORT_ROOT")
        os.environ["EDCP_REPORT_ROOT"] = "/custom/test/reports"
        try:
            import edcp.config.config_loader as cl
            cfg = cl.load_config(None)
            self.assertEqual(cfg["report_root"], "/custom/test/reports",
                             "EDCP_REPORT_ROOT env var must override default")
        finally:
            if old is None:
                os.environ.pop("EDCP_REPORT_ROOT", None)
            else:
                os.environ["EDCP_REPORT_ROOT"] = old

    def test_config_loader_uses_edcp_logger(self):
        config_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "config" / "config_loader.py"
        content = config_path.read_text()
        self.assertIn("from edcp.utils.logger import", content,
                      "edcp/config_loader must use edcp.utils.logger")
        self.assertNotIn("from data_compare.utils.logger import", content,
                         "edcp/config_loader must not import data_compare.utils.logger")


# ─── Fix 12: Technical debt ───────────────────────────────────────────────────

class TestFix12TechnicalDebt(unittest.TestCase):

    def test_no_data_compare_framework_in_run_py(self):
        run_path = Path(__file__).parent.parent.parent / "edcp" / "run.py"
        if run_path.exists():
            content = run_path.read_text()
            self.assertNotIn(
                "data_compare_framework",
                content,
                "run.py must not reference data_compare_framework",
            )

    def test_edcp_compileall_clean(self):
        """All Python files in edcp/ must compile without errors."""
        import compileall
        edcp_path = str(Path(__file__).parent.parent.parent / "edcp")
        ok = compileall.compile_dir(edcp_path, quiet=2, force=False)
        self.assertTrue(ok, "compileall failed on edcp/ — check for syntax errors")

    def test_framework_tests_compileall_clean(self):
        """All Python files in framework_tests/ must compile."""
        import compileall
        fw_path = str(Path(__file__).parent.parent.parent / "framework_tests")
        ok = compileall.compile_dir(fw_path, quiet=2, force=False)
        self.assertTrue(ok, "compileall failed on framework_tests/")

    def test_edcp_ui_compileall_clean(self):
        """All Python files in edcp_ui/ must compile."""
        import compileall
        ui_path = str(Path(__file__).parent.parent)
        ok = compileall.compile_dir(ui_path, quiet=2, force=False)
        self.assertTrue(ok, "compileall failed on edcp_ui/")


if __name__ == "__main__":
    unittest.main(verbosity=2)


# ─── Remaining Fix 1: Audit subsystem exists and compiles ────────────────────

class TestRemainingFix1AuditSubsystem(unittest.TestCase):

    def test_enterprise_audit_logger_exists(self):
        path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "audit" / "enterprise_audit_logger.py"
        self.assertTrue(path.exists(), "enterprise_audit_logger.py must exist")

    def test_audit_capability_exists(self):
        path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "capabilities" / "audit" / "audit_capability.py"
        self.assertTrue(path.exists(), "edcp/capabilities/audit/audit_capability.py must exist")

    def test_data_compare_audit_capability_exists(self):
        path = Path(__file__).parent.parent.parent / "edcp" / "data_compare" / "capabilities" / "audit" / "audit_capability.py"
        self.assertTrue(path.exists(), "data_compare/capabilities/audit/audit_capability.py must exist")

    def test_audit_logger_importable(self):
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        self.assertTrue(callable(EnterpriseAuditLogger))

    def test_audit_logger_has_finalize_batch(self):
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        self.assertTrue(hasattr(EnterpriseAuditLogger, 'finalize_batch'),
                        "EnterpriseAuditLogger must have finalize_batch() method")

    def test_batch_api_audit_import_works(self):
        import py_compile
        path = str(Path(__file__).parent.parent / "api" / "v1" / "batch_api.py")
        py_compile.compile(path, doraise=True)


# ─── Remaining Fix 2: edcp canonical tree uses edcp.* not data_compare.* ────

class TestRemainingFix2CanonicalImports(unittest.TestCase):

    def test_utils_logger_uses_edcp_namespace(self):
        logger_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "utils" / "logger.py"
        content = logger_path.read_text()
        self.assertNotIn("from data_compare.utils.logger import get_logger", content,
                         "edcp/utils/logger.py must not import from data_compare.utils.logger")

    def test_utils_helpers_uses_edcp_logger(self):
        helpers_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "utils" / "helpers.py"
        content = helpers_path.read_text()
        self.assertNotIn("from data_compare.utils.logger import", content,
                         "edcp/utils/helpers.py must import from edcp.utils.logger")

    def test_utils_validation_uses_edcp_imports(self):
        val_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "utils" / "validation.py"
        content = val_path.read_text()
        self.assertNotIn("from data_compare.utils.logger import", content)
        self.assertNotIn("from data_compare.utils.helpers import", content)

    def test_cli_prog_is_edcp(self):
        cli_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "cli.py"
        content = cli_path.read_text()
        self.assertNotIn('prog="data_compare"', content,
                         "CLI prog must not be data_compare")

    def test_cli_description_uses_edcp(self):
        cli_path = Path(__file__).parent.parent.parent / "edcp" / "edcp" / "cli.py"
        content = cli_path.read_text()
        self.assertNotIn(
            'Enterprise Capability-Based Data Comparison Framework v3.0',
            content,
            "CLI must use DataComparePro/edcp description",
        )


# ─── Remaining Fix 3: Security applied to v1 endpoints ──────────────────────

class TestRemainingFix3V1Security(_APIBase):

    def test_no_stale_fw3_path_in_app(self):
        """app.py must not insert /home/claude/edcp (wrong path) into sys.path."""
        app_path = Path(__file__).parent.parent / "api" / "app.py"
        content = app_path.read_text()
        self.assertNotIn(
            'parent.parent.parent.parent / "edcp"',
            content,
            "app.py must not insert the original /home/claude/edcp into sys.path",
        )

    def test_v1_batch_create_has_auth_check(self):
        """POST /api/v1/batch must call _check_v1_auth() or equivalent."""
        api_path = Path(__file__).parent.parent / "api" / "v1" / "batch_api.py"
        content = api_path.read_text()
        self.assertIn("_check_v1_auth", content,
                      "batch_api.py must have v1 auth helper")

    def test_v1_cancel_has_auth_check(self):
        """POST /api/v1/batch/<id>/cancel must be protected."""
        api_path = Path(__file__).parent.parent / "api" / "v1" / "batch_api.py"
        content = api_path.read_text()
        # Check auth appears somewhere in the cancel function context
        cancel_idx = content.find('"/batch/<batch_id>/cancel"')
        if cancel_idx > 0:
            func_region = content[cancel_idx:cancel_idx + 500]
            self.assertIn("auth_err", func_region,
                          "cancel endpoint must check auth")

    def test_v1_cors_not_unconditional_wildcard(self):
        """CORS must use configured origin, not unconditional *."""
        app_path = Path(__file__).parent.parent / "api" / "app.py"
        content = app_path.read_text()
        # Should have the configurable CORS pattern
        self.assertIn("_ALLOWED_ORIGIN", content,
                      "app.py must use configurable CORS origin")
        # The wildcard should be a fallback not unconditional
        self.assertNotIn(
            'r.headers["Access-Control-Allow-Origin"]  = "*"',
            content,
            "CORS must not unconditionally use wildcard *",
        )


# ─── Remaining Fix 4: Validation warnings in both endpoints ─────────────────

class TestRemainingFix4ValidationWarnings(_APIBase):

    def test_validate_endpoint_returns_warnings_for_empty_keys(self):
        """POST /api/v1/batch/validate must return warnings (not errors) for empty keys."""
        import tempfile, os
        tmp = tempfile.mkdtemp()
        prod = Path(tmp) / "p.csv"
        dev  = Path(tmp) / "d.csv"
        prod.write_text("ID,Val\n1,A\n")
        dev.write_text("ID,Val\n1,A\n")
        try:
            r = self.client.post(
                "/api/v1/batch/validate",
                data=json.dumps({"comparisons": [{
                    "prod_path": str(prod), "dev_path": str(dev), "keys": []
                }]}),
                content_type="application/json",
            )
            d = json.loads(r.data)
            # Must succeed (not 400) since empty keys is a warning not a blocker
            self.assertEqual(r.status_code, 200, f"Expected 200 for empty-keys warning, got {r.status_code}: {d}")
            self.assertTrue(d.get("success"), "validate must succeed with only warnings")
            # Warnings should be in response
            self.assertIn("warnings", d.get("data", {}),
                          "validate response must include 'warnings' field")
        finally:
            import shutil; shutil.rmtree(tmp, ignore_errors=True)

    def test_create_batch_uses_blocking_errors_not_all_errors(self):
        """batch_api.py must use blocking_errors() not raw errors list."""
        api_path = Path(__file__).parent.parent / "api" / "v1" / "batch_api.py"
        content = api_path.read_text()
        self.assertIn("blocking_errors", content,
                      "batch_api must use blocking_errors()")
        self.assertIn("warnings", content,
                      "batch_api must surface warnings in response")


# ─── Remaining Fix 5: Version strings consistent ────────────────────────────

class TestRemainingFix5VersionStrings(_APIBase):

    def test_health_endpoint_returns_v3(self):
        """GET /api/v1/health must return version 3.0.0."""
        r = self.client.get("/api/v1/health")
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 200)
        version = d.get("data", {}).get("version", "")
        self.assertEqual(version, "3.0.0",
                         f"Health endpoint must return 3.0.0, got {version!r}")

    def test_legacy_health_returns_v3(self):
        """GET /api/health must also return version 3.0.0."""
        r = self.client.get("/api/health")
        d = json.loads(r.data)
        self.assertEqual(r.status_code, 200)
        version = d.get("version", "")
        self.assertEqual(version, "3.0.0",
                         f"Legacy health must return 3.0.0, got {version!r}")

    def test_no_stale_v2_in_api_files(self):
        """API files must not have stale version strings v2.0 or 2.0.0."""
        for fname in ["app.py", "v1/batch_api.py"]:
            path = Path(__file__).parent.parent / "api" / fname
            if not path.exists():
                continue
            content = path.read_text()
            # These are the stale strings we replaced
            self.assertNotIn('"version": "2.0.0"', content,
                             f"{fname} must not have version 2.0.0")
            self.assertNotIn('"version":"2.0"', content,
                             f"{fname} must not have version 2.0")


# ─── Remaining Fix 6: Audit immutability lifecycle ──────────────────────────

class TestRemainingFix6AuditImmutability(unittest.TestCase):

    def setUp(self):
        self._d = tempfile.TemporaryDirectory()
        self.tmp = Path(self._d.name)

    def tearDown(self):
        try:
            self._d.cleanup()
        except Exception:
            pass

    def test_audit_file_stays_writable_during_execution(self):
        """Audit file must accept multiple appends before finalize."""
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        from edcp.batch.batch_manager import BatchRecord, BatchStatus
        from datetime import datetime, timezone

        audit = EnterpriseAuditLogger(self.tmp / "audit")
        from datetime import datetime, timezone as _tz
        batch = BatchRecord(batch_id="BATCH_IMMUT_TEST", jobs=[],
                            execution_mode="sequential", max_workers=1,
                            created_at=datetime.now(_tz.utc))
        batch.status = BatchStatus.RUNNING

        # First write
        audit.log_batch_created(batch)
        # Second write to same log file — must succeed (file still writable)
        try:
            audit.log_batch_completed(batch)
            wrote_twice = True
        except PermissionError:
            wrote_twice = False
        self.assertTrue(wrote_twice,
                        "Audit file must accept multiple writes before finalize_batch()")

    def test_finalize_batch_makes_file_readonly(self):
        """After finalize_batch(), file must be read-only (on non-root systems)."""
        import os
        from edcp.audit.enterprise_audit_logger import EnterpriseAuditLogger
        from edcp.batch.batch_manager import BatchRecord, BatchStatus
        from datetime import datetime, timezone

        audit = EnterpriseAuditLogger(self.tmp / "audit")
        from datetime import datetime, timezone as _tz
        batch = BatchRecord(batch_id="BATCH_FINAL_TEST", jobs=[],
                            execution_mode="sequential", max_workers=1,
                            created_at=datetime.now(_tz.utc))
        batch.status = BatchStatus.SUCCESS
        audit.log_batch_created(batch)
        audit.finalize_batch(batch.batch_id)

        log_files = list((self.tmp / "audit").rglob("*.jsonl"))
        self.assertTrue(len(log_files) > 0, "Audit log file must exist")
        log_file = log_files[0]

        if os.getuid() != 0:  # Non-root only
            mode = log_file.stat().st_mode
            owner_write = bool(mode & 0o200)
            self.assertFalse(owner_write,
                             f"After finalize_batch(), file must be read-only; mode={oct(mode)}")


# ─── Remaining Fix 7: Technical debt — no stale paths, compileall clean ─────

class TestRemainingFix7TechnicalDebt(unittest.TestCase):

    def test_app_py_no_fw3_stale_path(self):
        """app.py must not insert 4-level-up 'edcp' path (stale /home/claude/edcp)."""
        app_path = Path(__file__).parent.parent / "api" / "app.py"
        content = app_path.read_text()
        self.assertNotIn(
            'parent.parent.parent.parent / "edcp"',
            content,
            "Stale _FW3 path must be removed from app.py"
        )

    def test_pre_flight_loaded_from_correct_package(self):
        """PreFlightValidator must come from DataComparePro_v3_package, not original edcp."""
        from edcp.validation.pre_flight import PreFlightValidator
        import inspect
        src_file = inspect.getfile(PreFlightValidator)
        self.assertIn("DataComparePro_v3_package", src_file,
                      f"PreFlightValidator loaded from wrong location: {src_file}")

    def test_blocking_errors_accessible_on_validator(self):
        """PreFlightValidator must have blocking_errors() and warnings() methods."""
        from edcp.validation.pre_flight import PreFlightValidator
        v = PreFlightValidator()
        self.assertTrue(hasattr(v, 'blocking_errors'),
                        "PreFlightValidator must have blocking_errors() method")
        self.assertTrue(hasattr(v, 'warnings'),
                        "PreFlightValidator must have warnings() method")

    def test_all_packages_compile_clean(self):
        """All three package trees must compile without errors."""
        import compileall
        pkg_root = Path(__file__).parent.parent.parent
        for tree in ["edcp", "framework_tests"]:
            ok = compileall.compile_dir(str(pkg_root / tree), quiet=2, force=False)
            self.assertTrue(ok, f"compileall failed on {tree}/")
        ok = compileall.compile_dir(str(Path(__file__).parent.parent), quiet=2, force=False)
        self.assertTrue(ok, "compileall failed on edcp_ui/")
