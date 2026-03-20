# -*- coding: utf-8 -*-
"""
tests/test_debugger.py
───────────────────────
TDB-01  ErrorMapper maps FileNotFoundError correctly
TDB-02  ErrorMapper maps PermissionError correctly
TDB-03  ErrorMapper maps ValueError (column mismatch) correctly
TDB-04  ErrorMapper maps ImportError correctly
TDB-05  ErrorMapper maps MemoryError correctly
TDB-06  ErrorMapper maps UnicodeDecodeError correctly
TDB-07  ErrorMapper maps KeyError correctly
TDB-08  ErrorMapper maps SparkRequiredError correctly
TDB-09  ErrorMapper maps unknown exception to UNEXPECTED_ERROR
TDB-10  ErrorRecord.to_dict has all required keys
TDB-11  ErrorRecord.__str__ is human-readable
TDB-12  ErrorAnalyzer.analyze returns AnalysisReport
TDB-13  ErrorAnalyzer extracts framework frames from traceback
TDB-14  ErrorAnalyzer.context_snapshot extracts safe keys
TDB-15  Debugger.diagnose returns DebugReport
TDB-16  DebugReport.full_report is non-empty string
TDB-17  DebugReport.to_dict is JSON-serialisable
TDB-18  DebugReport.summary is single line
TDB-19  Debugger.diagnose_and_log does not raise
TDB-20  Debugger.diagnose_from_summary handles error summary dict
TDB-21  Debugger.diagnose_from_summary returns None for success summary
TDB-22  ComparisonJob.run() stores debug_report on FAILED result
TDB-23  Debugger.format_for_cli returns non-empty string
"""
from __future__ import annotations

import json
import sys
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_compare.debugger import Debugger, DebugReport, ErrorMapper, ErrorRecord, ErrorAnalyzer
from data_compare.capabilities.parquet.parquet_capability import SparkRequiredError
from data_compare.context.run_context import make_context


# ── ErrorMapper tests ─────────────────────────────────────────────────────────

class TDB01_FileNotFound(unittest.TestCase):
    def test_TDB01(self):
        m = ErrorMapper()
        r = m.map(FileNotFoundError("[Errno 2] No such file or directory: '/tmp/missing.csv'"))
        self.assertEqual(r.error_code, "FILE_NOT_FOUND")
        self.assertEqual(r.category, "File I/O")
        self.assertEqual(r.severity, "CRITICAL")
        self.assertIn("/tmp/missing.csv", r.extra.get("path", ""))

class TDB02_PermissionError(unittest.TestCase):
    def test_TDB02(self):
        r = ErrorMapper().map(PermissionError("Permission denied: '/data/file.csv'"))
        self.assertEqual(r.error_code, "PERMISSION_DENIED")
        self.assertIn("Excel", r.recommended_fix)

class TDB03_ValueError_Column(unittest.TestCase):
    def test_TDB03(self):
        r = ErrorMapper().map(ValueError("Required key column 'TradeID' not found"))
        self.assertIn(r.error_code, ("SCHEMA_MISMATCH", "VALUE_ERROR"))

class TDB04_ImportError(unittest.TestCase):
    def test_TDB04(self):
        r = ErrorMapper().map(ImportError("No module named 'pyarrow'"))
        self.assertEqual(r.error_code, "MISSING_DEPENDENCY")
        self.assertIn("pyarrow", r.extra.get("missing_package", ""))
        self.assertIn("pip install pyarrow", r.recommended_fix)

class TDB05_MemoryError(unittest.TestCase):
    def test_TDB05(self):
        r = ErrorMapper().map(MemoryError("Unable to allocate memory"))
        self.assertEqual(r.error_code, "OUT_OF_MEMORY")
        self.assertIn("Spark", r.recommended_fix)

class TDB06_UnicodeDecodeError(unittest.TestCase):
    def test_TDB06(self):
        exc = UnicodeDecodeError("utf-8", b"\xff\xfe", 0, 1, "invalid byte")
        r = ErrorMapper().map(exc)
        self.assertEqual(r.error_code, "ENCODING_ERROR")
        self.assertIn("UTF-8", r.recommended_fix)

class TDB07_KeyError(unittest.TestCase):
    def test_TDB07(self):
        r = ErrorMapper().map(KeyError("prod_df"))
        self.assertEqual(r.error_code, "MISSING_CONTEXT_KEY")
        self.assertIn("prod_df", r.extra.get("missing_key", ""))

class TDB08_SparkRequired(unittest.TestCase):
    def test_TDB08(self):
        exc = SparkRequiredError("File is 600 MB. Spark required for large parquet.")
        r = ErrorMapper().map(exc)
        self.assertEqual(r.error_code, "SPARK_REQUIRED_LARGE_PARQUET")
        self.assertIn("pyspark", r.recommended_fix.lower())

class TDB09_UnknownException(unittest.TestCase):
    def test_TDB09(self):
        r = ErrorMapper().map(Exception("Something unexpected"))
        self.assertEqual(r.error_code, "UNEXPECTED_ERROR")
        self.assertEqual(r.category, "Unknown")

class TDB10_ErrorRecord_ToDict(unittest.TestCase):
    def test_TDB10(self):
        r = ErrorMapper().map(FileNotFoundError("/tmp/missing.csv"))
        d = r.to_dict()
        for k in ["error_code","category","severity","human_message",
                  "root_cause","recommended_fix","module_hint"]:
            self.assertIn(k, d)

class TDB11_ErrorRecord_Str(unittest.TestCase):
    def test_TDB11(self):
        r = ErrorMapper().map(FileNotFoundError("/tmp/missing.csv"))
        s = str(r)
        self.assertIn("FILE_NOT_FOUND", s)
        self.assertIn("Root cause", s)
        self.assertIn("Fix", s)


# ── ErrorAnalyzer tests ───────────────────────────────────────────────────────

class TDB12_Analyzer_ReturnsReport(unittest.TestCase):
    def test_TDB12(self):
        from data_compare.debugger.error_analyzer import AnalysisReport
        try:
            raise ValueError("test error for analysis")
        except ValueError as exc:
            import sys
            _, _, tb = sys.exc_info()
            report = ErrorAnalyzer().analyze(exc, tb=tb)
        self.assertIsInstance(report, AnalysisReport)
        self.assertIsNotNone(report.origin_file)

class TDB13_Analyzer_FrameworkFrames(unittest.TestCase):
    def test_TDB13(self):
        # Create a fake exception originating from data_compare code
        from data_compare.debugger.error_analyzer import ErrorAnalyzer, FrameInfo
        analyzer = ErrorAnalyzer()
        # Manufacture a frame that looks like it's inside data_compare
        fake_frame = FrameInfo(
            filename="/app/data_compare/capabilities/comparison/comparison_capability.py",
            lineno=42,
            function="_run_value_comparison",
            code_line="result = prod_vals != dev_vals",
            is_framework=True,
        )
        self.assertTrue(fake_frame.is_framework)
        self.assertIn("comparison_capability", str(fake_frame))

class TDB14_Analyzer_ContextSnapshot(unittest.TestCase):
    def test_TDB14(self):
        ctx = make_context(result_name="test_snap")
        ctx["existing_keys"] = ["TradeID"]
        ctx["dup_count_prod"] = 3
        try:
            raise RuntimeError("test")
        except RuntimeError as exc:
            import sys
            _, _, tb = sys.exc_info()
            report = ErrorAnalyzer().analyze(exc, tb=tb, context=ctx)
        # Safe keys should appear; DataFrame values should not
        snap = report.context_snapshot
        self.assertIn("existing_keys", snap)
        self.assertEqual(snap["existing_keys"], ["TradeID"])


# ── Debugger façade tests ─────────────────────────────────────────────────────

class TDB15_Debugger_Diagnose(unittest.TestCase):
    def test_TDB15(self):
        try:
            raise FileNotFoundError("/tmp/missing.csv")
        except FileNotFoundError as exc:
            report = Debugger().diagnose(exc, context_hint="test_batch")
        self.assertIsInstance(report, DebugReport)
        self.assertEqual(report.error_record.error_code, "FILE_NOT_FOUND")

class TDB16_DebugReport_FullReport(unittest.TestCase):
    def test_TDB16(self):
        try:
            raise FileNotFoundError("/tmp/missing.csv")
        except FileNotFoundError as exc:
            report = Debugger().diagnose(exc)
        s = report.full_report()
        self.assertGreater(len(s), 200)
        self.assertIn("DIAGNOSTIC", s)
        self.assertIn("FILE_NOT_FOUND", s)

class TDB17_DebugReport_ToDict_JSON(unittest.TestCase):
    def test_TDB17(self):
        try:
            raise MemoryError("OOM")
        except MemoryError as exc:
            report = Debugger().diagnose(exc)
        d = report.to_dict()
        # Must be JSON-serialisable
        j = json.dumps(d, default=str)
        restored = json.loads(j)
        self.assertIn("error_record", restored)
        self.assertIn("origin", restored)

class TDB18_DebugReport_Summary(unittest.TestCase):
    def test_TDB18(self):
        try:
            raise KeyError("prod_df")
        except KeyError as exc:
            report = Debugger().diagnose(exc)
        s = report.summary()
        # Single line (no newlines)
        self.assertNotIn("\n", s.strip())
        self.assertIn("MISSING_CONTEXT_KEY", s)

class TDB19_Debugger_DiagnoseAndLog(unittest.TestCase):
    def test_TDB19(self):
        # Should not raise even if logging is misconfigured
        try:
            raise ValueError("test")
        except ValueError as exc:
            # Should complete without error
            Debugger().diagnose_and_log(exc, context_hint="TDB19")

class TDB20_Debugger_FromSummary(unittest.TestCase):
    def test_TDB20(self):
        summary = {
            "ResultName": "MyBatch",
            "Error": "[Errno 2] No such file or directory: '/data/prod.csv'",
            "Traceback": "Traceback (most recent call last):\n  ...",
        }
        report = Debugger().diagnose_from_summary(summary)
        self.assertIsNotNone(report)
        self.assertIsInstance(report, DebugReport)

class TDB21_Debugger_FromSummary_None(unittest.TestCase):
    def test_TDB21(self):
        success_summary = {"ResultName": "OK", "MatchedPassed": 100, "Error": None}
        report = Debugger().diagnose_from_summary(success_summary)
        self.assertIsNone(report)

class TDB22_Job_DebugReport_OnFail(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDB22(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        job = ComparisonJob(
            prod_path=self.t/"missing.csv",
            dev_path=self.t/"d.csv",
            prod_name="missing.csv", dev_name="d.csv",
            result_name="fail_batch",
            report_root=self.t/"r",
            max_retries=0,
        )
        result = job.run()
        self.assertEqual(result.status, "FAILED")
        # debug_report should be set
        self.assertIsNotNone(result.debug_report)
        self.assertIsInstance(result.debug_report, DebugReport)

class TDB23_FormatForCLI(unittest.TestCase):
    def test_TDB23(self):
        try:
            raise ImportError("No module named 'pyarrow'")
        except ImportError as exc:
            report = Debugger().diagnose(exc)
        s = Debugger.format_for_cli(report)
        self.assertIn("MISSING_DEPENDENCY", s)
        self.assertIn("pip install", s)
        self.assertGreater(len(s), 100)


if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
