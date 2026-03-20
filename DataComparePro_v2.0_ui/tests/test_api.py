# -*- coding: utf-8 -*-
"""
data_compare_ui/tests/test_api.py
───────────────────────────────────
Integration tests for the data_compare UI API.

Covers:
  TAPI01  GET /api/health returns 200 with version
  TAPI02  POST /api/detect-columns with valid CSV path
  TAPI03  POST /api/detect-columns with missing path returns 400
  TAPI04  POST /api/detect-columns with non-existent file returns 404
  TAPI05  GET /api/capabilities returns list of capabilities
  TAPI06  POST /api/run with valid payload returns 202 + job_id
  TAPI07  POST /api/run with missing prod_path returns 400
  TAPI08  GET /api/job/<id> returns job record
  TAPI09  GET /api/job/<unknown> returns 404
  TAPI10  GET /api/history returns paginated list
  TAPI11  GET /api/history with status filter works
  TAPI12  DELETE /api/job/<id> removes job
  TAPI13  GET /api/download/<id>/excel for unknown job returns 404
  TAPI14  Full E2E: submit → poll → SUCCESS result
  TAPI15  Full E2E: invalid file path → job FAILS with debug report
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

# ── ensure framework + UI are importable ─────────────────────────────────────
_UI_ROOT = Path(__file__).parent.parent
_FW_ROOT = _UI_ROOT.parent / "data_compare_framework"
for p in (str(_UI_ROOT), str(_FW_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from api.app import app, _init_db


class APITestCase(unittest.TestCase):
    """Base class: Flask test client + shared temp directory."""

    @classmethod
    def setUpClass(cls):
        app.config["TESTING"] = True
        cls.client = app.test_client()
        _init_db()

    def _post(self, url, body):
        return self.client.post(
            url,
            data=json.dumps(body),
            content_type="application/json",
        )

    def _get(self, url):
        return self.client.get(url)


class TAPI01_Health(APITestCase):
    def test_TAPI01(self):
        r = self._get("/api/health")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn("status", d)
        self.assertEqual(d["status"], "ok")
        self.assertIn("version", d)


class TAPI02_DetectColumns_Valid(APITestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TAPI02(self):
        csv = self.t / "data.csv"
        csv.write_text("TradeID,Portfolio,Price\n1,EQ,100\n")
        r = self._post("/api/detect-columns", {"path": str(csv)})
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn("columns", d)
        self.assertEqual(d["columns"], ["TradeID", "Portfolio", "Price"])


class TAPI03_DetectColumns_MissingPath(APITestCase):
    def test_TAPI03(self):
        r = self._post("/api/detect-columns", {})
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertIn("error", d)


class TAPI04_DetectColumns_NotFound(APITestCase):
    def test_TAPI04(self):
        r = self._post("/api/detect-columns", {"path": "/nonexistent/data.csv"})
        self.assertEqual(r.status_code, 404)
        d = json.loads(r.data)
        self.assertIn("error", d)


class TAPI05_Capabilities(APITestCase):
    def test_TAPI05(self):
        r = self._get("/api/capabilities")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIsInstance(d, list)
        self.assertGreater(len(d), 0)
        names = [c["name"] for c in d]
        self.assertIn("comparison", names)
        self.assertIn("parquet",    names)


class TAPI06_SubmitRun_Valid(APITestCase):
    def test_TAPI06(self):
        r = self._post("/api/run", {
            "prod_path":   "/tmp/prod.csv",
            "dev_path":    "/tmp/dev.csv",
            "result_name": "API_Test",
            "keys":        ["ID"],
        })
        self.assertEqual(r.status_code, 202)
        d = json.loads(r.data)
        self.assertIn("job_id", d)
        self.assertIn("status", d)
        self.assertGreater(len(d["job_id"]), 0)


class TAPI07_SubmitRun_MissingPaths(APITestCase):
    def test_TAPI07(self):
        r = self._post("/api/run", {"result_name": "no paths"})
        self.assertEqual(r.status_code, 400)
        d = json.loads(r.data)
        self.assertIn("error", d)


class TAPI08_GetJob(APITestCase):
    def test_TAPI08(self):
        # Submit first
        r1 = self._post("/api/run", {"prod_path":"/tmp/p.csv","dev_path":"/tmp/d.csv"})
        jid = json.loads(r1.data)["job_id"]
        # Now get it
        r2 = self._get(f"/api/job/{jid}")
        self.assertEqual(r2.status_code, 200)
        d = json.loads(r2.data)
        self.assertEqual(d["job_id"], jid)
        self.assertIn("status", d)


class TAPI09_GetJob_NotFound(APITestCase):
    def test_TAPI09(self):
        r = self._get("/api/job/nonexistent_job_id_xyz")
        self.assertEqual(r.status_code, 404)


class TAPI10_History(APITestCase):
    def test_TAPI10(self):
        r = self._get("/api/history?limit=10")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn("jobs",  d)
        self.assertIn("total", d)
        self.assertIsInstance(d["jobs"], list)


class TAPI11_History_StatusFilter(APITestCase):
    def test_TAPI11(self):
        r = self._get("/api/history?status=SUCCESS&limit=5")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        # All returned jobs must have SUCCESS status
        for job in d["jobs"]:
            self.assertEqual(job["status"], "SUCCESS")


class TAPI12_DeleteJob(APITestCase):
    def test_TAPI12(self):
        r1 = self._post("/api/run",{"prod_path":"/tmp/p.csv","dev_path":"/tmp/d.csv"})
        jid = json.loads(r1.data)["job_id"]
        r2 = self.client.delete(f"/api/job/{jid}")
        self.assertIn(r2.status_code, (200, 204))
        # Verify gone
        r3 = self._get(f"/api/job/{jid}")
        self.assertEqual(r3.status_code, 404)


class TAPI13_Download_NotFound(APITestCase):
    def test_TAPI13(self):
        r = self._get("/api/download/no_such_job/excel")
        self.assertEqual(r.status_code, 404)


class TAPI14_E2E_Success(APITestCase):
    """End-to-end: submit → poll → SUCCESS with real files."""
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TAPI14(self):
        prod = self.t / "prod.csv"
        dev  = self.t / "dev.csv"
        prod.write_text("ID,V\n1,A\n2,B\n3,C\n")
        dev.write_text( "ID,V\n1,A\n2,X\n3,C\n")   # row 2 differs

        r = self._post("/api/run", {
            "prod_path":   str(prod),
            "dev_path":    str(dev),
            "result_name": "E2E_Test",
            "keys":        ["ID"],
            "capabilities": {
                "parquet":False,"comparison":True,"tolerance":False,
                "duplicate":False,"schema":False,"data_quality":False,
                "audit":False,"alerts":False,"plugins":False,
            },
        })
        self.assertEqual(r.status_code, 202)
        jid = json.loads(r.data)["job_id"]

        # Poll until done (max 60s)
        deadline = time.time() + 60
        status = "QUEUED"
        while status in ("QUEUED","RUNNING") and time.time() < deadline:
            time.sleep(0.5)
            d = json.loads(self._get(f"/api/job/{jid}").data)
            status = d.get("status","QUEUED")

        self.assertEqual(status, "SUCCESS", f"Job did not succeed in time: {status}")
        final = json.loads(self._get(f"/api/job/{jid}").data)
        s = final.get("summary") or {}
        self.assertEqual(s.get("MatchedFailed"), 1)
        self.assertEqual(s.get("MatchedPassed"), 2)
        self.assertIsNotNone(final.get("report_xlsx"))


class TAPI15_E2E_Failure_DebugReport(APITestCase):
    """E2E: invalid file path → job FAILS with debug_report populated."""
    def test_TAPI15(self):
        r = self._post("/api/run", {
            "prod_path":   "/nonexistent/prod_missing.csv",
            "dev_path":    "/nonexistent/dev_missing.csv",
            "result_name": "Fail_Test",
        })
        self.assertEqual(r.status_code, 202)
        jid = json.loads(r.data)["job_id"]

        deadline = time.time() + 30
        status = "QUEUED"
        while status in ("QUEUED","RUNNING") and time.time() < deadline:
            time.sleep(0.5)
            d = json.loads(self._get(f"/api/job/{jid}").data)
            status = d.get("status","QUEUED")

        self.assertEqual(status, "FAILED")
        final = json.loads(self._get(f"/api/job/{jid}").data)
        self.assertIsNotNone(final.get("error_msg"))
        # debug_report or debug_summary should be populated
        has_debug = bool(final.get("debug_summary")) or bool(final.get("debug_report"))
        self.assertTrue(has_debug, "Expected debug information in failed job")


if __name__ == "__main__":
    unittest.main(verbosity=2)
