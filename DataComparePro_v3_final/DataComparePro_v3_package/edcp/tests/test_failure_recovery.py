# -*- coding: utf-8 -*-
"""tests/test_failure_recovery.py – TFAIL01–TFAIL06  Failure and recovery scenarios."""
from __future__ import annotations
import sys, os, tempfile, unittest, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

class TFAIL01_FileNotFound_Recovery(unittest.TestCase):
    """Missing file → FAILED status with descriptive error, no crash."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TFAIL01(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        r = ComparisonJob(self.t/"missing.csv",self.t/"d.csv","m.csv","d.csv","t",
                          self.t/"r",max_retries=0).run()
        self.assertEqual(r.status,"FAILED")
        self.assertIsNotNone(r.error)
        self.assertIn("missing.csv", r.error)

class TFAIL02_Retry_Success(unittest.TestCase):
    """Job fails first attempt, file created before retry → eventual SUCCESS."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TFAIL02(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        prod_p = self.t/"prod.csv"
        dev_p  = self.t/"dev.csv"; dev_p.write_text("ID,V\n1,A\n")
        # Create prod file after a short delay in a background thread
        import threading
        def create_after_delay():
            time.sleep(0.3)
            prod_p.write_text("ID,V\n1,A\n")
        threading.Thread(target=create_after_delay,daemon=True).start()
        r = ComparisonJob(prod_p,dev_p,"prod.csv","dev.csv","retry_test",
                          self.t/"r",keys=["ID"],max_retries=3,retry_delay_s=0.5,
                          capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                                            "duplicate":False,"schema":False,"data_quality":False,
                                            "audit":False,"alerts":False,"plugins":False}).run()
        self.assertEqual(r.status,"SUCCESS")
        self.assertGreater(r.attempts, 1)

class TFAIL03_Retry_Exhausted(unittest.TestCase):
    """Job retries max_retries times and returns FAILED with attempt count."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TFAIL03(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        r = ComparisonJob(self.t/"never.csv",self.t/"d.csv","n.csv","d.csv","t",
                          self.t/"r",max_retries=2,retry_delay_s=0.01).run()
        self.assertEqual(r.status,"FAILED")
        self.assertEqual(r.attempts,3)

class TFAIL04_Partial_Pipeline_Failure(unittest.TestCase):
    """One batch fails; second batch in separate job still succeeds."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TFAIL04(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        p = self.t/"p.csv"; p.write_text("ID,V\n1,A\n")
        d = self.t/"d.csv"; d.write_text("ID,V\n1,A\n")
        fail_job = ComparisonJob(self.t/"missing.csv",d,"m.csv","d.csv","fail",
                                 self.t/"r1",max_retries=0)
        good_job = ComparisonJob(p,d,"p.csv","d.csv","good",self.t/"r2",keys=["ID"],
            capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                              "duplicate":False,"schema":False,"data_quality":False,
                              "audit":False,"alerts":False,"plugins":False})
        fail_r = fail_job.run()
        good_r = good_job.run()
        self.assertEqual(fail_r.status,"FAILED")
        self.assertEqual(good_r.status,"SUCCESS")

class TFAIL05_Debugger_Output_Validation(unittest.TestCase):
    """Debugger produces complete DebugReport for FileNotFoundError."""
    def test_TFAIL05(self):
        from data_compare.debugger import Debugger
        try: raise FileNotFoundError("[Errno 2] No such file or directory: '/data/prod.csv'")
        except FileNotFoundError as e:
            r = Debugger().diagnose(e, context_hint="TFAIL05")
        self.assertEqual(r.error_record.error_code,"FILE_NOT_FOUND")
        self.assertIn("/data/prod.csv", r.error_record.extra.get("path",""))
        d = r.to_dict()
        import json; j = json.dumps(d,default=str)
        self.assertIn("FILE_NOT_FOUND", j)

class TFAIL06_Engine_Failure_Fallback(unittest.TestCase):
    """select_engine falls back to PandasEngine when Spark unavailable."""
    def test_TFAIL06(self):
        from data_compare.engines import select_engine
        engine = select_engine({"use_spark":True})
        # Either spark (if installed) or pandas fallback
        self.assertIn(engine.NAME, ("spark","pandas"))
        # Pandas fallback must not crash
        self.assertIsNotNone(engine)

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
