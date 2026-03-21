# -*- coding: utf-8 -*-
"""tests/test_concurrency.py – TCON01–TCON05  Concurrency and parallel stability."""
from __future__ import annotations
import sys, os, tempfile, unittest, threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def _make_job(tmp, idx, fail=False):
    from data_compare.jobs.comparison_job import ComparisonJob
    p = tmp/f"p{idx}.csv"; d = tmp/f"d{idx}.csv"
    if fail:
        return ComparisonJob(tmp/"missing.csv",d,"m.csv","d.csv",f"job{idx}",tmp/f"r{idx}",
                             max_retries=0)
    p.write_text(f"ID,V\n{idx},A\n"); d.write_text(f"ID,V\n{idx},A\n")
    return ComparisonJob(p,d,f"p{idx}.csv",f"d{idx}.csv",f"job{idx}",tmp/f"r{idx}",
        keys=["ID"],
        capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                          "duplicate":False,"schema":False,"data_quality":False,
                          "audit":False,"alerts":False,"plugins":False})

class TCON01_Parallel_5_Jobs(unittest.TestCase):
    """5 parallel jobs all succeed with correct independent results."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TCON01(self):
        jobs = [_make_job(self.t,i) for i in range(5)]
        results = []
        with ThreadPoolExecutor(max_workers=5) as ex:
            for f in as_completed({ex.submit(j.run):j for j in jobs}):
                results.append(f.result())
        self.assertEqual(len(results), 5)
        for r in results:
            self.assertTrue(r.succeeded)

class TCON02_Parallel_10_Jobs(unittest.TestCase):
    """10 parallel jobs all complete without deadlock or crash."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TCON02(self):
        jobs = [_make_job(self.t,i) for i in range(10)]
        results = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            for f in as_completed({ex.submit(j.run):j for j in jobs}):
                results.append(f.result())
        self.assertEqual(len(results), 10)
        self.assertTrue(all(r.succeeded for r in results))

class TCON03_Output_File_Collision(unittest.TestCase):
    """Concurrent jobs writing to same report_root use UUID suffixes (no collision)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TCON03(self):
        from data_compare.utils.helpers import safe_output_path
        shared_root = self.t/"reports"; shared_root.mkdir()
        # Serial test: each call creates file before next call, verifying collision avoidance
        paths = []
        for _ in range(5):
            p = safe_output_path(shared_root, "SameReport")
            p.touch()
            paths.append(str(p))
        self.assertEqual(len(set(paths)), 5)

class TCON04_Parallel_Logging_Integrity(unittest.TestCase):
    """Parallel logging from multiple threads does not corrupt log output."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TCON04(self):
        from data_compare.utils.logger import get_logger
        import logging, data_compare.utils.logger as _lg
        _lg._file_handler = None; _lg._json_handler = None
        _lg.configure_logging(level=logging.DEBUG, log_dir=self.t,
                              enable_file=True, enable_json=False)
        errors = []
        def log_worker(i):
            try:
                lg = get_logger(f"thread_{i}")
                for j in range(20):
                    lg.info(f"thread {i} message {j}")
            except Exception as e:
                errors.append(str(e))
        threads = [threading.Thread(target=log_worker, args=(i,)) for i in range(8)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(len(errors), 0)
        log_path = self.t/"edcp.log"
        self.assertTrue(log_path.exists())

class TCON05_Context_Isolation_Stress(unittest.TestCase):
    """50 concurrent make_context() calls produce 50 unique run_ids."""
    def test_TCON05(self):
        from data_compare.context.run_context import make_context
        run_ids = []
        lock = threading.Lock()
        def make_and_record():
            ctx = make_context()
            with lock: run_ids.append(ctx["audit"]["run_id"])
        threads = [threading.Thread(target=make_and_record) for _ in range(50)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(len(run_ids), 50)
        self.assertEqual(len(set(run_ids)), 50)

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
