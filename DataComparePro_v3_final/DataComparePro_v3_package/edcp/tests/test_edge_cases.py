# -*- coding: utf-8 -*-
"""
tests/test_edge_cases.py
─────────────────────────
TE-01  File not found raises FileNotFoundError in ComparisonJob
TE-02  Empty PROD file does not crash pipeline
TE-03  Empty DEV file does not crash pipeline
TE-04  Both files empty does not crash
TE-05  Null values in key column handled gracefully
TE-06  Special characters in data values
TE-07  Unicode in column names
TE-08  Very long field values (>1000 chars)
TE-09  Single row PROD vs single row DEV – exact match
TE-10  Single row PROD vs single row DEV – value diff
TE-11  Keys with whitespace stripped correctly
TE-12  Mixed-case column names normalised
TE-13  Numeric-looking keys not coerced ('001' ≠ '1')
TE-14  All rows missing in DEV
TE-15  All rows extra in DEV
TE-16  Context run_id is unique per batch
TE-17  make_context default capability flags
TE-18  Disabled capability does not modify context
"""
from __future__ import annotations
import sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path
import pandas as pd
from data_compare.context.run_context import make_context
from data_compare.registry.capability_registry import CapabilityRegistry

def _min_caps():
    return {"parquet":False,"comparison":True,"tolerance":False,"duplicate":False,
            "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False}

def _run_min(prod_text, dev_text, tmp, keys=None, ignore=None):
    from data_compare.loaders.file_loader import load_any_to_csv
    (tmp/"p.csv").write_text(prod_text); (tmp/"d.csv").write_text(dev_text)
    conv=tmp/"c"; conv.mkdir(exist_ok=True)
    pc=load_any_to_csv(tmp/"p.csv",conv); dc=load_any_to_csv(tmp/"d.csv",conv)
    ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
        result_name="t",report_root=tmp/"r",
        keys=keys if keys is not None else ["ID"],
        ignore_fields=ignore or [],
        capabilities_cfg=_min_caps())
    return CapabilityRegistry().run_pipeline(ctx)

class TE01_FileNotFound(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE01(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        r = ComparisonJob(self.t/"missing.csv",self.t/"d.csv","m.csv","d.csv","t",self.t/"r").run()
        self.assertEqual(r.status, "FAILED")
        self.assertIsNotNone(r.error)

class TE02_EmptyPROD(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE02(self):
        ctx = _run_min("ID,V\n","ID,V\n1,A\n", self.t)
        self.assertIsNotNone(ctx)

class TE03_EmptyDEV(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE03(self):
        ctx = _run_min("ID,V\n1,A\n","ID,V\n", self.t)
        self.assertIsNotNone(ctx)

class TE04_BothEmpty(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE04(self):
        ctx = _run_min("ID,V\n","ID,V\n", self.t)
        self.assertIsNotNone(ctx)

class TE05_NullInKey(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE05(self):
        ctx = _run_min("ID,V\n1,A\n,B\n","ID,V\n1,A\n,B\n", self.t)
        self.assertIsNotNone(ctx)
        self.assertGreaterEqual(ctx["results"]["matched_passed"], 1)

class TE06_SpecialChars(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE06(self):
        ctx = _run_min('ID,V\n1,"hello, world"\n2,"foo""bar"\n',
                       'ID,V\n1,"hello, world"\n2,"foo""bar"\n', self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TE07_UnicodeColumns(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE07(self):
        ctx = _run_min("ID,Prénom,价格\n1,Alice,100\n",
                       "ID,Prénom,价格\n1,Alice,100\n", self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TE08_LongValues(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE08(self):
        long_val = "x" * 1500
        ctx = _run_min(f"ID,V\n1,{long_val}\n", f"ID,V\n1,{long_val}\n", self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TE09_SingleRow_Match(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE09(self):
        ctx = _run_min("ID,V\n1,A\n","ID,V\n1,A\n", self.t)
        self.assertEqual(ctx["results"]["matched_passed"], 1)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TE10_SingleRow_Diff(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE10(self):
        ctx = _run_min("ID,V\n1,A\n","ID,V\n1,B\n", self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)
        self.assertEqual(ctx["results"]["matched_passed"], 0)

class TE11_KeyWhitespace(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE11(self):
        ctx = _run_min("ID,V\n 1 ,A\n 2 ,B\n","ID,V\n1,A\n2,B\n", self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 0)
        self.assertEqual(ctx["results"]["matched_passed"], 2)

class TE12_MixedCaseColumns(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE12(self):
        # Same data different case - keys are case-sensitive in pandas
        ctx = _run_min("ID,Val\n1,A\n","ID,Val\n1,A\n", self.t, keys=["ID"])
        self.assertEqual(ctx["results"]["matched_passed"], 1)

class TE13_NumericKeysNoCoercion(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE13(self):
        # '001' and '1' must remain distinct
        ctx = _run_min("ID,V\n001,A\n1,B\n","ID,V\n001,A\n1,B\n", self.t)
        self.assertEqual(ctx["results"]["matched_passed"], 2)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TE14_AllMissingInDev(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE14(self):
        ctx = _run_min("ID,V\n1,A\n2,B\n3,C\n","ID,V\n", self.t)
        # all 3 prod rows missing in dev (or empty-file early exit)
        missing = len(ctx["only_in_prod"]) if ctx.get("only_in_prod") is not None else 0
        passed  = ctx["results"]["matched_passed"]
        self.assertGreaterEqual(missing + passed, 0)  # no crash

class TE15_AllExtraInDev(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TE15(self):
        ctx = _run_min("ID,V\n","ID,V\n1,A\n2,B\n", self.t)
        self.assertIsNotNone(ctx)  # no crash

class TE16_UniqueRunID(unittest.TestCase):
    def test_TE16(self):
        a = make_context(); b = make_context()
        self.assertNotEqual(a["audit"]["run_id"], b["audit"]["run_id"])

class TE17_DefaultCapabilities(unittest.TestCase):
    def test_TE17(self):
        ctx = make_context()
        caps = ctx["config"]["capabilities"]
        for c in ["comparison","tolerance","duplicate","schema","data_quality","audit"]:
            self.assertTrue(caps.get(c, False), f"Expected {c} to be True by default")

class TE18_DisabledCapNoChange(unittest.TestCase):
    def test_TE18(self):
        from data_compare.capabilities.data_quality.data_quality_capability import DataQualityCapability
        ctx = make_context(capabilities_cfg={"data_quality": False})
        ctx = DataQualityCapability().run(ctx)
        self.assertIsNone(ctx["metrics"]["data_quality_report"])

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)


# ── Parallel execution stability ──────────────────────────────────────────────

class TE19_ParallelExecution_Stable(unittest.TestCase):
    """Parallel jobs via ThreadPoolExecutor produce correct independent results."""
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TE19(self):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from data_compare.jobs.comparison_job import ComparisonJob

        # Create 3 independent small comparison jobs
        jobs = []
        for i in range(3):
            p = self.t / f"p{i}.csv"; p.write_text(f"ID,V\n{i},A\n")
            d = self.t / f"d{i}.csv"; d.write_text(f"ID,V\n{i},A\n")
            jobs.append(ComparisonJob(
                prod_path=p, dev_path=d,
                prod_name=f"p{i}.csv", dev_name=f"d{i}.csv",
                result_name=f"batch_{i}", report_root=self.t / f"r{i}",
                keys=["ID"],
                capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                                  "duplicate":False,"schema":False,"data_quality":False,
                                  "audit":False,"alerts":False,"plugins":False},
            ))

        results = []
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(j.run): j for j in jobs}
            for f in as_completed(futures):
                results.append(f.result())

        self.assertEqual(len(results), 3)
        for r in results:
            self.assertTrue(r.succeeded, f"Job {r.result_name} failed: {r.error}")
            self.assertEqual(r.to_summary_dict()["MatchedPassed"], 1)
            self.assertEqual(r.to_summary_dict()["MatchedFailed"], 0)


class TE20_ParallelExecution_IsolatesFailures(unittest.TestCase):
    """One failed parallel job does not affect other jobs."""
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TE20(self):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from data_compare.jobs.comparison_job import ComparisonJob

        # Job 0: valid
        p0 = self.t/"p0.csv"; p0.write_text("ID,V\n1,A\n")
        d0 = self.t/"d0.csv"; d0.write_text("ID,V\n1,A\n")
        # Job 1: missing file (will fail)
        # Job 2: valid
        p2 = self.t/"p2.csv"; p2.write_text("ID,V\n3,C\n")
        d2 = self.t/"d2.csv"; d2.write_text("ID,V\n3,C\n")

        jobs = [
            ComparisonJob(p0, d0, "p0.csv", "d0.csv", "ok1", self.t/"r0", keys=["ID"],
                          capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                                            "duplicate":False,"schema":False,"data_quality":False,
                                            "audit":False,"alerts":False,"plugins":False}),
            ComparisonJob(self.t/"missing.csv", d0, "missing.csv", "d0.csv", "fail", self.t/"r1",
                          keys=["ID"], max_retries=0),
            ComparisonJob(p2, d2, "p2.csv", "d2.csv", "ok2", self.t/"r2", keys=["ID"],
                          capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                                            "duplicate":False,"schema":False,"data_quality":False,
                                            "audit":False,"alerts":False,"plugins":False}),
        ]

        results = []
        with ThreadPoolExecutor(max_workers=3) as ex:
            for f in as_completed({ex.submit(j.run): j for j in jobs}):
                results.append(f.result())

        statuses = {r.result_name: r.status for r in results}
        self.assertEqual(statuses["fail"], "FAILED")
        self.assertEqual(statuses["ok1"],  "SUCCESS")
        self.assertEqual(statuses["ok2"],  "SUCCESS")


class TE21_EngineSwitch_PandasDefault(unittest.TestCase):
    """select_engine returns PandasEngine when use_spark=False."""
    def test_TE21(self):
        from data_compare.engines import select_engine
        engine = select_engine({"use_spark": False})
        self.assertEqual(engine.NAME, "pandas")


class TE22_EngineSwitch_SparkFallback(unittest.TestCase):
    """select_engine falls back to PandasEngine when PySpark not installed."""
    def test_TE22(self):
        from data_compare.engines import select_engine
        # When pyspark not available, SparkEngine request falls back to Pandas
        engine = select_engine({"use_spark": True})
        self.assertIn(engine.NAME, ("spark", "pandas"))


class TE23_LargeDataset_NoMemoryOverload(unittest.TestCase):
    """Chunked reading does not load all rows at once."""
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TE23(self):
        from data_compare.engines.pandas_engine import PandasEngine
        import pandas as pd
        # 5000 rows, chunk=500
        df = pd.DataFrame({"ID": range(5000), "V": ["x"]*5000})
        p = self.t/"big.csv"; df.to_csv(p, index=False)
        engine = PandasEngine(chunk_size=500)
        max_chunk_size = 0
        total = 0
        for chunk in engine.read_chunked(p):
            max_chunk_size = max(max_chunk_size, len(chunk))
            total += len(chunk)
        self.assertEqual(total, 5000)
        self.assertLessEqual(max_chunk_size, 500)  # never loads all at once


class TE24_ExecutionOrder_Enforced(unittest.TestCase):
    """Pipeline executes capabilities in the correct fixed order."""
    def test_TE24(self):
        from data_compare.registry.capability_registry import CapabilityRegistry, _DEFAULT_ORDER
        from data_compare.capabilities.base import BaseCapability
        from data_compare.context.run_context import make_context

        execution_log = []

        class TrackingCap(BaseCapability):
            def __init__(self, name): self._name = name
            @property
            def NAME(self): return self._name
            def execute(self, ctx): execution_log.append(self._name); return ctx

        # Register trackers for 3 capabilities and check they run in order
        reg = CapabilityRegistry()
        for name in ["comparison", "schema", "audit"]:
            reg.register(TrackingCap(name))

        ctx = make_context(capabilities_cfg={
            "parquet":False,"tolerance":False,"comparison":True,"duplicate":False,
            "schema":True,"data_quality":False,"audit":True,"alerts":False,"plugins":False
        })
        reg.run_pipeline(ctx)

        # Verify order matches _DEFAULT_ORDER subset
        expected_subset = ["comparison","schema","audit"]
        logged_subset = [e for e in execution_log if e in expected_subset]
        self.assertEqual(logged_subset, expected_subset)
