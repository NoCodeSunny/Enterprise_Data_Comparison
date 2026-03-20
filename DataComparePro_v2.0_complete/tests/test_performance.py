# -*- coding: utf-8 -*-
"""
tests/test_performance.py – TPERF01–TPERF08
Performance and scalability tests. All use in-memory generated data.
No network, no disk IO beyond tempfile. Validate: no crash, correct results, bounded memory.
"""
from __future__ import annotations
import sys, os, tempfile, unittest, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path
import pandas as pd

def _caps_min():
    return {"parquet":False,"comparison":True,"tolerance":False,"duplicate":False,
            "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False}

def _write_csv(path, n_rows, n_cols=5, diff_pct=0.01):
    """Write a CSV with n_rows rows; diff_pct fraction have value differences."""
    import random, io
    cols = ["ID"] + [f"COL{i}" for i in range(n_cols)]
    rows = [",".join(cols)]
    n_diff = max(1, int(n_rows * diff_pct))
    diff_ids = set(random.sample(range(n_rows), min(n_diff, n_rows)))
    for i in range(n_rows):
        vals = [str(i)] + [f"val{i}_{j}" for j in range(n_cols)]
        rows.append(",".join(vals))
    path.write_text("\n".join(rows), encoding="utf-8")
    # dev: same but diff rows have altered COL0
    dev_rows = [",".join(cols)]
    for i in range(n_rows):
        if i in diff_ids:
            vals = [str(i)] + [f"DIFF{i}_0"] + [f"val{i}_{j}" for j in range(1, n_cols)]
        else:
            vals = [str(i)] + [f"val{i}_{j}" for j in range(n_cols)]
        dev_rows.append(",".join(vals))
    return "\n".join(dev_rows)

class TPERF01_Large_CSV_100k(unittest.TestCase):
    """TPERF01: 100k rows CSV comparison completes correctly."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF01(self):
        n = 100_000
        prod_p = self.t/"prod.csv"
        dev_text = _write_csv(prod_p, n, n_cols=4, diff_pct=0.005)
        (self.t/"dev.csv").write_text(dev_text, encoding="utf-8")
        from data_compare.loaders.file_loader import load_any_to_csv
        from data_compare.context.run_context import make_context
        from data_compare.registry.capability_registry import CapabilityRegistry
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(prod_p,conv); dc=load_any_to_csv(self.t/"dev.csv",conv)
        t0 = time.perf_counter()
        ctx = make_context(prod_csv_path=pc, dev_csv_path=dc,
            prod_name="prod.csv", dev_name="dev.csv",
            result_name="perf1", report_root=self.t/"r",
            keys=["ID"], capabilities_cfg=_caps_min())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        elapsed = time.perf_counter() - t0
        total = ctx["results"]["matched_passed"] + ctx["results"]["matched_failed"]
        self.assertEqual(total, n)
        self.assertLess(elapsed, 120)   # must complete within 2 minutes
        self.assertGreater(ctx["results"]["matched_passed"], 0)

class TPERF02_Large_CSV_500k(unittest.TestCase):
    """TPERF02: 500k rows CSV comparison — no OOM, correct counts."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF02(self):
        n = 500_000
        prod_p = self.t/"prod.csv"
        dev_text = _write_csv(prod_p, n, n_cols=3, diff_pct=0.001)
        (self.t/"dev.csv").write_text(dev_text, encoding="utf-8")
        from data_compare.loaders.file_loader import load_any_to_csv
        from data_compare.context.run_context import make_context
        from data_compare.registry.capability_registry import CapabilityRegistry
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(prod_p,conv); dc=load_any_to_csv(self.t/"dev.csv",conv)
        ctx = make_context(prod_csv_path=pc, dev_csv_path=dc,
            prod_name="prod.csv", dev_name="dev.csv",
            result_name="perf2", report_root=self.t/"r",
            keys=["ID"], capabilities_cfg=_caps_min())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        total = ctx["results"]["matched_passed"] + ctx["results"]["matched_failed"]
        self.assertEqual(total, n)

class TPERF03_Parquet_Auto_Select_No_Spark(unittest.TestCase):
    """TPERF03: Parquet capability activates for .parquet files without crashing."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF03(self):
        from data_compare.capabilities.parquet.parquet_capability import ParquetCapability
        p = self.t/"prod.parquet"; p.write_bytes(b"PAR1"+b"\x00"*8+b"PAR1")
        d = self.t/"dev.csv"; d.write_text("ID,V\n1,A\n")
        from data_compare.context.run_context import make_context
        ctx = make_context(prod_csv_path=p, dev_csv_path=d,
                           capabilities_cfg={"parquet":True})
        ctx = ParquetCapability().run(ctx)
        self.assertTrue(ctx["config"].get("use_spark", False))

class TPERF04_Skewed_Keys_Heavy_Join(unittest.TestCase):
    """TPERF04: Many rows sharing same key (high skew) — alignment stable."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF04(self):
        # 1000 rows with 10 distinct keys (100 rows per key = high skew)
        n = 1000
        rows = [f"K{i%10},{i}" for i in range(n)]
        (self.t/"p.csv").write_text("ID,V\n"+"\n".join(rows))
        (self.t/"d.csv").write_text("ID,V\n"+"\n".join(rows))
        from data_compare.loaders.file_loader import load_any_to_csv
        from data_compare.context.run_context import make_context
        from data_compare.registry.capability_registry import CapabilityRegistry
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc, dev_csv_path=dc,
            prod_name="p.csv", dev_name="d.csv",
            result_name="skew", report_root=self.t/"r",
            keys=["ID"],
            capabilities_cfg={**_caps_min(),"duplicate":True})
        ctx = CapabilityRegistry().run_pipeline(ctx)
        total = ctx["results"]["matched_passed"] + ctx["results"]["matched_failed"]
        self.assertEqual(total, n)
        self.assertTrue(ctx["using_seq"])

class TPERF05_High_Duplicate_Density(unittest.TestCase):
    """TPERF05: 50% of rows are duplicates — _SEQ_ alignment handles correctly."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF05(self):
        n = 2000
        rows_p = [f"K{i%1000},{i}" for i in range(n)]   # every key appears twice
        rows_d = [f"K{i%1000},{i}" for i in range(n)]
        (self.t/"p.csv").write_text("ID,V\n"+"\n".join(rows_p))
        (self.t/"d.csv").write_text("ID,V\n"+"\n".join(rows_d))
        from data_compare.loaders.file_loader import load_any_to_csv
        from data_compare.context.run_context import make_context
        from data_compare.registry.capability_registry import CapabilityRegistry
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc, dev_csv_path=dc,
            prod_name="p.csv", dev_name="d.csv",
            result_name="dups", report_root=self.t/"r",
            keys=["ID"],
            capabilities_cfg={**_caps_min(),"duplicate":True})
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertEqual(ctx["results"]["matched_failed"], 0)
        self.assertEqual(ctx["dup_count_prod"], 1000)

class TPERF06_Memory_Stability_Chunked(unittest.TestCase):
    """TPERF06: Chunked reading processes all rows without memory spike."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF06(self):
        from data_compare.engines.pandas_engine import PandasEngine
        n = 10_000
        rows = [f"{i},val{i}" for i in range(n)]
        p = self.t/"big.csv"
        p.write_text("ID,V\n"+"\n".join(rows))
        engine = PandasEngine(chunk_size=1000)
        total = sum(len(c) for c in engine.read_chunked(p))
        self.assertEqual(total, n)

class TPERF07_Chunking_Correctness(unittest.TestCase):
    """TPERF07: Chunked read yields exactly correct total rows and no duplicates."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF07(self):
        from data_compare.engines.pandas_engine import PandasEngine
        n = 7_777  # non-round number
        rows = [f"{i}" for i in range(n)]
        p = self.t/"f.csv"
        p.write_text("ID\n"+"\n".join(rows))
        engine = PandasEngine(chunk_size=1000)
        all_ids = []
        for chunk in engine.read_chunked(p):
            all_ids.extend(chunk["ID"].tolist())
        self.assertEqual(len(all_ids), n)
        self.assertEqual(len(set(all_ids)), n)   # no duplicates

class TPERF08_Auto_Engine_Threshold(unittest.TestCase):
    """TPERF08: select_engine auto-switches based on file size threshold."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TPERF08(self):
        from data_compare.engines import select_engine
        small_p = self.t/"s.csv"; small_p.write_text("ID\n1\n")
        engine = select_engine(
            {"spark_threshold_bytes": 1_000_000_000},
            prod_path=small_p, dev_path=small_p
        )
        self.assertEqual(engine.NAME, "pandas")  # small file → pandas

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
