# -*- coding: utf-8 -*-
"""tests/test_key_edge_cases.py – TKEY01–TKEY07  Key matching edge cases."""
from __future__ import annotations
import sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

def _run(prod, dev, tmp, keys=None, ignore=None):
    from data_compare.loaders.file_loader import load_any_to_csv
    from data_compare.context.run_context import make_context
    from data_compare.registry.capability_registry import CapabilityRegistry
    (tmp/"p.csv").write_text(prod); (tmp/"d.csv").write_text(dev)
    conv=tmp/"c"; conv.mkdir(exist_ok=True)
    pc=load_any_to_csv(tmp/"p.csv",conv); dc=load_any_to_csv(tmp/"d.csv",conv)
    ctx=make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
        result_name="t",report_root=tmp/"r",keys=keys or ["ID"],
        ignore_fields=ignore or [],
        capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                          "duplicate":True,"schema":False,"data_quality":False,
                          "audit":False,"alerts":False,"plugins":False})
    return CapabilityRegistry().run_pipeline(ctx)

class TKEY01_Dup_Keys_One_Side_Only(unittest.TestCase):
    """Duplicates in PROD only — extra row appears in only_in_prod after _SEQ_ alignment."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TKEY01(self):
        # PROD: T001 twice; DEV: T001 once
        ctx=_run("ID,V\nT001,A\nT001,B\n","ID,V\nT001,A\n",self.t)
        # One T001 matches, one is only in prod
        self.assertEqual(len(ctx["only_in_prod"]), 1)
        self.assertTrue(ctx["using_seq"])

class TKEY02_Null_Keys_Handling(unittest.TestCase):
    """Blank/null key values do not crash comparison."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TKEY02(self):
        ctx=_run("ID,V\n,A\n1,B\n","ID,V\n,A\n1,B\n",self.t)
        self.assertIsNotNone(ctx)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TKEY03_Case_Sensitive_Keys(unittest.TestCase):
    """Keys are case-sensitive: 'ABC' and 'abc' are different keys."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TKEY03(self):
        ctx=_run("ID,V\nABC,A\n","ID,V\nabc,A\n",self.t)
        # ABC not found in DEV (abc ≠ ABC)
        self.assertEqual(len(ctx["only_in_prod"]), 1)
        self.assertEqual(len(ctx["only_in_dev"]),  1)

class TKEY04_Whitespace_In_Keys(unittest.TestCase):
    """Key ' T001 ' with whitespace is normalised to 'T001' — matches 'T001'."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TKEY04(self):
        ctx=_run("ID,V\n T001 ,A\n","ID,V\nT001,A\n",self.t)
        self.assertEqual(ctx["results"]["matched_passed"], 1)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TKEY05_Multi_Key_Partial_Match(unittest.TestCase):
    """Composite key: (T001,EQ) matches but (T001,FI) does not exist in DEV."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TKEY05(self):
        ctx=_run("ID,Port,V\nT001,EQ,A\nT001,FI,B\n",
                 "ID,Port,V\nT001,EQ,A\n",
                 self.t, keys=["ID","Port"])
        self.assertEqual(ctx["results"]["matched_passed"], 1)
        self.assertEqual(len(ctx["only_in_prod"]), 1)

class TKEY06_Key_Order_Change(unittest.TestCase):
    """Row order reversed between PROD and DEV — key-based matching still correct."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TKEY06(self):
        ctx=_run("ID,V\n3,C\n1,A\n2,B\n","ID,V\n1,A\n2,B\n3,C\n",self.t)
        self.assertEqual(ctx["results"]["matched_passed"], 3)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TKEY07_Large_Composite_Key(unittest.TestCase):
    """5-column composite key matches correctly across 100 rows."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TKEY07(self):
        header = "K1,K2,K3,K4,K5,V"
        rows = [f"A{i},B{i},C{i},D{i},E{i},val{i}" for i in range(100)]
        data = header+"\n"+"\n".join(rows)
        ctx=_run(data, data, self.t, keys=["K1","K2","K3","K4","K5"])
        self.assertEqual(ctx["results"]["matched_passed"], 100)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
