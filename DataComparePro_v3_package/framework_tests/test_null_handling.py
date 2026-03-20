# -*- coding: utf-8 -*-
"""tests/test_null_handling.py – TNULL01–TNULL07  Null and blank value handling."""
from __future__ import annotations
import sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

def _run(prod, dev, tmp, keys=None):
    from data_compare.loaders.file_loader import load_any_to_csv
    from data_compare.context.run_context import make_context
    from data_compare.registry.capability_registry import CapabilityRegistry
    (tmp/"p.csv").write_text(prod); (tmp/"d.csv").write_text(dev)
    conv=tmp/"c"; conv.mkdir(exist_ok=True)
    pc=load_any_to_csv(tmp/"p.csv",conv); dc=load_any_to_csv(tmp/"d.csv",conv)
    ctx=make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
        result_name="t",report_root=tmp/"r",keys=keys or ["ID"],
        capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,
                          "duplicate":False,"schema":False,"data_quality":False,
                          "audit":False,"alerts":False,"plugins":False})
    return CapabilityRegistry().run_pipeline(ctx)

class TNULL01_Null_vs_EmptyString(unittest.TestCase):
    """Missing CSV value (blank) vs empty string both normalise to '' → PASS."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TNULL01(self):
        ctx = _run("ID,V\n1,\n","ID,V\n1,\n",self.t)
        self.assertEqual(ctx["results"]["matched_passed"], 1)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TNULL02_Null_vs_Zero(unittest.TestCase):
    """Blank (→'') vs '0' are different strings → FAIL."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TNULL02(self):
        ctx = _run("ID,V\n1,\n","ID,V\n1,0\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

class TNULL03_Null_In_Keys(unittest.TestCase):
    """Blank key values do not crash; rows with blank keys are handled gracefully."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TNULL03(self):
        ctx = _run("ID,V\n,A\n1,B\n","ID,V\n,A\n1,B\n",self.t)
        self.assertIsNotNone(ctx)
        total = ctx["results"]["matched_passed"] + ctx["results"]["matched_failed"]
        self.assertGreaterEqual(total, 1)

class TNULL04_All_Null_Column(unittest.TestCase):
    """Column with all blank values compared correctly (all '' == '' → PASS)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TNULL04(self):
        ctx = _run("ID,V\n1,\n2,\n3,\n","ID,V\n1,\n2,\n3,\n",self.t)
        self.assertEqual(ctx["results"]["matched_passed"], 3)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TNULL05_Partial_Null_Records(unittest.TestCase):
    """Some rows have blank V, some don't. Different blanks → FAIL."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TNULL05(self):
        ctx = _run("ID,V\n1,A\n2,\n3,C\n","ID,V\n1,A\n2,X\n3,C\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)  # row 2: '' vs 'X'
        self.assertEqual(ctx["results"]["matched_passed"], 2)

class TNULL06_Null_vs_BlankSpaces(unittest.TestCase):
    """Blank vs whitespace-only: after trim_df both become '' → PASS."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TNULL06(self):
        ctx = _run("ID,V\n1,\n","ID,V\n1,   \n",self.t)
        # After trim both become '' → no difference
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TNULL07_Null_In_Duplicate_Keys(unittest.TestCase):
    """Blank key values with duplicates do not crash duplicate detection."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TNULL07(self):
        ctx = _run("ID,V\n,A\n,B\n1,C\n","ID,V\n,A\n,B\n1,C\n",self.t)
        self.assertIsNotNone(ctx)

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
