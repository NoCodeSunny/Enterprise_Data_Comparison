# -*- coding: utf-8 -*-
"""tests/test_data_types.py – TDT01–TDT08  Data type edge cases."""
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

class TDT01_String_vs_Int(unittest.TestCase):
    """String '100' vs integer 100 — framework treats all values as strings."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT01(self):
        # Both loaded as str; '100' == '100' → PASS
        ctx = _run("ID,V\n1,100\n","ID,V\n1,100\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TDT02_String_vs_Float(unittest.TestCase):
    """'100.0' vs '100' without tolerance → FAIL (strict string equality)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT02(self):
        ctx = _run("ID,V\n1,100.0\n","ID,V\n1,100\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

class TDT03_Boolean_Variants(unittest.TestCase):
    """'true' vs 'TRUE' → FAIL (case-sensitive string comparison)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT03(self):
        ctx = _run("ID,V\n1,true\n","ID,V\n1,TRUE\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

class TDT04_Date_Format_Mismatch(unittest.TestCase):
    """'2024-01-15' vs '15/01/2024' → FAIL (different string representations)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT04(self):
        ctx = _run("ID,Date\n1,2024-01-15\n","ID,Date\n1,15/01/2024\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

class TDT05_Timestamp_Precision(unittest.TestCase):
    """Same date different precision: '2024-01-15 00:00:00' vs '2024-01-15' → FAIL."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT05(self):
        ctx = _run("ID,TS\n1,2024-01-15 00:00:00\n","ID,TS\n1,2024-01-15\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

class TDT06_Scientific_Notation(unittest.TestCase):
    """'1.5e3' vs '1500' → FAIL (different string representations)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT06(self):
        ctx = _run("ID,V\n1,1.5e3\n","ID,V\n1,1500\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

class TDT07_Leading_Zero_Keys(unittest.TestCase):
    """Key '001' vs '1' remain distinct (no numeric coercion)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT07(self):
        ctx = _run("ID,V\n001,A\n1,B\n","ID,V\n001,A\n1,B\n",self.t)
        self.assertEqual(ctx["results"]["matched_passed"], 2)
        self.assertEqual(ctx["results"]["matched_failed"], 0)

class TDT08_Currency_Format(unittest.TestCase):
    """'$100' vs '100' → FAIL without tolerance (different strings)."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TDT08(self):
        ctx = _run("ID,Price\n1,$100\n","ID,Price\n1,100\n",self.t)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
