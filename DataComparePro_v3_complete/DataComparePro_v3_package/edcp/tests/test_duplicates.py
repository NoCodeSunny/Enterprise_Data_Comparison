# -*- coding: utf-8 -*-
"""
tests/test_duplicates.py
─────────────────────────
TD-01  detect_duplicates returns groups with Count > 1
TD-02  detect_duplicates empty keys returns sentinel DataFrame
TD-03  detect_duplicates missing key returns empty
TD-04  add_sequence_for_duplicates adds _SEQ_ counter
TD-05  add_sequence_for_duplicates stable within key group
TD-06  build_duplicates_summary produces In_Prod/In_Dev columns
TD-07  Pipeline detects duplicates in PROD
TD-08  Pipeline using_seq=True when duplicates present
TD-09  Identical duplicates both sides → all pass
TD-10  Different values in duplicates detected as failures
TD-11  Missing duplicate row detected in Count Differences
TD-12  DuplicateCapability standalone populates dup_count
TD-13  DuplicateCapability standalone builds prod_aligned
TD-14  No duplicates → using_seq=False
"""
from __future__ import annotations
import sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path
import pandas as pd
from data_compare.comparator.duplicate import (
    detect_duplicates, add_sequence_for_duplicates, build_duplicates_summary
)
from data_compare.context.run_context import make_context
from data_compare.registry.capability_registry import CapabilityRegistry

def _caps_dup():
    return {"parquet":False,"comparison":True,"tolerance":False,"duplicate":True,
            "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False}

class TD01_Detect_Groups(unittest.TestCase):
    def test_TD01(self):
        df = pd.DataFrame({"ID":["A","A","B"],"V":["x","y","z"]})
        dups = detect_duplicates(df, ["ID"])
        self.assertEqual(len(dups), 1)
        self.assertEqual(dups.iloc[0]["Count"], 2)

class TD02_Detect_NoKeys(unittest.TestCase):
    def test_TD02(self):
        df = pd.DataFrame({"ID":["A","B"]})
        dups = detect_duplicates(df, [])
        self.assertIn("_NO_KEYS_", dups.columns)

class TD03_Detect_MissingKey(unittest.TestCase):
    def test_TD03(self):
        df = pd.DataFrame({"ID":["A","B"]})
        dups = detect_duplicates(df, ["MISSING"])
        self.assertTrue(dups.empty)

class TD04_SEQ_Added(unittest.TestCase):
    def test_TD04(self):
        df = pd.DataFrame({"ID":["A","A","B"],"V":["1","2","3"]})
        result = add_sequence_for_duplicates(df, ["ID"])
        self.assertIn("_SEQ_", result.columns)

class TD05_SEQ_Stable(unittest.TestCase):
    def test_TD05(self):
        df = pd.DataFrame({"ID":["A","A"],"V":["1","2"]})
        result = add_sequence_for_duplicates(df, ["ID"])
        seqs = sorted(result[result["ID"] == "A"]["_SEQ_"].tolist())
        self.assertEqual(seqs, [0, 1])

class TD06_Summary_Columns(unittest.TestCase):
    def test_TD06(self):
        prod_dups = pd.DataFrame({"ID":["A"],"Count":[2]})
        dev_dups  = pd.DataFrame({"ID":["A"],"Count":[1]})
        summary = build_duplicates_summary(prod_dups, dev_dups, ["ID"])
        self.assertIsNotNone(summary)

class TD07_Pipeline_DupDetected(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TD07(self):
        (self.t/"p.csv").write_text("ID,V\nA,1\nA,2\nB,3\n")
        (self.t/"d.csv").write_text("ID,V\nA,1\nA,2\nB,3\n")
        from data_compare.loaders.file_loader import load_any_to_csv
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],capabilities_cfg=_caps_dup())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertEqual(ctx["dup_count_prod"], 1)

class TD08_Pipeline_UsingSeq(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TD08(self):
        (self.t/"p.csv").write_text("ID,V\nA,1\nA,2\n")
        (self.t/"d.csv").write_text("ID,V\nA,1\nA,2\n")
        from data_compare.loaders.file_loader import load_any_to_csv
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],capabilities_cfg=_caps_dup())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertTrue(ctx["using_seq"])

class TD09_IdenticalDups_Pass(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TD09(self):
        (self.t/"p.csv").write_text("ID,V\nA,1\nA,1\nB,2\n")
        (self.t/"d.csv").write_text("ID,V\nA,1\nA,1\nB,2\n")
        from data_compare.loaders.file_loader import load_any_to_csv
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],capabilities_cfg=_caps_dup())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertEqual(ctx["results"]["matched_failed"], 0)
        self.assertEqual(ctx["results"]["matched_passed"], 3)

class TD10_DiffDups_Fail(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TD10(self):
        (self.t/"p.csv").write_text("ID,V\nA,1\nA,2\n")
        (self.t/"d.csv").write_text("ID,V\nA,1\nA,9\n")  # second dup has diff value
        from data_compare.loaders.file_loader import load_any_to_csv
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],capabilities_cfg=_caps_dup())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertEqual(ctx["results"]["matched_failed"], 1)

class TD11_MissingDupRow(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TD11(self):
        (self.t/"p.csv").write_text("ID,V\nA,1\nA,2\n")  # 2 rows for A
        (self.t/"d.csv").write_text("ID,V\nA,1\n")         # 1 row for A
        from data_compare.loaders.file_loader import load_any_to_csv
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],capabilities_cfg=_caps_dup())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertGreaterEqual(len(ctx["only_in_prod"]), 1)

class TD12_Standalone_Count(unittest.TestCase):
    def test_TD12(self):
        from data_compare.capabilities.duplicate.duplicate_capability import DuplicateCapability
        ctx = make_context(capabilities_cfg={"duplicate":True})
        ctx["prod_df"] = pd.DataFrame({"ID":["A","A","B"],"V":["1","2","3"]})
        ctx["dev_df"]  = pd.DataFrame({"ID":["A","B"],"V":["1","3"]})
        ctx["existing_keys"] = ["ID"]
        ctx = DuplicateCapability().run(ctx)
        self.assertEqual(ctx["dup_count_prod"], 1)
        self.assertEqual(ctx["dup_count_dev"],  0)

class TD13_Standalone_Aligned(unittest.TestCase):
    def test_TD13(self):
        from data_compare.capabilities.duplicate.duplicate_capability import DuplicateCapability
        ctx = make_context(capabilities_cfg={"duplicate":True})
        ctx["prod_df"] = pd.DataFrame({"ID":["A","A","B"],"V":["1","2","3"]})
        ctx["dev_df"]  = pd.DataFrame({"ID":["A","B"],"V":["1","3"]})
        ctx["existing_keys"] = ["ID"]
        ctx = DuplicateCapability().run(ctx)
        self.assertIsNotNone(ctx["prod_aligned"])

class TD14_NoDups_NoSeq(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TD14(self):
        (self.t/"p.csv").write_text("ID,V\n1,A\n2,B\n")
        (self.t/"d.csv").write_text("ID,V\n1,A\n2,B\n")
        from data_compare.loaders.file_loader import load_any_to_csv
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],capabilities_cfg=_caps_dup())
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertFalse(ctx["using_seq"])

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
