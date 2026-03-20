# -*- coding: utf-8 -*-
"""
tests/test_loader.py
─────────────────────
TL-01  CSV UTF-8 loads correctly
TL-02  CSV with BOM (utf-8-sig) loads correctly
TL-03  CSV empty returns empty DataFrame
TL-04  TXT pipe-delimited auto-detected
TL-05  TXT tab-delimited auto-detected
TL-06  TXT loads correct shape
TL-07  XLSX row count correct
TL-08  XLSX named sheet
TL-09  load_any_to_csv returns readable Path
TL-10  load_any_to_csv TXT → readable CSV
TL-11  detect_delimiter pipe
TL-12  detect_delimiter tab
TL-13  trim_df strips whitespace
TL-14  trim_df replaces NaN with empty string
TL-15  trim_df replaces NBSP
TL-16  normalize_keys strips whitespace
TL-17  normalize_keys no numeric coercion
TL-18  normalize_keys missing column skipped
TL-19  sanitise_filename removes illegal chars
TL-20  sanitise_filename empty returns default
TL-21  safe_output_path no collision on first call
TL-22  safe_output_path UUID suffix on collision
TL-23  PandasEngine.load_and_align correct result
TL-24  PandasEngine read_chunked total row count
TL-25  select_engine returns PandasEngine by default
TL-26  select_engine returns PandasEngine when use_spark=False
"""
from __future__ import annotations
import sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path
import pandas as pd

class TL01_CSV_UTF8(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL01_utf8(self):
        from data_compare.loaders.encoding import read_csv_robust
        p = self.t/"d.csv"; p.write_text("ID,Name\n1,Alice\n2,Bob\n", encoding="utf-8")
        df = read_csv_robust(p, dtype=str)
        self.assertEqual(len(df), 2); self.assertIn("ID", df.columns)

class TL02_CSV_BOM(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL02_bom(self):
        from data_compare.loaders.encoding import read_csv_robust
        p = self.t/"bom.csv"; p.write_bytes("ID,Val\n1,A\n".encode("utf-8-sig"))
        df = read_csv_robust(p, dtype=str)
        self.assertIn("ID", df.columns); self.assertEqual(len(df), 1)

class TL03_CSV_Empty(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL03_empty(self):
        from data_compare.loaders.encoding import read_csv_robust
        p = self.t/"e.csv"; p.write_text("ID,Val\n", encoding="utf-8")
        df = read_csv_robust(p, dtype=str)
        self.assertEqual(len(df), 0); self.assertIn("ID", df.columns)

class TL04_TXT_Pipe(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL04_pipe(self):
        from data_compare.loaders.encoding import detect_delimiter
        p = self.t/"p.txt"; p.write_text("ID|Name|Val\n1|Alice|100\n")
        self.assertEqual(detect_delimiter(p), "|")

class TL05_TXT_Tab(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL05_tab(self):
        from data_compare.loaders.encoding import detect_delimiter
        p = self.t/"t.txt"; p.write_text("ID\tName\n1\tAlice\n")
        self.assertEqual(detect_delimiter(p), "\t")

class TL06_TXT_Shape(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL06_shape(self):
        from data_compare.loaders.file_loader import load_any_to_csv
        p = self.t/"d.txt"; p.write_text("ID|V\n1|A\n2|B\n3|C\n")
        conv = self.t/"c"; conv.mkdir()
        csv_p = load_any_to_csv(p, conv)
        df = pd.read_csv(csv_p, dtype=str)
        self.assertEqual(df.shape, (3, 2))

class TL07_XLSX_Rows(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL07_rows(self):
        from data_compare.loaders.file_loader import load_any_to_csv
        p = self.t/"d.xlsx"
        pd.DataFrame({"ID":range(5),"V":["a"]*5}).to_excel(p, index=False)
        conv = self.t/"c"; conv.mkdir()
        df = pd.read_csv(load_any_to_csv(p, conv), dtype=str)
        self.assertEqual(len(df), 5)

class TL08_XLSX_NamedSheet(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL08_named_sheet(self):
        import openpyxl
        from data_compare.loaders.file_loader import load_any_to_csv
        wb = openpyxl.Workbook()
        ws1 = wb.active; ws1.title = "Wrong"; ws1.append(["ID"]); ws1.append([99])
        ws2 = wb.create_sheet("Data"); ws2.append(["ID","V"]); ws2.append([1,"A"])
        p = self.t/"m.xlsx"; wb.save(p)
        conv = self.t/"c"; conv.mkdir()
        df = pd.read_csv(load_any_to_csv(p, conv, sheet_name="Data"), dtype=str)
        self.assertEqual(len(df), 1); self.assertIn("V", df.columns)

class TL09_LoadAnyToCSV_Path(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL09_returns_path(self):
        from data_compare.loaders.file_loader import load_any_to_csv
        p = self.t/"x.csv"; p.write_text("A,B\n1,2\n")
        conv = self.t/"c"; conv.mkdir()
        result = load_any_to_csv(p, conv)
        self.assertIsInstance(result, Path); self.assertTrue(result.exists())

class TL10_LoadAnyToCSV_TXT(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL10_txt_to_csv(self):
        from data_compare.loaders.file_loader import load_any_to_csv
        p = self.t/"y.txt"; p.write_text("A|B\n1|2\n3|4\n")
        conv = self.t/"c"; conv.mkdir()
        df = pd.read_csv(load_any_to_csv(p, conv), dtype=str)
        self.assertEqual(df.shape, (2, 2))

class TL11_DetectPipe(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL11_pipe(self):
        from data_compare.loaders.encoding import detect_delimiter
        p = self.t/"f.txt"; p.write_text("A|B|C\n1|2|3\n4|5|6\n")
        self.assertEqual(detect_delimiter(p), "|")

class TL12_DetectTab(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL12_tab(self):
        from data_compare.loaders.encoding import detect_delimiter
        p = self.t/"f.txt"; p.write_text("A\tB\tC\n1\t2\t3\n")
        self.assertEqual(detect_delimiter(p), "\t")

class TL13_TrimDF_Whitespace(unittest.TestCase):
    def test_TL13(self):
        from data_compare.utils.helpers import trim_df
        df = pd.DataFrame({"A":["  hello  ","world "],"B":[" 1","2 "]})
        r = trim_df(df)
        self.assertEqual(r["A"].iloc[0], "hello"); self.assertEqual(r["B"].iloc[1], "2")

class TL14_TrimDF_NaN(unittest.TestCase):
    def test_TL14(self):
        from data_compare.utils.helpers import trim_df
        df = pd.DataFrame({"A":[None, float("nan"), "ok"]})
        r = trim_df(df)
        self.assertEqual(r["A"].iloc[0], ""); self.assertEqual(r["A"].iloc[2], "ok")

class TL15_TrimDF_NBSP(unittest.TestCase):
    def test_TL15(self):
        from data_compare.utils.helpers import trim_df
        df = pd.DataFrame({"A":["hello\u00a0world"]})
        self.assertEqual(trim_df(df)["A"].iloc[0], "hello world")

class TL16_NormalizeKeys_Whitespace(unittest.TestCase):
    def test_TL16(self):
        from data_compare.utils.helpers import normalize_keys
        df = pd.DataFrame({"ID":[" 001 ","002"],"V":["a","b"]})
        self.assertEqual(normalize_keys(df, ["ID"])["ID"].iloc[0], "001")

class TL17_NormalizeKeys_NoCoercion(unittest.TestCase):
    def test_TL17(self):
        from data_compare.utils.helpers import normalize_keys
        df = pd.DataFrame({"ID":["001","1"],"V":["a","b"]})
        r = normalize_keys(df, ["ID"])
        self.assertEqual(r["ID"].iloc[0], "001")  # must NOT become "1"

class TL18_NormalizeKeys_Missing(unittest.TestCase):
    def test_TL18(self):
        from data_compare.utils.helpers import normalize_keys
        df = pd.DataFrame({"V":["a","b"]})
        r = normalize_keys(df, ["ID","V"])  # ID absent – no raise
        self.assertIn("V", r.columns)

class TL19_Sanitise_Illegal(unittest.TestCase):
    def test_TL19(self):
        from data_compare.utils.helpers import sanitise_filename
        r = sanitise_filename("my/report:name<test>")
        for c in "/<>:": self.assertNotIn(c, r)

class TL20_Sanitise_Empty(unittest.TestCase):
    def test_TL20(self):
        from data_compare.utils.helpers import sanitise_filename
        self.assertEqual(sanitise_filename(""), "CompareRecords")
        self.assertEqual(sanitise_filename("..."), "CompareRecords")

class TL21_SafeOutputPath_NoCollision(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL21(self):
        from data_compare.utils.helpers import safe_output_path
        p = safe_output_path(self.t, "MyReport")
        self.assertFalse(p.exists())

class TL22_SafeOutputPath_Collision(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL22(self):
        from data_compare.utils.helpers import safe_output_path
        p1 = safe_output_path(self.t, "MyReport"); p1.touch()
        p2 = safe_output_path(self.t, "MyReport")
        self.assertNotEqual(p1.name, p2.name)

class TL23_PandasEngine_Align(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL23(self):
        from data_compare.engines.pandas_engine import PandasEngine
        (self.t/"p.csv").write_text("ID,Portfolio,Price\nT001,EQ,100\nT002,FI,200\nT003,EQ,300\n")
        (self.t/"d.csv").write_text("ID,Portfolio,Price\nT001,EQ,100\nT002,FI,199\nT004,EQ,400\n")
        result = PandasEngine().load_and_align(
            self.t/"p.csv", self.t/"d.csv",
            keys=["ID","Portfolio"], ignore_fields=[], tol_map={},
            prod_name="p.csv", dev_name="d.csv")
        self.assertEqual(result.engine_name, "pandas")
        self.assertEqual(len(result.prod_common), 2)
        self.assertEqual(len(result.only_in_prod), 1)
        self.assertEqual(len(result.only_in_dev),  1)

class TL24_PandasEngine_Chunked(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TL24(self):
        from data_compare.engines.pandas_engine import PandasEngine
        (self.t/"p.csv").write_text("ID,V\n1,A\n2,B\n3,C\n4,D\n5,E\n")
        total = sum(len(c) for c in PandasEngine(chunk_size=2).read_chunked(self.t/"p.csv"))
        self.assertEqual(total, 5)

class TL25_SelectEngine_Default(unittest.TestCase):
    def test_TL25(self):
        from data_compare.engines import select_engine
        self.assertEqual(select_engine({}).NAME, "pandas")

class TL26_SelectEngine_NoSpark(unittest.TestCase):
    def test_TL26(self):
        from data_compare.engines import select_engine
        self.assertEqual(select_engine({"use_spark": False}).NAME, "pandas")

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
