# -*- coding: utf-8 -*-
"""
tests/test_tolerance.py
────────────────────────
TT-01 through TT-36 - tolerance, config, jobs, logger, registry
"""
from __future__ import annotations
import json, sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path
import pandas as pd

class TT01_ROUND_HALF_UP(unittest.TestCase):
    def test_TT01(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("0.005", 2), "0.01")
class TT02_RoundDown(unittest.TestCase):
    def test_TT02(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("100.001", 2), "100.00")
class TT03_RoundUp(unittest.TestCase):
    def test_TT03(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("100.006", 2), "100.01")
class TT04_ZeroDecimals(unittest.TestCase):
    def test_TT04(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("99.7", 0), "100")
class TT05_StripCommas(unittest.TestCase):
    def test_TT05(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("1,234.567", 2), "1234.57")
class TT06_NonNumeric(unittest.TestCase):
    def test_TT06(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("N/A", 2), "N/A")
class TT07_Empty(unittest.TestCase):
    def test_TT07(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("", 2), "")
class TT08_Negative(unittest.TestCase):
    def test_TT08(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("-5.554", 2), "-5.55")
class TT09_LargeDecimal(unittest.TestCase):
    def test_TT09(self):
        from data_compare.comparator.tolerance import normalize_numeric_with_tolerance
        self.assertEqual(normalize_numeric_with_tolerance("3.14159265", 4), "3.1416")
class TT10_BuildMap(unittest.TestCase):
    def test_TT10(self):
        from data_compare.comparator.tolerance import build_tolerance_map
        df = pd.DataFrame([("p.csv","d.csv","Price",2),("p.csv","d.csv","Amount",0)],
                          columns=["Prod File Name","Dev File Name","Field Name","Tolerance Level ( in decimal )"])
        tol = build_tolerance_map(df)
        self.assertEqual(tol[("p.csv","d.csv","Price")], 2)
class TT11_BuildMap_Blanks(unittest.TestCase):
    def test_TT11(self):
        from data_compare.comparator.tolerance import build_tolerance_map
        df = pd.DataFrame([("p.csv","d.csv","Price",2),("","","","")],
                          columns=["Prod File Name","Dev File Name","Field Name","Tolerance Level ( in decimal )"])
        self.assertEqual(len(build_tolerance_map(df)), 1)
class TT12_BuildMap_Invalid(unittest.TestCase):
    def test_TT12(self):
        from data_compare.comparator.tolerance import build_tolerance_map
        df = pd.DataFrame([("p.csv","d.csv","Price","abc")],
                          columns=["Prod File Name","Dev File Name","Field Name","Tolerance Level ( in decimal )"])
        self.assertEqual(len(build_tolerance_map(df)), 0)
class TT13_BuildMap_Empty(unittest.TestCase):
    def test_TT13(self):
        from data_compare.comparator.tolerance import build_tolerance_map
        self.assertEqual(build_tolerance_map(pd.DataFrame()), {})
class TT14_Resolve_Pair(unittest.TestCase):
    def test_TT14(self):
        from data_compare.comparator.tolerance import resolve_pair_tolerance
        tol = {("p.csv","d.csv","Price"):2,("x.csv","y.csv","Price"):4}
        r = resolve_pair_tolerance(tol,"p.csv","d.csv",["Price"],["Price"],set())
        self.assertIn("Price", r); self.assertEqual(len(r), 1)
class TT15_Resolve_Ignored(unittest.TestCase):
    def test_TT15(self):
        from data_compare.comparator.tolerance import resolve_pair_tolerance
        r = resolve_pair_tolerance({("p.csv","d.csv","Price"):2},"p.csv","d.csv",["Price"],["Price"],{"Price"})
        self.assertNotIn("Price", r)
class TT16_Resolve_Absent(unittest.TestCase):
    def test_TT16(self):
        from data_compare.comparator.tolerance import resolve_pair_tolerance
        r = resolve_pair_tolerance({("p.csv","d.csv","Missing"):2},"p.csv","d.csv",["Price"],["Price"],set())
        self.assertNotIn("Missing", r)
class TT17_TolerancePass(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT17(self):
        from data_compare.loaders.file_loader import load_any_to_csv
        from data_compare.context.run_context import make_context
        from data_compare.registry.capability_registry import CapabilityRegistry
        (self.t/"p.csv").write_text("ID,Price\n1,100.001\n"); (self.t/"d.csv").write_text("ID,Price\n1,100.003\n")
        conv = self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],tol_map={("p.csv","d.csv","Price"):2},
            capabilities_cfg={"parquet":False,"comparison":True,"tolerance":True,"duplicate":False,
                              "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False})
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertEqual(ctx["results"]["matched_passed"], 1); self.assertEqual(ctx["results"]["matched_failed"], 0)
class TT18_NoToleranceFail(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT18(self):
        from data_compare.loaders.file_loader import load_any_to_csv
        from data_compare.context.run_context import make_context
        from data_compare.registry.capability_registry import CapabilityRegistry
        (self.t/"p.csv").write_text("ID,Price\n1,100.001\n"); (self.t/"d.csv").write_text("ID,Price\n1,100.003\n")
        conv=self.t/"c"; conv.mkdir()
        pc=load_any_to_csv(self.t/"p.csv",conv); dc=load_any_to_csv(self.t/"d.csv",conv)
        ctx = make_context(prod_csv_path=pc,dev_csv_path=dc,prod_name="p.csv",dev_name="d.csv",
            result_name="t",report_root=self.t,keys=["ID"],tol_map={},
            capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,"duplicate":False,
                              "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False})
        ctx = CapabilityRegistry().run_pipeline(ctx)
        self.assertEqual(ctx["results"]["matched_failed"], 1)
def _base_cfg():
    return {"input_sheet":"/tmp/fake.xlsx","report_root":"/tmp/r","email_to":"t@t.com",
            "log_level":"INFO","smtp_port":587,
            "capabilities":{"comparison":True,"tolerance":True,"duplicate":True,"schema":True,
                            "data_quality":True,"audit":True,"alerts":False,"plugins":False},
            "alert_rules":[],"data_quality_checks":{"null_rate":True,"distinct_count":True,"type_inference":True,"numeric_stats":True}}
class TT19_ValidConfig(unittest.TestCase):
    def test_TT19(self):
        from data_compare.config.config_schema import validate_config
        self.assertEqual(validate_config(_base_cfg())["log_level"], "INFO")
class TT20_InvalidLogLevel(unittest.TestCase):
    def test_TT20(self):
        from data_compare.config.config_schema import validate_config, ConfigError
        c=_base_cfg(); c["log_level"]="VERBOSE"
        with self.assertRaises(ConfigError) as cm: validate_config(c)
        self.assertIn("log_level", str(cm.exception))
class TT21_MissingKey(unittest.TestCase):
    def test_TT21(self):
        from data_compare.config.config_schema import validate_config, ConfigError
        c=_base_cfg(); del c["email_to"]
        with self.assertRaises(ConfigError) as cm: validate_config(c)
        self.assertIn("email_to", str(cm.exception))
class TT22_InvalidPort(unittest.TestCase):
    def test_TT22(self):
        from data_compare.config.config_schema import validate_config, ConfigError
        c=_base_cfg(); c["smtp_port"]=-1
        with self.assertRaises(ConfigError) as cm: validate_config(c)
        self.assertIn("smtp_port", str(cm.exception))
class TT23_InvalidAlertRule(unittest.TestCase):
    def test_TT23(self):
        from data_compare.config.config_schema import validate_config, ConfigError
        c=_base_cfg(); c["alert_rules"]=[{"operator":">","threshold":0}]
        with self.assertRaises(ConfigError) as cm: validate_config(c)
        self.assertIn("metric", str(cm.exception))
class TT24_MultipleErrors(unittest.TestCase):
    def test_TT24(self):
        from data_compare.config.config_schema import validate_config, ConfigError
        c=_base_cfg(); c["log_level"]="BAD"; c["smtp_port"]=-1; del c["email_to"]
        with self.assertRaises(ConfigError) as cm: validate_config(c)
        msg=str(cm.exception)
        self.assertIn("log_level",msg); self.assertIn("smtp_port",msg); self.assertIn("email_to",msg)
class TT25_UnknownCaps(unittest.TestCase):
    def test_TT25(self):
        from data_compare.config.config_schema import validate_config
        c=_base_cfg(); c["capabilities"]["my_custom"]=True
        self.assertIn("my_custom", validate_config(c)["capabilities"])
class TT26_BatchRow_Missing(unittest.TestCase):
    def test_TT26(self):
        from data_compare.config.config_schema import validate_batch_row, ConfigError
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp)/"dev.csv").write_text("ID\n1\n")
            with self.assertRaises(ConfigError) as cm:
                validate_batch_row(tmp,"missing.csv","dev.csv","R",["ID"],1)
            self.assertIn("PROD file not found", str(cm.exception))
class TT27_BatchRow_EmptyName(unittest.TestCase):
    def test_TT27(self):
        from data_compare.config.config_schema import validate_batch_row, ConfigError
        with self.assertRaises(ConfigError): validate_batch_row("","p.csv","d.csv","",[], 1)
class TT28_Job_Success(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT28(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        (self.t/"p.csv").write_text("ID,V\n1,A\n2,B\n"); (self.t/"d.csv").write_text("ID,V\n1,A\n2,X\n")
        r=ComparisonJob(self.t/"p.csv",self.t/"d.csv","p.csv","d.csv","t",self.t/"r",keys=["ID"],
            capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,"duplicate":False,
                              "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False}).run()
        self.assertTrue(r.succeeded); self.assertEqual(r.attempts, 1)
class TT29_Job_Counts(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT29(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        (self.t/"p.csv").write_text("ID,V\n1,A\n2,B\n"); (self.t/"d.csv").write_text("ID,V\n1,A\n2,X\n")
        s=ComparisonJob(self.t/"p.csv",self.t/"d.csv","p.csv","d.csv","t",self.t/"r",keys=["ID"],
            capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,"duplicate":False,
                              "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False}).run().to_summary_dict()
        self.assertEqual(s["MatchedFailed"],1); self.assertEqual(s["MatchedPassed"],1)
class TT30_Job_Retries(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT30(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        r=ComparisonJob(self.t/"missing.csv",self.t/"d.csv","m.csv","d.csv","r",self.t/"r",
                        max_retries=2,retry_delay_s=0.01).run()
        self.assertFalse(r.succeeded); self.assertEqual(r.attempts, 3)
class TT31_Job_ID(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT31(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        (self.t/"p.csv").write_text("ID,V\n1,A\n"); (self.t/"d.csv").write_text("ID,V\n1,A\n")
        r=ComparisonJob(self.t/"p.csv",self.t/"d.csv","p.csv","d.csv","t",self.t/"r",keys=["ID"],
            job_id="tid-xyz",
            capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,"duplicate":False,
                              "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False}).run()
        self.assertEqual(r.job_id, "tid-xyz")
class TT32_Job_SummaryKeys(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT32(self):
        from data_compare.jobs.comparison_job import ComparisonJob
        (self.t/"p.csv").write_text("ID,V\n1,A\n"); (self.t/"d.csv").write_text("ID,V\n1,A\n")
        d=ComparisonJob(self.t/"p.csv",self.t/"d.csv","p.csv","d.csv","t",self.t/"r",keys=["ID"],
            capabilities_cfg={"parquet":False,"comparison":True,"tolerance":False,"duplicate":False,
                              "schema":False,"data_quality":False,"audit":False,"alerts":False,"plugins":False}).run().to_summary_dict()
        for k in ["ResultName","ProdFile","DevFile","MatchedPassed","MatchedFailed","JobID"]:
            self.assertIn(k, d)
class TT33_JSON_Parseable(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT33(self):
        import logging; import data_compare.utils.logger as _lg
        _lg._file_handler=None; _lg._json_handler=None
        _lg.configure_logging(level=logging.DEBUG,log_dir=self.t,enable_json=True,enable_file=False)
        _lg.get_logger("TT33").info("json_test")
        jp=self.t/"data_compare.json.log"; self.assertTrue(jp.exists())
        for l in [x for x in jp.read_text().splitlines() if x.strip()]:
            r=json.loads(l); self.assertIn("timestamp",r); self.assertIn("message",r)
class TT34_RunID_JSON(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT34(self):
        import logging; import data_compare.utils.logger as _lg
        _lg._file_handler=None; _lg._json_handler=None
        _lg.set_run_id("test-run-abc")
        _lg.configure_logging(level=logging.DEBUG,log_dir=self.t,enable_json=True,enable_file=False)
        _lg.get_logger("TT34").info("run_id_check")
        records=[json.loads(l) for l in (self.t/"data_compare.json.log").read_text().splitlines() if l.strip()]
        self.assertIn("test-run-abc",{r.get("run_id") for r in records})
class TT35_FileLog(unittest.TestCase):
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TT35(self):
        import logging; import data_compare.utils.logger as _lg
        _lg._file_handler=None; _lg._json_handler=None
        _lg.configure_logging(level=logging.DEBUG,log_dir=self.t,enable_file=True,enable_json=False)
        _lg.get_logger("TT35").info("file_log_test")
        lp=self.t/"data_compare.log"; self.assertTrue(lp.exists()); self.assertIn("file_log_test",lp.read_text())
class TT36_CustomCap(unittest.TestCase):
    def test_TT36(self):
        from data_compare.capabilities.base import BaseCapability
        from data_compare.registry.capability_registry import CapabilityRegistry
        from data_compare.context.run_context import make_context
        class MyCap(BaseCapability):
            NAME="my_custom"
            def execute(self,ctx): ctx["plugin_outputs"]["my_custom"]="ran"; return ctx
        reg=CapabilityRegistry(); reg.register(MyCap())
        caps=reg.list_capabilities()
        self.assertGreater(caps.index("my_custom"), max(i for i,n in enumerate(caps) if n!="my_custom"))
        ctx=make_context(capabilities_cfg={"my_custom":True})
        ctx=reg.run_pipeline(ctx,override_order=["my_custom"])
        self.assertEqual(ctx["plugin_outputs"].get("my_custom"),"ran")
if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
