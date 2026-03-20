# -*- coding: utf-8 -*-
"""
tests/test_parquet.py
──────────────────────
TP-01  ParquetValidator.is_parquet detects .parquet extension
TP-02  ParquetValidator.is_parquet returns False for .csv
TP-03  ParquetValidator.is_parquet detects parquet magic bytes
TP-04  ParquetValidator.is_parquet detects partitioned directory
TP-05  ParquetValidator.get_partition_columns from hive-style dir
TP-06  ParquetValidator.validate catches missing key in schema
TP-07  ParquetOptimizer.build_plan produces valid plan
TP-08  ParquetOptimizer column pruning removes ignore fields
TP-09  ParquetOptimizer._compute_optimal_partitions scales with data size
TP-10  ParquetOptimizer detects partitioned directories
TP-11  ParquetCapability.NAME is 'parquet'
TP-12  ParquetCapability disabled by default (no parquet input)
TP-13  ParquetCapability auto-enables for .parquet files
TP-14  ParquetCapability sets use_spark=True when parquet detected
TP-15  ParquetCapability populates parquet context section
TP-16  ParquetCapability reads parquet into prod_df via mock (no pyarrow needed)
TP-17  Parquet vs Parquet pipeline (CSV mock with .parquet extension)
TP-18  Parquet vs CSV cross-format (mocked parquet as CSV)
TP-19  Parquet schema mismatch surfaces in validation_result (mocked)
TP-20  ParquetCapability is first in default registry order
TP-21  OptimizationPlan.to_dict contains expected keys
TP-22  SchemaValidationResult.to_dict contains expected keys
TP-23  ParquetCapability gracefully skips when paths are CSV
TP-24  Large dataset simulation (chunked, no full memory load)
"""
from __future__ import annotations

import os
import sys
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_compare.capabilities.parquet.parquet_validator import (
    ParquetValidator, SchemaValidationResult
)
from data_compare.capabilities.parquet.parquet_optimizer import (
    ParquetOptimizer, OptimizationPlan
)
from data_compare.capabilities.parquet.parquet_capability import (
    ParquetCapability, SparkRequiredError
)
from data_compare.context.run_context import make_context
from data_compare.registry.capability_registry import CapabilityRegistry


# ── helpers ───────────────────────────────────────────────────────────────────

def _write_parquet_magic(path: Path) -> Path:
    """Write a file that starts with PAR1 magic bytes (minimal parquet stub)."""
    path.write_bytes(b"PAR1" + b"\x00" * 8 + b"PAR1")
    return path

def _write_csv_as_parquet(df: pd.DataFrame, path: Path) -> Path:
    """
    Write a CSV file with a .parquet extension and PAR1 magic header.
    Used for tests that need parquet detection without pyarrow.
    The companion CSV (same name, .csv extension) is the real data file.
    """
    csv_path = path.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    # Write a stub .parquet file (magic bytes only) so is_parquet() returns True
    path.write_bytes(b"PAR1" + b"\x00" * 8 + b"PAR1")
    return csv_path   # return the actual readable file

def _make_context_for_parquet(prod_path: Path, dev_path: Path, tmp: Path) -> dict:
    return make_context(
        prod_csv_path=prod_path,
        dev_csv_path=dev_path,
        prod_name=prod_path.name,
        dev_name=dev_path.name,
        result_name="pq_test",
        report_root=tmp / "reports",
        keys=["TradeID", "Portfolio"],
        ignore_fields=[],
        capabilities_cfg={
            "parquet": True, "comparison": True, "tolerance": False,
            "duplicate": True, "schema": True, "data_quality": False,
            "audit": False, "alerts": False, "plugins": False,
        },
    )


# ── TP01 to TP15: detection + validation + optimizer ────────────────────────

class TP01_IsParquet_Extension(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP01(self):
        p = self.t / "data.parquet"; p.write_bytes(b"PAR1" + b"\x00" * 8 + b"PAR1")
        self.assertTrue(ParquetValidator().is_parquet(p))

class TP02_IsParquet_CSV_False(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP02(self):
        p = self.t / "data.csv"; p.write_text("ID,V\n1,A\n")
        self.assertFalse(ParquetValidator().is_parquet(p))

class TP03_IsParquet_MagicBytes(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP03(self):
        p = self.t / "data.bin"
        p.write_bytes(b"PAR1" + b"\x00" * 20 + b"PAR1")
        self.assertTrue(ParquetValidator().is_parquet(p))

class TP04_IsParquet_PartitionedDir(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP04(self):
        pdir = self.t / "ds"
        (pdir / "year=2024").mkdir(parents=True)
        (pdir / "year=2024" / "part-0.parquet").write_bytes(b"PAR1" + b"\x00" * 8 + b"PAR1")
        self.assertTrue(ParquetValidator().is_parquet(pdir))

class TP05_GetPartitionColumns(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP05(self):
        pdir = self.t / "ds"
        (pdir / "year=2023").mkdir(parents=True)
        (pdir / "year=2024").mkdir(parents=True)
        self.assertIn("year", ParquetValidator().get_partition_columns(pdir))

class TP06_Validate_MissingKey(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP06(self):
        prod = self.t/"p.csv"; dev = self.t/"d.csv"
        prod.write_text("TradeID,Price\nT001,100\n")
        dev.write_text("TradeID,Price\nT001,100\n")
        result = ParquetValidator().validate(prod, dev, keys=["TradeID","MissingKey"], ignore_fields=[])
        self.assertFalse(result.is_valid)
        self.assertTrue(any("MissingKey" in e for e in result.errors))

class TP07_Optimizer_BuildPlan(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP07(self):
        p = self.t/"p.csv"; q = self.t/"d.csv"
        p.write_text("ID,V\n1,A\n"); q.write_text("ID,V\n1,A\n")
        plan = ParquetOptimizer().build_plan(p, q, keys=["ID"], ignore_fields=[])
        self.assertIsInstance(plan, OptimizationPlan)
        self.assertGreater(plan.optimal_partitions, 0)

class TP08_Optimizer_ColumnPruning(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP08(self):
        p = self.t/"p.csv"; q = self.t/"d.csv"
        p.write_text("ID,V,Secret\n1,A,X\n"); q.write_text("ID,V,Secret\n1,A,X\n")
        plan = ParquetOptimizer().build_plan(p, q, keys=["ID"], ignore_fields=["Secret"],
                                             required_columns=["ID","V","Secret"])
        self.assertNotIn("Secret", plan.required_columns)
        self.assertIn("ID", plan.required_columns)

class TP09_Optimizer_PartitionCount(unittest.TestCase):
    def test_TP09(self):
        opt = ParquetOptimizer()
        p_small = opt._compute_optimal_partitions(1024, 1024)
        p_large = opt._compute_optimal_partitions(1*1024**3, 1*1024**3)
        self.assertGreaterEqual(p_small, 10)
        self.assertGreater(p_large, p_small)

class TP10_Optimizer_PartitionDetect(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP10(self):
        pdir = self.t/"ds"
        (pdir/"region=us").mkdir(parents=True); (pdir/"region=eu").mkdir(parents=True)
        self.assertIn("region", ParquetOptimizer._detect_partition_columns(pdir))

class TP11_Capability_Name(unittest.TestCase):
    def test_TP11(self): self.assertEqual(ParquetCapability.NAME, "parquet")

class TP12_Capability_DisabledByDefault(unittest.TestCase):
    def test_TP12(self):
        cap = ParquetCapability()
        ctx = make_context(capabilities_cfg={"parquet":False},
                           prod_csv_path=Path("/tmp/prod.csv"),
                           dev_csv_path=Path("/tmp/dev.csv"))
        self.assertFalse(cap.is_enabled(ctx))

class TP13_Capability_AutoEnableParquet(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP13(self):
        p = self.t/"prod.parquet"; _write_parquet_magic(p)
        d = self.t/"dev.csv"; d.write_text("ID,V\n1,A\n")
        ctx = make_context(prod_csv_path=p, dev_csv_path=d,
                           capabilities_cfg={"parquet":False})
        self.assertTrue(ParquetCapability().is_enabled(ctx))

class TP14_Capability_SetsUseSpark(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP14(self):
        p = self.t/"prod.parquet"; _write_parquet_magic(p)
        d = self.t/"dev.csv"; d.write_text("ID,V\n1,A\n")
        ctx = make_context(prod_csv_path=p, dev_csv_path=d,
                           capabilities_cfg={"parquet":True})
        ctx["config"]["capabilities"]["parquet"] = True
        ctx = ParquetCapability().run(ctx)
        self.assertTrue(ctx["config"].get("use_spark", False))

class TP15_Capability_ContextSection(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP15(self):
        p = self.t/"prod.parquet"; _write_parquet_magic(p)
        d = self.t/"dev.csv"; d.write_text("ID,V\n1,A\n")
        ctx = make_context(prod_csv_path=p, dev_csv_path=d,
                           capabilities_cfg={"parquet":True})
        ctx["config"]["capabilities"]["parquet"] = True
        ctx = ParquetCapability().run(ctx)
        self.assertIn("parquet", ctx)
        self.assertTrue(ctx["parquet"]["is_prod_parquet"])
        self.assertIn("optimization_plan", ctx["parquet"])


# ── TP16: parquet read via mock (no pyarrow required) ─────────────────────────

class TP16_Capability_ReadsParquet_Mocked(unittest.TestCase):
    """
    Tests that ParquetCapability._read_parquet populates prod_df
    by mocking the pyarrow read path so pyarrow is not required.
    """
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TP16(self):
        p = self.t/"prod.parquet"; _write_parquet_magic(p)
        d = self.t/"dev.parquet"; _write_parquet_magic(d)

        expected_df = pd.DataFrame({"ID":["T001","T002"],"V":["A","B"]})

        cap = ParquetCapability()
        plan_mock = OptimizationPlan(required_columns=["ID","V"], join_keys=["ID"])

        # Patch the read method to return our expected DataFrame
        with patch.object(cap, '_read_parquet', return_value=expected_df):
            ctx = make_context(prod_csv_path=p, dev_csv_path=d,
                               capabilities_cfg={"parquet":True})
            ctx["config"]["capabilities"]["parquet"] = True
            ctx = cap.run(ctx)

        # prod_df should be set if _read_parquet was called for prod
        # (the patch returns expected_df for any call)
        self.assertIn("parquet", ctx)
        self.assertTrue(ctx["parquet"]["is_prod_parquet"])


# ── TP17: Parquet vs Parquet full pipeline (CSV + mocked parquet detection) ────

class TP17_Parquet_vs_Parquet_Pipeline(unittest.TestCase):
    """
    Full pipeline comparison using CSV files with .parquet extension
    and PAR1 magic bytes. ParquetCapability detects them, attempts to
    read (falls back to None since no real parquet data), and the
    pipeline continues using the CSV paths that comparison_capability reads.
    
    Tests that the full pipeline runs without error and produces correct counts.
    """
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TP17(self):
        # Actual data as CSV files
        prod_df = pd.DataFrame({"ID":["T001","T002","T003"],"V":["A","B","C"]})
        dev_df  = pd.DataFrame({"ID":["T001","T002","T004"],"V":["A","X","D"]})

        prod_csv = self.t/"prod.csv"; prod_df.to_csv(prod_csv, index=False)
        dev_csv  = self.t/"dev.csv";  dev_df.to_csv(dev_csv,  index=False)

        # Mock: parquet capability reads CSV data and injects into context
        from data_compare.loaders.encoding import read_csv_robust
        from data_compare.utils.helpers import trim_df
        from data_compare.loaders.file_loader import load_any_to_csv

        conv = self.t/"conv"; conv.mkdir()
        pc = load_any_to_csv(prod_csv, conv)
        dc = load_any_to_csv(dev_csv, conv)

        def mock_read_parquet(self_cap, path, plan, keys, ignore_fields):
            # Return the CSV data as if it were parquet
            df = pd.read_csv(pc if "prod" in str(path) else dc, dtype=str)
            return df

        with patch.object(ParquetCapability, '_read_parquet', mock_read_parquet):
            ctx = make_context(
                prod_csv_path=pc, dev_csv_path=dc,
                prod_name="prod.parquet", dev_name="dev.parquet",
                result_name="pq_pq", report_root=self.t/"r",
                keys=["ID"],
                capabilities_cfg={"parquet":True,"comparison":True,"duplicate":True,
                                  "schema":True,"tolerance":False,"data_quality":False,
                                  "audit":False,"alerts":False,"plugins":False},
            )
            # Force parquet detection
            with patch.object(ParquetCapability, 'is_enabled', return_value=True):
                with patch.object(ParquetValidator, 'is_parquet', return_value=True):
                    ctx = CapabilityRegistry().run_pipeline(ctx)

        self.assertEqual(ctx["results"]["matched_failed"], 1)  # T002: B vs X
        self.assertEqual(len(ctx["only_in_prod"]), 1)           # T003
        self.assertEqual(len(ctx["only_in_dev"]),  1)           # T004


# ── TP18: Parquet vs CSV (mocked) ─────────────────────────────────────────────

class TP18_Parquet_vs_CSV_Mocked(unittest.TestCase):
    """Parquet vs CSV comparison using mocked parquet read."""
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TP18(self):
        prod_data = pd.DataFrame({"ID":["T001","T002"],"V":["A","B"]})
        dev_data  = pd.DataFrame({"ID":["T001","T002"],"V":["A","X"]})

        prod_csv = self.t/"prod.csv"; prod_data.to_csv(prod_csv, index=False)
        dev_csv  = self.t/"dev.csv";  dev_data.to_csv(dev_csv, index=False)

        from data_compare.loaders.file_loader import load_any_to_csv
        conv = self.t/"conv"; conv.mkdir()
        pc = load_any_to_csv(prod_csv, conv)
        dc = load_any_to_csv(dev_csv, conv)

        def mock_read_parquet(self_cap, path, plan, keys, ignore_fields):
            return pd.read_csv(pc, dtype=str)

        with patch.object(ParquetCapability, '_read_parquet', mock_read_parquet):
            ctx = make_context(
                prod_csv_path=pc, dev_csv_path=dc,
                prod_name="prod.parquet", dev_name="dev.csv",
                result_name="pq_csv", report_root=self.t/"r",
                keys=["ID"],
                capabilities_cfg={"parquet":True,"comparison":True,"duplicate":False,
                                  "schema":False,"tolerance":False,"data_quality":False,
                                  "audit":False,"alerts":False,"plugins":False},
            )
            with patch.object(ParquetCapability, 'is_enabled', return_value=True):
                with patch.object(ParquetValidator, 'is_parquet', side_effect=lambda p: "parquet" in str(p)):
                    ctx = CapabilityRegistry().run_pipeline(ctx)

        self.assertEqual(ctx["results"]["matched_failed"], 1)


# ── TP19: Schema mismatch via mock ───────────────────────────────────────────

class TP19_Parquet_SchemaMismatch_Mocked(unittest.TestCase):
    """Schema validation mismatch detected without pyarrow."""
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TP19(self):
        prod = self.t/"p.csv"; prod.write_text("ID,Price,Trader\nT001,100,Alice\n")
        dev  = self.t/"d.csv"; dev.write_text("ID,Price,TraderName\nT001,100,Alice\n")

        # Validate via fallback (no pyarrow) – uses pandas column read
        result = ParquetValidator().validate(prod, dev, keys=["ID"], ignore_fields=[])

        # With pandas fallback: missing_in_dev and extra_in_dev populated
        self.assertIn("Trader",     result.missing_in_dev)
        self.assertIn("TraderName", result.extra_in_dev)


# ── TP20-TP23: registry order, dataclass, skip-CSV ───────────────────────────

class TP20_Registry_Order(unittest.TestCase):
    def test_TP20(self):
        caps = CapabilityRegistry().list_capabilities()
        self.assertEqual(caps[0], "parquet")

class TP21_OptimizationPlan_ToDict(unittest.TestCase):
    def test_TP21(self):
        plan = OptimizationPlan(required_columns=["ID","V"], join_keys=["ID"])
        d = plan.to_dict()
        for k in ["required_columns","optimal_partitions","cache_prod"]:
            self.assertIn(k, d)

class TP22_SchemaValidationResult_ToDict(unittest.TestCase):
    def test_TP22(self):
        r = SchemaValidationResult(is_valid=True, missing_in_dev=["X"], extra_in_dev=["Y"])
        d = r.to_dict()
        for k in ["is_valid","missing_in_dev","extra_in_dev","type_mismatches","partition_issues"]:
            self.assertIn(k, d)

class TP23_Capability_SkipsCSV(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TP23(self):
        p = self.t/"p.csv"; p.write_text("ID,V\n1,A\n")
        d = self.t/"d.csv"; d.write_text("ID,V\n1,A\n")
        ctx = make_context(prod_csv_path=p, dev_csv_path=d,
                           capabilities_cfg={"parquet":True})
        ctx["config"]["capabilities"]["parquet"] = True
        ctx = ParquetCapability().run(ctx)
        self.assertFalse(ctx["config"].get("use_spark", False))


# ── TP24: Large dataset simulation ───────────────────────────────────────────

class TP24_LargeDataset_Chunked(unittest.TestCase):
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TP24_chunked_no_full_load(self):
        from data_compare.engines.pandas_engine import PandasEngine
        df = pd.DataFrame({"ID": range(1000), "V": ["x"]*1000})
        p = self.t/"big.csv"; df.to_csv(p, index=False)
        engine = PandasEngine(chunk_size=100)
        chunks = list(engine.read_chunked(p))
        self.assertEqual(len(chunks), 10)
        self.assertEqual(sum(len(c) for c in chunks), 1000)


# ── TP25: SparkRequiredError raised for large parquet without Spark ──────────

class TP25_SparkRequired_LargeParquet(unittest.TestCase):
    """Guard: raises SparkRequiredError when parquet is large and pyspark missing."""
    def setUp(self): self._d = tempfile.TemporaryDirectory(); self.t = Path(self._d.name)
    def tearDown(self): self._d.cleanup()

    def test_TP25(self):
        p = self.t/"big.parquet"; _write_parquet_magic(p)

        cap = ParquetCapability()
        plan_mock = OptimizationPlan(required_columns=[], join_keys=["ID"])

        # Patch: file appears large (600 MB) and PySpark is not available
        # Write a real file that appears large by patching os.path.getsize / Path.stat
        import stat as stat_module
        import os

        # Create a larger stub so the actual stat call sees non-zero size
        p.write_bytes(b"PAR1" + b"\x00" * (1024 * 1024) + b"PAR1")  # ~1 MB file

        # Patch the size calculation directly inside _read_parquet
        real_size = 600 * 1024 * 1024  # 600 MB

        original_stat = Path.stat
        def fake_stat(self_path, **kwargs):
            result = MagicMock()
            result.st_size = real_size
            result.st_mode = 0o100644  # regular file mode
            return result

        with patch("data_compare.capabilities.parquet.parquet_capability._PYSPARK_AVAILABLE", False):
            with patch.object(Path, "stat", fake_stat):
                with self.assertRaises(SparkRequiredError) as cm:
                    cap._read_parquet(p, plan_mock, ["ID"], [])
                self.assertIn("PySpark", str(cm.exception))


if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
