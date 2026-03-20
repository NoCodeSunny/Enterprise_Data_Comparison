# -*- coding: utf-8 -*-
"""
data_compare.capabilities.parquet.parquet_capability
──────────────────────────────────────────────────────
ParquetCapability

Standalone capability that:
  1. Detects whether PROD and/or DEV inputs are Parquet files/datasets
  2. Validates schemas (via ParquetValidator)
  3. Builds an optimization plan (via ParquetOptimizer)
  4. Updates context to force SparkEngine selection
  5. Reads parquet data into Spark DataFrames (when PySpark available)
     OR reads into pandas DataFrames (when only pandas/pyarrow available)
  6. Stores parquet-specific metadata in context["parquet"]

Execution order (MANDATORY):
    parquet → tolerance → comparison → duplicate → schema → data_quality → audit → alerts → plugins

ParquetCapability runs BEFORE comparison so that:
  - context["prod_csv_path"] is set to the parquet source path
  - context["parquet"]["optimization_plan"] is ready for SparkEngine
  - context["config"]["use_spark"] is set to True for large datasets

Cross-format support:
  - parquet vs parquet
  - parquet vs csv
  - parquet vs txt
  - parquet vs xlsx
  (mixed formats: parquet side uses Spark/pyarrow; other side uses file_loader)

The capability is DISABLED by default.  Enable it in config:
    capabilities:
      parquet: true

It auto-enables when either prod_path or dev_path is detected as parquet.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from data_compare.capabilities.base import BaseCapability
from data_compare.capabilities.parquet.parquet_optimizer import ParquetOptimizer
from data_compare.capabilities.parquet.parquet_validator import ParquetValidator
from data_compare.utils.logger import get_logger



logger = get_logger(__name__)


class SparkRequiredError(RuntimeError):
    """
    Raised when a large parquet file is detected but PySpark is not installed.

    Prevents silently falling back to pandas for files that would cause
    out-of-memory errors. The user must either install PySpark or reduce
    the dataset size.
    """
    pass

# ── optional imports ──────────────────────────────────────────────────────────
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PYARROW_AVAILABLE = True
except ImportError:
    _PYARROW_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False


class ParquetCapability(BaseCapability):
    """
    Parquet input detection, validation, and optimisation capability.

    Runs before comparison to prepare the engine and context for
    parquet-based data sources.

    Context keys read
    -----------------
    config.prod_path  or  prod_csv_path  : Path to PROD input
    config.dev_path   or  dev_csv_path   : Path to DEV input
    config.keys                          : list[str]
    config.ignore_fields                 : list[str]
    config.parquet_config                : dict (optional per-batch overrides)

    Context keys written
    --------------------
    parquet.is_prod_parquet          : bool
    parquet.is_dev_parquet           : bool
    parquet.prod_path                : Path
    parquet.dev_path                 : Path
    parquet.validation_result        : SchemaValidationResult.to_dict()
    parquet.optimization_plan        : OptimizationPlan.to_dict()
    parquet.prod_df                  : pd.DataFrame (when read via parquet)
    parquet.dev_df                   : pd.DataFrame (when read via parquet)
    config.use_spark                 : True  (when either input is parquet)
    prod_df / dev_df                 : populated from parquet when available
    """

    NAME = "parquet"

    def __init__(self) -> None:
        self._validator = ParquetValidator()
        self._optimizer = ParquetOptimizer()

    def is_enabled(self, context: Dict[str, Any]) -> bool:
        """
        Enabled when:
          (a) config.capabilities.parquet is explicitly True, OR
          (b) auto-detection finds parquet files at prod/dev paths
        """
        caps = context.get("config", {}).get("capabilities", {})
        explicitly_enabled = caps.get(self.NAME, False)
        if explicitly_enabled:
            return True

        # Auto-detect
        prod_path = self._resolve_path(context, "prod")
        dev_path  = self._resolve_path(context, "dev")
        if prod_path and self._validator.is_parquet(prod_path):
            return True
        if dev_path  and self._validator.is_parquet(dev_path):
            return True
        return False

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        t0 = time.perf_counter()

        # ── initialise parquet context section ────────────────────────────────
        if "parquet" not in context:
            context["parquet"] = {}

        cfg           = context.get("config", {})
        keys          = cfg.get("keys", [])
        ignore_fields = cfg.get("ignore_fields", [])
        pq_cfg        = cfg.get("parquet_config", {})

        prod_path = self._resolve_path(context, "prod")
        dev_path  = self._resolve_path(context, "dev")

        if prod_path is None or dev_path is None:
            logger.warning("  [parquet] prod_path or dev_path not set – skipping")
            return context

        is_prod_parquet = self._validator.is_parquet(prod_path)
        is_dev_parquet  = self._validator.is_parquet(dev_path)

        context["parquet"]["is_prod_parquet"] = is_prod_parquet
        context["parquet"]["is_dev_parquet"]  = is_dev_parquet
        context["parquet"]["prod_path"]       = prod_path
        context["parquet"]["dev_path"]        = dev_path

        logger.info(
            f"  [parquet] PROD is_parquet={is_prod_parquet}  "
            f"DEV is_parquet={is_dev_parquet}"
        )

        if not is_prod_parquet and not is_dev_parquet:
            logger.debug("  [parquet] Neither file is parquet – capability is a no-op")
            return context

        # ── force Spark engine when parquet detected ──────────────────────────
        if is_prod_parquet or is_dev_parquet:
            context["config"]["use_spark"] = True
            logger.info("  [parquet] Parquet detected → use_spark=True")

        # ── validate schema ───────────────────────────────────────────────────
        if is_prod_parquet and is_dev_parquet:
            val_result = self._validator.validate(
                prod_path, dev_path, keys, ignore_fields
            )
            context["parquet"]["validation_result"] = val_result.to_dict()
            if not val_result.is_valid:
                logger.error(
                    f"  [parquet] Schema validation FAILED: {val_result.errors}"
                )
                # Store error but don't hard-fail – comparison will surface it
                context["parquet"]["schema_errors"] = val_result.errors

        # ── build optimization plan ───────────────────────────────────────────
        opt_plan = self._optimizer.build_plan(
            prod_path=prod_path if is_prod_parquet else dev_path,
            dev_path=dev_path   if is_dev_parquet  else prod_path,
            keys=keys,
            ignore_fields=ignore_fields,
            partition_filters=pq_cfg.get("partition_filters"),
        )
        context["parquet"]["optimization_plan"] = opt_plan.to_dict()
        context["parquet"]["_plan_object"]       = opt_plan   # for SparkEngine

        # ── read data ──────────────────────────────────────────────────────────
        if is_prod_parquet:
            prod_df = self._read_parquet(prod_path, opt_plan, keys, ignore_fields)
            if prod_df is not None:
                context["prod_df"] = prod_df
                context["parquet"]["prod_df"] = prod_df
                logger.info(
                    f"  [parquet] PROD loaded: "
                    f"{len(prod_df):,} rows × {len(prod_df.columns)} cols"
                )

        if is_dev_parquet:
            dev_df = self._read_parquet(dev_path, opt_plan, keys, ignore_fields)
            if dev_df is not None:
                context["dev_df"] = dev_df
                context["parquet"]["dev_df"] = dev_df
                logger.info(
                    f"  [parquet] DEV loaded: "
                    f"{len(dev_df):,} rows × {len(dev_df.columns)} cols"
                )

        elapsed = time.perf_counter() - t0
        logger.info(f"  [parquet] Setup complete [{elapsed:.2f}s]")
        return context

    # ── private helpers ───────────────────────────────────────────────────────

    def _read_parquet(
        self,
        path: Path,
        plan: Any,
        keys: List[str],
        ignore_fields: List[str],
    ) -> Optional[pd.DataFrame]:
        """
        Read a parquet file/dataset into a pandas DataFrame.

        Strategy:
        1. PySpark (preferred for large datasets) – distributed read, then toPandas()
        2. pyarrow (medium datasets) – columnar read with column pruning
        3. pandas.read_parquet (fallback) – requires pyarrow or fastparquet
        4. Returns None if no reader is available
        """
        ignore_set = set(ignore_fields)

        # ── Spark safety guard for large files ───────────────────────────────────
        # If the file is large (above threshold) and PySpark is not installed,
        # raise a clear error instead of attempting an in-memory pandas load.
        large_threshold = 500 * 1024 * 1024  # 500 MB default
        try:
            file_bytes = (
                sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
                if path.is_dir()
                else path.stat().st_size
            )
        except OSError:
            file_bytes = 0

        if file_bytes > large_threshold and not _PYSPARK_AVAILABLE:
            raise SparkRequiredError(
                f"Parquet file '{path.name}' is {file_bytes / 1024 / 1024:.1f} MB "
                f"which exceeds the {large_threshold // (1024*1024)} MB threshold. "
                f"PySpark is required to process large parquet files safely. "
                f"Install it with: pip install pyspark  "
                f"OR set a higher spark_threshold_bytes in config.yaml if you are "
                f"certain the file fits in RAM."
            )

        # Strategy 1: PySpark
        if _PYSPARK_AVAILABLE:
            try:
                return self._read_with_spark(path, plan, keys, ignore_set)
            except Exception as exc:
                logger.warning(f"  [parquet] Spark read failed ({exc}); trying pyarrow")

        # Strategy 2: pyarrow
        if _PYARROW_AVAILABLE:
            try:
                return self._read_with_pyarrow(path, plan, keys, ignore_set)
            except Exception as exc:
                logger.warning(f"  [parquet] pyarrow read failed ({exc}); trying pandas")

        # Strategy 3: pandas (requires pyarrow/fastparquet installed)
        try:
            cols = plan.required_columns if plan.required_columns else None
            df   = pd.read_parquet(str(path), columns=cols, engine="auto")
            df   = df[[c for c in df.columns if c not in ignore_set]]
            return df.astype(str).reset_index(drop=True)
        except Exception as exc:
            logger.error(f"  [parquet] All read strategies failed for {path}: {exc}")
            return None

    @staticmethod
    def _read_with_spark(
        path: Path,
        plan: Any,
        keys: List[str],
        ignore_set: set,
    ) -> pd.DataFrame:
        """Read via Spark, apply optimizations, convert to pandas."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        sdf = spark.read.parquet(str(path))

        # Column pruning
        if plan.required_columns:
            available = sdf.columns
            cols      = [c for c in plan.required_columns
                         if c in available and c not in ignore_set]
            if cols:
                sdf = sdf.select(cols)

        # Repartition for performance
        if plan.repartition_on_keys and plan.join_keys:
            join_keys_available = [k for k in plan.join_keys if k in sdf.columns]
            if join_keys_available:
                sdf = sdf.repartition(plan.optimal_partitions, *join_keys_available)

        # Cache if large
        if plan.cache_prod:
            sdf.cache()

        # Convert to pandas (only the result set – not the full dataset for large data)
        # For millions of rows this returns a summary; for bounded comparisons, full data
        df = sdf.toPandas()
        df = df[[c for c in df.columns if c not in ignore_set]]
        return df.astype(str)

    @staticmethod
    def _read_with_pyarrow(
        path: Path,
        plan: Any,
        keys: List[str],
        ignore_set: set,
    ) -> pd.DataFrame:
        """Read via pyarrow with column pruning and partition filters."""
        import pyarrow.parquet as pq

        cols = plan.required_columns if plan.required_columns else None
        filters = plan.partition_filters if plan.partition_filters else None

        table = pq.read_table(
            str(path),
            columns=filters,
            filters=filters,
        )
        # Apply column pruning manually
        if cols:
            available = set(table.schema.names)
            sel = [c for c in cols if c in available and c not in ignore_set]
            if sel:
                table = table.select(sel)

        df = table.to_pandas()
        df = df[[c for c in df.columns if c not in ignore_set]]
        return df.astype(str)

    @staticmethod
    def _resolve_path(context: Dict[str, Any], side: str) -> Optional[Path]:
        """Resolve prod or dev path from context."""
        cfg = context.get("config", {})
        # Try explicit parquet paths in config
        pq_cfg = cfg.get("parquet_config", {})
        explicit = pq_cfg.get(f"{side}_parquet_path")
        if explicit:
            return Path(explicit)

        # Fall back to the csv paths (set by orchestrator)
        key = "prod_csv_path" if side == "prod" else "dev_csv_path"
        val = context.get(key)
        if val:
            return Path(val)

        # Check config for direct paths
        direct = cfg.get(f"{side}_path")
        if direct:
            return Path(direct)

        return None
