# -*- coding: utf-8 -*-
"""
data_compare.engines.spark_engine
────────────────────────────────────
SparkEngine – distributed comparison engine backed by PySpark.

Used automatically when:
  - File size exceeds config["spark_threshold_bytes"]  (default: 500 MB)
  - OR config["use_spark"] is True

Architecture
─────────────
The SparkEngine replicates the same alignment contract as PandasEngine:
  1. Read PROD and DEV CSVs via Spark
  2. Apply ignore_fields
  3. Normalise key columns
  4. Detect duplicates (via Spark window functions)
  5. Build _SEQ_ when duplicates exist (monotonically increasing row number per key)
  6. Inner-join on align_keys for matched records
  7. Anti-join for only_in_prod / only_in_dev
  8. Convert matched partitions to pandas for the capability pipeline

The output (EngineResult) is identical in structure to PandasEngine output.
The capability pipeline receives pandas DataFrames regardless of engine used.

PySpark is an OPTIONAL dependency.  If PySpark is not installed, constructing
a SparkEngine raises ImportError.  The orchestrator checks availability before
selecting this engine.

Activation
──────────
config.yaml:
    use_spark: true
    spark_threshold_bytes: 524288000   # 500 MB

Or pass use_spark=True to make_context().
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from data_compare.engines.base_engine import BaseEngine, EngineResult
from data_compare.comparator.tolerance import resolve_pair_tolerance
from data_compare.utils.helpers import normalize_keys, trim_df
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# ── optional PySpark import ───────────────────────────────────────────────────
try:
    from pyspark.sql import SparkSession, DataFrame as SparkDF
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False


def _require_pyspark() -> None:
    if not _PYSPARK_AVAILABLE:
        raise ImportError(
            "PySpark is required for SparkEngine.  "
            "Install it with:  pip install pyspark>=3.0.0"
        )


class SparkEngine(BaseEngine):
    """
    Distributed comparison engine using PySpark.

    Parameters
    ----------
    spark : SparkSession | None
        Existing SparkSession to reuse.  If None, a local session is created.
    app_name : str
        Name passed to SparkSession.builder when creating a new session.
    """

    NAME = "spark"

    def __init__(
        self,
        spark: Optional[Any] = None,
        app_name: str = "DataCompareFramework",
    ) -> None:
        _require_pyspark()
        if spark is not None:
            self._spark = spark
        else:
            self._spark = (
                SparkSession.builder
                .appName(app_name)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.shuffle.partitions", "200")
                .getOrCreate()
            )
            logger.info(f"  [spark] SparkSession created: app={app_name}")

    @property
    def spark(self) -> Any:
        return self._spark

    # ── public interface ──────────────────────────────────────────────────────

    def load_and_align(
        self,
        prod_path: Path,
        dev_path: Path,
        keys: List[str],
        ignore_fields: List[str],
        tol_map: Dict,
        prod_name: str,
        dev_name: str,
    ) -> EngineResult:
        _require_pyspark()
        t0 = time.perf_counter()
        result = EngineResult()
        result.engine_name = self.NAME

        # ── load via Spark ─────────────────────────────────────────────────────
        logger.info(f"  [spark] Loading PROD: {prod_path.name}")
        prod_sdf = (
            self._spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("encoding", "UTF-8")
            .csv(str(prod_path))
        )
        logger.info(f"  [spark] Loading DEV : {dev_path.name}")
        dev_sdf = (
            self._spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("encoding", "UTF-8")
            .csv(str(dev_path))
        )

        # ── ignore fields ──────────────────────────────────────────────────────
        ignore_set = {s.strip() for s in ignore_fields if str(s).strip()}
        if ignore_set:
            for col in ignore_set:
                if col in prod_sdf.columns:
                    prod_sdf = prod_sdf.drop(col)
                if col in dev_sdf.columns:
                    dev_sdf = dev_sdf.drop(col)
            logger.info(f"  [spark] Ignored fields: {sorted(ignore_set)}")
        result.ignore_set = ignore_set

        # ── tolerance ──────────────────────────────────────────────────────────
        result.tol_for_pair = resolve_pair_tolerance(
            tol_map, prod_name, dev_name,
            prod_sdf.columns, dev_sdf.columns,
            ignore_set,
        )

        # ── keys ───────────────────────────────────────────────────────────────
        keys_clean = [k for k in keys if k]
        missing_keys  = [k for k in keys_clean
                         if k not in prod_sdf.columns or k not in dev_sdf.columns]
        existing_keys = [k for k in keys_clean
                         if k in prod_sdf.columns and k in dev_sdf.columns]
        result.existing_keys = existing_keys
        result.missing_keys  = missing_keys

        if missing_keys:
            logger.warning(f"  [spark] Key columns missing: {missing_keys}")

        # Trim key columns (Spark equivalent of normalize_keys)
        for k in existing_keys:
            prod_sdf = prod_sdf.withColumn(k, F.trim(F.col(k)))
            dev_sdf  = dev_sdf.withColumn(k, F.trim(F.col(k)))

        # ── duplicate detection + _SEQ_ ────────────────────────────────────────
        prod_sdf, dev_sdf, align_keys, using_seq, dup_cp, dup_cd = (
            self._add_seq_if_needed(prod_sdf, dev_sdf, existing_keys)
        )
        result.align_keys     = align_keys
        result.using_seq      = using_seq
        result.dup_count_prod = dup_cp
        result.dup_count_dev  = dup_cd

        # ── alignment: join + anti-join ────────────────────────────────────────
        if not existing_keys:
            # Row-order: add row index
            prod_sdf = prod_sdf.withColumn("_ROW_ID_", F.monotonically_increasing_id())
            dev_sdf  = dev_sdf.withColumn("_ROW_ID_", F.monotonically_increasing_id())
            join_keys = ["_ROW_ID_"]
        else:
            join_keys = align_keys

        joined = prod_sdf.alias("prod").join(
            dev_sdf.alias("dev"),
            on=join_keys,
            how="inner",
        )

        only_prod_sdf = prod_sdf.join(dev_sdf.select(join_keys), on=join_keys, how="left_anti")
        only_dev_sdf  = dev_sdf.join(prod_sdf.select(join_keys), on=join_keys, how="left_anti")

        matched_count  = joined.count()
        only_prod_count = only_prod_sdf.count()
        only_dev_count  = only_dev_sdf.count()

        logger.info(
            f"  [spark] Alignment: matched={matched_count:,}  "
            f"only-prod={only_prod_count:,}  "
            f"only-dev={only_dev_count:,}"
        )

        # ── convert to pandas for capability pipeline ──────────────────────────
        logger.info("  [spark] Converting matched records to pandas …")

        # Extract prod and dev column sets from the joined result
        prod_cols_in_joined = [c for c in prod_sdf.columns if c not in join_keys]
        dev_cols_in_joined  = [c for c in dev_sdf.columns  if c not in join_keys]

        # Select prod side of join
        prod_common_sdf = joined.select(
            join_keys + ["prod." + c for c in prod_cols_in_joined]
        )
        dev_common_sdf = joined.select(
            join_keys + ["dev." + c for c in dev_cols_in_joined]
        )

        # Convert to pandas
        prod_common_pd = prod_common_sdf.toPandas().astype(str)
        dev_common_pd  = dev_common_sdf.toPandas().astype(str)

        # Build index for compatibility with pandas capability pipeline
        prod_common_pd = trim_df(prod_common_pd.set_index(join_keys, drop=False))
        dev_common_pd  = trim_df(dev_common_pd.set_index(join_keys, drop=False))

        # Full DataFrames for DuplicateCapability and schema
        prod_pd = trim_df(prod_sdf.toPandas().astype(str))
        dev_pd  = trim_df(dev_sdf.toPandas().astype(str))

        # only_in_prod / only_in_dev indices
        only_prod_pd = only_prod_sdf.select(join_keys).toPandas()
        only_dev_pd  = only_dev_sdf.select(join_keys).toPandas()

        only_prod_index = pd.MultiIndex.from_frame(only_prod_pd) if len(join_keys) > 1 else pd.Index(only_prod_pd[join_keys[0]])
        only_dev_index  = pd.MultiIndex.from_frame(only_dev_pd)  if len(join_keys) > 1 else pd.Index(only_dev_pd[join_keys[0]])

        # Build aligned DataFrames (needed by reporting for missing/extra records)
        prod_aligned_pd = trim_df(prod_sdf.toPandas().astype(str))
        dev_aligned_pd  = trim_df(dev_sdf.toPandas().astype(str))
        prod_aligned_pd = prod_aligned_pd.set_index(join_keys, drop=False).sort_index()
        dev_aligned_pd  = dev_aligned_pd.set_index(join_keys, drop=False).sort_index()

        result.prod_df       = prod_pd
        result.dev_df        = dev_pd
        result.prod_aligned  = prod_aligned_pd
        result.dev_aligned   = dev_aligned_pd
        result.prod_common   = prod_common_pd
        result.dev_common    = dev_common_pd
        result.only_in_prod  = only_prod_index
        result.only_in_dev   = only_dev_index

        result.elapsed_load_s = time.perf_counter() - t0
        logger.info(f"  [spark] Alignment + pandas conversion complete [{result.elapsed_load_s:.2f}s]")
        return result

    # ── parquet-specific methods ─────────────────────────────────────────────

    def read_parquet(
        self,
        path: Path,
        columns: Optional[List[str]] = None,
        filters: Optional[List] = None,
        partition_keys: Optional[List[str]] = None,
        optimal_partitions: int = 200,
    ):
        """
        Read a Parquet file or partitioned directory into a Spark DataFrame.

        Features:
          - Column pruning (reads only required columns)
          - Partition filter pushdown (skips irrelevant partitions)
          - Repartition on join keys for performance
          - NO full .collect() – returns a Spark DataFrame

        Parameters
        ----------
        path : Path
            File or directory path.
        columns : list[str] | None
            Column subset to read (column pruning).
        filters : list | None
            Partition filter predicates (pyarrow/Spark filter syntax).
        partition_keys : list[str] | None
            Columns to repartition on for join performance.
        optimal_partitions : int
            Target Spark partition count.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        _require_pyspark()
        logger.info(f"  [spark] Reading parquet: {path.name}")

        reader = self._spark.read.format("parquet")

        if filters:
            # Convert pyarrow-style filter list to Spark SQL WHERE clause
            try:
                from pyspark.sql.functions import col as spark_col
                sdf = reader.load(str(path))
                for flt in filters:
                    if isinstance(flt, str):
                        sdf = sdf.filter(flt)
            except Exception:
                sdf = reader.load(str(path))
        else:
            sdf = reader.load(str(path))

        # Column pruning
        if columns:
            available = sdf.columns
            sel = [c for c in columns if c in available]
            if sel:
                sdf = sdf.select(sel)
                logger.debug(f"  [spark] Parquet column pruning: {len(sel)} columns selected")

        # Repartition for join performance
        if partition_keys:
            keys_avail = [k for k in partition_keys if k in sdf.columns]
            if keys_avail:
                sdf = sdf.repartition(optimal_partitions, *keys_avail)
                logger.debug(
                    f"  [spark] Repartitioned on {keys_avail} "
                    f"into {optimal_partitions} partitions"
                )

        row_count_approx = sdf.count()   # necessary for alignment check
        logger.info(f"  [spark] Parquet loaded: ~{row_count_approx:,} rows")
        return sdf

    def read_parquet_with_optimization(
        self,
        path: Path,
        optimization_plan: Any,
        ignore_set: set,
    ):
        """
        Read parquet using a pre-built OptimizationPlan.
        Called by ParquetCapability or directly by comparison pipeline.

        Applies:
          - Column pruning from plan.required_columns
          - Partition filters from plan.partition_filters
          - Repartitioning from plan.join_keys + plan.optimal_partitions
          - Caching when plan.cache_prod/cache_dev is set

        Returns Spark DataFrame (NOT pandas) for large datasets.
        """
        _require_pyspark()

        cols     = getattr(optimization_plan, "required_columns", [])
        filters  = getattr(optimization_plan, "partition_filters", [])
        join_keys= getattr(optimization_plan, "join_keys", [])
        n_parts  = getattr(optimization_plan, "optimal_partitions", 200)

        sdf = self.read_parquet(
            path=path,
            columns=[c for c in cols if c not in ignore_set] if cols else None,
            filters=filters or None,
            partition_keys=join_keys,
            optimal_partitions=n_parts,
        )
        return sdf

        def read_chunked(self, path: Path, chunk_size: int = 200_000, **kwargs):
        """
        Read a CSV in chunks by collecting N rows at a time from a Spark DataFrame.
        """
        _require_pyspark()
        sdf = (
            self._spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(str(path))
        )
        total = sdf.count()
        offset = 0
        chunk_num = 0
        sdf_indexed = sdf.withColumn("_row_n", F.monotonically_increasing_id())

        while offset < total:
            chunk_num += 1
            chunk_sdf = sdf_indexed.filter(
                (F.col("_row_n") >= offset) & (F.col("_row_n") < offset + chunk_size)
            ).drop("_row_n")
            chunk_pd = trim_df(chunk_sdf.toPandas().astype(str))
            logger.debug(f"  [spark] Chunk {chunk_num}: {len(chunk_pd):,} rows")
            yield chunk_pd
            offset += chunk_size

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _add_seq_if_needed(
        prod_sdf: Any,
        dev_sdf: Any,
        existing_keys: List[str],
    ):
        """
        Detect duplicate key groups in prod and dev.
        If duplicates exist, add _SEQ_ via Spark window function.

        Returns (prod_sdf, dev_sdf, align_keys, using_seq, dup_cp, dup_cd).
        """
        if not existing_keys:
            return prod_sdf, dev_sdf, existing_keys, False, 0, 0

        # Count duplicates in each file
        prod_counts = prod_sdf.groupBy(existing_keys).count()
        dev_counts  = dev_sdf.groupBy(existing_keys).count()
        dup_cp = prod_counts.filter(F.col("count") > 1).count()
        dup_cd = dev_counts.filter(F.col("count") > 1).count()

        if dup_cp == 0 and dup_cd == 0:
            return prod_sdf, dev_sdf, existing_keys, False, dup_cp, dup_cd

        logger.info(
            f"  [spark] Duplicates detected (prod={dup_cp}, dev={dup_cd}) "
            "– adding _SEQ_ via window function"
        )

        w = Window.partitionBy(existing_keys).orderBy(existing_keys)
        prod_sdf = prod_sdf.withColumn("_SEQ_", F.row_number().over(w) - 1)
        dev_sdf  = dev_sdf.withColumn("_SEQ_", F.row_number().over(w) - 1)

        return prod_sdf, dev_sdf, existing_keys + ["_SEQ_"], True, dup_cp, dup_cd


# ── module-level availability flag ───────────────────────────────────────────

def is_spark_available() -> bool:
    """Return True if PySpark can be imported."""
    return _PYSPARK_AVAILABLE
