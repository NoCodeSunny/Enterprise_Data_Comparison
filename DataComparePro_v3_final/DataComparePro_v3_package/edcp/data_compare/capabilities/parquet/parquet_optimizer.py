# -*- coding: utf-8 -*-
"""
data_compare.capabilities.parquet.parquet_optimizer
─────────────────────────────────────────────────────
ParquetOptimizer

Computes and applies performance optimizations for Parquet comparisons:

  1. Column pruning    – only project columns needed for comparison
  2. Partition pruning – push predicates into partition paths
  3. Row group stats   – skip row groups that cannot contain diff rows
  4. Caching strategy  – mark which DataFrames to cache in Spark
  5. Repartitioning    – optimal partition count for join operations
  6. Filter pushdown   – row-level predicates for scoped comparisons

The optimizer produces an OptimizationPlan that is stored in context and
consumed by SparkEngine.read_parquet() and ParquetCapability.execute().

No parquet reading happens in this module – it only produces plans.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# ── optional pyarrow import ───────────────────────────────────────────────────
try:
    import pyarrow.parquet as pq
    _PYARROW_AVAILABLE = True
except ImportError:
    _PYARROW_AVAILABLE = False


@dataclass
class OptimizationPlan:
    """
    Complete optimization plan for a parquet comparison batch.
    Consumed by SparkEngine / ParquetCapability when reading files.
    """
    # Column pruning
    required_columns:     List[str]           = field(default_factory=list)
    prune_columns:        bool                = True

    # Partition awareness
    partition_columns:    List[str]           = field(default_factory=list)
    partition_filters:    List[Any]           = field(default_factory=list)
    use_partition_pruning: bool               = False

    # Repartitioning
    optimal_partitions:   int                = 200
    repartition_on_keys:  bool               = True
    join_keys:            List[str]           = field(default_factory=list)

    # Caching
    cache_prod:           bool               = False
    cache_dev:            bool               = False
    cache_threshold_rows: int               = 10_000_000

    # Row group stats
    use_row_group_stats:  bool               = True

    # Read configuration
    read_options:         Dict[str, Any]     = field(default_factory=dict)

    # Metadata
    prod_estimated_rows:  Optional[int]      = None
    dev_estimated_rows:   Optional[int]      = None
    prod_size_bytes:      Optional[int]      = None
    dev_size_bytes:       Optional[int]      = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "required_columns":     self.required_columns,
            "prune_columns":        self.prune_columns,
            "partition_columns":    self.partition_columns,
            "use_partition_pruning": self.use_partition_pruning,
            "optimal_partitions":   self.optimal_partitions,
            "repartition_on_keys":  self.repartition_on_keys,
            "cache_prod":           self.cache_prod,
            "cache_dev":            self.cache_dev,
            "prod_estimated_rows":  self.prod_estimated_rows,
            "dev_estimated_rows":   self.dev_estimated_rows,
            "prod_size_bytes":      self.prod_size_bytes,
            "dev_size_bytes":       self.dev_size_bytes,
        }


class ParquetOptimizer:
    """
    Builds an OptimizationPlan for a given comparison batch.

    Parameters
    ----------
    default_partitions : int
        Default Spark partition count. Override per-batch via config.
    cache_threshold_rows : int
        Cache DataFrames in Spark when estimated row count exceeds this.
    """

    def __init__(
        self,
        default_partitions: int = 200,
        cache_threshold_rows: int = 10_000_000,
    ) -> None:
        self._default_partitions   = default_partitions
        self._cache_threshold_rows = cache_threshold_rows

    # ── public API ────────────────────────────────────────────────────────────

    def build_plan(
        self,
        prod_path: Path,
        dev_path: Path,
        keys: List[str],
        ignore_fields: List[str],
        required_columns: Optional[List[str]] = None,
        partition_filters: Optional[List[Any]] = None,
    ) -> OptimizationPlan:
        """
        Analyse both paths and return an OptimizationPlan.

        Parameters
        ----------
        prod_path : Path
        dev_path : Path
        keys : list[str]
            Key columns used for join.
        ignore_fields : list[str]
            Columns to exclude – never included in required_columns.
        required_columns : list[str] | None
            Explicit column list. If None, derived from schema minus ignore_fields.
        partition_filters : list | None
            Explicit partition filter expressions (pyarrow filter syntax).

        Returns
        -------
        OptimizationPlan
        """
        plan = OptimizationPlan(join_keys=keys)
        plan.cache_threshold_rows = self._cache_threshold_rows

        # ── file size metadata ────────────────────────────────────────────────
        plan.prod_size_bytes = self._dir_size(prod_path)
        plan.dev_size_bytes  = self._dir_size(dev_path)
        logger.debug(
            f"  [parquet_optimizer] PROD size: "
            f"{plan.prod_size_bytes / 1024 / 1024:.1f} MB  "
            f"DEV size: {plan.dev_size_bytes / 1024 / 1024:.1f} MB"
        )

        # ── partition detection ───────────────────────────────────────────────
        prod_parts = self._detect_partition_columns(prod_path)
        dev_parts  = self._detect_partition_columns(dev_path)
        if prod_parts:
            plan.partition_columns   = prod_parts
            plan.use_partition_pruning = True
            logger.info(f"  [parquet_optimizer] Partition columns: {prod_parts}")

        if partition_filters:
            plan.partition_filters = partition_filters

        # ── column pruning ────────────────────────────────────────────────────
        if required_columns:
            plan.required_columns = [
                c for c in required_columns
                if c not in set(ignore_fields)
            ]
        else:
            plan.required_columns = self._resolve_required_columns(
                prod_path, dev_path, keys, ignore_fields
            )
        logger.debug(f"  [parquet_optimizer] Pruned columns: {plan.required_columns}")

        # ── row count estimation ──────────────────────────────────────────────
        if _PYARROW_AVAILABLE:
            plan.prod_estimated_rows = self._estimate_rows(prod_path)
            plan.dev_estimated_rows  = self._estimate_rows(dev_path)
            if plan.prod_estimated_rows is not None:
                logger.info(
                    f"  [parquet_optimizer] Estimated rows – "
                    f"PROD: {plan.prod_estimated_rows:,}  "
                    f"DEV: {plan.dev_estimated_rows or 0:,}"
                )

        # ── caching strategy ──────────────────────────────────────────────────
        if plan.prod_estimated_rows and plan.prod_estimated_rows >= self._cache_threshold_rows:
            plan.cache_prod = True
            logger.info(
                f"  [parquet_optimizer] PROD will be cached "
                f"({plan.prod_estimated_rows:,} rows ≥ threshold)"
            )
        if plan.dev_estimated_rows and plan.dev_estimated_rows >= self._cache_threshold_rows:
            plan.cache_dev = True

        # ── repartitioning ────────────────────────────────────────────────────
        plan.optimal_partitions = self._compute_optimal_partitions(
            plan.prod_size_bytes, plan.dev_size_bytes
        )
        logger.debug(
            f"  [parquet_optimizer] Optimal partitions: {plan.optimal_partitions}"
        )

        return plan

    def apply_to_spark_read(
        self,
        plan: OptimizationPlan,
        spark,
        path: Path,
        is_prod: bool = True,
    ):
        """
        Apply the optimization plan when reading a Parquet file via Spark.

        Returns a Spark DataFrame with pruning and filters applied.
        """
        try:
            from pyspark.sql import functions as F
        except ImportError:
            raise ImportError("PySpark required for apply_to_spark_read()")

        label = "PROD" if is_prod else "DEV"

        # Base read
        reader = spark.read.format("parquet")

        # Column pruning: tell Spark which columns to project
        if plan.prune_columns and plan.required_columns:
            logger.debug(
                f"  [parquet_optimizer] Applying column pruning to {label}: "
                f"{len(plan.required_columns)} columns"
            )

        sdf = reader.load(str(path))

        # Apply column selection (column pruning)
        if plan.prune_columns and plan.required_columns:
            available = sdf.columns
            cols_to_read = [c for c in plan.required_columns if c in available]
            if cols_to_read:
                sdf = sdf.select(cols_to_read)

        # Apply partition filters
        if plan.partition_filters and plan.use_partition_pruning:
            for flt in plan.partition_filters:
                try:
                    sdf = sdf.filter(flt)
                except Exception as exc:
                    logger.warning(
                        f"  [parquet_optimizer] Could not apply partition filter {flt}: {exc}"
                    )

        # Repartition for join performance
        if plan.repartition_on_keys and plan.join_keys:
            available_keys = [k for k in plan.join_keys if k in sdf.columns]
            if available_keys:
                sdf = sdf.repartition(plan.optimal_partitions, *available_keys)

        # Cache
        cache_this = plan.cache_prod if is_prod else plan.cache_dev
        if cache_this:
            sdf.cache()
            logger.info(f"  [parquet_optimizer] {label} DataFrame cached in Spark")

        return sdf

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _dir_size(path: Path) -> int:
        """Return total byte size of a file or directory."""
        if path.is_file():
            return path.stat().st_size
        total = 0
        for dirpath, _, filenames in os.walk(path):
            for fname in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, fname))
                except OSError:
                    pass
        return total

    @staticmethod
    def _detect_partition_columns(path: Path) -> List[str]:
        """Return Hive-style partition column names from directory structure."""
        if not path.is_dir():
            return []
        parts = []
        for child in path.iterdir():
            if child.is_dir() and "=" in child.name:
                col = child.name.split("=")[0]
                if col not in parts:
                    parts.append(col)
        return sorted(parts)

    def _resolve_required_columns(
        self,
        prod_path: Path,
        dev_path: Path,
        keys: List[str],
        ignore_fields: List[str],
    ) -> List[str]:
        """
        Derive required columns: all columns in both files, minus ignored ones.
        Uses pyarrow if available, falls back to empty list (Spark reads all).
        """
        if not _PYARROW_AVAILABLE:
            return []  # Spark will read all columns; pruning applied later

        ignore_set = set(ignore_fields)
        try:
            import pyarrow.parquet as pq
            prod_cols = {f.name for f in pq.read_schema(str(prod_path))}
            dev_cols  = {f.name for f in pq.read_schema(str(dev_path))}
            return sorted((prod_cols | dev_cols) - ignore_set)
        except Exception as exc:
            logger.debug(f"  [parquet_optimizer] Column resolution fallback: {exc}")
            return []

    @staticmethod
    def _estimate_rows(path: Path) -> Optional[int]:
        """Estimate row count from Parquet metadata (no full scan)."""
        if not _PYARROW_AVAILABLE:
            return None
        try:
            import pyarrow.parquet as pq
            meta = pq.read_metadata(str(path))
            return meta.num_rows
        except Exception:
            return None

    def _compute_optimal_partitions(
        self,
        prod_bytes: Optional[int],
        dev_bytes: Optional[int],
        target_partition_size_mb: int = 128,
    ) -> int:
        """
        Compute optimal Spark partition count.
        Rule of thumb: one partition per 128 MB of data.
        """
        total = (prod_bytes or 0) + (dev_bytes or 0)
        if total == 0:
            return self._default_partitions
        target_bytes = target_partition_size_mb * 1024 * 1024
        computed = max(1, total // target_bytes)
        # Clamp between 10 and 2000
        return min(max(computed, 10), 2000)
