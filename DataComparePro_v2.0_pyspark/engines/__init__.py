# -*- coding: utf-8 -*-
"""
data_compare.engines
─────────────────────
Engine abstraction layer.

Auto-selection logic
────────────────────
select_engine(config, prod_path, dev_path) → BaseEngine

Rules (evaluated in order):
  1. If config["use_spark"] is True                          → SparkEngine
  2. If either file size > config["spark_threshold_bytes"]   → SparkEngine
  3. Otherwise                                               → PandasEngine

The threshold defaults to 500 MB.  SparkEngine is only selected when PySpark
is installed; otherwise PandasEngine is used with a warning.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from data_compare.engines.base_engine import BaseEngine, EngineResult
from data_compare.engines.pandas_engine import PandasEngine
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# Default Spark threshold: 500 MB
_DEFAULT_SPARK_THRESHOLD = 500 * 1024 * 1024  # bytes


def select_engine(
    config: Dict[str, Any],
    prod_path: Optional[Path] = None,
    dev_path: Optional[Path] = None,
    spark_session: Optional[Any] = None,
) -> BaseEngine:
    """
    Select and return the appropriate comparison engine based on config and
    file sizes.

    Parameters
    ----------
    config : dict
        Run configuration dict (from load_config()).
        Keys consulted:
            use_spark              : bool  – force Spark engine
            spark_threshold_bytes  : int   – file-size threshold for auto-Spark
            pandas_chunk_size      : int   – chunk size for PandasEngine
    prod_path : Path | None
        Path to the PROD file (used for file-size check).
    dev_path : Path | None
        Path to the DEV file (used for file-size check).
    spark_session : SparkSession | None
        Existing SparkSession to reuse when SparkEngine is selected.

    Returns
    -------
    BaseEngine
        Instantiated engine ready to call load_and_align().
    """
    use_spark_cfg    = bool(config.get("use_spark", False))
    threshold        = int(config.get("spark_threshold_bytes", _DEFAULT_SPARK_THRESHOLD))
    pandas_chunk_sz  = int(config.get("pandas_chunk_size", 200_000))

    want_spark = use_spark_cfg
    if not want_spark and prod_path and dev_path:
        try:
            prod_sz = Path(prod_path).stat().st_size if Path(prod_path).exists() else 0
            dev_sz  = Path(dev_path).stat().st_size  if Path(dev_path).exists() else 0
            if prod_sz > threshold or dev_sz > threshold:
                logger.info(
                    f"  [engine_select] File size exceeds threshold "
                    f"({max(prod_sz, dev_sz) / 1024 / 1024:.1f} MB > "
                    f"{threshold / 1024 / 1024:.0f} MB) – requesting Spark"
                )
                want_spark = True
        except OSError:
            pass  # files not found yet – decide at runtime

    if want_spark:
        try:
            from data_compare.engines.spark_engine import SparkEngine, is_spark_available
            if is_spark_available():
                logger.info("  [engine_select] → SparkEngine selected")
                return SparkEngine(spark=spark_session)
            else:
                logger.warning(
                    "  [engine_select] Spark requested but PySpark not installed – "
                    "falling back to PandasEngine"
                )
        except Exception as exc:
            logger.warning(
                f"  [engine_select] SparkEngine unavailable ({exc}) – "
                "falling back to PandasEngine"
            )

    logger.info("  [engine_select] → PandasEngine selected")
    return PandasEngine(chunk_size=pandas_chunk_sz)


__all__ = [
    "BaseEngine",
    "EngineResult",
    "PandasEngine",
    "select_engine",
]
