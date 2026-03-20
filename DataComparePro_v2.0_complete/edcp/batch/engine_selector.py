# -*- coding: utf-8 -*-
"""
edcp.batch.engine_selector
───────────────────────────
EnterpriseEngineSelector — routes jobs to the correct execution engine.

Rules (F-ENGINE-001 to 006):
  - Parquet files → ALWAYS Spark (F-ENGINE-004)
  - File size < 500 MB → Pandas (F-ENGINE-002)
  - File size ≥ 500 MB → Spark (F-ENGINE-003)
  - User override via config (F-ENGINE-006)

Engine decision is logged in audit (F-ENGINE-005).
"""
from __future__ import annotations

from pathlib import Path

SPARK_THRESHOLD_BYTES = 500 * 1024 * 1024   # 500 MB
PARQUET_EXTENSIONS    = {".parquet", ".parq"}


def select_job_engine(prod_path: str, dev_path: str, override: str = "") -> str:
    """
    Determine which engine to use for this job.

    Parameters
    ----------
    prod_path : str
        Path to PROD file.
    dev_path : str
        Path to DEV file.
    override : str
        "pandas" or "spark" to force a specific engine.

    Returns
    -------
    str
        "pandas" or "spark"
    """
    if override.lower() in ("pandas", "spark"):
        return override.lower()

    prod_p = Path(prod_path)
    dev_p  = Path(dev_path)

    # F-ENGINE-004: Parquet always Spark
    if prod_p.suffix.lower() in PARQUET_EXTENSIONS or dev_p.suffix.lower() in PARQUET_EXTENSIONS:
        return "spark"

    # Check magic bytes for parquet without extension
    if _is_parquet_by_magic(prod_p) or _is_parquet_by_magic(dev_p):
        return "spark"

    # F-ENGINE-002/003: File size threshold
    try:
        prod_size = prod_p.stat().st_size if prod_p.exists() else 0
        dev_size  = dev_p.stat().st_size  if dev_p.exists()  else 0
        if max(prod_size, dev_size) >= SPARK_THRESHOLD_BYTES:
            return "spark"
    except OSError:
        pass

    return "pandas"


def engine_selection_reason(prod_path: str, dev_path: str, engine: str) -> str:
    """Human-readable reason for engine selection (for audit log)."""
    prod_p = Path(prod_path)
    dev_p  = Path(dev_path)
    if prod_p.suffix.lower() in PARQUET_EXTENSIONS or dev_p.suffix.lower() in PARQUET_EXTENSIONS:
        return f"Engine=spark: parquet file detected (F-ENGINE-004)"
    try:
        prod_size = prod_p.stat().st_size if prod_p.exists() else 0
        dev_size  = dev_p.stat().st_size  if dev_p.exists()  else 0
        mb = max(prod_size, dev_size) / (1024*1024)
        if mb >= 500:
            return f"Engine=spark: file size {mb:.1f} MB ≥ 500 MB threshold (F-ENGINE-003)"
        return f"Engine=pandas: file size {mb:.1f} MB < 500 MB threshold (F-ENGINE-002)"
    except OSError:
        return f"Engine={engine}: could not determine file size"


def _is_parquet_by_magic(path: Path) -> bool:
    """Check PAR1 magic bytes without loading the file."""
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"PAR1"
    except OSError:
        return False
