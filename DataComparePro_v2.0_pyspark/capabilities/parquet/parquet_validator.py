# -*- coding: utf-8 -*-
"""
data_compare.capabilities.parquet.parquet_validator
─────────────────────────────────────────────────────
ParquetValidator

Standalone utility that validates:
  1. Parquet schema consistency between PROD and DEV files
  2. Partition structure consistency (if datasets are partitioned)
  3. File-level metadata (row groups, compression, format version)
  4. Column presence and type compatibility

Runs BEFORE comparison to surface schema issues early.

Requires pyarrow for full functionality.  If pyarrow is not installed,
falls back to pandas-based schema comparison (CSV/basic parquet only).

Used exclusively by ParquetCapability — not called by any other module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# ── optional pyarrow import ───────────────────────────────────────────────────
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PYARROW_AVAILABLE = True
except ImportError:
    _PYARROW_AVAILABLE = False


@dataclass
class SchemaValidationResult:
    """Outcome of a parquet schema validation check."""
    is_valid:          bool
    prod_schema:       Optional[Any]         = None   # pyarrow Schema or list
    dev_schema:        Optional[Any]         = None
    missing_in_dev:    List[str]             = field(default_factory=list)
    extra_in_dev:      List[str]             = field(default_factory=list)
    type_mismatches:   List[Tuple[str, str, str]] = field(default_factory=list)
    partition_issues:  List[str]             = field(default_factory=list)
    metadata_warnings: List[str]             = field(default_factory=list)
    errors:            List[str]             = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid":          self.is_valid,
            "missing_in_dev":    self.missing_in_dev,
            "extra_in_dev":      self.extra_in_dev,
            "type_mismatches":   self.type_mismatches,
            "partition_issues":  self.partition_issues,
            "metadata_warnings": self.metadata_warnings,
            "errors":            self.errors,
        }


class ParquetValidator:
    """
    Validates Parquet files/datasets before comparison runs.

    Supports:
    - Single .parquet files
    - Partitioned directories (Hive-style partitioning)
    - Both pyarrow and pandas fallback
    """

    def __init__(self) -> None:
        self._has_pyarrow = _PYARROW_AVAILABLE

    # ── public API ────────────────────────────────────────────────────────────

    def validate(
        self,
        prod_path: Path,
        dev_path: Path,
        keys: List[str],
        ignore_fields: List[str],
    ) -> SchemaValidationResult:
        """
        Validate schema + metadata consistency between PROD and DEV parquet sources.

        Parameters
        ----------
        prod_path : Path
            Path to PROD parquet file or partition directory.
        dev_path : Path
            Path to DEV parquet file or partition directory.
        keys : list[str]
            Key columns — validated to be present in both.
        ignore_fields : list[str]
            Fields excluded from validation.

        Returns
        -------
        SchemaValidationResult
        """
        logger.info(f"  [parquet_validator] Validating schemas: {prod_path.name} vs {dev_path.name}")

        result = SchemaValidationResult(is_valid=True)

        if self._has_pyarrow:
            self._validate_with_pyarrow(result, prod_path, dev_path, keys, ignore_fields)
        else:
            self._validate_with_pandas_fallback(result, prod_path, dev_path, keys, ignore_fields)

        # Check key columns present
        self._validate_keys(result, keys)

        # Log summary
        if result.is_valid:
            logger.info("  [parquet_validator] ✅ Schema validation passed")
        else:
            for err in result.errors:
                logger.error(f"  [parquet_validator] ❌ {err}")
        for warn in result.metadata_warnings:
            logger.warning(f"  [parquet_validator] ⚠  {warn}")

        return result

    def is_parquet(self, path: Path) -> bool:
        """
        Detect whether *path* points to a parquet file or partitioned directory.

        Detection strategy:
        1. File extension is .parquet or .parq
        2. Directory containing .parquet files (partitioned dataset)
        3. File starts with Parquet magic bytes PAR1
        """
        if not path.exists():
            return False
        if path.is_file():
            if path.suffix.lower() in (".parquet", ".parq"):
                return True
            # Check magic bytes
            try:
                with open(path, "rb") as f:
                    magic = f.read(4)
                return magic == b"PAR1"
            except OSError:
                return False
        if path.is_dir():
            # Hive-style partitioned directory
            for child in path.rglob("*.parquet"):
                return True
            for child in path.rglob("*.parq"):
                return True
        return False

    def get_partition_columns(self, path: Path) -> List[str]:
        """
        Return partition column names from a partitioned parquet directory.
        Detects Hive-style partitioning: key=value/ subdirectory names.
        """
        if not path.is_dir():
            return []
        parts = []
        for child in path.iterdir():
            if child.is_dir() and "=" in child.name:
                col = child.name.split("=")[0]
                if col not in parts:
                    parts.append(col)
        return sorted(parts)

    # ── pyarrow validation ────────────────────────────────────────────────────

    def _validate_with_pyarrow(
        self,
        result: SchemaValidationResult,
        prod_path: Path,
        dev_path: Path,
        keys: List[str],
        ignore_fields: List[str],
    ) -> None:
        import pyarrow.parquet as pq

        ignore_set = set(ignore_fields)

        try:
            prod_schema = pq.read_schema(str(prod_path))
            result.prod_schema = prod_schema
        except Exception as exc:
            result.errors.append(f"Cannot read PROD schema: {exc}")
            result.is_valid = False
            return

        try:
            dev_schema = pq.read_schema(str(dev_path))
            result.dev_schema = dev_schema
        except Exception as exc:
            result.errors.append(f"Cannot read DEV schema: {exc}")
            result.is_valid = False
            return

        prod_cols = {f.name for f in prod_schema if f.name not in ignore_set}
        dev_cols  = {f.name for f in dev_schema  if f.name not in ignore_set}

        result.missing_in_dev = sorted(prod_cols - dev_cols)
        result.extra_in_dev   = sorted(dev_cols  - prod_cols)

        if result.missing_in_dev or result.extra_in_dev:
            result.metadata_warnings.append(
                f"Schema diff: missing={result.missing_in_dev}, extra={result.extra_in_dev}"
            )

        # Type mismatch check
        common = prod_cols & dev_cols
        prod_type_map = {f.name: str(f.type) for f in prod_schema}
        dev_type_map  = {f.name: str(f.type) for f in dev_schema}
        for col in sorted(common):
            pt = prod_type_map.get(col, "?")
            dt = dev_type_map.get(col, "?")
            if pt != dt:
                result.type_mismatches.append((col, pt, dt))
                result.metadata_warnings.append(
                    f"Type mismatch on '{col}': PROD={pt}, DEV={dt}"
                )

        # Row group metadata
        try:
            prod_meta = pq.read_metadata(str(prod_path))
            dev_meta  = pq.read_metadata(str(dev_path))
            logger.debug(
                f"  [parquet_validator] PROD: {prod_meta.num_rows:,} rows, "
                f"{prod_meta.num_row_groups} row groups"
            )
            logger.debug(
                f"  [parquet_validator] DEV : {dev_meta.num_rows:,} rows, "
                f"{dev_meta.num_row_groups} row groups"
            )
        except Exception:
            pass

        # Partition consistency
        prod_parts = self.get_partition_columns(prod_path)
        dev_parts  = self.get_partition_columns(dev_path)
        if prod_parts != dev_parts:
            result.partition_issues.append(
                f"Partition columns differ: PROD={prod_parts}, DEV={dev_parts}"
            )
            result.metadata_warnings.append(result.partition_issues[-1])

    # ── pandas fallback validation ────────────────────────────────────────────

    def _validate_with_pandas_fallback(
        self,
        result: SchemaValidationResult,
        prod_path: Path,
        dev_path: Path,
        keys: List[str],
        ignore_fields: List[str],
    ) -> None:
        """
        Column-name schema validation using pandas (no pyarrow required).
        Only validates column names and key presence – no type information.
        """
        import pandas as pd

        ignore_set = set(ignore_fields)

        def _read_columns(path: Path) -> Optional[List[str]]:
            try:
                if path.suffix.lower() in (".parquet", ".parq"):
                    # Minimal parquet: try read with pandas if it can
                    try:
                        df = pd.read_parquet(path, engine="auto")
                        return list(df.columns)
                    except Exception:
                        pass
                # Fall back to reading first row of CSV representation
                df = pd.read_csv(path, nrows=0, dtype=str)
                return list(df.columns)
            except Exception as exc:
                logger.warning(f"  [parquet_validator] Cannot read columns from {path}: {exc}")
                return None

        prod_cols_raw = _read_columns(prod_path)
        dev_cols_raw  = _read_columns(dev_path)

        if prod_cols_raw is None:
            result.errors.append(f"Cannot read PROD file: {prod_path}")
            result.is_valid = False
            return
        if dev_cols_raw is None:
            result.errors.append(f"Cannot read DEV file: {dev_path}")
            result.is_valid = False
            return

        result.prod_schema = prod_cols_raw
        result.dev_schema  = dev_cols_raw

        prod_cols = {c for c in prod_cols_raw if c not in ignore_set}
        dev_cols  = {c for c in dev_cols_raw  if c not in ignore_set}

        result.missing_in_dev = sorted(prod_cols - dev_cols)
        result.extra_in_dev   = sorted(dev_cols  - prod_cols)

        if result.missing_in_dev or result.extra_in_dev:
            result.metadata_warnings.append(
                f"Schema diff: missing={result.missing_in_dev}, extra={result.extra_in_dev}"
            )

        logger.debug("  [parquet_validator] Fallback validation used (pyarrow not available)")

    # ── key validation ────────────────────────────────────────────────────────

    def _validate_keys(
        self,
        result: SchemaValidationResult,
        keys: List[str],
    ) -> None:
        if not keys:
            return

        def _col_names(schema) -> List[str]:
            if schema is None:
                return []
            if isinstance(schema, list):
                return schema
            # pyarrow Schema
            try:
                return [f.name for f in schema]
            except Exception:
                return []

        prod_cols = set(_col_names(result.prod_schema))
        dev_cols  = set(_col_names(result.dev_schema))

        for k in keys:
            if k not in prod_cols:
                result.errors.append(f"Key column '{k}' not found in PROD schema")
                result.is_valid = False
            if k not in dev_cols:
                result.errors.append(f"Key column '{k}' not found in DEV schema")
                result.is_valid = False
