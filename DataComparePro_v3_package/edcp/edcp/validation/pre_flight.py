# -*- coding: utf-8 -*-
"""
edcp.validation.pre_flight
───────────────────────────
PreFlightValidator — validates jobs before batch execution starts.

Requirements covered: F-VALID-001 to F-VALID-006

All validations run for all jobs, and ALL errors are returned
together (fail-fast per batch, but collect all errors first).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


class ValidationError:
    """One validation issue (error or warning) for one job."""
    def __init__(self, job_index: int, field: str, message: str, is_warning: bool = False):
        self.job_index  = job_index
        self.field      = field
        self.message    = message
        self.is_warning = is_warning   # True = advisory only, does not block execution

    def to_dict(self) -> Dict[str, Any]:
        d = {"job": self.job_index + 1, "field": self.field, "message": self.message}
        if self.is_warning:
            d["severity"] = "WARNING"
        else:
            d["severity"] = "ERROR"
        return d


class PreFlightValidator:
    """
    Validates a list of comparison specs before a batch is executed.

    Usage
    -----
        validator = PreFlightValidator()
        errors = validator.validate_batch(comparisons)
        if errors:
            raise ValueError(errors)
    """

    def validate_batch(self, comparisons: List[Dict[str, Any]]) -> List[ValidationError]:
        """
        Validate all comparisons.
        Returns list of ValidationError (may include warnings).
        Use blocking_errors() to check if batch should be prevented.
        """
        errors: List[ValidationError] = []

        n = len(comparisons)
        if not (1 <= n <= 20):
            errors.append(ValidationError(-1, "count",
                f"Batch must have 1–20 comparisons. Got {n}."))
            return errors   # further checks pointless

        for i, spec in enumerate(comparisons):
            errors.extend(self._validate_job(i, spec))

        return errors

    @staticmethod
    def blocking_errors(errors: List["ValidationError"]) -> List["ValidationError"]:
        """Return only the errors that should block batch execution."""
        return [e for e in errors if not e.is_warning]

    @staticmethod
    def warnings(errors: List["ValidationError"]) -> List["ValidationError"]:
        """Return only advisory warnings."""
        return [e for e in errors if e.is_warning]

    def _validate_job(self, idx: int, spec: Dict[str, Any]) -> List[ValidationError]:
        errors: List[ValidationError] = []
        prod_path = spec.get("prod_path", "").strip()
        dev_path  = spec.get("dev_path",  "").strip()
        keys      = spec.get("keys", [])

        # F-VALID-001: File existence
        if not prod_path:
            errors.append(ValidationError(idx, "prod_path", "PROD path is required."))
        elif not Path(prod_path).exists():
            errors.append(ValidationError(idx, "prod_path",
                f"File not found: {prod_path}"))

        if not dev_path:
            errors.append(ValidationError(idx, "dev_path", "DEV path is required."))
        elif not Path(dev_path).exists():
            errors.append(ValidationError(idx, "dev_path",
                f"File not found: {dev_path}"))

        # F-VALID-002: File accessibility
        if prod_path and Path(prod_path).exists():
            if not self._is_readable(prod_path):
                errors.append(ValidationError(idx, "prod_path",
                    f"Permission denied: {prod_path}"))

        if dev_path and Path(dev_path).exists():
            if not self._is_readable(dev_path):
                errors.append(ValidationError(idx, "dev_path",
                    f"Permission denied: {dev_path}"))

        # F-VALID-003/004: Schema readability + key column presence
        # Only check if files exist
        if prod_path and dev_path and Path(prod_path).exists() and Path(dev_path).exists():
            prod_cols = self._get_columns(prod_path)
            dev_cols  = self._get_columns(dev_path)

            if prod_cols is None:
                errors.append(ValidationError(idx, "prod_path",
                    f"Cannot read schema from: {prod_path}"))
            if dev_cols is None:
                errors.append(ValidationError(idx, "dev_path",
                    f"Cannot read schema from: {dev_path}"))

            if keys and prod_cols is not None and dev_cols is not None:
                for key in keys:
                    if key not in prod_cols:
                        errors.append(ValidationError(idx, "keys",
                            f"Key column '{key}' not found in PROD file {Path(prod_path).name}"))
                    if key not in dev_cols:
                        errors.append(ValidationError(idx, "keys",
                            f"Key column '{key}' not found in DEV file {Path(dev_path).name}"))

        # F-VALID-005: keys list should not be empty (WARNING — does not block execution)
        if not keys:
            errors.append(ValidationError(idx, "keys",
                "No key columns specified. Row-order comparison will be used — "
                "results may be inaccurate if row order differs between files.",
                is_warning=True))

        # Tolerance values must be non-negative integers
        tolerance = spec.get("tolerance", {})
        for col, val in tolerance.items():
            try:
                v = int(val)
                if v < 0:
                    raise ValueError
            except (TypeError, ValueError):
                errors.append(ValidationError(idx, "tolerance",
                    f"Tolerance for '{col}' must be a non-negative integer. Got: {val}"))

        return errors

    @staticmethod
    def _is_readable(path: str) -> bool:
        try:
            with open(path, "rb") as f:
                f.read(4)
            return True
        except (OSError, PermissionError):
            return False

    @staticmethod
    def _get_columns(path: str) -> Optional[List[str]]:
        """Read just the header row to get column names."""
        p = Path(path)
        try:
            ext = p.suffix.lower()
            if ext in (".xlsx", ".xls"):
                df = pd.read_excel(str(p), nrows=0, engine="openpyxl")
                return list(df.columns)
            if ext in (".parquet", ".parq"):
                try:
                    import pyarrow.parquet as pq
                    return list(pq.read_schema(str(p)).names)
                except ImportError:
                    df = pd.read_parquet(str(p)).head(0)
                    return list(df.columns)
            # CSV / TXT
            from edcp.loaders.encoding import read_csv_robust, detect_delimiter
            delim = detect_delimiter(p)
            df = read_csv_robust(p, dtype=str, nrows=0, delimiter=delim)
            return list(df.columns)
        except Exception:
            return None
