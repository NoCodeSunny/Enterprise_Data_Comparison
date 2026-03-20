# -*- coding: utf-8 -*-
"""
data_compare.debugger.error_mapper
────────────────────────────────────
ErrorMapper

Maps Python exception instances to structured, human-readable error records.
Each record contains:
  - error_code     : short unique identifier (e.g. "FILE_NOT_FOUND")
  - category       : broad group   (e.g. "File I/O", "Configuration", "Schema")
  - human_message  : plain-English description of what went wrong
  - root_cause     : likely technical reason
  - recommended_fix: actionable steps the user should take
  - module_hint    : which data_compare module is most likely responsible
  - severity       : "CRITICAL" | "ERROR" | "WARNING"

ErrorMapper is pure data — it never raises, logs, or reads files.
It is called exclusively by ErrorAnalyzer and Debugger.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ErrorRecord:
    """Structured error record produced by ErrorMapper."""
    error_code:      str
    category:        str
    human_message:   str
    root_cause:      str
    recommended_fix: str
    module_hint:     str
    severity:        str = "ERROR"
    extra:           Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_code":      self.error_code,
            "category":        self.category,
            "severity":        self.severity,
            "human_message":   self.human_message,
            "root_cause":      self.root_cause,
            "recommended_fix": self.recommended_fix,
            "module_hint":     self.module_hint,
            "extra":           self.extra,
        }

    def __str__(self) -> str:
        lines = [
            f"[{self.severity}] {self.error_code}  –  {self.category}",
            f"  What happened : {self.human_message}",
            f"  Root cause    : {self.root_cause}",
            f"  Fix           : {self.recommended_fix}",
            f"  Module        : {self.module_hint}",
        ]
        if self.extra:
            for k, v in self.extra.items():
                lines.append(f"  {k:<14}: {v}")
        return "\n".join(lines)


class ErrorMapper:
    """
    Maps exceptions to ErrorRecord instances.

    Usage
    -----
        mapper = ErrorMapper()
        record = mapper.map(exc)
        print(record)
    """

    def map(self, exc: Exception, context_hint: str = "") -> ErrorRecord:
        """
        Map *exc* to a structured ErrorRecord.

        Parameters
        ----------
        exc : Exception
        context_hint : str
            Optional extra text (e.g. capability name, file path) injected
            into the record for added context.

        Returns
        -------
        ErrorRecord
        """
        msg = str(exc)
        exc_type = type(exc).__name__

        # ── FileNotFoundError ─────────────────────────────────────────────────
        if isinstance(exc, FileNotFoundError):
            path = self._extract_path(msg)
            return ErrorRecord(
                error_code="FILE_NOT_FOUND",
                category="File I/O",
                severity="CRITICAL",
                human_message=f"Required file was not found: {path or msg}",
                root_cause=(
                    "The file path specified in the InputSheet or config.yaml does not exist "
                    "on disk, or the path contains a typo."
                ),
                recommended_fix=(
                    "1. Check the 'Files Path' and 'Prod/Dev File Name' columns in your InputSheet.\n"
                    "2. Verify the file exists at the specified path.\n"
                    "3. Check for leading/trailing spaces in the path.\n"
                    "4. Ensure the network drive is mounted if the path is remote."
                ),
                module_hint="data_compare.loaders.file_loader / data_compare.orchestrator",
                extra={"path": path or "unknown", "context": context_hint},
            )

        # ── PermissionError ───────────────────────────────────────────────────
        if isinstance(exc, PermissionError):
            return ErrorRecord(
                error_code="PERMISSION_DENIED",
                category="File I/O",
                severity="CRITICAL",
                human_message=f"Permission denied accessing a file or directory: {msg}",
                root_cause=(
                    "The process does not have read (or write) permission on the file. "
                    "This commonly happens when an Excel file is open in another application."
                ),
                recommended_fix=(
                    "1. Close the file in Excel or any other application.\n"
                    "2. Check file permissions (right-click → Properties → Security).\n"
                    "3. Run the comparison from a user account with read access."
                ),
                module_hint="data_compare.loaders.file_loader",
                extra={"context": context_hint},
            )

        # ── ConfigError ───────────────────────────────────────────────────────
        if "ConfigError" in exc_type or "config" in msg.lower()[:60]:
            return ErrorRecord(
                error_code="CONFIG_INVALID",
                category="Configuration",
                severity="CRITICAL",
                human_message=f"Configuration validation failed: {msg[:200]}",
                root_cause=(
                    "One or more required fields are missing or have invalid values "
                    "in config.yaml or the hardcoded defaults in config_loader.py."
                ),
                recommended_fix=(
                    "1. Run: python -m data_compare validate config/config.yaml\n"
                    "2. Check the error message for the specific field that failed.\n"
                    "3. Review config.yaml against the full field reference in the documentation.\n"
                    "4. Ensure log_level is one of: DEBUG, INFO, WARNING, ERROR."
                ),
                module_hint="data_compare.config.config_schema / data_compare.config.config_loader",
                extra={"context": context_hint},
            )

        # ── UnicodeDecodeError (before ValueError since it's a subclass) ────────
        if isinstance(exc, UnicodeDecodeError):
            return ErrorRecord(
                error_code="ENCODING_ERROR",
                category="File I/O",
                severity="ERROR",
                human_message="File contains characters that cannot be decoded.",
                root_cause=(
                    "The file encoding is not one of the supported encodings "
                    "(utf-8-sig, utf-8, cp1252, latin1). The file may use an exotic "
                    "code page or may be binary (not a text file)."
                ),
                recommended_fix=(
                    "1. Open the file in Excel and save as CSV UTF-8.\n"
                    "2. Use Notepad++ to convert encoding to UTF-8.\n"
                    "3. Check that the file is not a binary/zip file named .csv."
                ),
                module_hint="data_compare.loaders.encoding",
                extra={"context": context_hint},
            )

        # ── ValueError: schema/column mismatch ───────────────────────────────
        if isinstance(exc, ValueError):
            low = msg.lower()
            if "column" in low or "key" in low or "field" in low:
                return ErrorRecord(
                    error_code="SCHEMA_MISMATCH",
                    category="Schema",
                    severity="ERROR",
                    human_message=f"Column or key mismatch detected: {msg[:200]}",
                    root_cause=(
                        "A required key column is missing from one or both files, "
                        "or the InputSheet references a column that does not exist."
                    ),
                    recommended_fix=(
                        "1. Check that all Key ColumnN values in the InputSheet exist in both files.\n"
                        "2. Compare column names in prod vs dev (case-sensitive).\n"
                        "3. Look at the 'Schema Differences' sheet in the generated Excel report.\n"
                        "4. Remove or correct missing key columns in the InputSheet."
                    ),
                    module_hint="data_compare.utils.validation / data_compare.capabilities.schema",
                    extra={"context": context_hint},
                )
            if "series" in low or "shape" in low or "broadcast" in low or "identically" in low:
                return ErrorRecord(
                    error_code="ALIGNMENT_ERROR",
                    category="Comparison",
                    severity="ERROR",
                    human_message=f"DataFrame alignment error during comparison: {msg[:200]}",
                    root_cause=(
                        "PROD and DEV DataFrames have mismatched indices or shapes. "
                        "This typically occurs when duplicate keys are present and "
                        "_SEQ_ alignment is not being applied correctly."
                    ),
                    recommended_fix=(
                        "1. Check for duplicate key values in both files.\n"
                        "2. Verify the DuplicateCapability is enabled in config.yaml.\n"
                        "3. Ensure key columns have no null/blank values.\n"
                        "4. Review the 'Duplicates Records' sheet in the Excel report."
                    ),
                    module_hint="data_compare.capabilities.comparison.comparison_capability",
                    extra={"context": context_hint},
                )
            return ErrorRecord(
                error_code="VALUE_ERROR",
                category="Data",
                severity="ERROR",
                human_message=f"Unexpected value error: {msg[:200]}",
                root_cause="Invalid data value or type encountered during processing.",
                recommended_fix=(
                    "1. Check the InputSheet for blank cells in required columns.\n"
                    "2. Ensure numeric tolerance values are valid integers.\n"
                    "3. Enable DEBUG logging to see the full stack trace."
                ),
                module_hint="data_compare.capabilities.comparison / data_compare.comparator",
                extra={"context": context_hint},
            )

        # ── ImportError / ModuleNotFoundError ─────────────────────────────────
        if isinstance(exc, (ImportError, ModuleNotFoundError)):
            module_name = self._extract_module(msg)
            fixes = {
                "pyarrow":   "pip install pyarrow",
                "pyspark":   "pip install pyspark",
                "pywin32":   "pip install pywin32",
                "yaml":      "pip install PyYAML",
                "openpyxl":  "pip install openpyxl",
                "pandas":    "pip install pandas",
            }
            fix_cmd = fixes.get(module_name, f"pip install {module_name}")
            severity = "WARNING" if module_name in ("pyarrow", "pyspark", "pywin32") else "CRITICAL"
            return ErrorRecord(
                error_code="MISSING_DEPENDENCY",
                category="Dependencies",
                severity=severity,
                human_message=f"Required package '{module_name}' is not installed.",
                root_cause=(
                    f"The Python package '{module_name}' could not be imported. "
                    "It may not be installed in the active virtual environment."
                ),
                recommended_fix=(
                    f"1. Activate your virtual environment: .venv\\Scripts\\Activate.ps1\n"
                    f"2. Install the package: {fix_cmd}\n"
                    f"3. Verify: python -c \"import {module_name}\"\n"
                    f"4. Restart VS Code / your terminal after installation."
                ),
                module_hint="virtual environment / requirements",
                extra={"missing_package": module_name, "context": context_hint},
            )

        # ── Parquet-specific: large file without Spark ────────────────────────
        if "SparkRequired" in exc_type or (
            "spark" in msg.lower() and "large" in msg.lower()
        ):
            return ErrorRecord(
                error_code="SPARK_REQUIRED_LARGE_PARQUET",
                category="Parquet / Engine",
                severity="CRITICAL",
                human_message="Parquet file is too large to process without Spark.",
                root_cause=(
                    "The parquet file exceeds the spark_threshold_bytes setting but "
                    "PySpark is not installed. Falling back to pandas for large parquet "
                    "would cause an out-of-memory error."
                ),
                recommended_fix=(
                    "1. Install PySpark: pip install pyspark\n"
                    "2. OR reduce the dataset size (sample / filter partitions).\n"
                    "3. OR lower spark_threshold_bytes in config.yaml to force pandas.\n"
                    "   (only safe for files < available RAM)"
                ),
                module_hint="data_compare.capabilities.parquet.parquet_capability",
                extra={"context": context_hint},
            )

        # ── MemoryError ───────────────────────────────────────────────────────
        if isinstance(exc, MemoryError):
            return ErrorRecord(
                error_code="OUT_OF_MEMORY",
                category="Performance",
                severity="CRITICAL",
                human_message="Process ran out of memory during comparison.",
                root_cause=(
                    "The dataset is too large to fit in available RAM when using "
                    "the PandasEngine. This typically occurs with files > 2 GB."
                ),
                recommended_fix=(
                    "1. Enable Spark: set use_spark: true in config.yaml\n"
                    "2. OR install PySpark and let auto-selection handle it.\n"
                    "3. OR reduce pandas_chunk_size in config.yaml (e.g. 50000).\n"
                    "4. OR filter/partition the data before comparison."
                ),
                module_hint="data_compare.engines.pandas_engine",
                extra={"context": context_hint},
            )

        # ── KeyError ──────────────────────────────────────────────────────────
        if isinstance(exc, KeyError):
            key_name = msg.strip("'\"")
            return ErrorRecord(
                error_code="MISSING_CONTEXT_KEY",
                category="Internal / Context",
                severity="ERROR",
                human_message=f"Expected context key '{key_name}' was not found.",
                root_cause=(
                    "A capability attempted to read a key from the pipeline context "
                    "that was never set. This usually means a prerequisite capability "
                    "was disabled or failed silently."
                ),
                recommended_fix=(
                    f"1. Ensure all prerequisite capabilities are enabled for your workflow.\n"
                    f"2. Check the execution order: parquet → tolerance → comparison → "
                    f"duplicate → schema → data_quality → audit → alerts → plugins.\n"
                    f"3. Look for earlier capability failures in the log output."
                ),
                module_hint="data_compare.context.run_context",
                extra={"missing_key": key_name, "context": context_hint},
            )

        # ── Timeout ───────────────────────────────────────────────────────────
        if "timeout" in exc_type.lower() or "timeout" in msg.lower():
            return ErrorRecord(
                error_code="TIMEOUT",
                category="Performance",
                severity="ERROR",
                human_message="Operation timed out.",
                root_cause="A file read, network call, or Spark job exceeded the time limit.",
                recommended_fix=(
                    "1. Check network connectivity to remote file paths.\n"
                    "2. Increase timeouts in Spark configuration if using SparkEngine.\n"
                    "3. Verify the file path is local and accessible."
                ),
                module_hint="data_compare.engines / data_compare.loaders",
                extra={"context": context_hint},
            )

        # ── Generic fallback ──────────────────────────────────────────────────
        return ErrorRecord(
            error_code="UNEXPECTED_ERROR",
            category="Unknown",
            severity="ERROR",
            human_message=f"{exc_type}: {msg[:300]}",
            root_cause=(
                "An unexpected error occurred. This may be a bug in the framework "
                "or an unusual data condition."
            ),
            recommended_fix=(
                "1. Enable DEBUG logging: set log_level: DEBUG in config.yaml\n"
                "2. Check the full stack trace in the log file at reports/logs/data_compare.log\n"
                "3. Run the comparison with a smaller sample of data to isolate the issue.\n"
                "4. Check the data_compare.json.log for structured error context."
            ),
            module_hint="data_compare (unknown – check stack trace)",
            extra={"exc_type": exc_type, "context": context_hint},
        )

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _extract_path(msg: str) -> str:
        """Extract a file path from a FileNotFoundError message."""
        # Pattern: [Errno 2] No such file or directory: '/path/to/file'
        m = re.search(r"['\"]([^'\"]+)['\"]", msg)
        return m.group(1) if m else ""

    @staticmethod
    def _extract_module(msg: str) -> str:
        """Extract the module name from a ModuleNotFoundError message."""
        m = re.search(r"No module named ['\"]?([a-zA-Z0-9_.\-]+)['\"]?", msg)
        if m:
            return m.group(1).split(".")[0]
        # Fallback: last word
        return msg.split()[-1].strip("'\"")
