# -*- coding: utf-8 -*-
"""
edcp.batch.recovery_manager
────────────────────────────
RecoveryManager — handles retry logic and error classification.

Backoff schedule (F-RETRY-002):
  attempt 1 → 5s
  attempt 2 → 25s
  attempt 3 → 125s (5^n series)

Non-retryable errors (F-RETRY-003):
  Schema errors, validation errors, key-not-found errors.
  These will not improve on retry.
"""
from __future__ import annotations

import re
from typing import List

# Patterns that indicate a non-retryable error
_NON_RETRYABLE = [
    r"key column.* not found",
    r"schema.*mismatch",
    r"ConfigError",
    r"config.*invalid",
    r"SCHEMA_MISMATCH",
    r"MISSING_CONTEXT_KEY",
    r"validation failed",
    r"File not found",     # file won't appear on retry
    r"PermissionError",    # permissions won't change on retry
]


class RecoveryManager:
    """
    Determines retry behaviour and calculates backoff delays.
    """

    BACKOFF_BASE = 5  # seconds — series: 5, 25, 125

    def backoff_seconds(self, attempt: int) -> float:
        """
        Return seconds to wait before attempt N (1-indexed).
        Series: 5^1=5, 5^2=25, 5^3=125
        """
        return self.BACKOFF_BASE ** attempt

    def is_retryable(self, error_message: str) -> bool:
        """
        Return True if the error is likely to resolve on retry.
        Network timeouts, transient OOM, and engine errors are retryable.
        Schema, validation, and config errors are NOT retryable.
        """
        if error_message is None:
            return True   # default: retry unknown errors
        msg_lower = str(error_message).lower()
        for pattern in _NON_RETRYABLE:
            if re.search(pattern, str(error_message or ""), re.IGNORECASE):
                return False
        # Retryable signals
        retryable_signals = [
            "timeout", "timed out", "connection", "memoryerror",
            "sparkexception", "java", "oserror", "ioerror",
        ]
        for signal in retryable_signals:
            if signal in msg_lower:
                return True
        # Default: retry for unknown errors
        return True

    def classify_error(self, error_message: str) -> str:
        """
        Return a category label for the error.
        Used for UI failure view and audit logs.
        """
        m = error_message.lower()
        if "file not found" in m or "no such file" in m:
            return "FILE_NOT_FOUND"
        if "permission" in m:
            return "PERMISSION_DENIED"
        if "schema" in m or "column" in m:
            return "SCHEMA_ERROR"
        if "config" in m:
            return "CONFIG_ERROR"
        if "memory" in m or "oom" in m:
            return "OUT_OF_MEMORY"
        if "timeout" in m or "timed out" in m:
            return "TIMEOUT"
        if "spark" in m or "java" in m:
            return "ENGINE_ERROR"
        if "encoding" in m or "unicode" in m:
            return "ENCODING_ERROR"
        return "UNKNOWN_ERROR"
