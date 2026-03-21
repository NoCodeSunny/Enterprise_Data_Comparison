# -*- coding: utf-8 -*-
"""
data_compare.utils.logger
─────────────────────────
Centralised logging with three output channels:

  1. Console (StreamHandler)      – human-readable format
  2. File (RotatingFileHandler)   – same format, max 10 MB, 5 backups
  3. JSON (FileHandler)           – machine-readable structured records

Every JSON record includes: timestamp, level, logger, message, run_id, job_name.

All modules obtain their logger via:
    from data_compare.utils.logger import get_logger
    logger = get_logger(__name__)
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional

_LOG_FMT   = "%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s"
_DATE_FMT  = "%Y-%m-%d %H:%M:%S"
_ROOT_NAME = "DataComparator"

_run_id:   str = "unknown"
_job_name: str = "unknown"
_configured      = False
_file_handler:   Optional[logging.Handler] = None
_json_handler:   Optional[logging.Handler] = None


def set_run_id(run_id: str) -> None:
    global _run_id; _run_id = run_id

def set_job_name(job_name: str) -> None:
    global _job_name; _job_name = job_name

def get_run_id() -> str:
    return _run_id

def get_job_name() -> str:
    return _job_name


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
            "run_id":    _run_id,
            "job_name":  _job_name,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(
    level: int = logging.DEBUG,
    log_dir: Optional[Path] = None,
    run_id: Optional[str] = None,
    job_name: Optional[str] = None,
    enable_json: bool = True,
    enable_file: bool = True,
) -> None:
    """Configure the DataComparator root logger. Safe to call multiple times."""
    global _configured, _file_handler, _json_handler, _run_id, _job_name
    if run_id:  _run_id   = run_id
    if job_name: _job_name = job_name

    root = logging.getLogger(_ROOT_NAME)
    root.setLevel(level)

    if not _configured:
        if not root.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setLevel(level)
            h.setFormatter(logging.Formatter(fmt=_LOG_FMT, datefmt=_DATE_FMT))
            root.addHandler(h)
        root.propagate = False
        _configured = True

    if log_dir and (enable_file or enable_json):
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        if enable_file and _file_handler is None:
            txt_path = log_dir / "edcp.log"
            h2 = logging.handlers.RotatingFileHandler(
                txt_path, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
            h2.setLevel(level)
            h2.setFormatter(logging.Formatter(fmt=_LOG_FMT, datefmt=_DATE_FMT))
            root.addHandler(h2)
            _file_handler = h2

        if enable_json and _json_handler is None:
            json_path = log_dir / "edcp.json.log"
            h3 = logging.FileHandler(json_path, encoding="utf-8")
            h3.setLevel(level)
            h3.setFormatter(_JsonFormatter())
            root.addHandler(h3)
            _json_handler = h3


def get_logger(name: Optional[str] = None) -> logging.Logger:
    configure_logging()
    if name is None or name == _ROOT_NAME:
        return logging.getLogger(_ROOT_NAME)
    if not name.startswith(_ROOT_NAME):
        child_name = f"{_ROOT_NAME}.{name}"
    else:
        child_name = name
    return logging.getLogger(child_name)


def banner(title: str, char: str = "─", width: int = 72) -> None:
    lg  = get_logger()
    pad = max(0, width - len(title) - 4)
    lg.info(char * (pad // 2) + "  " + title + "  " + char * (pad - pad // 2))


def progress(current: int, total: int, label: str = "") -> None:
    lg      = get_logger()
    pct     = int(100 * current / total) if total else 0
    bar_len = 30
    filled  = int(bar_len * current / total) if total else 0
    bar     = "█" * filled + "░" * (bar_len - filled)
    msg     = f"[{bar}] {pct:3d}%  {current}/{total}"
    if label: msg += f"  {label}"
    lg.debug(msg)
