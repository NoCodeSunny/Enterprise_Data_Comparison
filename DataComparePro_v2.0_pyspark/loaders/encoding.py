# -*- coding: utf-8 -*-
"""
data_compare.loaders.encoding
──────────────────────────────
Encoding-robust file readers for CSV / fixed-width text files.

Encoding waterfall (F-06):
    utf-8-sig → utf-8 → cp1252 → latin1 → lossy latin1(replace)

All public functions accept a pathlib.Path and keyword arguments that are
forwarded to the underlying pandas reader.
"""

import csv
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# ── encoding waterfall ────────────────────────────────────────────────────────
ENCODINGS_TO_TRY: List[str] = ["utf-8-sig", "utf-8", "cp1252", "latin1"]

# ── candidate field delimiters for Sniffer + count heuristic ─────────────────
_CANDIDATE_DELIMITERS = "|,\t;~^"


# ── low-level multi-encoding reader ──────────────────────────────────────────

def _try_pandas_read(func, path: Path, *, encodings: List[str], **kwargs) -> pd.DataFrame:
    """
    Attempt *func(path, encoding=enc, **kwargs)* for each encoding in order.
    Falls back to lossy latin1 replacement if all attempts raise UnicodeDecodeError.
    """
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            df = func(path, encoding=enc, **kwargs)
            logger.debug(f"  Encoding '{enc}' succeeded for {path.name}")
            return df
        except UnicodeDecodeError as exc:
            logger.debug(f"  Encoding '{enc}' failed for {path.name}: {exc}")
            last_err = exc
        except Exception:
            raise

    # Lossy last-resort – prevents hard crash on exotic encodings
    try:
        logger.warning(
            f"  All encodings failed for {path.name}; "
            f"using latin1 with replacement chars (data may be slightly mangled)"
        )
        return func(path, encoding="latin1", encoding_errors="replace", **kwargs)
    except TypeError:
        # pandas version too old to support encoding_errors keyword
        pass

    if last_err:
        raise last_err
    raise UnicodeDecodeError("utf-8", b"", 0, 1, "All encodings exhausted")


# ── public readers ────────────────────────────────────────────────────────────

def read_csv_robust(path: Path, **kwargs) -> pd.DataFrame:
    """
    Read a CSV file trying each encoding in ENCODINGS_TO_TRY.
    All extra kwargs are passed directly to pd.read_csv.
    """
    return _try_pandas_read(pd.read_csv, path, encodings=ENCODINGS_TO_TRY, **kwargs)


def read_fwf_robust(path: Path, **kwargs) -> pd.DataFrame:
    """
    Read a fixed-width text file trying each encoding in ENCODINGS_TO_TRY.
    All extra kwargs are passed directly to pd.read_fwf.
    """
    return _try_pandas_read(pd.read_fwf, path, encodings=ENCODINGS_TO_TRY, **kwargs)


def open_text_robust(path: Path) -> Tuple[object, str]:
    """
    Open a text file for reading, trying each encoding in ENCODINGS_TO_TRY.

    Returns
    -------
    (file_object, encoding_used)
        Caller is responsible for closing the returned file object.
    """
    for enc in ENCODINGS_TO_TRY:
        try:
            f = open(path, "r", encoding=enc)
            f.read(4096)  # force decode to validate
            f.seek(0)
            return f, enc
        except (UnicodeDecodeError, OSError):
            continue

    # Last-resort lossy open
    f = open(path, "r", encoding="latin1", errors="replace")
    f.seek(0)
    return f, "latin1(replace)"


def detect_encoding(path: Path) -> str:
    """
    Determine the first encoding from ENCODINGS_TO_TRY that can decode *path*.
    Returns 'latin1(replace)' if none succeed cleanly.
    """
    for enc in ENCODINGS_TO_TRY:
        try:
            path.read_bytes().decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin1(replace)"


# ── delimiter detection (F-07) ────────────────────────────────────────────────

def detect_delimiter(path: Path) -> str:
    """
    Detect the field delimiter for a text/CSV file.

    Strategy:
    1. Read first 8 KB with open_text_robust.
    2. Use csv.Sniffer on the sample.
    3. Fall back to raw character-count heuristic on the first non-empty line.
    4. Default to comma if nothing is found.
    """
    sample = ""
    try:
        f, _enc = open_text_robust(path)
        with f:
            sample = f.read(8192)
    except Exception:
        pass

    if sample:
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=_CANDIDATE_DELIMITERS)
            logger.debug(f"  csv.Sniffer detected delimiter {repr(dialect.delimiter)}")
            return dialect.delimiter
        except csv.Error:
            logger.debug(
                "  csv.Sniffer could not determine delimiter; "
                "falling back to count heuristic"
            )

    # Count heuristic on the first non-empty line only
    first_line = ""
    for line in sample.splitlines():
        stripped = line.strip()
        if stripped:
            first_line = stripped
            break

    counts = {d: first_line.count(d) for d in _CANDIDATE_DELIMITERS}
    best = max(counts, key=counts.get)
    if counts[best] == 0:
        logger.debug("  No delimiter candidates found; defaulting to comma")
        return ","
    logger.debug(f"  Count heuristic chose delimiter {repr(best)}")
    return best
