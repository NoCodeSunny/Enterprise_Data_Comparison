# -*- coding: utf-8 -*-
"""
data_compare.loaders.file_loader
──────────────────────────────────
File ingestion layer.  Normalises CSV, TXT, and XLSX/XLS files into a single
UTF-8 CSV representation ready for comparison.

Supported features:
  [F-02]  CSV  – robust encoding read; non-UTF-8 files re-saved as UTF-8
  [F-03]  TXT  – pipe / comma / tab / semicolon auto-detect + fixed-width fallback
  [F-04]  XLSX/XLS – converted via openpyxl; SheetName config column honoured
  [F-05]  Cross-type comparisons fully supported
  [F-08]  U+00A0 normalised everywhere
  [F-09]  Leading/trailing whitespace stripped
  [F-55]  XLSX multi-sheet via sheet_name parameter
"""

import pandas as pd
from pathlib import Path

from data_compare.loaders.encoding import (
    ENCODINGS_TO_TRY,
    read_csv_robust,
    read_fwf_robust,
    detect_delimiter,
    detect_encoding,
)
from data_compare.utils.helpers import ensure_dir, trim_df
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


# ── TXT helpers ───────────────────────────────────────────────────────────────

def _read_txt_file(path: Path) -> pd.DataFrame:
    """
    Read a TXT file attempting strategies in order:
      1. Delimiter-detected CSV read (pipe / tab / comma / semicolon)
      2. Fixed-width read via pd.read_fwf
      3. Whitespace-split fallback

    Returns a trimmed DataFrame.
    """
    delim = detect_delimiter(path)
    logger.debug(f"  TXT read: delimiter={repr(delim)}, file={path.name}")

    # Strategy 1 – delimited
    try:
        df = read_csv_robust(
            path,
            dtype=str,
            delimiter=delim,
            low_memory=False,
            skipinitialspace=True,
        )
        if df.shape[1] > 1:
            df.columns = [str(c).strip() for c in df.columns]
            logger.debug(
                f"  TXT delimited read OK: {df.shape[0]} rows × {df.shape[1]} cols"
            )
            return trim_df(df)
        logger.debug("  TXT delimited read yielded 1 column – trying fixed-width")
    except Exception as exc:
        logger.debug(f"  TXT delimited read failed ({exc}) – trying fixed-width")

    # Strategy 2 – fixed-width
    try:
        df = read_fwf_robust(path, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]
        logger.debug(
            f"  Fixed-width read OK: {df.shape[0]} rows × {df.shape[1]} cols"
        )
        return trim_df(df)
    except Exception as exc:
        logger.warning(
            f"  Fixed-width read also failed ({exc}); "
            f"falling back to whitespace split"
        )

    # Strategy 3 – whitespace split
    df = read_csv_robust(path, dtype=str, sep=r"\s+", engine="python")
    df.columns = [str(c).strip() for c in df.columns]
    return trim_df(df)


def convert_txt_to_csv(src: Path, out_dir: Path) -> Path:
    """
    Convert *src* (TXT file) to a UTF-8 CSV in *out_dir*.
    Returns the path of the newly created CSV.
    """
    ensure_dir(out_dir)
    out_csv = out_dir / (src.stem + ".csv")
    logger.info(f"  ↳ Converting TXT → CSV: {src.name} → {out_csv.name}")
    df = _read_txt_file(src)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    logger.info(f"  ✔ TXT conversion done: {len(df):,} rows, {len(df.columns)} cols")
    return out_csv


# ── main ingestion entry point ────────────────────────────────────────────────

def load_any_to_csv(
    path: Path,
    converted_root: Path,
    sheet_name=0,
) -> Path:
    """
    Normalise any supported file type to a UTF-8 CSV ready for comparison.

    Parameters
    ----------
    path : Path
        Source file (.csv, .txt, .xlsx, .xls).
    converted_root : Path
        Directory where converted / normalised copies are saved.
    sheet_name : int or str
        Sheet index or name; only used for XLSX/XLS files.  Defaults to 0.

    Returns
    -------
    Path
        Path to the CSV file to use for comparison.
        - CSV: original file (if UTF-8) or UTF-8 normalised copy
        - TXT: converted CSV in converted_root
        - XLSX/XLS: converted CSV in converted_root

    Raises
    ------
    ValueError
        For unsupported file extensions.
    """
    ensure_dir(converted_root)
    ext = path.suffix.lower()
    logger.info(f"  Loading file: {path.name}  (type={ext!r})")

    # ── CSV ───────────────────────────────────────────────────────────────────
    if ext == ".csv":
        logger.debug("  CSV: reading with robust encoding")
        df = read_csv_robust(path, dtype=str, low_memory=False)
        df = trim_df(df)

        out = converted_root / path.name
        df.to_csv(out, index=False, encoding="utf-8-sig")

        # Detect if the original uses a non-UTF-8 encoding; if so use the
        # normalised UTF-8 copy for comparison so downstream reads never fail.
        original_enc = detect_encoding(path)
        if original_enc not in ("utf-8-sig", "utf-8"):
            logger.info(
                f"  ✔ CSV loaded ({original_enc}): "
                f"{len(df):,} rows × {len(df.columns)} cols  "
                f"(normalised UTF-8 copy used for comparison)"
            )
            return out

        logger.info(
            f"  ✔ CSV loaded: {len(df):,} rows × {len(df.columns)} cols  "
            f"(original used for comparison)"
        )
        return path

    # ── TXT ───────────────────────────────────────────────────────────────────
    if ext == ".txt":
        return convert_txt_to_csv(path, converted_root)

    # ── XLSX / XLS ────────────────────────────────────────────────────────────
    if ext in (".xlsx", ".xls"):
        out = converted_root / (path.stem + ".csv")
        engine = "openpyxl"
        logger.debug(f"  XLSX: reading sheet={sheet_name!r}")
        try:
            df = pd.read_excel(
                path, dtype=str, engine=engine, sheet_name=sheet_name
            )
        except Exception as exc:
            logger.warning(
                f"  XLSX read with sheet={sheet_name!r} failed ({exc}); "
                f"retrying with sheet 0"
            )
            df = pd.read_excel(path, dtype=str, engine=engine, sheet_name=0)
        df = trim_df(df)
        df.to_csv(out, index=False, encoding="utf-8-sig")
        logger.info(
            f"  ✔ XLSX converted: {len(df):,} rows × {len(df.columns)} cols "
            f"→ {out.name}"
        )
        return out

    raise ValueError(
        f"Unsupported file extension: '{ext}'  "
        f"(supported: .csv, .txt, .xlsx, .xls)"
    )
