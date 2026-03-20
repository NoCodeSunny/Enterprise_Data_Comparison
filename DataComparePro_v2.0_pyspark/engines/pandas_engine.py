# -*- coding: utf-8 -*-
"""
data_compare.engines.pandas_engine
─────────────────────────────────────
PandasEngine – default comparison engine.

Implements the full alignment pipeline using pandas:
  - Encoding-robust CSV loading (waterfall: utf-8-sig → utf-8 → cp1252 → latin1)
  - Ignore-field removal
  - Key normalisation (string-only, no numeric coercion)
  - Duplicate detection + _SEQ_ alignment
  - MultiIndex intersection for matched / missing / extra records

This engine supports chunked loading for large files via read_chunked().
Chunk-based comparison is used when context["config"]["use_chunked"] is True
or when the file exceeds the chunk_threshold_rows setting.

All business logic unchanged from the original compare_records() monolith.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import numpy as np
import pandas as pd

from data_compare.comparator.duplicate import (
    add_sequence_for_duplicates,
    detect_duplicates,
)
from data_compare.comparator.tolerance import resolve_pair_tolerance
from data_compare.engines.base_engine import BaseEngine, EngineResult
from data_compare.loaders.encoding import read_csv_robust
from data_compare.utils.helpers import normalize_keys, trim_df
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class PandasEngine(BaseEngine):
    """
    Standard in-memory comparison engine backed by pandas DataFrames.

    Parameters
    ----------
    chunk_size : int
        Number of rows per chunk when chunked reading is enabled.
        Default: 200 000.
    """

    NAME = "pandas"

    def __init__(self, chunk_size: int = 200_000) -> None:
        self.chunk_size = chunk_size

    # ── public interface ──────────────────────────────────────────────────────

    def load_and_align(
        self,
        prod_path: Path,
        dev_path: Path,
        keys: List[str],
        ignore_fields: List[str],
        tol_map: Dict,
        prod_name: str,
        dev_name: str,
    ) -> EngineResult:
        t0 = time.perf_counter()
        result = EngineResult()
        result.engine_name = self.NAME

        # ── load ───────────────────────────────────────────────────────────────
        logger.info(f"  [pandas] Loading PROD: {Path(prod_path).name}")
        prod_df = read_csv_robust(prod_path, dtype=str, low_memory=False)
        prod_df = trim_df(prod_df)
        logger.info(
            f"  [pandas] PROD: {len(prod_df):>10,} rows × {len(prod_df.columns)} cols"
        )

        logger.info(f"  [pandas] Loading DEV : {Path(dev_path).name}")
        dev_df = read_csv_robust(dev_path, dtype=str, low_memory=False)
        dev_df = trim_df(dev_df)
        logger.info(
            f"  [pandas] DEV : {len(dev_df):>10,} rows × {len(dev_df.columns)} cols"
        )

        # ── ignore fields ──────────────────────────────────────────────────────
        ignore_set = {s.strip() for s in ignore_fields if str(s).strip()}
        if ignore_set:
            prod_df.drop(
                columns=[c for c in prod_df.columns if c in ignore_set],
                errors="ignore", inplace=True,
            )
            dev_df.drop(
                columns=[c for c in dev_df.columns if c in ignore_set],
                errors="ignore", inplace=True,
            )
            logger.info(f"  [pandas] Ignored fields: {sorted(ignore_set)}")
        result.ignore_set = ignore_set

        # ── tolerance ──────────────────────────────────────────────────────────
        result.tol_for_pair = resolve_pair_tolerance(
            tol_map, prod_name, dev_name,
            list(prod_df.columns), list(dev_df.columns),
            ignore_set,
        )

        # ── keys ───────────────────────────────────────────────────────────────
        keys_clean = [k for k in keys if k]
        prod_df = normalize_keys(prod_df, keys_clean)
        dev_df  = normalize_keys(dev_df,  keys_clean)

        missing_keys  = [k for k in keys_clean
                         if k not in prod_df.columns or k not in dev_df.columns]
        existing_keys = [k for k in keys_clean
                         if k in prod_df.columns and k in dev_df.columns]
        if missing_keys:
            logger.warning(f"  [pandas] Key columns missing from both files: {missing_keys}")
        if existing_keys:
            logger.info(f"  [pandas] Keys in use: {existing_keys}")
        else:
            logger.warning("  [pandas] No valid keys – row-position alignment")

        result.prod_df       = prod_df
        result.dev_df        = dev_df
        result.existing_keys = existing_keys
        result.missing_keys  = missing_keys

        # ── duplicate detection + alignment ────────────────────────────────────
        self._build_alignment(result, prod_df, dev_df, existing_keys)

        result.elapsed_load_s = time.perf_counter() - t0
        logger.info(f"  [pandas] Alignment complete [{result.elapsed_load_s:.2f}s]")
        return result

    def read_chunked(
        self,
        path: Path,
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Yield DataFrame chunks from a CSV file.

        Uses encoding-waterfall opening so non-UTF-8 files are handled
        transparently before chunked reading begins.

        Parameters
        ----------
        path : Path
            Path to CSV file.
        chunk_size : int | None
            Rows per chunk.  Uses self.chunk_size if None.
        **kwargs
            Extra arguments for pd.read_csv.

        Yields
        ------
        pd.DataFrame
        """
        from data_compare.loaders.encoding import detect_encoding, ENCODINGS_TO_TRY

        cs = chunk_size or self.chunk_size
        enc = detect_encoding(path)
        reader = pd.read_csv(
            path,
            chunksize=cs,
            dtype=str,
            low_memory=False,
            encoding=enc,
            **kwargs,
        )
        chunk_num = 0
        for chunk in reader:
            chunk_num += 1
            logger.debug(
                f"  [pandas] Chunk {chunk_num}: "
                f"{len(chunk):,} rows from {path.name}"
            )
            yield trim_df(chunk)

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _build_alignment(
        result: EngineResult,
        prod_df: pd.DataFrame,
        dev_df: pd.DataFrame,
        existing_keys: List[str],
    ) -> None:
        """
        Detect duplicates, build _SEQ_ if needed, set index, split into
        common / only_in_prod / only_in_dev.  Mutates *result* in place.
        """
        if not existing_keys:
            # Row-order fallback
            prod_aligned = (
                prod_df.reset_index(drop=True)
                .reset_index()
                .rename(columns={"index": "_ROW_ID_"})
            )
            dev_aligned = (
                dev_df.reset_index(drop=True)
                .reset_index()
                .rename(columns={"index": "_ROW_ID_"})
            )
            align_keys = ["_ROW_ID_"]
            using_seq  = False
            dup_count_prod = dup_count_dev = 0
        else:
            dup_prod = detect_duplicates(prod_df, existing_keys)
            dup_dev  = detect_duplicates(dev_df,  existing_keys)
            dup_count_prod = len(dup_prod)
            dup_count_dev  = len(dup_dev)
            has_dups = dup_count_prod > 0 or dup_count_dev > 0

            if has_dups:
                using_seq  = True
                align_keys = existing_keys + ["_SEQ_"]
                logger.info(
                    f"  [pandas] Duplicates detected "
                    f"(prod={dup_count_prod}, dev={dup_count_dev}) "
                    "– applying _SEQ_ alignment"
                )
                prod_aligned = add_sequence_for_duplicates(prod_df, existing_keys)
                dev_aligned  = add_sequence_for_duplicates(dev_df,  existing_keys)
            else:
                using_seq    = False
                align_keys   = existing_keys[:]
                prod_aligned = prod_df.copy()
                dev_aligned  = dev_df.copy()

        prod_aligned = prod_aligned.set_index(align_keys, drop=False).sort_index()
        dev_aligned  = dev_aligned.set_index(align_keys,  drop=False).sort_index()

        common_idx   = prod_aligned.index.intersection(dev_aligned.index)
        only_in_prod = prod_aligned.index.difference(dev_aligned.index)
        only_in_dev  = dev_aligned.index.difference(prod_aligned.index)

        try:
            common_idx = common_idx.sort_values()
        except Exception:
            pass

        logger.info(
            f"  [pandas] Alignment: matched={len(common_idx):,}  "
            f"only-prod={len(only_in_prod):,}  "
            f"only-dev={len(only_in_dev):,}"
        )

        result.prod_aligned   = prod_aligned
        result.dev_aligned    = dev_aligned
        result.prod_common    = prod_aligned.loc[common_idx]
        result.dev_common     = dev_aligned.loc[common_idx]
        result.only_in_prod   = only_in_prod
        result.only_in_dev    = only_in_dev
        result.align_keys     = align_keys
        result.using_seq      = using_seq
        result.dup_count_prod = dup_count_prod
        result.dup_count_dev  = dup_count_dev
