# -*- coding: utf-8 -*-
"""
data_compare.engines.base_engine
──────────────────────────────────
Abstract base class for all comparison engines.

Every engine must implement the same interface so the orchestrator and
capabilities can swap Pandas ↔ Spark without changing any business logic.

The engine abstraction decouples:
  - HOW data is loaded and compared  (engine responsibility)
  - WHAT comparisons are run         (capability responsibility)
  - HOW results are reported         (reporting responsibility)

Engine contract
───────────────
All engines produce the same output format:
  - aligned DataFrames (prod_common, dev_common) with the same columns
  - index built from align_keys
  - dup_count_prod / dup_count_dev counts
  - only_in_prod / only_in_dev indices

This output is written into the shared context dict by ComparisonCapability
regardless of which engine performed the work.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class EngineResult:
    """
    Structured result returned by every engine after alignment and comparison.

    Attributes
    ----------
    prod_df : pd.DataFrame
        Full PROD DataFrame after loading, trimming, and ignore-field removal.
    dev_df : pd.DataFrame
        Full DEV DataFrame after loading, trimming, and ignore-field removal.
    prod_aligned : pd.DataFrame
        PROD DataFrame indexed by align_keys (with _SEQ_ if duplicates present).
    dev_aligned : pd.DataFrame
        DEV DataFrame indexed by align_keys.
    prod_common : pd.DataFrame
        Rows whose key appears in both files.
    dev_common : pd.DataFrame
        Matching rows from DEV for each key in prod_common.
    only_in_prod : pd.Index
        Index values present only in PROD.
    only_in_dev : pd.Index
        Index values present only in DEV.
    align_keys : list[str]
        Keys used to build the index (includes _SEQ_ when active).
    using_seq : bool
        True when _SEQ_ duplicate alignment was applied.
    dup_count_prod : int
        Number of duplicate key groups in PROD.
    dup_count_dev : int
        Number of duplicate key groups in DEV.
    existing_keys : list[str]
        Key columns found in both files.
    missing_keys : list[str]
        Key columns defined in config but absent from one or both files.
    ignore_set : set[str]
        Column names excluded from comparison.
    tol_for_pair : dict[str, int]
        Validated tolerance rules for this batch {column: decimal_places}.
    engine_name : str
        Identifier of the engine that produced this result.
    elapsed_load_s : float
        Time spent loading and aligning data (seconds).
    """

    __slots__ = (
        "prod_df", "dev_df",
        "prod_aligned", "dev_aligned",
        "prod_common", "dev_common",
        "only_in_prod", "only_in_dev",
        "align_keys", "using_seq",
        "dup_count_prod", "dup_count_dev",
        "existing_keys", "missing_keys",
        "ignore_set", "tol_for_pair",
        "engine_name", "elapsed_load_s",
    )

    def __init__(self) -> None:
        self.prod_df: Optional[pd.DataFrame]      = None
        self.dev_df:  Optional[pd.DataFrame]      = None
        self.prod_aligned: Optional[pd.DataFrame] = None
        self.dev_aligned:  Optional[pd.DataFrame] = None
        self.prod_common:  Optional[pd.DataFrame] = None
        self.dev_common:   Optional[pd.DataFrame] = None
        self.only_in_prod: Optional[pd.Index]     = None
        self.only_in_dev:  Optional[pd.Index]     = None
        self.align_keys:   List[str]              = []
        self.using_seq:    bool                   = False
        self.dup_count_prod: int                  = 0
        self.dup_count_dev:  int                  = 0
        self.existing_keys:  List[str]            = []
        self.missing_keys:   List[str]            = []
        self.ignore_set:     set                  = set()
        self.tol_for_pair:   Dict[str, int]       = {}
        self.engine_name:    str                  = "base"
        self.elapsed_load_s: float                = 0.0

    def apply_to_context(self, context: Dict[str, Any]) -> None:
        """
        Write all engine result fields into the shared context dict.
        Called by ComparisonCapability after the engine completes alignment.
        """
        context["prod_df"]        = self.prod_df
        context["dev_df"]         = self.dev_df
        context["prod_aligned"]   = self.prod_aligned
        context["dev_aligned"]    = self.dev_aligned
        context["prod_common"]    = self.prod_common
        context["dev_common"]     = self.dev_common
        context["only_in_prod"]   = self.only_in_prod
        context["only_in_dev"]    = self.only_in_dev
        context["align_keys"]     = self.align_keys
        context["using_seq"]      = self.using_seq
        context["dup_count_prod"] = self.dup_count_prod
        context["dup_count_dev"]  = self.dup_count_dev
        context["existing_keys"]  = self.existing_keys
        context["missing_keys"]   = self.missing_keys
        context["ignore_set"]     = self.ignore_set
        context["tol_for_pair"]   = self.tol_for_pair


class BaseEngine(ABC):
    """
    Abstract base for all comparison engines.

    Subclasses implement:
        load_and_align(...)  → EngineResult
        read_chunked(...)    → Iterator[pd.DataFrame]

    The engine is responsible for:
        1. Loading files into DataFrames
        2. Applying ignore_fields
        3. Normalising key columns
        4. Detecting duplicate key groups
        5. Building the aligned index (with _SEQ_ when needed)
        6. Splitting into common / only-in-prod / only-in-dev

    The engine is NOT responsible for:
        - Column-level value comparison (handled by ComparisonCapability)
        - Tolerance application in value comparison (handled by ComparisonCapability)
        - Writing reports (handled by reporting modules)
        - Sending email (handled by html_report)
    """

    #: Unique identifier used in logs and context["engine_name"]
    NAME: str = "base"

    @abstractmethod
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
        """
        Load PROD and DEV files, apply ignore_fields, normalise keys,
        detect duplicates, build aligned DataFrames, and return an EngineResult.

        Parameters
        ----------
        prod_path : Path
            Path to the PROD CSV (UTF-8 normalised by file_loader).
        dev_path : Path
            Path to the DEV CSV.
        keys : list[str]
            Key column names from config.
        ignore_fields : list[str]
            Column names to exclude from all comparisons.
        tol_map : dict
            Full tolerance map {(prod_name, dev_name, field): decimals}.
        prod_name : str
            Original PROD filename (used to look up tolerance rules).
        dev_name : str
            Original DEV filename.

        Returns
        -------
        EngineResult
            Fully populated result ready for ComparisonCapability to consume.
        """

    def read_chunked(
        self,
        path: Path,
        chunk_size: int = 100_000,
        **kwargs,
    ):
        """
        Yield DataFrame chunks from *path*.

        Default implementation uses pd.read_csv with chunksize.
        Subclasses may override for engine-specific chunked reading.

        Parameters
        ----------
        path : Path
            File path to read.
        chunk_size : int
            Number of rows per chunk. Default 100 000.
        **kwargs
            Additional arguments passed to the underlying reader.

        Yields
        ------
        pd.DataFrame
            One chunk at a time.
        """
        reader = pd.read_csv(path, chunksize=chunk_size, dtype=str, low_memory=False, **kwargs)
        for chunk in reader:
            yield chunk

    def __repr__(self) -> str:
        return f"<Engine: {self.NAME}>"

    def describe(self) -> str:
        """Return a human-readable description of this engine."""
        return f"{self.NAME} engine"
