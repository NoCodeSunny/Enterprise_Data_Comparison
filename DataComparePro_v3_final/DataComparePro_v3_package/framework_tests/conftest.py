# -*- coding: utf-8 -*-
"""
tests/conftest.py
──────────────────
Shared pytest fixtures and test data factories.

Provides:
  - tmp_path  (built-in pytest fixture)
  - prod_csv_path / dev_csv_path fixtures
  - parquet_paths fixture (generates parquet or CSV proxy)
  - standard test data constants shared across all test files
"""
from __future__ import annotations
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest
from pathlib import Path

# ── test data constants ────────────────────────────────────────────────────────
PROD_ROWS = [
    ("T001","EQ","100.001","500","Alice","USD","X"),
    ("T002","FI","50.500", "200","Bob",  "EUR","Y"),
    ("T003","EQ","75.000", "300","Carol","GBP","X"),
    ("T003","EQ","75.000", "300","Carol","GBP","X"),  # duplicate
    ("T004","FI","200.000","100","Dave", "USD","Y"),
    ("T005","EQ","999.000"," 50","Eve",  "EUR","X"),  # missing in dev
]
PROD_COLS = ["TradeID","Portfolio","Price","Quantity","Trader","Currency","IgnoreFlag"]

DEV_ROWS = [
    ("T001","EQ","100.003","500","Alice","USD","AAA"),  # Price within 2dp tol
    ("T002","FI","50.500", "999","Bob",  "EUR","BBB"),  # Quantity differs
    ("T003","EQ","75.000", "300","Carol","GBP","AA"),   # 1 row (no dup in dev)
    ("T004","FI","200.000","100","Dave", "USD","AA"),   # exact match
    ("T006","EQ","123.000","777","Frank","JPY","BB"),   # extra in dev
]
DEV_COLS = ["TradeID","Portfolio","Price","Quantity","TraderName","Currency","Rating"]

KEYS         = ["TradeID","Portfolio"]
IGNORE_FIELDS= ["IgnoreFlag"]
TOL_MAP_CSV  = {("prod.csv","dev.csv","Price"): 2}


@pytest.fixture
def prod_df():
    return pd.DataFrame(PROD_ROWS, columns=PROD_COLS)

@pytest.fixture
def dev_df():
    return pd.DataFrame(DEV_ROWS, columns=DEV_COLS)

@pytest.fixture
def prod_csv_path(tmp_path, prod_df):
    p = tmp_path / "prod.csv"
    prod_df.to_csv(p, index=False)
    return p

@pytest.fixture
def dev_csv_path(tmp_path, dev_df):
    p = tmp_path / "dev.csv"
    dev_df.to_csv(p, index=False)
    return p

@pytest.fixture
def conv_dir(tmp_path):
    d = tmp_path / "conv"; d.mkdir(); return d

def write_parquet_or_csv(df: pd.DataFrame, path: Path) -> Path:
    """Write DataFrame as parquet if pyarrow available, else CSV fallback."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(path))
        return path
    except ImportError:
        # Fall back to CSV with .parquet extension
        csv_path = path.with_suffix('.csv')
        df.to_csv(csv_path, index=False)
        return csv_path

def make_parquet(df: pd.DataFrame, path: Path) -> Path:
    """Create a parquet file (with pyarrow) or CSV fallback."""
    return write_parquet_or_csv(df, path)

@pytest.fixture
def parquet_paths(tmp_path, prod_df, dev_df):
    """Return (prod_parquet_path, dev_parquet_path) — real .parquet or .csv fallback."""
    prod_p = make_parquet(prod_df, tmp_path / "prod.parquet")
    dev_p  = make_parquet(dev_df,  tmp_path / "dev.parquet")
    return prod_p, dev_p
