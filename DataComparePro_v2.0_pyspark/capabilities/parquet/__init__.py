# -*- coding: utf-8 -*-
"""data_compare.capabilities.parquet – Parquet comparison capability."""
from data_compare.capabilities.parquet.parquet_capability import ParquetCapability
from data_compare.capabilities.parquet.parquet_validator import ParquetValidator, SchemaValidationResult
from data_compare.capabilities.parquet.parquet_optimizer import ParquetOptimizer, OptimizationPlan

__all__ = [
    "ParquetCapability",
    "ParquetValidator",
    "SchemaValidationResult",
    "ParquetOptimizer",
    "OptimizationPlan",
]
