# -*- coding: utf-8 -*-
"""
data_compare — compatibility wrapper for DataComparePro v3.0.0
================================================================
Canonical package is edcp.*  This module is a thin compat shim.

Quick start:
    from data_compare.orchestrator import run_comparison
    run_comparison("config/config.yaml")
"""

__version__ = "3.0.0"
__all__     = ["run_comparison"]


def __getattr__(name: str):
    """Lazy-load heavy symbols so `import data_compare` stays lightweight."""
    if name == "run_comparison":
        from data_compare.orchestrator import run_comparison
        return run_comparison
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
