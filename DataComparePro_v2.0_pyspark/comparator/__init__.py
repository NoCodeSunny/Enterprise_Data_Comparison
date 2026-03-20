# -*- coding: utf-8 -*-
"""data_compare.comparator – core comparison engine, tolerance, duplicates, schema."""

from data_compare.comparator.core import compare_records
from data_compare.comparator.tolerance import (
    normalize_numeric_with_tolerance,
    build_tolerance_map,
    resolve_pair_tolerance,
    ToleranceMap,
)
from data_compare.comparator.duplicate import (
    detect_duplicates,
    add_sequence_for_duplicates,
    build_duplicates_summary,
)
from data_compare.comparator.schema import (
    build_schema_diff_dataframe,
    annotate_schema_counts,
    read_schema_items_from_report,
    render_schema_items_html,
)

__all__ = [
    "compare_records",
    "normalize_numeric_with_tolerance",
    "build_tolerance_map",
    "resolve_pair_tolerance",
    "ToleranceMap",
    "detect_duplicates",
    "add_sequence_for_duplicates",
    "build_duplicates_summary",
    "build_schema_diff_dataframe",
    "annotate_schema_counts",
    "read_schema_items_from_report",
    "render_schema_items_html",
]
