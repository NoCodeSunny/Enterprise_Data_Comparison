# -*- coding: utf-8 -*-
"""data_compare.loaders – file ingestion (CSV / TXT / XLSX) and encoding."""

from data_compare.loaders.encoding import (
    ENCODINGS_TO_TRY,
    read_csv_robust,
    read_fwf_robust,
    open_text_robust,
    detect_encoding,
    detect_delimiter,
)
from data_compare.loaders.file_loader import (
    load_any_to_csv,
    convert_txt_to_csv,
)

__all__ = [
    "ENCODINGS_TO_TRY",
    "read_csv_robust",
    "read_fwf_robust",
    "open_text_robust",
    "detect_encoding",
    "detect_delimiter",
    "load_any_to_csv",
    "convert_txt_to_csv",
]
