# -*- coding: utf-8 -*-
"""data_compare.reporting – Excel, HTML, JSON report generation."""
from data_compare.reporting.excel_report import write_workbook_once
from data_compare.reporting.html_report import (
    write_final_html,
    build_html_summary_table,
    send_email,
)
from data_compare.reporting.json_audit import write_json_audit
from data_compare.reporting.report_builder import build_report

__all__ = [
    "write_workbook_once",
    "write_final_html",
    "build_html_summary_table",
    "send_email",
    "write_json_audit",
    "build_report",
]
