# -*- coding: utf-8 -*-
"""
data_compare.reporting.html_report
────────────────────────────────────
HTML summary report generation and email dispatch.

Features:
  [F-42]  Rich HTML summary table – one row per comparison batch
  [F-43]  Colour coding: yellow=failures/schema, orange=duplicates, red=missing/extra
  [F-44]  Schema difference mini-table rendered inline (up to 8 items)
  [F-45]  Run timestamp + total elapsed time in HTML header
  [F-46]  Legacy build_html_summary_table() retained for backward compatibility
  [F-47]  HTML written to Final_Comparison_Summary.html in report root
  [F-48]  Outlook COM send (win32com) with HTML body + XLSX attachments
  [F-49]  smtplib fallback when pywin32 not installed (cross-platform)
  [F-50]  Graceful no-op with clear warning when neither is available
"""

import html as _html
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from data_compare.comparator.schema import (
    read_schema_items_from_report,
    render_schema_items_html,
)
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)

# ── optional Outlook COM (Windows only – isolated here, not in core) ──────────
try:
    import win32com.client as _win32
    HAS_WIN32 = True
except Exception:
    HAS_WIN32 = False


# ══════════════════════════════════════════════════════════════════════════════
#  LEGACY SIMPLE TABLE  (F-46)
# ══════════════════════════════════════════════════════════════════════════════

def build_html_summary_table(summaries: List[Dict]) -> str:
    """
    Build a simple HTML summary table.

    This legacy function is retained for backward compatibility.
    The richer write_final_html() is used by default in the orchestrator.
    """
    style = (
        "<style>"
        "body{font-family:Segoe UI,Arial,sans-serif;font-size:13px}"
        "table{border-collapse:collapse;margin:8px 0}"
        "th,td{border:1px solid #ccc;padding:6px 8px;white-space:nowrap}"
        "th{background:#f2f6ff;font-weight:600}"
        ".y{background:#fff3b0}.o{background:#ffe0b2}.r{background:#ffc7ce}"
        ".ok{background:#e6ffe6}.mono{font-family:Consolas,monospace}"
        ".cap{color:#555;margin:4px 0 10px}"
        "</style>"
    )
    cols_th = [
        "#", "Result Name", "Prod File", "Dev File",
        "Total Prod", "Total Dev",
        "Matched Passed", "Matched Failed", "Missing Dev", "Extra Dev",
        "Dup Prod", "Dup Dev", "Schema Missing", "Schema Extra", "Keys",
    ]
    header_html = (
        "<table><thead><tr>"
        + "".join(f"<th>{c}</th>" for c in cols_th)
        + "</tr></thead><tbody>"
    )
    rows = []
    for i, s in enumerate(summaries, 1):
        mf = s.get("MatchedFailed", 0)
        md = s.get("MissingDev",    0)
        ed = s.get("ExtraDev",      0)
        dp = s.get("DuplicateProd", 0)
        dd = s.get("DuplicateDev",  0)
        rows.append(
            f"<tr>"
            f"<td>{i}</td>"
            f"<td class='mono'>{s.get('ResultName','')}</td>"
            f"<td class='mono'>{s.get('ProdFile','')}</td>"
            f"<td class='mono'>{s.get('DevFile','')}</td>"
            f"<td>{s.get('TotalProd',0):,}</td>"
            f"<td>{s.get('TotalDev',0):,}</td>"
            f"<td>{s.get('MatchedPassed',0):,}</td>"
            f"<td class='{'y' if mf else 'ok'}'>{mf:,}</td>"
            f"<td class='{'r' if md else 'ok'}'>{md:,}</td>"
            f"<td class='{'r' if ed else 'ok'}'>{ed:,}</td>"
            f"<td class='{'o' if dp else 'ok'}'>{dp:,}</td>"
            f"<td class='{'o' if dd else 'ok'}'>{dd:,}</td>"
            f"<td>{s.get('SchemaMissingCount',0)}</td>"
            f"<td>{s.get('SchemaExtraCount',0)}</td>"
            f"<td class='mono'>"
            f"{', '.join(s.get('UsedKeys',[])) or '-'}</td>"
            f"</tr>"
        )
    return (
        style
        + "<div class='cap'>Automated comparison summary</div>"
        + header_html
        + "".join(rows)
        + "</tbody></table>"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  RICH HTML SUMMARY  (F-42 … F-47)
# ══════════════════════════════════════════════════════════════════════════════

def write_final_html(
    summaries: List[Dict],
    report_root: Path,
    total_elapsed_secs: float,
) -> Path:
    """
    Write a rich HTML summary file (Final_Comparison_Summary.html).

    One table row per comparison batch.  Colour coding:
      yellow  = failed validations / schema mismatch
      orange  = duplicate records
      red     = missing / extra records

    Includes run timestamp, total elapsed time, and inline schema diff tables.

    Parameters
    ----------
    summaries : list[dict]
        Collected summary dicts from compare_records().
    report_root : Path
        Directory where the HTML file is written.
    total_elapsed_secs : float
        Wall-clock seconds for the full run.

    Returns
    -------
    Path
        Path to the written HTML file.
    """
    YELLOW = "#FFF2CC"
    ORANGE = "#FFE6CC"
    RED    = "#FCE4E4"

    run_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mins   = int(math.ceil(total_elapsed_secs / 60))

    def _th(text: str, width=None, align: str = "left") -> str:
        s = (
            f"padding:8px 10px;"
            f"border-bottom:1px solid #e0e0e0;"
            f"background:#EEF2F7;"
            f"font-weight:600;"
            f"color:#1f2937;"
            f"text-align:{align};"
            f"vertical-align:top;"
            f"white-space:nowrap;"
        )
        if width:
            s += f"width:{width}px;"
        return f"<th style='{s}'>{_html.escape(text)}</th>"

    def _td(
        text: str,
        align: str = "right",
        bg=None,
        bold: bool = False,
        wrap: bool = False,
    ) -> str:
        s = (
            f"padding:7px 10px;"
            f"border-bottom:1px solid #ebebeb;"
            f"text-align:{align};"
            f"vertical-align:top;"
        )
        if not wrap:
            s += "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
        if bg:
            s += f"background:{bg};"
        if bold:
            s += "font-weight:600;"
        return f"<td style='{s}'>{text}</td>"

    head = (
        "<html><head><meta charset='utf-8'>"
        "<style>*{box-sizing:border-box}"
        "body{font-family:Segoe UI,Roboto,Arial,sans-serif;"
        "font-size:13px;color:#222;margin:0;padding:18px}"
        "</style></head><body>"
        "<h1 style='font-size:19px;margin:0 0 3px 0'>"
        "Data Comparison Summary</h1>"
        f"<div style='font-size:12px;color:#555;margin-bottom:4px'>"
        f"Run: {run_dt} &nbsp;·&nbsp; "
        f"Batches: {len(summaries)} &nbsp;·&nbsp; "
        f"Total time: {mins} min ({total_elapsed_secs:.1f}s)"
        f"</div>"
        f"<div style='margin:8px 0 14px;font-size:12px'>"
        f"<span style='padding:2px 9px;border-radius:9px;"
        f"background:{YELLOW};border:1px solid #e0c845;margin-right:6px'>"
        f"Failed / Schema mismatch</span>"
        f"<span style='padding:2px 9px;border-radius:9px;"
        f"background:{ORANGE};border:1px solid #f4a85a;margin-right:6px'>"
        f"Duplicates</span>"
        f"<span style='padding:2px 9px;border-radius:9px;"
        f"background:{RED};border:1px solid #e09090'>"
        f"Missing / Extra</span>"
        f"</div>"
        "<div style='overflow-x:auto'>"
        "<table role='presentation' style='border-collapse:separate;"
        "border-spacing:0;width:100%;table-layout:fixed;min-width:1400px'>"
        "<thead><tr>"
        + _th("#",                36,  "right")
        + _th("Result Name",      220)
        + _th("Prod file",        240)
        + _th("Dev file",         240)
        + _th("Total Prod",       90,  "right")
        + _th("Total Dev",        90,  "right")
        + _th("Schema",           80,  "center")
        + _th("Schema differences", 300)
        + _th("Matched Passed",   115, "right")
        + _th("Matched Failed",   115, "right")
        + _th("Extra Dev",        95,  "right")
        + _th("Missing Dev",      100, "right")
        + _th("Dup Prod",         85,  "right")
        + _th("Dup Dev",          85,  "right")
        + _th("Elapsed",          75,  "right")
        + _th("Keys Used",        260)
        + "</tr></thead><tbody>"
    )

    row_htmls = []
    for i, s in enumerate(summaries, 1):
        report_path  = Path(s.get("ReportPath", "")) if s.get("ReportPath") else None
        schema_items = (
            read_schema_items_from_report(report_path)
            if report_path and report_path.exists()
            else []
        )
        schema_ok = len(schema_items) == 0

        fail_i  = int(s.get("MatchedFailed", 0))
        extra_i = int(s.get("ExtraDev",      0))
        miss_i  = int(s.get("MissingDev",    0))
        dup_p_i = int(s.get("DuplicateProd", 0))
        dup_d_i = int(s.get("DuplicateDev",  0))
        elapsed = s.get("ElapsedSeconds", 0)

        error_msg  = s.get("Error", "")
        row_style  = " style='background:#fff0f0'" if error_msg else ""

        cells = (
            _td(str(i), "right")
            + _td(_html.escape(str(s.get("ResultName", ""))), "left", wrap=True)
            + _td(_html.escape(os.path.basename(str(s.get("ProdFile", "")))), "left", wrap=True)
            + _td(_html.escape(os.path.basename(str(s.get("DevFile",  "")))), "left", wrap=True)
            + _td(f"{s.get('TotalProd',0):,}")
            + _td(f"{s.get('TotalDev',0):,}")
            + _td(
                "Yes" if schema_ok else "No",
                "center",
                YELLOW if not schema_ok else None,
                bold=not schema_ok,
            )
            + _td(render_schema_items_html(schema_items), "left", wrap=True)
            + _td(f"{s.get('MatchedPassed',0):,}")
            + _td(f"{fail_i:,}",  bg=YELLOW if fail_i  else None)
            + _td(f"{extra_i:,}", bg=RED    if extra_i else None)
            + _td(f"{miss_i:,}",  bg=RED    if miss_i  else None)
            + _td(f"{dup_p_i:,}", bg=ORANGE if dup_p_i else None)
            + _td(f"{dup_d_i:,}", bg=ORANGE if dup_d_i else None)
            + _td(f"{elapsed:.1f}s", "right")
            + _td(
                _html.escape(", ".join(s.get("UsedKeys", [])) or "—"),
                "left",
                wrap=True,
            )
        )
        row_htmls.append(f"<tr{row_style}>{cells}</tr>")

    tail = "</tbody></table></div></body></html>"
    out  = Path(report_root) / "Final_Comparison_Summary.html"
    out.write_text(head + "".join(row_htmls) + tail, encoding="utf-8")
    logger.info(f"  HTML summary written: {out.name}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  EMAIL  (F-48 … F-50)
# ══════════════════════════════════════════════════════════════════════════════

def send_email(
    email_to: str,
    subject: str,
    html_body_path: Path,
    attachments: List[Path],
    smtp_host: str = "",
    smtp_port: int = 587,
    smtp_from: str = "",
    smtp_user: str = "",
    smtp_pass: str = "",
) -> None:
    """
    Send the HTML summary email.

    Dispatch order:
      1. Outlook COM (win32com) if available
      2. smtplib SMTP if smtp_host is configured
      3. Log a warning and skip if neither is available

    win32com dependency is fully isolated here so the rest of the package
    can run in PySpark (Linux) without any import errors.
    """
    if HAS_WIN32:
        _send_via_outlook(email_to, subject, html_body_path, attachments)
    elif smtp_host and smtp_host not in ("", "smtp.yourdomain.com"):
        _send_via_smtp(
            email_to, subject, html_body_path, attachments,
            smtp_host, smtp_port, smtp_from, smtp_user, smtp_pass,
        )
    else:
        logger.warning(
            "⚠ Email skipped: pywin32 not installed and SMTP_HOST not configured. "
            "Set SMTP_HOST/SMTP_PORT/SMTP_FROM in config to enable SMTP fallback."
        )


def _send_via_outlook(
    email_to: str,
    subject: str,
    html_body_path: Path,
    attachments: List[Path],
) -> None:
    """Send via Outlook COM (Windows only)."""
    try:
        outlook = _win32.Dispatch("outlook.application")
        mail    = outlook.CreateItem(0)
        mail.To            = email_to
        mail.Subject       = subject
        mail.BodyFormat    = 2  # olFormatHTML
        mail.HTMLBody      = html_body_path.read_text(encoding="utf-8")
        for p in attachments:
            if Path(p).exists():
                mail.Attachments.Add(str(p))
        mail.Send()
        logger.info(f"📧 Email sent via Outlook to {email_to}")
    except Exception as exc:
        logger.error(f"Outlook send failed: {exc}")


def _send_via_smtp(
    email_to: str,
    subject: str,
    html_body_path: Path,
    attachments: List[Path],
    smtp_host: str,
    smtp_port: int,
    smtp_from: str,
    smtp_user: str,
    smtp_pass: str,
) -> None:
    """Send via smtplib SMTP (cross-platform fallback)."""
    import smtplib
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg              = MIMEMultipart("mixed")
    msg["From"]      = smtp_from
    msg["To"]        = email_to
    msg["Subject"]   = subject
    html_body        = html_body_path.read_text(encoding="utf-8")
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    for p in attachments:
        p = Path(p)
        if p.exists():
            part = MIMEApplication(p.read_bytes(), Name=p.name)
            part["Content-Disposition"] = f'attachment; filename="{p.name}"'
            msg.attach(part)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            if smtp_user:
                server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_from, [email_to], msg.as_string())
        logger.info(f"📧 Email sent via SMTP to {email_to}")
    except Exception as exc:
        logger.error(f"SMTP send failed: {exc}")
