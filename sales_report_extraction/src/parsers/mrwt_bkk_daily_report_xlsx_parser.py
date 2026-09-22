"""
Moulin Rouge! The Musical — Bangkok Daily Report XLSX Parser
=============================================================
Source format: MRWT Bangkok "Daily Report" XLSX (single sheet, one row per
performance round, plus a trailing SUMMARY row with report-stated totals).

Source columns (row 1 header):
  Performance | Round | Com. Ticket | Pro. Ticket | Sold Ticket |
  Total Ticket | Total Amount

'Round' is a real datetime cell for a normal performance, but special
performances (e.g. Gala Night) come through as a string with the date/time
embedded plus a trailing label, e.g. "17 Aug 2027 19:30 GALA NIGHT".

'Pro. Ticket' (promotional tickets) isn't part of the Doc ID mapping and is
deliberately left out of the output.

Doc ID output mapping:
  Performance Date / Time  <- Round
  Gross Potential          <- blank (not supplied)
  Capacity                 <- blank (not supplied)
  Gross                    <- Total Amount
  Tickets Sold             <- Sold Ticket
  Comps                    <- Com. Ticket
  Reserved Gross           <- blank (not supplied)
  Reserved Tickets         <- 0 (not supplied; hard-coded rather than blank)
"""

import os
import re
from datetime import datetime

import pandas as pd
from dateutil import parser as date_parser
from prefect import task, get_run_logger

from src.models import ValidationResult

# Source workbook columns, as they appear in row 1
EXPECTED_RAW_COLUMNS = {
    "Performance", "Round", "Com. Ticket", "Pro. Ticket",
    "Sold Ticket", "Total Ticket", "Total Amount"
}

# Doc ID output contract
EXPECTED_SCHEMA = {
    "Performance Date / Time", "Gross Potential", "Capacity", "Gross",
    "Tickets Sold", "Comps", "Reserved Gross", "Reserved Tickets"
}

# Matches the leading "17 Aug 2027 19:30" in a labelled round like
# "17 Aug 2027 19:30 GALA NIGHT" — trailing labels are stripped off before
# parsing so a word like a show name can never be misread as a timezone
# (dateutil's fuzzy mode will happily treat 'EST' as a real one).
_DATE_PREFIX = re.compile(r"^\s*\d{1,2}\s+\w+\s+\d{4}\s+\d{1,2}:\d{2}")


def _format_round(value) -> str:
    """
    Normalise 'Round' into 'YYYY-MM-DD HH:MM'. Handles both a real datetime
    cell and a labelled string (e.g. Gala Night). Falls back to the raw
    string if it genuinely can't be parsed, rather than dropping the row.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, str):
        match = _DATE_PREFIX.match(value)
        date_text = match.group(0) if match else value
        try:
            dt = date_parser.parse(date_text)
            return dt.strftime("%Y-%m-%d %H:%M")
        except (ValueError, OverflowError):
            return value.strip()
    return str(value)


@task(name="Parse MRWT Bangkok Daily Report XLSX")
def mrwt_bkk_daily_report_xlsx_parser(file_path):
    logger = get_run_logger()
    filename = os.path.basename(file_path)
    logger.info(f"📂 Opening XLSX file: {filename}")

    df = pd.read_excel(file_path, engine='openpyxl')

    # --- STRICT SOURCE SCHEMA CHECK ---
    actual_raw_columns = set(df.columns)
    if actual_raw_columns != EXPECTED_RAW_COLUMNS:
        error_msg = f"Source schema mismatch! Expected exact columns: {EXPECTED_RAW_COLUMNS}, but got: {actual_raw_columns}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    # The SUMMARY row plays the same role a PDF's stated totals row does —
    # split it off before building performance records. The vendor's export
    # isn't consistent about which column carries the 'SUMMARY' label (seen
    # in both 'Round' and 'Performance' across different files), so check both.
    summary_mask = (
        df['Round'].astype(str).str.strip().str.upper().eq('SUMMARY')
        | df['Performance'].astype(str).str.strip().str.upper().eq('SUMMARY')
    )
    if not summary_mask.any():
        error_msg = f"No 'SUMMARY' row found in {filename}. Cannot validate totals."
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    summary_row = df[summary_mask].iloc[0]
    data_df = df[~summary_mask]

    if data_df.empty:
        error_msg = f"No performance rows found in {filename}."
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    extracted_rows = [
        {
            "Performance Date / Time": _format_round(row['Round']),
            "Gross Potential": "",
            "Capacity": "",
            "Gross": float(row['Total Amount']),
            "Tickets Sold": int(row['Sold Ticket']),
            "Comps": int(row['Com. Ticket']),
            "Reserved Gross": "",
            "Reserved Tickets": 0,
        }
        for _, row in data_df.iterrows()
    ]

    # --- STRICT OUTPUT SCHEMA CHECK ---
    actual_schema = set(extracted_rows[0].keys())
    if actual_schema != EXPECTED_SCHEMA:
        error_msg = f"Output schema mismatch! Expected exact columns: {EXPECTED_SCHEMA}, but got: {actual_schema}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)
    logger.info(f"✅ Schema validation passed. Extracted {len(extracted_rows)} performance rows.")

    # --- DYNAMIC VALIDATION AGAINST THE SUMMARY ROW ---
    calc_sold = sum(r["Tickets Sold"] for r in extracted_rows)
    calc_comps = sum(r["Comps"] for r in extracted_rows)
    calc_gross = sum(r["Gross"] for r in extracted_rows)

    rep_sold = int(summary_row['Sold Ticket'])
    rep_comps = int(summary_row['Com. Ticket'])
    rep_gross = float(summary_row['Total Amount'])

    failures = []
    if calc_sold != rep_sold:
        failures.append(f"Tickets Sold mismatch — extracted {calc_sold}, report states {rep_sold}")
    if calc_comps != rep_comps:
        failures.append(f"Comps mismatch — extracted {calc_comps}, report states {rep_comps}")
    if abs(calc_gross - rep_gross) > 0.01:
        failures.append(f"Gross mismatch — extracted {calc_gross:,.2f}, report states {rep_gross:,.2f}")

    metrics = {
        "Performances extracted": len(extracted_rows),
        "Tickets Sold": calc_sold,
        "Reported Tickets Sold": rep_sold,
        "Comps": calc_comps,
        "Reported Comps": rep_comps,
        "Gross": f"{calc_gross:,.2f}",
        "Reported Gross": f"{rep_gross:,.2f}",
    }

    if failures:
        status = "FAILED"
        message = " | ".join(failures)
        logger.error(f"❌ {message}")
    else:
        status = "PASSED"
        message = f"Extracted {len(extracted_rows)} performances. All totals match the SUMMARY row exactly."
        logger.info(f"✅ {message}")

    validation_result = ValidationResult(status=status, message=message, metrics=metrics)
    return extracted_rows, validation_result