"""
Tixly — Daily Sales PDF Parser (Doc ID 347)
===========================================
Source format: Tixly-generated "Daily Sales" PDF (one table, one row per performance).

Report layout (pdfplumber layout=True text):
  Header block   : title / Type / Date generated / Event Group (all ignored)
  Column header  : Start Date & Time | Event Group | Reserved | Total | Total Revenue | Free
  Data rows      : dd.mm.yyyy hh:mm  <event group>  <reserved> <total> £<revenue> <free>
  Totals row     : "Total" followed by the same four numeric columns

Doc ID 347 output mapping:
  Performance Date / Time  <- Start Date & Time
  Gross Potential          <- blank (not supplied by Tixly)
  Capacity                 <- blank (not supplied by Tixly)
  Gross                    <- Total Revenue
  Tickets Sold             <- Total minus Free
  Comps                    <- Free
  Reserved Gross           <- blank (not supplied by Tixly)
  Reserved Tickets         <- Reserved
"""

import os
import re

import pdfplumber
from prefect import task, get_run_logger

from src.models import ValidationResult


def parse_currency(value_str):
    if not value_str: return 0.0
    clean = value_str.replace('£', '').replace(',', '').strip()
    try:
        return float(clean)
    except ValueError:
        return 0.0


def parse_int(value_str):
    if not value_str: return 0
    clean = value_str.replace(',', '').strip()
    try:
        return int(clean)
    except ValueError:
        return 0


# Strict Data Contract — Doc ID 347
EXPECTED_SCHEMA = {
    "Performance Date / Time", "Gross Potential", "Capacity", "Gross",
    "Tickets Sold", "Comps", "Reserved Gross", "Reserved Tickets"
}

# Anchored to end of line so the four numeric columns are always taken from the
# right. This keeps event group names containing digits (e.g. "MAMMA MIA! 2027")
# from being mistaken for the Reserved column.
row_pattern = re.compile(
    r"^\s*(?P<date>\d{2}\.\d{2}\.\d{4})\s+"     # 06.10.2026
    r"(?P<time>\d{2}:\d{2})\s+"                 # 19:30
    r"(?P<group>.+?)\s+"                        # The Silence of the Lambs
    r"(?P<reserved>[\d,]+)\s+"                  # Reserved
    r"(?P<total>[\d,]+)\s+"                     # Total
    r"(?P<revenue>[£\d\.,]+)\s+"                # Total Revenue
    r"(?P<free>[\d,]+)\s*$"                     # Free
)

summary_pattern = re.compile(
    r"^\s*Total\s+"
    r"(?P<reserved>[\d,]+)\s+"
    r"(?P<total>[\d,]+)\s+"
    r"(?P<revenue>[£\d\.,]+)\s+"
    r"(?P<free>[\d,]+)\s*$"
)


@task(name="Parse Tixly Daily Sales PDF")
def tixly_pdf_extractor(pdf_path):
    logger = get_run_logger()
    extracted_rows = []

    logger.info(f"📂 Opening PDF file: {os.path.basename(pdf_path)}")

    calc_reserved = 0
    calc_tickets_sold = 0
    calc_comps = 0
    calc_gross = 0.0

    report_reserved = 0
    report_total = 0
    report_comps = 0
    report_gross = 0.0
    verification_found = False

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text(layout=True)
                if not text: continue

                for line in text.split('\n'):
                    # 1. Check Data Row
                    match = row_pattern.search(line)
                    if match:
                        d = match.groupdict()

                        total = parse_int(d['total'])
                        comps = parse_int(d['free'])
                        reserved = parse_int(d['reserved'])
                        gross = parse_currency(d['revenue'])
                        tickets_sold = total - comps

                        calc_reserved += reserved
                        calc_tickets_sold += tickets_sold
                        calc_comps += comps
                        calc_gross += gross

                        # dd.mm.yyyy hh:mm -> yyyy-mm-dd hh:mm
                        day, month, year = d['date'].split('.')
                        perf_dt = f"{year}-{month}-{day} {d['time']}"

                        extracted_rows.append({
                            "Performance Date / Time": perf_dt,
                            "Gross Potential": "",
                            "Capacity": "",
                            "Gross": gross,
                            "Tickets Sold": tickets_sold,
                            "Comps": comps,
                            "Reserved Gross": "",
                            "Reserved Tickets": reserved
                        })
                        continue

                    # 2. Check Summary Line
                    match_sum = summary_pattern.search(line)
                    if match_sum:
                        logger.info(f"🏁 Found 'Total' summary line on Page {i+1}.")
                        s = match_sum.groupdict()
                        report_reserved = parse_int(s['reserved'])
                        report_total = parse_int(s['total'])
                        report_comps = parse_int(s['free'])
                        report_gross = parse_currency(s['revenue'])
                        verification_found = True

        # --- STRICT SCHEMA VALIDATION ---
        if extracted_rows:
            actual_schema = set(extracted_rows[0].keys())
            if actual_schema != EXPECTED_SCHEMA:
                error_msg = f"Data schema mismatch! Expected exact columns: {EXPECTED_SCHEMA}, but got: {actual_schema}"
                logger.error(f"❌ {error_msg}")
                raise ValueError(error_msg)
            else:
                logger.info(f"✅ Schema validation passed. Extracted {len(extracted_rows)} performance rows.")
        else:
            error_msg = (
                f"No performance rows found in {os.path.basename(pdf_path)}. "
                f"Expected rows starting 'dd.mm.yyyy hh:mm'. The Tixly export format may have changed."
            )
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)

    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {str(e)}")
        raise e

    # --- DYNAMIC VALIDATION RESULT ---
    metrics = {
        "Calculated Tickets Sold": calc_tickets_sold,
        "Calculated Comps": calc_comps,
        "Calculated Reserved": calc_reserved,
        "Calculated Gross": f"£{calc_gross:,.2f}"
    }

    if verification_found:
        # Tixly reports 'Total' (all issued tickets), so paid = Total - Free.
        report_tickets_sold = report_total - report_comps

        metrics["Reported Tickets Sold"] = report_tickets_sold
        metrics["Reported Comps"] = report_comps
        metrics["Reported Reserved"] = report_reserved
        metrics["Reported Gross"] = f"£{report_gross:,.2f}"

        tickets_match = (calc_tickets_sold == report_tickets_sold)
        comps_match = (calc_comps == report_comps)
        reserved_match = (calc_reserved == report_reserved)
        gross_matches = (abs(calc_gross - report_gross) < 1.0)

        if tickets_match and comps_match and reserved_match and gross_matches:
            status = "PASSED"
            message = "Calculated totals successfully match the report summary."
            logger.info(f"✅ {message}")
        else:
            status = "FAILED"
            message = (
                f"Mismatch! Calculated (Sold: {calc_tickets_sold}, Comps: {calc_comps}, "
                f"Reserved: {calc_reserved}, Gross: £{calc_gross:,.2f}) vs "
                f"Reported (Sold: {report_tickets_sold}, Comps: {report_comps}, "
                f"Reserved: {report_reserved}, Gross: £{report_gross:,.2f})"
            )
            logger.error(f"❌ {message}")
    else:
        status = "UNVALIDATED"
        message = "No stated totals found in PDF, manual review required."
        logger.warning(f"⚠️ {message}")

    validation_result = ValidationResult(
        status=status,
        message=message,
        metrics=metrics
    )

    return extracted_rows, validation_result
