"""
Tixly — Daily Sales PDF Parser (Doc ID 347)
===========================================
Source format: Tixly-generated "Daily Sales" PDF (one table, one row per performance).

Three layout variants have been observed from Tixly so far, auto-detected
per file from the column header line:

  1. Full layout             : Start Date & Time | Event Group | Reserved | Total | Total Revenue | Free
  2. Reduced layout          : Start Date & Time | Event Group | Total | Total Revenue | Free
                                ("All events" export — no Reserved column at all;
                                Event Group is often blank on every row too)
  3. Reserved-Revenue layout : Start Date & Time | Event Group | Reserved | Reserved Revenue | Total | Total Revenue | Free
                                (adds a real Reserved Gross figure — the only
                                variant where that column isn't blank)

Doc ID 347 output mapping:
  Performance Date / Time  <- Start Date & Time
  Gross Potential          <- blank (not supplied)
  Capacity                 <- blank (not supplied)
  Gross                    <- Total Revenue
  Tickets Sold             <- Total minus Free
  Comps                    <- Free
  Reserved Gross           <- Reserved Revenue (variant 3 only; else blank)
  Reserved Tickets         <- Reserved (variants 1 & 3; blank for variant 2)
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

# --- Variant 1: full layout (Reserved, no Reserved Revenue) ---
ROW_FULL = re.compile(
    r"^\s*(?P<date>\d{2}\.\d{2}\.\d{4})\s+"
    r"(?P<time>\d{2}:\d{2})\s+"
    r"(?P<group>.+?)\s+"
    r"(?P<reserved>[\d,]+)\s+"
    r"(?P<total>[\d,]+)\s+"
    r"(?P<revenue>[£\d\.,]+)\s+"
    r"(?P<free>[\d,]+)\s*$"
)
SUMMARY_FULL = re.compile(
    r"^\s*Total\s+"
    r"(?P<reserved>[\d,]+)\s+"
    r"(?P<total>[\d,]+)\s+"
    r"(?P<revenue>[£\d\.,]+)\s+"
    r"(?P<free>[\d,]+)\s*$"
)

# --- Variant 2: reduced layout (no Reserved column at all) ---
ROW_REDUCED = re.compile(
    r"^\s*(?P<date>\d{2}\.\d{2}\.\d{4})\s+"
    r"(?P<time>\d{2}:\d{2})\s+"
    r"(?P<group>.*?)\s*"
    r"(?P<total>[\d,]+)\s+"
    r"(?P<revenue>[£\d\.,]+)\s+"
    r"(?P<free>[\d,]+)\s*$"
)
SUMMARY_REDUCED = re.compile(
    r"^\s*Total\s+"
    r"(?P<total>[\d,]+)\s+"
    r"(?P<revenue>[£\d\.,]+)\s+"
    r"(?P<free>[\d,]+)\s*$"
)

# --- Variant 3: Reserved + Reserved Revenue layout ---
ROW_RESERVED_REVENUE = re.compile(
    r"^\s*(?P<date>\d{2}\.\d{2}\.\d{4})\s+"
    r"(?P<time>\d{2}:\d{2})\s+"
    r"(?P<group>.+?)\s+"
    r"(?P<reserved>[\d,]+)\s+"
    r"(?P<reserved_revenue>[£\d\.,]+)\s+"
    r"(?P<total>[\d,]+)\s+"
    r"(?P<revenue>[£\d\.,]+)\s+"
    r"(?P<free>[\d,]+)\s*$"
)
SUMMARY_RESERVED_REVENUE = re.compile(
    r"^\s*Total\s+"
    r"(?P<reserved>[\d,]+)\s+"
    r"(?P<reserved_revenue>[£\d\.,]+)\s+"
    r"(?P<total>[\d,]+)\s+"
    r"(?P<revenue>[£\d\.,]+)\s+"
    r"(?P<free>[\d,]+)\s*$"
)


def _detect_layout(text: str) -> str:
    """Look at the column header line to decide which of the three known
    layouts this file is. Checked in this order because 'Reserved Revenue'
    contains the substring 'Reserved', so it must be checked first."""
    for line in text.split("\n"):
        if "Start Date & Time" in line:
            if "Reserved Revenue" in line:
                return "reserved_revenue"
            if "Reserved" in line:
                return "full"
            return "reduced"
    return "reduced"


@task(name="Parse Tixly Daily Sales PDF")
def tixly_pdf_extractor(pdf_path):
    logger = get_run_logger()
    extracted_rows = []

    logger.info(f"📂 Opening PDF file: {os.path.basename(pdf_path)}")

    calc_reserved = 0
    calc_tickets_sold = 0
    calc_comps = 0
    calc_gross = 0.0
    calc_reserved_gross = 0.0

    report_reserved = 0
    report_total = 0
    report_comps = 0
    report_gross = 0.0
    report_reserved_gross = 0.0
    verification_found = False

    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = "\n".join(
                page.extract_text(layout=True) or "" for page in pdf.pages
            )

        layout = _detect_layout(full_text)
        logger.info(f"ℹ️ Detected layout: {layout}")

        if layout == "reserved_revenue":
            row_pattern, summary_pattern = ROW_RESERVED_REVENUE, SUMMARY_RESERVED_REVENUE
        elif layout == "full":
            row_pattern, summary_pattern = ROW_FULL, SUMMARY_FULL
        else:
            row_pattern, summary_pattern = ROW_REDUCED, SUMMARY_REDUCED

        for line in full_text.split('\n'):
            # 1. Check Data Row
            match = row_pattern.search(line)
            if match:
                d = match.groupdict()

                total = parse_int(d['total'])
                comps = parse_int(d['free'])
                gross = parse_currency(d['revenue'])
                tickets_sold = total - comps

                reserved = parse_int(d['reserved']) if 'reserved' in d else None
                reserved_gross = parse_currency(d['reserved_revenue']) if 'reserved_revenue' in d else None

                calc_tickets_sold += tickets_sold
                calc_comps += comps
                calc_gross += gross
                if reserved is not None:
                    calc_reserved += reserved
                if reserved_gross is not None:
                    calc_reserved_gross += reserved_gross

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
                    "Reserved Gross": reserved_gross if reserved_gross is not None else "",
                    "Reserved Tickets": reserved if reserved is not None else ""
                })
                continue

            # 2. Check Summary Line
            match_sum = summary_pattern.search(line)
            if match_sum:
                logger.info(f"🏁 Found 'Total' summary line.")
                s = match_sum.groupdict()
                report_total = parse_int(s['total'])
                report_comps = parse_int(s['free'])
                report_gross = parse_currency(s['revenue'])
                if 'reserved' in s:
                    report_reserved = parse_int(s['reserved'])
                if 'reserved_revenue' in s:
                    report_reserved_gross = parse_currency(s['reserved_revenue'])
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
        "Calculated Gross": f"£{calc_gross:,.2f}"
    }
    if layout != "reduced":
        metrics["Calculated Reserved"] = calc_reserved
    if layout == "reserved_revenue":
        metrics["Calculated Reserved Gross"] = f"£{calc_reserved_gross:,.2f}"

    if verification_found:
        report_tickets_sold = report_total - report_comps

        metrics["Reported Tickets Sold"] = report_tickets_sold
        metrics["Reported Comps"] = report_comps
        metrics["Reported Gross"] = f"£{report_gross:,.2f}"
        if layout != "reduced":
            metrics["Reported Reserved"] = report_reserved
        if layout == "reserved_revenue":
            metrics["Reported Reserved Gross"] = f"£{report_reserved_gross:,.2f}"

        checks = [
            calc_tickets_sold == report_tickets_sold,
            calc_comps == report_comps,
            abs(calc_gross - report_gross) < 1.0,
        ]
        if layout != "reduced":
            checks.append(calc_reserved == report_reserved)
        if layout == "reserved_revenue":
            checks.append(abs(calc_reserved_gross - report_reserved_gross) < 1.0)

        if all(checks):
            status = "PASSED"
            message = "Calculated totals successfully match the report summary."
            logger.info(f"✅ {message}")
        else:
            status = "FAILED"
            message = (
                f"Mismatch! Calculated (Sold: {calc_tickets_sold}, Comps: {calc_comps}, "
                f"Reserved: {calc_reserved}, Gross: £{calc_gross:,.2f}, "
                f"Reserved Gross: £{calc_reserved_gross:,.2f}) vs "
                f"Reported (Sold: {report_tickets_sold}, Comps: {report_comps}, "
                f"Reserved: {report_reserved}, Gross: £{report_gross:,.2f}, "
                f"Reserved Gross: £{report_reserved_gross:,.2f})"
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