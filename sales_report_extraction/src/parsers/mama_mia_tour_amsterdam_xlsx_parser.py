"""
RAI Amsterdam — Summary XLSX Sales Report Parser (Doc 514)
==========================================================
Source format: Dutch "SUMMARY" export (e.g. "MAMMA MIA! - RAI AMSTERDAM 2027 - SUMMARY")

Report layout:
  Top rows    : Title, "Laatste update data" stamp, event list blob (ignored)
  Header row  : Contains "Datum/tijd Evenement" (currently col B, row 6)
  Data rows   : One per performance — date/time cell is a datetime object
  Totals row  : Date/time column reads "Totaal"
  Footer      : "Pagina 1 van 1", report name, user (ignored)

Dynamic by design: columns are located by their Dutch HEADER TEXT, not by
letter, so the parser survives the vendor adding, removing or reordering
columns. Only the header row needs to contain the required labels below.

Doc 514 mapping:
  Performance Date / Time  <- Datum/tijd Evenement
  Gross Potential          <- blank
  Capacity                 <- blank
  Gross                    <- Verkocht ticketbedrag
  Tickets Sold             <- Verkocht
  Comps                    <- Vrijkaart
  Reserved Gross           <- 0.0 (not supplied by this report)
  Reserved Tickets         <- Totaal gereserveerde tickets (0 if column absent)
"""

import os
from datetime import datetime

import openpyxl
from prefect import task, get_run_logger

from src.models import ValidationResult


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Source header text -> internal key. Matched case-insensitively, whitespace trimmed.
# Required: the parser fails loudly if any of these are missing.
_REQUIRED_HEADERS = {
    "date":  "Datum/tijd Evenement",
    "paid":  "Verkocht",
    "gross": "Verkocht ticketbedrag",
    "comps": "Vrijkaart",
}

# Optional: mapped when present, otherwise the output column defaults to 0.
_OPTIONAL_HEADERS = {
    "reserved": "Totaal gereserveerde tickets",
}

# The totals row is identified by this string in the date/time column
_TOTALS_MARKER = "totaal"

# Strict Data Contract — Doc 514, in output column order (matches e.g. Waitress Salford 514)
EXPECTED_SCHEMA = [
    "Performance Date / Time",
    "Gross Potential",
    "Capacity",
    "Gross",
    "Tickets Sold",
    "Comps",
    "Reserved Gross",
    "Reserved Tickets",
]


# ---------------------------------------------------------------------------
# Internal helpers — each does exactly one thing
# ---------------------------------------------------------------------------

def _norm(value) -> str:
    """Normalise a cell to a lower-case, trimmed string for header/marker matching."""
    return str(value).strip().lower() if value is not None else ""


def _to_int(value) -> int:
    if value is None or value == "":
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(float(str(value).replace(",", "").replace(".", "").strip()))
    except ValueError:
        return 0


def _to_float(value) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    # Dutch formatting: "1.234,56" -> 1234.56
    clean = str(value).replace("€", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(clean)
    except ValueError:
        return 0.0


def _load_rows(file_path: str) -> list[tuple]:
    """Read the active sheet into a plain list of tuples. Read-only for safety."""
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    rows = list(wb.active.iter_rows(values_only=True))
    wb.close()
    return rows


def _find_header_row(rows: list[tuple]) -> tuple[int, dict]:
    """
    Locate the header row and return (row_index, {internal_key: col_index}).
    Raises ValueError naming any required headers that are missing.
    """
    date_label = _norm(_REQUIRED_HEADERS["date"])

    for i, row in enumerate(rows):
        normalised = [_norm(v) for v in row]
        if date_label not in normalised:
            continue

        col_map, missing = {}, []
        for key, label in _REQUIRED_HEADERS.items():
            target = _norm(label)
            if target in normalised:
                col_map[key] = normalised.index(target)
            else:
                missing.append(label)

        for key, label in _OPTIONAL_HEADERS.items():
            target = _norm(label)
            if target in normalised:
                col_map[key] = normalised.index(target)

        if missing:
            raise ValueError(
                f"Header row found at row {i}, but required columns are missing: {missing}. "
                f"Headers present: {[v for v in row if v is not None]}"
            )
        return i, col_map

    raise ValueError(
        f"Could not find the column header row containing '{_REQUIRED_HEADERS['date']}'. "
        f"Check that this is a RAI Amsterdam SUMMARY report."
    )


def _optional_int(row: tuple, cols: dict, key: str) -> int:
    """Read an optional column as int, defaulting to 0 when the column isn't in the file."""
    return _to_int(row[cols[key]]) if key in cols else 0


def _extract_performance_record(row: tuple, cols: dict) -> dict:
    """Convert a single performance row into a Doc 514 record."""
    paid  = _to_int(row[cols["paid"]])
    comps = _to_int(row[cols["comps"]])

    return {
        "Performance Date / Time": row[cols["date"]].strftime("%Y-%m-%d %H:%M"),
        "Gross Potential":         None,
        "Capacity":                None,
        "Gross":                   _to_float(row[cols["gross"]]),
        "Tickets Sold":            paid,
        "Comps":                   comps,
        "Reserved Gross":          0.0,   # not supplied by this report; matches Doc 514
        "Reserved Tickets":        _optional_int(row, cols, "reserved"),
    }


def _validate(extracted: list[dict], totals_row: tuple | None, cols: dict, logger) -> ValidationResult:
    """Compare extracted sums against the report's own 'Totaal' row."""
    calc = {
        "Tickets Sold":     sum(r["Tickets Sold"] for r in extracted),
        "Comps":            sum(r["Comps"] for r in extracted),
        "Reserved Tickets": sum(r["Reserved Tickets"] for r in extracted),
        "Gross":            round(sum(r["Gross"] for r in extracted), 2),
    }

    metrics = {
        "Performances extracted": len(extracted),
        "Calculated Tickets Sold": calc["Tickets Sold"],
        "Calculated Comps":        calc["Comps"],
        "Calculated Gross":        f"€{calc['Gross']:,.2f}",
    }

    if totals_row is None:
        message = "No 'Totaal' row found in report, manual review required."
        logger.warning(f"⚠️ {message}")
        return ValidationResult(status="UNVALIDATED", message=message, metrics=metrics)

    reported = {
        "Tickets Sold":     _to_int(totals_row[cols["paid"]]),
        "Comps":            _to_int(totals_row[cols["comps"]]),
        "Reserved Tickets": _optional_int(totals_row, cols, "reserved"),
        "Gross":            round(_to_float(totals_row[cols["gross"]]), 2),
    }

    metrics["Reported Tickets Sold"] = reported["Tickets Sold"]
    metrics["Reported Comps"]        = reported["Comps"]
    metrics["Reported Gross"]        = f"€{reported['Gross']:,.2f}"

    failures = []
    for key in ("Tickets Sold", "Comps", "Reserved Tickets"):
        if calc[key] != reported[key]:
            failures.append(f"{key} mismatch — extracted {calc[key]:,}, report states {reported[key]:,}")
    if abs(calc["Gross"] - reported["Gross"]) >= 0.01:
        failures.append(f"Gross mismatch — extracted €{calc['Gross']:,.2f}, report states €{reported['Gross']:,.2f}")

    if failures:
        message = " | ".join(failures)
        logger.error(f"❌ Validation failed: {message}")
        return ValidationResult(status="FAILED", message=message, metrics=metrics)

    message = f"Extracted {len(extracted)} performances. Calculated totals match the 'Totaal' row."
    logger.info(f"✅ {message}")
    return ValidationResult(status="PASSED", message=message, metrics=metrics)


# ---------------------------------------------------------------------------
# Public parser — entry point called by the processing engine
# ---------------------------------------------------------------------------

@task(name="Parse Mamma Mia Tour Amsterdam XLSX")
def mama_mia_tour_amsterdam_xlsx_parser(file_path: str) -> tuple[list[dict], ValidationResult]:
    """
    Parse a RAI Amsterdam SUMMARY XLSX sales report into Doc 514 records.

    Returns:
        records           : list of dicts, one per performance
        validation_result : ValidationResult with status PASSED, FAILED or UNVALIDATED
    """
    logger = get_run_logger()
    filename = os.path.basename(file_path)
    logger.info(f"📂 Opening: {filename}")

    rows = _load_rows(file_path)

    # --- Locate structural landmarks ---
    header_idx, cols = _find_header_row(rows)
    logger.info(f"   Header row: {header_idx} | Column map: {cols}")

    # --- Walk data rows until the totals marker ---
    performance_rows = []
    totals_row = None

    for row in rows[header_idx + 1:]:
        if all(v is None for v in row):
            continue                                    # skip blank rows
        date_cell = row[cols["date"]]
        if _norm(date_cell) == _TOTALS_MARKER:
            totals_row = row
            break                                       # stop — footer follows
        if isinstance(date_cell, datetime):
            performance_rows.append(row)

    if not performance_rows:
        raise ValueError(
            f"No performance rows found in {filename}. "
            f"Expected datetime values under '{_REQUIRED_HEADERS['date']}' after row {header_idx}."
        )

    logger.info(f"   Found {len(performance_rows)} performance rows")

    # --- Extract records ---
    extracted = [_extract_performance_record(row, cols) for row in performance_rows]

    # --- Strict schema validation ---
    actual_schema = list(extracted[0].keys())
    if actual_schema != EXPECTED_SCHEMA:
        error_msg = f"Data schema mismatch! Expected: {EXPECTED_SCHEMA}, but got: {actual_schema}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)
    logger.info(f"✅ Schema validation passed ({len(EXPECTED_SCHEMA)} Doc 514 columns).")

    # --- Validate against report totals ---
    validation_result = _validate(extracted, totals_row, cols, logger)

    return extracted, validation_result