"""
Chicago Tour 2025 (Lugano) — XLSX Sales Report Parser
======================================================
Source format: LAC - Sala Teatro (Lugano) XLSX sales reports, Italian locale.

Report layout in the sample file (all rows zero-indexed):
  Row 0       : Report title, e.g. "Chicago Tour Lugano" (ignored)
  Rows 1-4    : Metadata block — includes a "del <date>" run-date row (ignored)
  Row 5       : Column header — Date | Time | Show | Venue | Capacity |
                Paid Tickets | Comps | Reserved tickets | Total Tickets |
                Occupancy rate | Gross Sales Total
  Rows 6+     : Performance data — col "Date" holds a real datetime
  Totals row  : first cell reads "Totale" (Italian for Total)

Nothing above is hardcoded by position. The header row is located by name and
every column is then read through a name -> index map, so the table can move
down the sheet or have its columns reordered without breaking the parser.

Target output schema (from Structure.xlsx) and its mapping:
  Performance Date / Time  <- Date + Time, combined as "dd/mm/YYYY HH:MM"
  Gross Potential          <- always blank (no source column)
  Capacity                 <- Capacity
  Gross                    <- Gross Sales Total
  Tickets Sold             <- Paid Tickets
  Comps                    <- Comps
  Reserved Gross           <- always blank (no source column)
  Reserved Tickets         <- Reserved tickets

Why Tickets Sold maps to Paid Tickets and not to the source's "Total Tickets":
the source defines Total Tickets as Paid + Comps. Since we emit Comps as its
own column, mapping Tickets Sold to Total Tickets would count the comps twice
downstream. Total Tickets is still read, but only to sanity-check that
identity — see _check_composition.
"""

import os
import re
from datetime import datetime, time

import openpyxl
from prefect import task, get_run_logger

from src.models import ValidationResult


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Normalised source header -> the key we use internally. Only the columns we
# actually need are listed; anything else in the sheet (Show, Venue,
# Occupancy rate) is ignored.
_COLUMN_ALIASES = {
    "date": "date",
    "time": "time",
    "capacity": "capacity",
    "paid tickets": "paid_tickets",
    "comps": "comps",
    "reserved tickets": "reserved_tickets",
    "total tickets": "total_tickets",
    "gross sales total": "gross",
}

# A row is only accepted as the header row if it contains all of these.
# "date" and "time" are the structural anchors; paid_tickets and gross are the
# two figures we cannot produce output without. "total_tickets" is deliberately
# NOT required — it feeds a sanity check only, so a source that stops emitting
# it should still parse.
_REQUIRED_COLUMNS = {"date", "time", "paid_tickets", "gross"}

# The totals row marker. Italian reports use "Totale"; we accept the English
# spelling too in case the source system's locale is ever switched.
_TOTALS_MARKERS = {"totale", "total"}

# How far down the sheet we search for the header before giving up.
_MAX_HEADER_SEARCH_ROWS = 30

# Gross tolerance, matching the Ticketek and Malvern parsers: absorbs the
# source's own rounding without letting a real discrepancy through.
_GROSS_TOLERANCE = 1.0

# Blank-by-design output columns — no source data exists to map to them.
_BLANK = ""


# ---------------------------------------------------------------------------
# Internal helpers — each does exactly one thing
# ---------------------------------------------------------------------------

def _load_rows(file_path: str) -> list[tuple]:
    """Read the active sheet into a plain list of tuples. Read-only for safety."""
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    rows = list(wb.active.iter_rows(values_only=True))
    wb.close()
    return rows


def _norm(value) -> str:
    """
    Normalise a cell for text comparison: lowercase, trimmed, inner runs of
    whitespace collapsed to one space. This is what makes header matching
    survive "Reserved tickets" vs "Reserved Tickets" vs "Reserved  Tickets".
    """
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def _map_columns(row: tuple) -> dict:
    """Build {internal_key: column_index} for the columns we recognise in `row`."""
    mapping = {}
    for idx, cell in enumerate(row):
        key = _COLUMN_ALIASES.get(_norm(cell))
        # First occurrence wins, so a stray duplicate header later in the row
        # cannot hijack a column we have already resolved.
        if key and key not in mapping:
            mapping[key] = idx
    return mapping


def _find_header(rows: list[tuple]) -> tuple[int, dict]:
    """
    Locate the column header row by name and return (row_index, column_map).
    Raises ValueError with a diagnostic message if it cannot be found.
    """
    for i, row in enumerate(rows[:_MAX_HEADER_SEARCH_ROWS]):
        mapping = _map_columns(row)
        if _REQUIRED_COLUMNS.issubset(mapping):
            return i, mapping

    raise ValueError(
        f"Could not find the column header row in the first "
        f"{_MAX_HEADER_SEARCH_ROWS} rows. Expected a row containing at least: "
        f"Date, Time, Paid Tickets, Gross Sales Total. "
        f"Check that this is a Chicago Tour Lugano sales report."
    )


def _cell(row: tuple, columns: dict, key: str):
    """
    Safely pull one mapped column out of a row.

    Returns None when the column was not present in the header or when the row
    is shorter than the mapped index — ragged rows are common in exported
    sheets and must not raise IndexError.
    """
    idx = columns.get(key)
    if idx is None or idx >= len(row):
        return None
    return row[idx]


def _to_int(value) -> int:
    """Coerce a cell to int, tolerating thousands separators and blanks."""
    if value is None or value == "":
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    clean = re.sub(r"[^\d\-]", "", str(value))
    try:
        return int(clean)
    except ValueError:
        return 0


def _to_float(value) -> float:
    """Coerce a cell to float, stripping currency symbols and separators."""
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    clean = str(value).replace("CHF", "").replace("£", "").replace("€", "")
    clean = clean.replace(",", "").replace("'", "").strip()
    try:
        return float(clean)
    except ValueError:
        return 0.0


def _format_time(value) -> str:
    """
    Render the Time column as HH:MM.

    The column arrives as a plain string ("20:00") in the sample file, but
    Excel silently promotes a time-formatted cell to datetime.time, and a cell
    typed as "20:00:00" becomes a datetime. All three are handled.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    if isinstance(value, time):
        return value.strftime("%H:%M")

    text = str(value).strip()
    match = re.match(r"^(\d{1,2})[:.](\d{2})", text)
    if match:
        return f"{int(match.group(1)):02d}:{match.group(2)}"
    return text


def _combine_datetime(date_value, time_value) -> str:
    """
    Combine the Date and Time columns into the single "dd/mm/YYYY HH:MM"
    string the target structure expects.
    """
    if isinstance(date_value, datetime):
        date_str = date_value.strftime("%d/%m/%Y")
    else:
        date_str = str(date_value).strip() if date_value is not None else ""

    time_str = _format_time(time_value)
    return f"{date_str} {time_str}".strip()


def _is_totals_row(row: tuple) -> bool:
    """
    True when this row is the report's own totals line.

    We scan every cell rather than just the first, because the marker sits in
    column A here but other reports in this estate put it mid-row.
    """
    return any(_norm(cell) in _TOTALS_MARKERS for cell in row)


def _is_data_row(row: tuple, columns: dict) -> bool:
    """
    A data row is one whose Date column holds a real date. Blank spacer rows
    and stray footnotes are rejected by this single check.
    """
    date_value = _cell(row, columns, "date")
    if isinstance(date_value, datetime):
        return True
    # Fallback for sources that export the date as text rather than a date cell.
    return bool(re.match(r"^\d{1,4}[/\-.]\d{1,2}[/\-.]\d{1,4}", _norm(date_value)))


def _extract_record(row: tuple, columns: dict) -> dict:
    """
    Convert a single performance row into an output record.

    Key order here defines the CSV column order downstream, so it deliberately
    matches Structure.xlsx exactly.
    """
    return {
        "Performance Date / Time": _combine_datetime(
            _cell(row, columns, "date"), _cell(row, columns, "time")
        ),
        "Gross Potential": _BLANK,
        "Capacity": _to_int(_cell(row, columns, "capacity")),
        "Gross": _to_float(_cell(row, columns, "gross")),
        "Tickets Sold": _to_int(_cell(row, columns, "paid_tickets")),
        "Comps": _to_int(_cell(row, columns, "comps")),
        "Reserved Gross": _BLANK,
        "Reserved Tickets": _to_int(_cell(row, columns, "reserved_tickets")),
    }


def _check_composition(
    performance_rows: list[tuple],
    columns: dict,
    logger,
) -> int:
    """
    Verify the source's own arithmetic: Paid Tickets + Comps == Total Tickets.

    We map Tickets Sold from Paid Tickets *because* Total Tickets is the sum of
    the two. This confirms that assumption still holds, so if the source ever
    redefines its Total Tickets column we learn it from a warning rather than
    from silently wrong figures downstream.

    Log-only by design: the Totale row already hard-guards Paid Tickets and
    Gross, and a derived column disagreeing is a prompt to investigate, not a
    reason to quarantine a file whose money is correct.

    Returns the number of rows that disagreed (0 when the check is skipped).
    """
    # BOTH columns must be present. Guarding only one would make _cell return
    # None for the other, _to_int turn that into 0, and every row look broken.
    if "total_tickets" not in columns or "comps" not in columns:
        logger.debug(
            "   Total Tickets or Comps column absent, skipping composition check"
        )
        return 0

    offenders = []
    for row in performance_rows:
        paid = _to_int(_cell(row, columns, "paid_tickets"))
        comps = _to_int(_cell(row, columns, "comps"))
        total = _to_int(_cell(row, columns, "total_tickets"))
        if paid + comps != total:
            label = _combine_datetime(
                _cell(row, columns, "date"), _cell(row, columns, "time")
            )
            offenders.append(f"{label} (paid {paid} + comps {comps} != total {total})")

    if offenders:
        shown = "; ".join(offenders[:3])
        suffix = f" (+{len(offenders) - 3} more)" if len(offenders) > 3 else ""
        logger.warning(
            f"⚠️ Paid + Comps does not equal Total Tickets on "
            f"{len(offenders)} of {len(performance_rows)} row(s): {shown}{suffix}. "
            f"Check whether the source has redefined its Total Tickets column."
        )

    return len(offenders)


def _validate(
    extracted: list[dict],
    performance_rows: list[tuple],
    totals_row: tuple,
    columns: dict,
    logger,
) -> ValidationResult:
    """
    Compare our extracted sums against the report's own "Totale" row.

    Hard fail on Tickets Sold and Gross — those are the figures the business
    reports on, so a mismatch must stop the pipeline. Capacity, Comps and
    Reserved Tickets are soft checks: the source leaves them blank on the
    totals row often enough that failing on them would cause false alarms.
    """
    calc_tickets = sum(r["Tickets Sold"] for r in extracted)
    calc_gross = sum(r["Gross"] for r in extracted)

    # Compared against Paid Tickets, since that is what Tickets Sold maps to.
    rep_tickets = _to_int(_cell(totals_row, columns, "paid_tickets"))
    rep_gross = _to_float(_cell(totals_row, columns, "gross"))

    failures = []
    if calc_tickets != rep_tickets:
        failures.append(
            f"Tickets Sold mismatch — extracted {calc_tickets:,}, "
            f"report states {rep_tickets:,}"
        )
    if abs(calc_gross - rep_gross) >= _GROSS_TOLERANCE:
        failures.append(
            f"Gross mismatch — extracted {calc_gross:,.2f}, "
            f"report states {rep_gross:,.2f}"
        )

    metrics = {
        "Performances extracted": len(extracted),
        "Tickets Sold": f"{calc_tickets:,}",
        "Reported Tickets Sold": f"{rep_tickets:,}",
        "Gross": f"{calc_gross:,.2f}",
        "Reported Gross": f"{rep_gross:,.2f}",
    }

    # --- Soft checks against the totals row: logged, surfaced, never fatal ---
    for label, key in (
        ("Capacity", "capacity"),
        ("Comps", "comps"),
        ("Reserved Tickets", "reserved_tickets"),
    ):
        reported_cell = _cell(totals_row, columns, key)
        if reported_cell is None:
            logger.debug(f"   {label}: no total stated in the report, skipping check")
            continue

        calc = sum(_to_int(r[label]) for r in extracted)
        reported = _to_int(reported_cell)
        metrics[f"{label} (calc/reported)"] = f"{calc:,} / {reported:,}"
        if calc != reported:
            logger.warning(
                f"⚠️ {label} does not match the totals row — "
                f"extracted {calc:,}, report states {reported:,}"
            )

    # --- Soft check on the source's internal arithmetic ---
    composition_breaks = _check_composition(performance_rows, columns, logger)
    metrics["Paid + Comps = Total breaks"] = composition_breaks

    if failures:
        return ValidationResult(
            status="FAILED", message=" | ".join(failures), metrics=metrics
        )

    return ValidationResult(
        status="PASSED",
        message=(
            f"Extracted {len(extracted)} performances. Tickets and gross match "
            f"the report's Totale row."
        ),
        metrics=metrics,
    )


# ---------------------------------------------------------------------------
# Public parser — this is the entry point called by the processing engine
# ---------------------------------------------------------------------------

@task(name="Parse Chicago Tour Lugano XLSX")
def chicago_tour_lugano_parser(file_path: str) -> tuple[list[dict], ValidationResult]:
    """
    Parse a Chicago Tour 2025 (Lugano) XLSX sales report.

    Returns:
        records           : list of dicts, one per performance, keyed to the
                            target structure
        validation_result : ValidationResult with status PASSED or FAILED
    """
    logger = get_run_logger()
    filename = os.path.basename(file_path)
    logger.info(f"📂 Opening: {filename}")

    rows = _load_rows(file_path)

    header_idx, columns = _find_header(rows)
    resolved = {k: v for k, v in sorted(columns.items(), key=lambda kv: kv[1])}
    logger.info(f"   Header row: {header_idx} | Columns resolved: {resolved}")

    missing = [
        name
        for name, key in (
            ("Capacity", "capacity"),
            ("Comps", "comps"),
            ("Reserved tickets", "reserved_tickets"),
        )
        if key not in columns
    ]
    if missing:
        logger.warning(
            f"⚠️ Optional column(s) not found, will be written as 0: {', '.join(missing)}"
        )

    # --- Walk the data rows until the Totale marker ---
    performance_rows = []
    totals_row = None

    for row in rows[header_idx + 1:]:
        if all(v is None for v in row):
            continue                          # blank spacer row
        if _is_totals_row(row):
            totals_row = row
            break                             # nothing useful after this
        if _is_data_row(row, columns):
            performance_rows.append(row)

    # --- Guards: we need both the data and the totals to trust the file ---
    if not performance_rows:
        raise ValueError(
            f"No performance rows found in {filename}. Expected rows with a date "
            f"in the 'Date' column after row {header_idx}."
        )
    if totals_row is None:
        raise ValueError(
            f"Totals row ('Totale') not found in {filename}. The report may be "
            f"incomplete or the format has changed."
        )

    logger.info(f"   Found {len(performance_rows)} performance rows")

    extracted = [_extract_record(row, columns) for row in performance_rows]
    validation_result = _validate(
        extracted, performance_rows, totals_row, columns, logger
    )

    if validation_result.status == "PASSED":
        logger.info(f"✅ {validation_result.message}")
    else:
        logger.error(f"❌ Validation failed: {validation_result.message}")
        raise ValueError(f"Validation Failed: {validation_result.message}")

    return extracted, validation_result
