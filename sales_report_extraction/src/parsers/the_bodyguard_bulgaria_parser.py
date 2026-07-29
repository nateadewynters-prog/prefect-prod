"""
The Bodyguard (Bulgaria) 'play_details' sales export parser.

Named after this show/venue rather than the source format, so treat it as
specific to this rule. If another show on the same ticketing system turns
up, copy this file under its own name rather than repointing this rule at
a different show -- see EXPECTED_SHOW_KEYWORD below for how to enforce that.

The venue is in Bulgaria but the ticketing system is Greek-language: the
block headers are English, while the totals row and the export furniture are
in Greek. Don't let the Greek text mislead you about the territory -- the
rule's Timezone is Europe/Sofia.

WHY THIS PARSER IS DIFFERENT
----------------------------
The attachment arrives with an .xls extension but it is NOT an Excel file --
it is a plain HTML <table> saved with the wrong extension. pandas.read_excel()
fails on it, so we use read_html().

SOURCE LAYOUT
-------------
A two-tier header: six blocks, each with a ticket count and a value column.

    idx  col  block                  our use
    0    A    Date                   Performance Date / Time
    1    B    All tickets            unused (= paid + notpaid + free)
    2    C    All Price              unused
    3    D    Sales tickets          Tickets Sold
    4    E    Sales Price            Gross
    5    F    Reservations tickets   Reserved Tickets
    6    G    Reservations Price     Reserved Gross
    7    H    Invitations tickets    Comps
    8    I    Invitations Price      unused (always 0.00)
    9    J    Printed tickets        unused (tickets issued/collected)
    10   K    Printed Price          unused
    11   L    Cancelled tickets      reported as a metric only
    12   M    Cancelled Price        reported as a metric only (negative)

'Sales', 'Reservations' and 'Invitations' are the ticketing system's own
internal fields 'paid', 'notpaid' and 'free', which is what confirms that
Reservations are unpaid holds and Invitations are free comps.

The last row of the table is the export's own totals row (Greek: Sunola).
It is excluded from the output and used to validate our figures.

WHY WE ASSERT THE HEADER
------------------------
Figures are read by column position, which is simple and fast. The risk is a
vendor reshuffling the blocks: the data rows and the totals row would shift
together, so the totals would still reconcile and we would quietly ship
Reservations figures labelled as Sales.

So before reading anything we assert the full two-tier header matches
EXPECTED_HEADER exactly. That pins both the order and the number of blocks in
one check. If the vendor reorders the blocks, adds one, or localises the
labels into Greek, this parser stops rather than producing plausible-looking
wrong numbers. Updating EXPECTED_HEADER is then a deliberate decision.

HOW FAILURES ARE HANDLED
------------------------
Two kinds of problem, handled two ways:

  * The file can't be trusted structurally (header doesn't match, no
    performance rows, no totals row) -> raise ValueError. There's nothing
    sensible to return.

  * The figures don't reconcile against the totals row -> return a
    ValidationResult with status FAILED. src.file_processor turns that into a
    ValueError, which keeps the raise in one place.

Either way the file is quarantined and a Teams alert fires. Nothing reaches
the contractor.

OUTPUT
------
Eight columns, matching the agreed reporting structure. Gross Potential and
Capacity are not in this feed and are written as 0, as in the reference file.
"""

import os
import re
from io import StringIO

import pandas as pd
from prefect import task

from src.env_setup import get_universal_logger
from src.models import ValidationResult

# The pipeline writes this rule's processed file as .xlsx instead of the default
# .csv, because the contractor consumes Excel. src.file_processor reads this
# constant; parsers that don't define it still get a .csv.
OUTPUT_EXT = ".xlsx"

# Exact output schema, in order.
OUTPUT_COLUMNS = [
    "Performance Date / Time",
    "Gross Potential",
    "Capacity",
    "Gross",
    "Tickets Sold",
    "Comps",
    "Reserved Gross",
    "Reserved Tickets",
]

# The two-tier header we require, in order. See "WHY WE ASSERT THE HEADER".
EXPECTED_HEADER = [
    ("date", "date"),
    ("all", "tickets"), ("all", "price"),
    ("sales", "tickets"), ("sales", "price"),
    ("reservations", "tickets"), ("reservations", "price"),
    ("invitations", "tickets"), ("invitations", "price"),
    ("printed", "tickets"), ("printed", "price"),
    ("cancelled", "tickets"), ("cancelled", "price"),
]

# Off by default, so renaming this file alone doesn't change runtime behaviour.
# Since this parser is now named for one show, consider setting this to
# "Bodyguard" to reject an export for a different show outright -- belt and
# braces alongside the rule's own sender/subject routing. Left as a decision
# rather than made silently.
EXPECTED_SHOW_KEYWORD = None

COL_DATE = 0
COL_SALES_TICKETS = 3
COL_SALES_VALUE = 4
COL_RESERVED_TICKETS = 5
COL_RESERVED_VALUE = 6
COL_COMP_TICKETS = 7
COL_CANCELLED_TICKETS = 11
COL_CANCELLED_VALUE = 12

# Matches '03/10/2026 15:00'. Distinguishes performance rows from the totals row.
DATE_PATTERN = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}$")

# Ticket counts must match exactly; money is compared to the penny.
MONEY_TOLERANCE = 0.01


def _to_float(value) -> float:
    """
    Turn '89,720.00 EUR' or '-595.00 EUR' into a float.

    Drops thousands commas, then every character that isn't a digit, a minus
    sign or a decimal point. That makes it indifferent to however the euro
    sign happens to be encoded in the attachment.

    This assumes comma-thousands / dot-decimal formatting, which is what the
    feed uses. If the vendor switched to European formatting, the totals
    reconciliation would fail rather than let wrong numbers through.
    """
    if value is None:
        return 0.0
    cleaned = re.sub(r"[^0-9.\-]", "", str(value).replace(",", ""))
    if cleaned in ("", "-", ".", "-."):
        return 0.0
    return float(cleaned)


def _to_int(value) -> int:
    """Ticket counts are whole numbers. Route through _to_float for cleaning."""
    return int(round(_to_float(value)))


def _is_performance_row(row) -> bool:
    return bool(DATE_PATTERN.match(str(row[COL_DATE]).strip()))


def _check_header(table: pd.DataFrame) -> None:
    """Refuse to read the file unless the header is exactly what we expect."""
    if table.columns.nlevels != 2:
        raise ValueError(
            f"Source layout has changed: expected a two-tier header, found "
            f"{table.columns.nlevels} tier(s)."
        )

    found = [
        (str(block).strip().lower(), str(sub).strip().lower())
        for block, sub in table.columns
    ]
    if found != EXPECTED_HEADER:
        raise ValueError(
            f"Source layout has changed, so column positions can no longer be "
            f"trusted.\n  expected: {EXPECTED_HEADER}\n  found:    {found}"
        )


def _load_table(file_path: str) -> tuple:
    """Read the file once and return (raw_html, rows) with the header verified."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
        raw_html = handle.read()

    tables = pd.read_html(StringIO(raw_html))
    if not tables:
        raise ValueError("No HTML table found in the attachment.")

    table = tables[0]
    _check_header(table)
    return raw_html, table.values.tolist()


def _log_source_details(raw_html: str, logger) -> None:
    """Log the show name and export timestamp; optionally enforce the show."""
    match = re.search(r'id="Content_divPlay"[^>]*>(.*?)</div>', raw_html, re.DOTALL)
    show_name = match.group(1).strip() if match else ""
    logger.info(f"🎭 Export show name: '{show_name or 'not found'}'")

    stamp = re.search(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", raw_html)
    if stamp:
        logger.info(f"🕒 Export generated at: {stamp.group(1)}")

    if EXPECTED_SHOW_KEYWORD and EXPECTED_SHOW_KEYWORD.lower() not in show_name.lower():
        raise ValueError(
            f"Show mismatch: expected the export to mention "
            f"'{EXPECTED_SHOW_KEYWORD}' but found '{show_name}'."
        )


def _build_records(performance_rows: list) -> list:
    """Map each source row onto the eight output columns."""
    return [
        {
            "Performance Date / Time": str(row[COL_DATE]).strip(),
            "Gross Potential": 0,
            "Capacity": 0,
            "Gross": _to_float(row[COL_SALES_VALUE]),
            "Tickets Sold": _to_int(row[COL_SALES_TICKETS]),
            "Comps": _to_int(row[COL_COMP_TICKETS]),
            "Reserved Gross": _to_float(row[COL_RESERVED_VALUE]),
            "Reserved Tickets": _to_int(row[COL_RESERVED_TICKETS]),
        }
        for row in performance_rows
    ]


def _validate(records: list, totals_row: list, logger) -> ValidationResult:
    """
    Reconcile every mapped figure against the export's own totals row.

    Returns FAILED rather than raising: src.file_processor already converts a
    FAILED result into a ValueError, so the raise stays in one place.

    The failure message deliberately avoids the words 'lookup', 'mapping',
    'code' and 'unmapped'. main.py sniffs for those to decide whether a
    failure was a data-mapping problem, and a false positive there would write
    a spurious row to the lookup-failures database.
    """
    ticket_checks = [
        ("Tickets Sold", COL_SALES_TICKETS),
        ("Comps", COL_COMP_TICKETS),
        ("Reserved Tickets", COL_RESERVED_TICKETS),
    ]
    money_checks = [
        ("Gross", COL_SALES_VALUE),
        ("Reserved Gross", COL_RESERVED_VALUE),
    ]

    metrics = {"Performances": len(records)}
    mismatches = []

    for label, col in ticket_checks:
        calculated = sum(r[label] for r in records)
        reported = _to_int(totals_row[col])
        metrics[f"Calc {label}"] = calculated
        metrics[f"Rep {label}"] = reported
        if calculated != reported:
            mismatches.append(f"{label} (calculated {calculated}, reported {reported})")

    for label, col in money_checks:
        calculated = round(sum(r[label] for r in records), 2)
        reported = round(_to_float(totals_row[col]), 2)
        metrics[f"Calc {label}"] = calculated
        metrics[f"Rep {label}"] = reported
        if abs(calculated - reported) > MONEY_TOLERANCE:
            mismatches.append(f"{label} (calculated {calculated}, reported {reported})")

    # Cancelled isn't part of the output schema, but showing it here means
    # anyone reading the Prefect artifact can see refund activity at a glance.
    metrics["Cancelled Tickets (source)"] = _to_int(totals_row[COL_CANCELLED_TICKETS])
    metrics["Cancelled Value (source)"] = round(_to_float(totals_row[COL_CANCELLED_VALUE]), 2)

    if mismatches:
        message = f"Totals do not reconcile: {'; '.join(mismatches)}"
        logger.error(f"❌ {message}")
        return ValidationResult(status="FAILED", message=message, metrics=metrics)

    message = "All mapped column sums match the export's own totals row."
    logger.info(f"✅ {message}")
    return ValidationResult(status="PASSED", message=message, metrics=metrics)


@task(name="Parse The Bodyguard Bulgaria play_details")
def the_bodyguard_bulgaria_parser(file_path: str) -> tuple:
    """
    Pipeline entry point. Returns (records, ValidationResult), the contract
    every parser in this project follows.
    """
    logger = get_universal_logger(__name__)
    logger.info(f"📂 Opening play_details export: {os.path.basename(file_path)}")

    raw_html, rows = _load_table(file_path)
    _log_source_details(raw_html, logger)

    performance_rows = [row for row in rows if _is_performance_row(row)]
    if not performance_rows:
        raise ValueError(
            "No performance rows found. Expected rows whose first cell looks "
            "like 'dd/mm/yyyy HH:MM'."
        )

    # The totals row is the last row, after every performance row.
    totals_row = rows[-1]
    if _is_performance_row(totals_row):
        raise ValueError(
            "Totals row not found: the last row is a performance row. Without "
            "it we cannot verify the figures, so the file is rejected."
        )

    logger.info(f"📊 Found {len(performance_rows)} performance row(s).")

    records = _build_records(performance_rows)
    validation_result = _validate(records, totals_row, logger)

    return records, validation_result


if __name__ == "__main__":
    # Debug a file by hand:
    #   python -m src.parsers.the_bodyguard_bulgaria_parser <path-to-export>
    import sys

    parsed, result = the_bodyguard_bulgaria_parser(sys.argv[1])

    print(pd.DataFrame(parsed).to_string(index=False))
    print(f"\nStatus: {result.status}\n{result.message}\n")
    for key, value in result.metrics.items():
        print(f"  {key}: {value}")
