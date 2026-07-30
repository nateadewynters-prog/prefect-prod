"""
The Bodyguard 2025 (Bulgaria / Sofia) — Sales Report Parser
============================================================
Source format: an HTML table saved with an .xls extension.

IMPORTANT: despite the file extension, this is NOT an Excel workbook. It is a
plain HTML document. openpyxl raises InvalidFileException on it and xlrd raises
"Unsupported format, or corrupt file", so the Ticketek .xls parser cannot read
these files. We parse the HTML directly with BeautifulSoup instead.

We use the "html.parser" backend rather than lxml because lxml is not in
requirements.txt, and src/link_extractor.py already standardises on html.parser.

Report layout:
  <div>            Show title and a Greek export-time line (ignored)
  <table>          A single table, id="reportTotals"
    <thead>        A padding row, then a TWO-TIER header:
                     tier 1: Date (rowspan 2) | All | Sales | Reservations |
                             Invitations | Printed | Cancelled  (each colspan 2)
                     tier 2: tickets | Price   repeated under each block
    <tbody>        One row per performance
    <tfoot>        Totals row, marked "Σύνολα" (Greek for "Totals")

Nothing is read by column position. The two header tiers are flattened into
combined names ("Sales tickets", "Reservations Price", ...) and every value is
then fetched by name, so the source can add, remove or reorder whole blocks
without breaking the parser.

Column mapping (source block -> target column):
  Date                    -> Performance Date / Time
  Sales tickets           -> Tickets Sold
  Sales Price             -> Gross
  Reservations tickets    -> Reserved Tickets
  Reservations Price      -> Reserved Gross
  Invitations tickets     -> Comps
  Gross Potential         -> always blank (no source column)
  Capacity                -> always blank (no source column)

Read but not mapped to output:
  All tickets / All Price          - cross-checked: All = Sales + Reservations
                                    + Invitations, on tickets and on price
  Invitations Price               - always 0.00 in practice
  Printed tickets / Printed Price - tickets issued/collected, not a sales figure
  Cancelled tickets / Price       - surfaced as validation metrics only
                                    (Cancelled Price is negative)
"""

import os
import re

from bs4 import BeautifulSoup
from prefect import task, get_run_logger

from src.models import ValidationResult


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Combined "block + sub-column" header (normalised) -> internal key.
# The keys on the left are what _flatten_header produces.
_COLUMN_ALIASES = {
    "date": "date",
    "all tickets": "all_tickets",
    "all price": "all_price",
    "sales tickets": "tickets_sold",
    "sales price": "gross",
    "reservations tickets": "reserved_tickets",
    "reservations price": "reserved_gross",
    "invitations tickets": "comps",
    "invitations price": "invitations_price",
    "printed tickets": "printed_tickets",
    "printed price": "printed_price",
    "cancelled tickets": "cancelled_tickets",
    "cancelled price": "cancelled_price",
}

# A header is only accepted if it yields all of these: the date anchor plus the
# two figures we cannot produce output without.
_REQUIRED_COLUMNS = {"date", "tickets_sold", "gross"}

# Totals row markers. "Σύνολα" is what this source emits; the Latin spellings
# are accepted in case the export locale is ever switched.
_TOTALS_MARKERS = {
    "σύνολα", "συνολα",        # Greek, with and without the accent
    "totals", "total", "totale",
}

# Currency symbols and separators stripped before numeric conversion.
_CURRENCY_SYMBOLS = ("€", "£", "$", "лв", "BGN", "EUR", "CHF")

# Matches "dd/mm/yyyy hh:mm", allowing single-digit day/month.
_DATETIME_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})")

# Loose check used to spot a data row when the table has no <tbody>.
_DATE_START_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}")

# OLE2 / Compound File magic bytes. A real binary .xls starts with these.
_OLE2_MAGIC = b"\xd0\xcf\x11\xe0"
# A modern .xlsx is a zip.
_ZIP_MAGIC = b"PK\x03\x04"

# Blank-by-design output columns — no source data exists to map to them.
_BLANK = ""

_GROSS_TOLERANCE = 0.01


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _load_table(file_path: str):
    """
    Read the file and return the report's <table> element.

    Passing raw bytes to BeautifulSoup lets it detect the encoding itself,
    which matters here: the export contains Greek text and the venue is
    Bulgarian, so the charset is not safely assumable.
    """
    with open(file_path, "rb") as f:
        raw = f.read()

    # Fail early and clearly if this is genuinely a binary spreadsheet, rather
    # than letting BeautifulSoup return an empty soup and reporting a
    # confusing "no header found" further down.
    if raw.startswith(_OLE2_MAGIC):
        raise ValueError(
            "File is a real binary .xls (OLE2), not the HTML export this "
            "parser expects. The source system may have changed its export "
            "format — a different parser is needed."
        )
    if raw.startswith(_ZIP_MAGIC):
        raise ValueError(
            "File is a zip-based workbook (.xlsx), not the HTML export this "
            "parser expects. The source system may have changed its export "
            "format — a different parser is needed."
        )

    soup = BeautifulSoup(raw, "html.parser")
    table = soup.find("table")
    if table is None:
        raise ValueError(
            "No <table> found in the file. Expected an HTML sales report "
            "export. Check that this is a Bodyguard Bulgaria report."
        )
    return table


def _row_cells(tr) -> list:
    """The cell elements of a row, in order."""
    return tr.find_all(["td", "th"])


def _text(cell) -> str:
    """Cell text with inner whitespace collapsed."""
    return re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()


def _norm(value) -> str:
    """Lowercase, trim and collapse whitespace, for name comparison."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


# ---------------------------------------------------------------------------
# Header flattening
# ---------------------------------------------------------------------------

def _flatten_header(header_rows: list) -> list:
    """
    Flatten a multi-tier HTML header into one label per column index.

    HTML headers are a grid, not a list: a cell with colspan=2 covers two
    columns and a cell with rowspan=2 covers the tier below it. We expand that
    grid properly, then join the labels stacked in each column.

        tier 1:  Date(rs2) |     All     |    Sales    | ...
        tier 2:            | tickets|Price| tickets|Price| ...
        result:  "Date", "All tickets", "All Price", "Sales tickets", ...

    Doing it this way is what lets the mapping survive a reordered or
    newly-inserted block, which reading by column index would not.
    """
    grid = {}
    occupied = set()

    for r, tr in enumerate(header_rows):
        col = 0
        for cell in _row_cells(tr):
            # Step over slots already claimed by a rowspan from an earlier tier.
            while (r, col) in occupied:
                col += 1

            try:
                colspan = max(1, int(cell.get("colspan") or 1))
            except (TypeError, ValueError):
                colspan = 1
            try:
                rowspan = max(1, int(cell.get("rowspan") or 1))
            except (TypeError, ValueError):
                rowspan = 1

            label = _text(cell)
            for dr in range(rowspan):
                for dc in range(colspan):
                    occupied.add((r + dr, col + dc))
                    grid[(r + dr, col + dc)] = label
            col += colspan

    if not grid:
        return []

    n_cols = max(c for _, c in grid) + 1
    n_rows = max(r for r, _ in grid) + 1

    labels = []
    for c in range(n_cols):
        parts = []
        for r in range(n_rows):
            part = grid.get((r, c), "")
            # Skip blanks and don't repeat a label a rowspan duplicated.
            if part and part not in parts:
                parts.append(part)
        labels.append(" ".join(parts))
    return labels


def _map_columns(labels: list) -> dict:
    """Build {internal_key: column_index} from flattened header labels."""
    mapping = {}
    for idx, label in enumerate(labels):
        key = _COLUMN_ALIASES.get(_norm(label))
        # First occurrence wins, so a repeated label cannot hijack a column
        # already resolved.
        if key and key not in mapping:
            mapping[key] = idx
    return mapping


def _is_totals_row(cells: list) -> bool:
    """True when any cell in the row carries a totals marker."""
    return any(_norm(_text(c)) in _TOTALS_MARKERS for c in cells)


def _is_data_row(cells: list) -> bool:
    """A data row starts with something that looks like a date."""
    if not cells:
        return False
    return bool(_DATE_START_RE.match(_text(cells[0])))


def _split_sections(table) -> tuple:
    """
    Return (header_rows, data_rows, totals_row) as lists of cell-lists.

    Prefers the table's own thead/tbody/tfoot structure, because that is the
    source's explicit statement of what each row is. Falls back to inspecting
    row content when those elements are absent, so a flatter export still works.
    """
    all_rows = table.find_all("tr")

    def section_of(tr):
        parent = tr.find_parent(["thead", "tbody", "tfoot"])
        return parent.name if parent else None

    header_rows = [tr for tr in all_rows if section_of(tr) == "thead"]
    foot_rows = [tr for tr in all_rows if section_of(tr) == "tfoot"]
    body_rows = [tr for tr in all_rows if section_of(tr) == "tbody"]

    # --- Fallback: no thead. Header is everything before the first data row.
    if not header_rows:
        first_data = next(
            (i for i, tr in enumerate(all_rows) if _is_data_row(_row_cells(tr))),
            None,
        )
        # `is None` rather than a truthiness test on purpose: first_data == 0 is
        # a real answer (data starts immediately, so there is no header), while
        # None means no data row exists at all — in which case treat every row
        # as header so the error message can report what labels were present.
        header_rows = all_rows if first_data is None else all_rows[:first_data]

    # --- Data rows: prefer tbody, else any row that looks like data.
    candidates = body_rows or all_rows
    data_rows = [
        _row_cells(tr)
        for tr in candidates
        if _is_data_row(_row_cells(tr)) and not _is_totals_row(_row_cells(tr))
    ]

    # --- Totals: prefer tfoot, else the marker.
    totals_row = None
    for tr in foot_rows or all_rows:
        cells = _row_cells(tr)
        if _is_totals_row(cells):
            totals_row = cells
            break

    return header_rows, data_rows, totals_row


# ---------------------------------------------------------------------------
# Value coercion
# ---------------------------------------------------------------------------

def _cell_text(cells: list, columns: dict, key: str) -> str:
    """
    Fetch one mapped column's text from a row.

    Returns "" when the column is absent from the header or the row is shorter
    than the mapped index — ragged HTML rows must not raise IndexError.
    """
    idx = columns.get(key)
    if idx is None or idx >= len(cells):
        return ""
    return _text(cells[idx])


def _strip_currency(text: str) -> str:
    """Remove currency symbols and all whitespace, including non-breaking."""
    clean = str(text).replace("\xa0", " ")
    for symbol in _CURRENCY_SYMBOLS:
        clean = clean.replace(symbol, "")
    return re.sub(r"\s+", "", clean)


def _to_float(text) -> float:
    """
    Parse a money or count cell to float.

    Handles the two formats this source mixes: data rows are separated
    ("89,720.00 €") while the totals row is not ("491468.50 €"). Negatives
    appear on Cancelled Price ("-595.00 €").

    Comma handling is deliberate rather than a blanket strip: if both a comma
    and a dot are present the comma is a thousands separator, but a lone comma
    followed by exactly two digits is a European decimal comma ("1234,50"),
    which stripping would turn into 123450.
    """
    clean = _strip_currency(text)
    if not clean or clean in ("-", "."):
        return 0.0

    has_comma = "," in clean
    has_dot = "." in clean

    if has_comma and has_dot:
        # Whichever separator comes last is the decimal point; the other is a
        # thousands separator. "89,720.00" -> dot decimal (this source's
        # format); "1.234,50" -> comma decimal (European, if the export locale
        # is ever switched).
        if clean.rfind(",") > clean.rfind("."):
            clean = clean.replace(".", "").replace(",", ".")
        else:
            clean = clean.replace(",", "")
    elif has_comma:
        # A lone comma followed by exactly two digits is a decimal comma;
        # stripping it would turn "1234,50" into 123450.
        if re.search(r",\d{2}$", clean):
            clean = clean.replace(",", ".")
        else:
            clean = clean.replace(",", "")              # thousands only

    try:
        return float(clean)
    except ValueError:
        return 0.0


def _to_int(text) -> int:
    """Parse a ticket-count cell to int, via float so "1,080" works."""
    return int(round(_to_float(text)))


def _format_datetime(text: str) -> str:
    """
    Normalise the Date cell to "dd/mm/YYYY HH:MM".

    The source already uses that layout, so this mostly zero-pads single digits
    and standardises spacing. Day-first is confirmed by the file itself: the
    export-time stamp reads 13/07/2026, and 13 cannot be a month.

    Anything unrecognised is passed through unchanged rather than dropped, so a
    format change shows up in the output instead of silently vanishing.
    """
    raw = str(text).strip()
    match = _DATETIME_RE.match(raw)
    if not match:
        return raw
    day, month, year, hour, minute = match.groups()
    return f"{int(day):02d}/{int(month):02d}/{year} {int(hour):02d}:{minute}"


def _extract_record(cells: list, columns: dict) -> dict:
    """
    Convert one performance row into an output record.

    Key order defines the CSV column order downstream, so it matches
    Structure.xlsx exactly.
    """
    return {
        "Performance Date / Time": _format_datetime(
            _cell_text(cells, columns, "date")
        ),
        "Gross Potential": _BLANK,
        "Capacity": _BLANK,
        "Gross": _to_float(_cell_text(cells, columns, "gross")),
        "Tickets Sold": _to_int(_cell_text(cells, columns, "tickets_sold")),
        "Comps": _to_int(_cell_text(cells, columns, "comps")),
        "Reserved Gross": _to_float(_cell_text(cells, columns, "reserved_gross")),
        "Reserved Tickets": _to_int(
            _cell_text(cells, columns, "reserved_tickets")
        ),
    }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _check_all_block(data_rows: list, columns: dict, logger) -> int:
    """
    Cross-check the source's own arithmetic on each row:
        All tickets == Sales + Reservations + Invitations
        All Price   == Sales Price + Reservations Price + Invitations Price

    This is independent of the totals row, so it catches a block that has been
    reordered or redefined even in a file whose totals happen to add up.

    Log-only: the totals row already hard-guards the figures we output, and the
    All block is not one of them.
    """
    needed = ("all_tickets", "tickets_sold", "reserved_tickets", "comps")
    if not all(k in columns for k in needed):
        logger.debug("   'All' block incomplete, skipping cross-check")
        return 0

    breaks = 0
    for cells in data_rows:
        label = _format_datetime(_cell_text(cells, columns, "date"))

        parts = (
            _to_int(_cell_text(cells, columns, "tickets_sold"))
            + _to_int(_cell_text(cells, columns, "reserved_tickets"))
            + _to_int(_cell_text(cells, columns, "comps"))
        )
        reported = _to_int(_cell_text(cells, columns, "all_tickets"))
        if parts != reported:
            breaks += 1
            logger.warning(
                f"⚠️ {label}: Sales + Reservations + Invitations = {parts:,}, "
                f"but 'All tickets' says {reported:,}."
            )

        if "all_price" in columns and "reserved_gross" in columns:
            price_parts = (
                _to_float(_cell_text(cells, columns, "gross"))
                + _to_float(_cell_text(cells, columns, "reserved_gross"))
                + _to_float(_cell_text(cells, columns, "invitations_price"))
            )
            reported_price = _to_float(_cell_text(cells, columns, "all_price"))
            if abs(price_parts - reported_price) >= _GROSS_TOLERANCE:
                breaks += 1
                logger.warning(
                    f"⚠️ {label}: Sales + Reservations + Invitations price = "
                    f"{price_parts:,.2f}, but 'All Price' says "
                    f"{reported_price:,.2f}."
                )

    if breaks:
        logger.warning(
            f"⚠️ {breaks} cross-check(s) failed against the 'All' block. "
            f"The source may have redefined a column."
        )
    return breaks


def _validate(
    extracted: list,
    data_rows: list,
    totals_row: list,
    columns: dict,
    logger,
) -> ValidationResult:
    """
    Compare extracted sums against the report's own Σύνολα row.

    Hard fail on Tickets Sold and Gross — the figures the business reports on.
    Everything else is a soft check: logged and surfaced as a metric, but not a
    reason to quarantine a file whose sales figures reconcile.
    """
    calc_tickets = sum(r["Tickets Sold"] for r in extracted)
    calc_gross = sum(r["Gross"] for r in extracted)

    rep_tickets = _to_int(_cell_text(totals_row, columns, "tickets_sold"))
    rep_gross = _to_float(_cell_text(totals_row, columns, "gross"))

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

    # --- Soft checks on the columns we output but do not hard-guard ---
    soft_int = (
        ("Reserved Tickets", "reserved_tickets"),
        ("Comps", "comps"),
    )
    for label, key in soft_int:
        if key not in columns:
            continue
        calc = sum(r[label] for r in extracted)
        reported = _to_int(_cell_text(totals_row, columns, key))
        metrics[f"{label} (calc/reported)"] = f"{calc:,} / {reported:,}"
        if calc != reported:
            logger.warning(
                f"⚠️ {label} does not match the totals row — "
                f"extracted {calc:,}, report states {reported:,}"
            )

    if "reserved_gross" in columns:
        calc = sum(r["Reserved Gross"] for r in extracted)
        reported = _to_float(_cell_text(totals_row, columns, "reserved_gross"))
        metrics["Reserved Gross (calc/reported)"] = f"{calc:,.2f} / {reported:,.2f}"
        if abs(calc - reported) >= _GROSS_TOLERANCE:
            logger.warning(
                f"⚠️ Reserved Gross does not match the totals row — "
                f"extracted {calc:,.2f}, report states {reported:,.2f}"
            )

    # --- Cancelled block: reported for visibility, never mapped or validated ---
    if "cancelled_tickets" in columns:
        metrics["Cancelled Tickets (reported)"] = (
            f"{_to_int(_cell_text(totals_row, columns, 'cancelled_tickets')):,}"
        )
    if "cancelled_price" in columns:
        metrics["Cancelled Value (reported)"] = (
            f"{_to_float(_cell_text(totals_row, columns, 'cancelled_price')):,.2f}"
        )

    metrics["All-block cross-check breaks"] = _check_all_block(
        data_rows, columns, logger
    )

    if failures:
        return ValidationResult(
            status="FAILED", message=" | ".join(failures), metrics=metrics
        )

    return ValidationResult(
        status="PASSED",
        message=(
            f"Extracted {len(extracted)} performances. Tickets and gross match "
            f"the report's totals row."
        ),
        metrics=metrics,
    )


# ---------------------------------------------------------------------------
# Public parser — this is the entry point called by the processing engine
# ---------------------------------------------------------------------------

@task(name="Parse Bodyguard Bulgaria Sales Report")
def the_bodyguard_tour_bulgaria_sofia(file_path: str) -> tuple:
    """
    Parse a The Bodyguard 2025 (Bulgaria / Sofia) HTML sales report.

    Returns:
        records           : list of dicts, one per performance, keyed to the
                            target structure
        validation_result : ValidationResult with status PASSED or FAILED
    """
    logger = get_run_logger()
    filename = os.path.basename(file_path)
    logger.info(f"📂 Opening: {filename}")

    table = _load_table(file_path)
    header_rows, data_rows, totals_row = _split_sections(table)

    labels = _flatten_header(header_rows)
    columns = _map_columns(labels)

    if not _REQUIRED_COLUMNS.issubset(columns):
        found = [l for l in labels if l]
        raise ValueError(
            f"Could not resolve the required columns in {filename}. Needed "
            f"Date, Sales tickets and Sales Price. Header labels found: {found}"
        )

    resolved = {k: v for k, v in sorted(columns.items(), key=lambda kv: kv[1])}
    logger.info(f"   Columns resolved: {resolved}")

    optional = [
        name
        for name, key in (
            ("Reservations tickets", "reserved_tickets"),
            ("Reservations Price", "reserved_gross"),
            ("Invitations tickets", "comps"),
        )
        if key not in columns
    ]
    if optional:
        logger.warning(
            f"⚠️ Optional column(s) not found, will be written as 0: "
            f"{', '.join(optional)}"
        )

    if not data_rows:
        raise ValueError(
            f"No performance rows found in {filename}. Expected rows starting "
            f"with a dd/mm/yyyy date."
        )
    if totals_row is None:
        raise ValueError(
            f"Totals row not found in {filename}. Expected a row marked "
            f"'Σύνολα' (or Totals). The report may be incomplete or the format "
            f"has changed."
        )

    logger.info(f"   Found {len(data_rows)} performance rows")

    extracted = [_extract_record(cells, columns) for cells in data_rows]
    validation_result = _validate(
        extracted, data_rows, totals_row, columns, logger
    )

    if validation_result.status == "PASSED":
        logger.info(f"✅ {validation_result.message}")
    else:
        logger.error(f"❌ Validation failed: {validation_result.message}")
        raise ValueError(f"Validation Failed: {validation_result.message}")

    return extracted, validation_result
