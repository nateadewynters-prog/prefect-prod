"""
Shared number parsing for every parser in this package.

Each parser used to carry its own copy of this - seven near-identical helpers
under four different names (clean_currency, parse_currency, clean_to_float,
_to_float), each accepting a different set of currency symbols. The same cell
therefore produced different numbers depending on which parser read it, and
only one of the seven handled a European decimal comma.

The worse problem was that all seven returned 0.0 when they could not parse a
value. Parsers read their data rows *and* the report's own totals row through
the same helper, so a supplier format change zeroed both sides equally and the
totals reconciliation passed on 0 == 0. Wrong figures - all zeros, or 100x too
large on a space-thousands locale - reached the sales database reporting PASSED,
with no error and no alert.

These helpers raise MoneyParseError instead. A format change now stops that one
report with a message naming the offending value, and the reconciliation is
once again an independent check rather than one that can agree with itself.

Genuinely empty cells still return 0 - every one of these reports has them.
"""
import re

# Currency symbols and unit labels seen across the feeds this package parses,
# plus the Swiss apostrophe thousands separator ("1'234.50" in the Lugano
# export). Longer labels are listed first so "EUR" is removed before "E" could
# ever match part of it.
_CURRENCY_SYMBOLS = ("BGN", "EUR", "GBP", "USD", "CHF", "лв", "€", "£", "$", "'")

# How these feeds write "no value" in a numeric cell.
_MEANS_ZERO = ("", "-", ".", "-.")


class MoneyParseError(ValueError):
    """
    A money or ticket-count cell could not be parsed.

    Deliberately subclasses ValueError, because main.py's failure handler
    already treats a ValueError as a data problem (reported as a parsing
    failure) rather than an unexpected system error, so existing alerting
    keeps working without change.
    """


def _is_blank(value) -> bool:
    """True for the empty cells every one of these reports legitimately has."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    # NaN is the only float not equal to itself. This catches the empty cells
    # pandas hands back from read_excel without needing pandas imported here.
    return isinstance(value, float) and value != value


def _strip_currency(text: str) -> str:
    """Remove currency symbols and all whitespace, leaving digits and separators."""
    clean = str(text)
    for symbol in _CURRENCY_SYMBOLS:
        clean = clean.replace(symbol, "")
    return re.sub(r"\s+", "", clean)


def _normalise_separators(clean: str) -> str:
    """
    Work out what the commas and dots mean and return a plain float string.

    These feeds mix conventions, so guessing wrong is how you get a figure that
    is 100x out:
        "89,720.00"  -> 89720.0   comma thousands, dot decimal
        "1.234,50"   -> 1234.50   European: dot thousands, comma decimal
        "1234,50"    -> 1234.50   lone decimal comma
        "1,080"      -> 1080      lone thousands comma
    """
    has_comma = "," in clean
    has_dot = "." in clean

    if has_comma and has_dot:
        # Whichever separator comes last is the decimal point; the other is a
        # thousands separator.
        if clean.rfind(",") > clean.rfind("."):
            return clean.replace(".", "").replace(",", ".")
        return clean.replace(",", "")

    if has_comma:
        # A lone comma followed by exactly two digits is a decimal comma;
        # stripping it would turn "1234,50" into 123450.
        if re.search(r",\d{2}$", clean):
            return clean.replace(",", ".")
        return clean.replace(",", "")

    return clean


def to_float(value, *, field: str = "") -> float:
    """
    Parse a money cell to float, or raise MoneyParseError.

    Pass `field` (e.g. field="Total Gross") to name the column in the error
    message - worth doing at any call site where the column is known, because
    it turns "could not read a number" into an instantly diagnosable failure.
    """
    if _is_blank(value):
        return 0.0

    # bool is a subclass of int, so it would otherwise silently become 1.0/0.0.
    # A True in a money column means the source layout has shifted.
    if isinstance(value, bool):
        raise MoneyParseError(_problem(value, field, "expected a number, got a boolean"))

    if isinstance(value, (int, float)):
        return float(value)

    clean = _strip_currency(value)
    if clean in _MEANS_ZERO:
        return 0.0

    try:
        return float(_normalise_separators(clean))
    except ValueError:
        raise MoneyParseError(
            _problem(value, field, "the source format has probably changed")
        ) from None


def to_int(value, *, field: str = "") -> int:
    """
    Parse a ticket-count cell to int, or raise MoneyParseError.

    Routes through to_float so "1,080" and "1080.0" both work, then rounds -
    counts are whole numbers, and rounding beats truncating a 1079.9999 that
    came back off a spreadsheet.
    """
    return int(round(to_float(value, field=field)))


def _problem(value, field: str, reason: str) -> str:
    """Build an error message that says what was read and where."""
    where = f" in {field}" if field else ""
    return (
        f"Could not read a number{where} from {value!r} "
        f"({type(value).__name__}): {reason}. "
        f"Check the source file before trusting any figures in it."
    )
