import pytest

from src.parsers._common import MoneyParseError, to_float, to_int


# ---------------------------------------------------------------------------
# Values that must parse, and to exactly what
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value,expected", [
    # Plain numbers pass straight through.
    (1234.56, 1234.56),
    (1234, 1234.0),
    (0, 0.0),
    (-595.0, -595.0),
    # The format this feed family normally uses.
    ("1,234.56", 1234.56),
    ("$1,234.56", 1234.56),
    ("£1,234.56", 1234.56),
    ("89,720.00 EUR", 89720.00),
    ("-595.00 EUR", -595.00),
    ("491468.50 €", 491468.50),
    # European conventions. Getting these wrong is a 100x error, which is the
    # whole reason this module exists.
    ("1.234,56", 1234.56),          # dot thousands, comma decimal
    ("1234,50", 1234.50),           # lone decimal comma
    ("1 234,56", 1234.56),          # space thousands + decimal comma
    ("1 234.56", 1234.56),          # space thousands + decimal dot
    ("1'234.56", 1234.56),          # Swiss apostrophe thousands (Lugano export)
    # Lone thousands comma, no decimals.
    ("1,080", 1080.0),
    ("12,345,678", 12345678.0),
])
def test_to_float_parses(value, expected):
    assert to_float(value) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Empty cells are legitimate and must stay 0.0
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value", [None, "", "   ", float("nan"), "-", ".", "-.", "£", "EUR"])
def test_blank_and_dash_mean_zero(value):
    """Every one of these reports has empty cells; a dash is how they write nothing."""
    assert to_float(value) == 0.0
    assert to_int(value) == 0


# ---------------------------------------------------------------------------
# The point of the module: unparseable values must RAISE, not return 0.0
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value", [
    "AUD 1,234.56",       # unknown currency label
    "1,234.56 CR",        # credit suffix
    "(595.00)",           # bracketed negative
    "1.234.567",          # ambiguous: multiple dots, no comma
    "not a number",
    "12-34",
    True,                 # bool is an int subclass; would have become 1.0
    False,
])
def test_unparseable_values_raise(value):
    with pytest.raises(MoneyParseError):
        to_float(value)


def test_the_error_names_the_value_and_the_field():
    """The message has to be enough to debug from a Prefect log line alone."""
    with pytest.raises(MoneyParseError) as exc:
        to_float("AUD 1,234.56", field="Total Gross")

    message = str(exc.value)
    assert "Total Gross" in message
    assert "AUD 1,234.56" in message


def test_money_parse_error_is_a_valueerror():
    """main.py routes ValueError to the data-problem branch, so this must hold."""
    assert issubclass(MoneyParseError, ValueError)


# ---------------------------------------------------------------------------
# Why this matters: a broken helper used to agree with itself
# ---------------------------------------------------------------------------
def test_a_format_change_no_longer_reconciles_against_itself():
    """
    The old helpers returned 0.0 on failure. Parsers read their data rows and
    the report's own totals row through the same helper, so a format change
    zeroed both and abs(0.0 - 0.0) < 1.0 passed. Both sides must now raise.
    """
    data_row_cell = "AUD 1,234.56"
    totals_row_cell = "AUD 1,234.56"

    with pytest.raises(MoneyParseError):
        to_float(data_row_cell)
    with pytest.raises(MoneyParseError):
        to_float(totals_row_cell)


# ---------------------------------------------------------------------------
# to_int
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value,expected", [
    (1080, 1080),
    (1080.0, 1080),
    ("1,080", 1080),
    ("1080.0", 1080),
    ("1 080", 1080),
    # Rounds rather than truncates: a spreadsheet 1079.9999 is 1080 tickets.
    (1079.9999, 1080),
    ("-5", -5),
])
def test_to_int_parses(value, expected):
    assert to_int(value) == expected


def test_to_int_keeps_the_decimal_before_rounding():
    """
    chicago's old _to_int stripped the decimal point with a regex, so the string
    "1,080.50" became 108050 - a 100x error. Routing through to_float fixes it.
    """
    assert to_int("1,080.50") == 1080
