"""
Every parser keeps its own copy of the money/count helpers, deliberately: the
parsers are meant to be readable and fixable in isolation, without anyone
needing to understand a shared module.

The trade-off is that the copies can drift apart. This file is the guard against
that - it runs one table of cases against every parser's own pair, so a change
made to one copy and not the others shows up as a failing test rather than as
two parsers disagreeing about what a cell is worth.
"""
import importlib

import pytest

# (module, float helper name, int helper name). Each parser names them
# differently, which is fine - the behaviour is what has to match.
PARSERS = [
    ("ticketek_event_settlement_excel_parser", "clean_currency", "clean_int"),
    ("malvern_theatre_contractual_report_pdf_parser", "parse_currency", "parse_int"),
    ("nederlandaer_devil_wears_prada_cumulative_extraction_pdf", "parse_currency", "parse_int"),
    ("gmg_hk_jesus_christ_superstar_xlsx_parser", "clean_to_float", "clean_to_int"),
    ("chicago_tour_lugano_parser", "_to_float", "_to_int"),
    ("the_bodyguard_bulgaria_parser", "_to_float", "_to_int"),
]

# the_bodyguard_tour_bulgaria_sofia.py is owned by another user with an ACL that
# makes it read-only for this account, so it still returns 0.0 on an unreadable
# cell. It is listed here as an expected failure rather than left out, so the
# gap stays visible until someone with write access closes it.
SOFIA = ("the_bodyguard_tour_bulgaria_sofia", "_to_float", "_to_int")


def _helpers(entry):
    module_name, float_name, int_name = entry
    module = importlib.import_module(f"src.parsers.{module_name}")
    return getattr(module, float_name), getattr(module, int_name)


def _ids(entries):
    return [e[0].split("_")[0] for e in entries]


# ---------------------------------------------------------------------------
# Values every copy must parse identically
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("entry", PARSERS + [SOFIA], ids=_ids(PARSERS + [SOFIA]))
@pytest.mark.parametrize("value,expected", [
    (1234.56, 1234.56),
    (1234, 1234.0),
    (0, 0.0),
    (-595.0, -595.0),
    ("1,234.56", 1234.56),          # the format these feeds normally use
    ("1,080", 1080.0),              # lone thousands comma
    ("12,345,678", 12345678.0),
    ("491468.50", 491468.50),       # totals rows are often unseparated
    ("-595.00", -595.00),
    # European conventions. Getting these wrong is a 100x error, which is the
    # whole reason these helpers do not just strip every comma.
    ("1.234,56", 1234.56),          # dot thousands, comma decimal
    ("1234,50", 1234.50),           # lone decimal comma
    ("1 234,56", 1234.56),          # space thousands + decimal comma
    ("1 234.56", 1234.56),          # space thousands + decimal dot
])
def test_every_parser_parses_the_same_values(entry, value, expected):
    to_float, _ = _helpers(entry)
    assert to_float(value) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Empty cells are legitimate everywhere and must stay 0
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("entry", PARSERS + [SOFIA], ids=_ids(PARSERS + [SOFIA]))
@pytest.mark.parametrize("value", [None, "", "   ", "-", "."])
def test_blank_and_dash_mean_zero(entry, value):
    """These reports all have empty cells, and a dash is how they write nothing."""
    to_float, to_int = _helpers(entry)
    assert to_float(value) == 0.0
    assert to_int(value) == 0


# ---------------------------------------------------------------------------
# The point of the change: an unreadable cell must RAISE, not become 0.0
# ---------------------------------------------------------------------------
_SOFIA_XFAIL = pytest.param(
    SOFIA,
    marks=pytest.mark.xfail(
        reason="read-only for this account (ACL grants write to alexc/fij only); "
               "still returns 0.0 on an unreadable cell",
        strict=True,
    ),
    id="sofia",
)


@pytest.mark.parametrize("entry", PARSERS + [_SOFIA_XFAIL], ids=_ids(PARSERS) + [None])
@pytest.mark.parametrize("value", [
    "AUD 1,234.56",       # an unknown currency label
    "1,234.56 CR",        # a credit suffix
    "(595.00)",           # a bracketed negative
    "1.234.567",          # ambiguous: several dots, no comma
    "not a number",
    "12-34",
    True,                 # bool is an int subclass; would have become 1.0
])
def test_unreadable_cells_raise(entry, value):
    to_float, _ = _helpers(entry)
    with pytest.raises(ValueError):
        to_float(value)


@pytest.mark.parametrize("entry", PARSERS, ids=_ids(PARSERS))
def test_the_error_names_the_offending_value(entry):
    """The message has to be enough to debug from a Prefect log line alone."""
    to_float, _ = _helpers(entry)
    with pytest.raises(ValueError, match="AUD 1,234.56"):
        to_float("AUD 1,234.56")


@pytest.mark.parametrize("entry", PARSERS, ids=_ids(PARSERS))
def test_a_format_change_no_longer_reconciles_against_itself(entry):
    """
    The old helpers returned 0.0 on failure. Each parser reads its data rows and
    the report's own totals row through the same helper, so a format change
    zeroed both and the totals check passed on 0 == 0. Both sides must raise.
    """
    to_float, _ = _helpers(entry)
    for cell in ("AUD 1,234.56", "AUD 1,234.56"):
        with pytest.raises(ValueError):
            to_float(cell)


# ---------------------------------------------------------------------------
# Counts
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("entry", PARSERS + [SOFIA], ids=_ids(PARSERS + [SOFIA]))
@pytest.mark.parametrize("value,expected", [
    (1080, 1080),
    (1080.0, 1080),
    ("1,080", 1080),
    ("1080.0", 1080),
    ("1 080", 1080),
    (1079.9999, 1080),      # rounds rather than truncates
    ("-5", -5),
    # chicago's old _to_int stripped the decimal point with a regex, so this
    # string became 108050 - a 100x error.
    ("1,080.50", 1080),
])
def test_every_parser_counts_the_same(entry, value, expected):
    _, to_int = _helpers(entry)
    assert to_int(value) == expected
