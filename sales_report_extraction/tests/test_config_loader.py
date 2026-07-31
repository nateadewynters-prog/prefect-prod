import pytest
from unittest.mock import patch, MagicMock

from src.config_loader import SharePointRuleLoader, _to_bool, _to_id_str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _resp(payload):
    """A fake requests.Response with .json() and a no-op raise_for_status()."""
    r = MagicMock()
    r.json.return_value = payload
    r.raise_for_status.return_value = None
    return r


def _item(fields, item_id="1"):
    """Graph returns list items as {'id': ..., 'fields': {...}}."""
    return {"id": item_id, "fields": fields}


@pytest.fixture
def loader(monkeypatch):
    """A loader with env vars set and MSAL bypassed (so no real auth happens)."""
    for k, v in {
        "AZURE_TENANT_ID": "t",
        "AZURE_CLIENT_ID": "c",
        "AZURE_CLIENT_SECRET": "s",
        "SHAREPOINT_SALES_REPORTING_SITE_ID": "site",
    }.items():
        monkeypatch.setenv(k, v)
    l = SharePointRuleLoader("My List")
    l._get_token = lambda: "fake-token"   # skip MSAL entirely
    return l


def _run_load(loader, items, list_display="My List", list_id="LID"):
    """
    Drive load_rules() with a single page of `items`, mocking the two Graph
    calls (resolve list id, then fetch items).
    """
    def fake_get(url, headers=None):
        if url.endswith("/lists"):
            return _resp({"value": [{"displayName": list_display, "id": list_id}]})
        if f"/lists/{list_id}/items" in url:
            return _resp({"value": items})
        raise AssertionError(f"Unexpected URL requested: {url}")

    with patch("src.config_loader.requests.get", side_effect=fake_get):
        return loader.load_rules()


# ---------------------------------------------------------------------------
# _to_bool
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value,expected", [
    (True, True),
    (False, False),
    (None, False),          # blank / missing cell -> inactive
    ("true", True),
    ("Yes", True),
    ("1", True),
    ("false", False),
    ("no", False),
    ("", False),
    ("false-ish", False),   # the classic trap: bool("false") is True, we must not
])
def test_to_bool(value, expected):
    assert _to_bool(value) is expected


# ---------------------------------------------------------------------------
# _to_id_str  (Number columns hand back floats like 180.0)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value,expected", [
    (180.0, "180"),         # Number column
    (287, "287"),           # plain int
    ("41", "41"),           # Text column
    ("  71  ", "71"),       # trimmed
    (None, ""),             # blank
])
def test_to_id_str(value, expected):
    assert _to_id_str(value) == expected


# ---------------------------------------------------------------------------
# load_rules: parser rule
# ---------------------------------------------------------------------------
def test_parser_rule_is_shaped_correctly(loader):
    fields = {
        "Title": "Jesus Christ Superstar",
        "VenueName": "Manila",
        "SenderDomain": "info@ticketek.com.sg",
        "SubjectKeyword": "Jesus Christ Superstar (Sales Summary)",
        "AttachmentType": ".xls",
        "ReportType": "Advance",
        "ShowID": 287.0,        # Number -> float
        "VenueID": "220",       # Text
        "DocumentID": 504.0,    # Number -> float
        "Timezone": "Asia/Manila",
        "SalesDayOffsetHours": 2.0,
        "ParserModule": "src.parsers.ticketek_event_settlement_excel_parser",
        "ParserFunction": "extract_settlement_data",
        "NeedsLookup": True,
        "Active": True,
    }
    rules = _run_load(loader, [_item(fields)])

    assert len(rules) == 1
    rule = rules[0]

    assert rule["rule_name"] == "JESUS_CHRIST_SUPERSTAR_MANILA_ADVANCE"
    assert rule["active"] is True
    assert rule["processing"] == {
        "parser_module": "src.parsers.ticketek_event_settlement_excel_parser",
        "parser_function": "extract_settlement_data",
        "needs_lookup": True,
    }
    # IDs coerced to clean strings (no trailing .0)
    assert rule["metadata"]["show_id"] == "287"
    assert rule["metadata"]["venue_id"] == "220"
    assert rule["metadata"]["document_id"] == "504"
    assert rule["metadata"]["sales_day_offset_hours"] == 2


# ---------------------------------------------------------------------------
# load_rules: passthrough rule
# ---------------------------------------------------------------------------
def test_passthrough_rule_when_parser_columns_blank(loader):
    fields = {
        "Title": "Mamma Mia!",
        "VenueName": "Zurich",
        "SenderDomain": "noreply.reporting@eventim.de",
        "SubjectKeyword": "MAMMA MIA! 2027 - Sales Report",
        "AttachmentType": ".xlsx",
        "AttachmentSource": "html_link",
        "ReportType": "Advance",
        "ShowID": 234.0,
        "VenueID": "100",
        "DocumentID": 486.0,
        "Timezone": "Europe/Zurich",
        "Active": True,
        # ParserModule / ParserFunction deliberately absent
    }
    rules = _run_load(loader, [_item(fields)])

    assert len(rules) == 1
    rule = rules[0]
    assert rule["rule_name"] == "MAMMA_MIA!_ZURICH_ADVANCE"
    assert rule["processing"] == {"passthrough_only": True}
    # attachment_source is only included when set
    assert rule["match_criteria"]["attachment_source"] == "html_link"


# ---------------------------------------------------------------------------
# load_rules: the Test column
# ---------------------------------------------------------------------------
def _test_column_fields(**overrides):
    fields = {
        "Title": "Mamma Mia!", "VenueName": "Zurich",
        "SenderDomain": "x@y.com", "SubjectKeyword": "Report",
        "AttachmentType": ".xlsx", "ReportType": "Advance",
        "ShowID": 234.0, "VenueID": "100", "DocumentID": 486.0,
        "Active": True,
    }
    fields.update(overrides)
    return fields


def test_test_flag_is_read_when_ticked(loader):
    rules = _run_load(loader, [_item(_test_column_fields(Test=True))])
    assert rules[0]["test"] is True


# A missing column, a None value and an unticked box must all mean "live".
@pytest.mark.parametrize("value", [False, None, "", "   ", "No", "false"])
def test_test_flag_defaults_to_false(loader, value):
    rules = _run_load(loader, [_item(_test_column_fields(Test=value))])
    assert rules[0]["test"] is False


def test_test_flag_absent_column_defaults_to_false(loader):
    """The 19 live rules predate this column, so their rows have no Test key."""
    fields = _test_column_fields()          # Test deliberately absent
    assert "Test" not in fields
    rules = _run_load(loader, [_item(fields)])
    assert rules[0]["test"] is False


def test_attachment_source_omitted_when_blank(loader):
    fields = {
        "Title": "Show", "VenueName": "Venue",
        "SenderDomain": "x@y.com", "SubjectKeyword": "Report",
        "AttachmentType": ".xlsx", "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0,
        "Active": True,
    }
    rules = _run_load(loader, [_item(fields)])
    assert "attachment_source" not in rules[0]["match_criteria"]


# ---------------------------------------------------------------------------
# load_rules: skip behaviour
# ---------------------------------------------------------------------------
def test_half_configured_parser_rule_is_skipped(loader):
    """ParserModule set but ParserFunction missing -> skip, don't passthrough."""
    fields = {
        "Title": "Show", "VenueName": "Venue",
        "SenderDomain": "x@y.com", "SubjectKeyword": "Report",
        "AttachmentType": ".xlsx", "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0,
        "Active": True,
        "ParserModule": "src.parsers.something",
        # ParserFunction missing
    }
    fake_logger = MagicMock()
    with patch("src.config_loader.get_universal_logger", return_value=fake_logger):
        rules = _run_load(loader, [_item(fields)])

    assert rules == []
    assert fake_logger.warning.called


def test_row_with_missing_show_or_venue_is_skipped(loader):
    good = {
        "Title": "Good Show", "VenueName": "Good Venue",
        "SenderDomain": "x@y.com", "SubjectKeyword": "Report",
        "AttachmentType": ".xlsx", "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0, "Active": True,
    }
    blank_venue = {**good, "Title": "Has Show", "VenueName": ""}
    blank_show = {**good, "Title": "", "VenueName": "Has Venue"}

    rules = _run_load(loader, [
        _item(good, "1"), _item(blank_venue, "2"), _item(blank_show, "3"),
    ])
    assert len(rules) == 1
    assert rules[0]["rule_name"] == "GOOD_SHOW_GOOD_VENUE_ADVANCE"


def test_duplicate_rule_name_is_skipped(loader):
    """Two rows generating the same rule_name -> keep the first, skip the second."""
    base = {
        "VenueName": "Zurich", "SenderDomain": "x@y.com",
        "SubjectKeyword": "Report", "AttachmentType": ".xlsx",
        "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0, "Active": True,
    }
    # Only the letter case differs, but rule_name is uppercased, so these clash.
    first = {**base, "Title": "Mamma Mia!"}
    second = {**base, "Title": "mamma mia!"}

    fake_logger = MagicMock()
    with patch("src.config_loader.get_universal_logger", return_value=fake_logger):
        rules = _run_load(loader, [_item(first, "1"), _item(second, "2")])

    assert len(rules) == 1
    assert rules[0]["_sp_item_id"] == "1"   # the row that got there first wins
    assert fake_logger.error.called


def test_inactive_row_does_not_block_an_active_row_with_the_same_name(loader):
    """A parked row must not reserve a name: main.py never searches it anyway."""
    base = {
        "VenueName": "Zurich", "SenderDomain": "x@y.com",
        "SubjectKeyword": "Report", "AttachmentType": ".xlsx",
        "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0,
        "Title": "Mamma Mia!",
    }
    # The old, deactivated row is returned first because it has the lower item id.
    rules = _run_load(loader, [
        _item({**base, "Active": False}, "10"),
        _item({**base, "Active": True}, "42"),
    ])

    assert [r["_sp_item_id"] for r in rules] == ["10", "42"]


# A missing column and a None value take the same path, because the loader reads
# every field with (f.get(...) or "").strip().
@pytest.mark.parametrize("blank_value", ["", "   ", None])
def test_blank_subject_keyword_is_skipped(loader, blank_value):
    """An empty Graph $search query 400s and would abort the whole fetch task."""
    fields = {
        "Title": "Show", "VenueName": "Venue",
        "SenderDomain": "x@y.com", "AttachmentType": ".xlsx",
        "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0, "Active": True,
        "SubjectKeyword": blank_value,
    }

    fake_logger = MagicMock()
    with patch("src.config_loader.get_universal_logger", return_value=fake_logger):
        rules = _run_load(loader, [_item(fields, "7")])

    assert rules == []
    assert fake_logger.error.called
    # The message must name the row so ops can find it in SharePoint.
    assert "item id 7" in fake_logger.error.call_args[0][0]


@pytest.mark.parametrize("blank_value", ["", "   ", None])
def test_blank_sender_domain_is_skipped(loader, blank_value):
    """A blank domain matches every sender, so the rule must not load at all."""
    fields = {
        "Title": "Show", "VenueName": "Venue",
        "SubjectKeyword": "Report", "AttachmentType": ".xlsx",
        "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0, "Active": True,
        "SenderDomain": blank_value,
    }

    fake_logger = MagicMock()
    with patch("src.config_loader.get_universal_logger", return_value=fake_logger):
        rules = _run_load(loader, [_item(fields, "8")])

    assert rules == []
    assert fake_logger.error.called
    # The message must name the row so ops can find it in SharePoint.
    assert "item id 8" in fake_logger.error.call_args[0][0]


def test_inactive_row_with_blank_fields_still_loads(loader):
    """The blank-cell guards only protect a live run, so a parked row is left alone."""
    fields = {
        "Title": "Next Season Show", "VenueName": "Venue",
        "AttachmentType": ".xlsx", "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0,
        "Active": False,
        # SubjectKeyword / SenderDomain not filled in yet
    }
    rules = _run_load(loader, [_item(fields)])

    assert len(rules) == 1
    assert rules[0]["active"] is False
    # The blank cell is carried through untouched rather than rejected.
    assert rules[0]["match_criteria"]["subject_keyword"] == ""


def test_inactive_rule_still_loads_but_marked_inactive(loader):
    """Loader returns all rows; main.py is responsible for skipping inactive ones."""
    fields = {
        "Title": "Show", "VenueName": "Venue",
        "SenderDomain": "x@y.com", "SubjectKeyword": "Report",
        "AttachmentType": ".xlsx", "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0,
        "Active": False,
    }
    rules = _run_load(loader, [_item(fields)])
    assert len(rules) == 1
    assert rules[0]["active"] is False


# ---------------------------------------------------------------------------
# load_rules: pagination
# ---------------------------------------------------------------------------
def test_pagination_follows_next_link(loader):
    base = {
        "SenderDomain": "x@y.com", "SubjectKeyword": "Report",
        "AttachmentType": ".xlsx", "ReportType": "Advance",
        "ShowID": 1.0, "VenueID": "2", "DocumentID": 3.0, "Active": True,
    }
    page1_item = _item({**base, "Title": "Show A", "VenueName": "V1"}, "1")
    page2_item = _item({**base, "Title": "Show B", "VenueName": "V2"}, "2")

    def fake_get(url, headers=None):
        if url.endswith("/lists"):
            return _resp({"value": [{"displayName": "My List", "id": "LID"}]})
        if "page=2" in url:
            return _resp({"value": [page2_item]})
        if "/lists/LID/items" in url:
            return _resp({
                "value": [page1_item],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/next?page=2",
            })
        raise AssertionError(f"Unexpected URL: {url}")

    with patch("src.config_loader.requests.get", side_effect=fake_get):
        rules = loader.load_rules()

    names = {r["rule_name"] for r in rules}
    assert names == {"SHOW_A_V1_ADVANCE", "SHOW_B_V2_ADVANCE"}


# ---------------------------------------------------------------------------
# load_rules: list not found
# ---------------------------------------------------------------------------
def test_list_not_found_raises(loader):
    def fake_get(url, headers=None):
        if url.endswith("/lists"):
            return _resp({"value": [{"displayName": "Some Other List", "id": "X"}]})
        raise AssertionError("Should not reach items call")

    with patch("src.config_loader.requests.get", side_effect=fake_get):
        with pytest.raises(ValueError, match="not found"):
            loader.load_rules()