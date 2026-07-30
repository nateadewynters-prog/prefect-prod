"""Minimum viable suite for the DocID tool.

Tests 1 and 2 exist specifically because commit abaa21e shipped a half-finished
cache migration to production: CACHE_LOCK/DOCID_CACHE were referenced but bound
nowhere, so every /api/docids request raised NameError. `import app` did not
catch it — the undefined names were inside function bodies — and GET / kept
working, so the page looked fine. Only calling the API fails.
"""
import pytest

from conftest import FakeOdbcError, ROW_A, ROW_B


# --- 1. Smoke: every route responds without a server error ------------------

@pytest.mark.parametrize("method,route", [
    ("get", "/"),
    ("get", "/health"),
    ("get", "/api/docids"),
    ("post", "/api/docids/refresh"),
])
def test_routes_do_not_500(client, fake_sql, method, route):
    fake_sql([ROW_A, ROW_B])
    response = getattr(client, method)(route)
    assert response.status_code < 500, response.get_data(as_text=True)


def test_index_contains_element_ids_the_javascript_depends_on(client):
    """The inline JS getElementById()s these; a rename silently half-breaks it."""
    html = client.get("/").get_data(as_text=True)
    for element_id in (
        "searchInput", "showFilter", "theatreFilter", "docTypeFilter",
        "docid-table", "refreshBtn", "refreshIcon", "errorBanner",
    ):
        assert f'id="{element_id}"' in html


# --- 2. Response shape contract with the frontend ---------------------------

def test_api_returns_array_of_six_element_lists(client, fake_sql):
    """docid.html indexes row[0]..row[5] positionally and calls response.map."""
    fake_sql([ROW_A, ROW_B])
    payload = client.get("/api/docids").get_json()

    assert isinstance(payload, list)
    assert len(payload) == 2
    for row in payload:
        assert isinstance(row, list)
        assert len(row) == 6


# --- 3. Cold cache populates and serves -------------------------------------

def test_cold_cache_fetches_and_serves_rows(client, fake_sql, app_module):
    fake_sql([ROW_A, ROW_B])
    payload = client.get("/api/docids").get_json()

    assert payload[0] == ["Hamilton", "Victoria Palace", "Contract", 1, 10, 100]
    assert app_module.pyodbc.connect.call_count == 1


def test_warm_cache_does_not_hit_sql_again(client, fake_sql, app_module):
    fake_sql([ROW_A])
    client.get("/api/docids")
    client.get("/api/docids")
    assert app_module.pyodbc.connect.call_count == 1


# --- 4. Dedupe --------------------------------------------------------------

def test_identical_id_triples_collapse_to_one_row(client, fake_sql):
    fake_sql([ROW_A, ROW_A])
    assert len(client.get("/api/docids").get_json()) == 1


def test_same_triple_different_names_keeps_only_the_first(client, fake_sql):
    """Pins current behaviour. The dedupe key excludes the display names, so
    the second row's DocumentName is dropped — see ANALYSIS.md H1, which is
    pending a decision on whether that is the wanted behaviour."""
    variant = ("Hamilton", "Victoria Palace", "Rider", 1, 10, 100)
    fake_sql([ROW_A, variant])

    payload = client.get("/api/docids").get_json()
    assert len(payload) == 1
    assert payload[0][2] == "Contract"


def test_distinct_triples_are_all_kept(client, fake_sql):
    fake_sql([ROW_A, ROW_B])
    assert len(client.get("/api/docids").get_json()) == 2


# --- 5. ID coercion ---------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    (None, 0),
    (0, 0),
    ("", 0),
    ("  ", 0),      # whitespace-only is treated as missing; it used to raise
    ("42", 42),
    (" 42 ", 42),
])
def test_id_coercion(app_module, raw, expected):
    assert app_module._as_int(raw) == expected


@pytest.mark.parametrize("raw", ["ABC", "12.7"])
def test_unparseable_ids_raise_so_the_row_can_be_skipped(app_module, raw):
    """Genuinely non-numeric values must raise, so the caller skips that row
    rather than silently publishing a wrong ID."""
    with pytest.raises(ValueError):
        app_module._as_int(raw)


def test_one_malformed_row_does_not_discard_the_good_rows(client, fake_sql):
    """Regression: a single dirty cell used to raise out of the parse loop and
    fail the entire refresh, taking the tool offline until the source was fixed."""
    bad = ("Wicked", "Apollo Victoria", "Contract", "ABC", 30, 300)
    fake_sql([ROW_A, bad, ROW_B])

    payload = client.get("/api/docids").get_json()
    assert len(payload) == 2
    assert [row[0] for row in payload] == ["Hamilton", "Mamma Mia!"]


# --- 6. TTL, failure fallback and the empty-result guard --------------------

def test_expired_cache_triggers_exactly_one_refetch(client, fake_sql, app_module):
    fake_sql([ROW_A])
    client.get("/api/docids")

    app_module.CACHE_TTL_SECONDS = -1  # force expiry
    app_module.FAILED_REFRESH_BACKOFF_SECONDS = 0
    client.get("/api/docids")

    assert app_module.pyodbc.connect.call_count == 2


def test_failed_refresh_serves_stale_data_rather_than_failing(client, fake_sql, app_module):
    fake_sql([ROW_A])
    client.get("/api/docids")

    app_module.CACHE_TTL_SECONDS = -1
    app_module.FAILED_REFRESH_BACKOFF_SECONDS = 0
    fake_sql([], error=FakeOdbcError("server unreachable"))

    response = client.get("/api/docids")
    assert response.status_code == 200
    assert response.get_json()[0][0] == "Hamilton"


def test_failure_with_no_cache_returns_an_error_not_an_empty_table(client, fake_sql):
    fake_sql([], error=FakeOdbcError("server unreachable"))
    response = client.get("/api/docids")

    assert response.status_code == 503
    assert "error" in response.get_json()


def test_backoff_suppresses_repeated_sql_attempts(client, fake_sql, app_module):
    """An unreachable SQL server must not be retried on every request."""
    fake_sql([], error=FakeOdbcError("server unreachable"))
    for _ in range(3):
        client.get("/api/docids")

    assert app_module.pyodbc.connect.call_count == 1


def test_empty_result_does_not_overwrite_a_good_cache(client, fake_sql, app_module):
    fake_sql([ROW_A])
    client.get("/api/docids")

    app_module.CACHE_TTL_SECONDS = -1
    app_module.FAILED_REFRESH_BACKOFF_SECONDS = 0
    fake_sql([])  # query succeeds, returns nothing

    assert client.get("/api/docids").get_json()[0][0] == "Hamilton"


def test_refresh_endpoint_does_not_leak_the_sql_error_to_the_client(client, fake_sql):
    fake_sql([], error=FakeOdbcError("Login failed for user 'bilogin' on sql-prod-01"))
    body = client.post("/api/docids/refresh").get_json()

    assert "bilogin" not in body["message"]
    assert "sql-prod-01" not in body["message"]


# --- 7. SQLite cache round-trip --------------------------------------------

def test_write_then_read_preserves_list_shape(app_module):
    rows = [["Hamilton", "Victoria Palace", "Contract", 1, 10, 100]]
    app_module.write_cache(rows)

    cached, updated_at = app_module.read_cache()
    assert cached == rows
    assert updated_at is not None


def test_second_write_upserts_to_exactly_one_row(app_module):
    app_module.write_cache([["a", "b", "c", 1, 2, 3]])
    app_module.write_cache([["d", "e", "f", 4, 5, 6]])

    cached, _ = app_module.read_cache()
    assert cached == [["d", "e", "f", 4, 5, 6]]

    with app_module.closing(app_module.get_db_conn()) as conn:
        assert conn.execute("SELECT COUNT(*) FROM docid_cache").fetchone()[0] == 1


def test_init_db_is_idempotent(app_module):
    app_module.init_db()
    app_module.init_db()
    assert app_module.read_cache() == ([], None)


def test_corrupt_cache_is_discarded_rather_than_raising(app_module):
    """Otherwise the failure is self-perpetuating: read_cache runs before the
    code can decide to refresh, so the repair path is never reached."""
    app_module.write_cache([["a", "b", "c", 1, 2, 3]])
    with app_module.closing(app_module.get_db_conn()) as conn, conn:
        conn.execute("UPDATE docid_cache SET payload = 'not json' WHERE id = 1")

    assert app_module.read_cache() == ([], None)


def test_health_reports_cache_state(client, fake_sql):
    fake_sql([ROW_A])
    client.get("/api/docids")

    body = client.get("/health").get_json()
    assert body["status"] == "ok"
    assert body["rows"] == 1
    assert body["stale"] is False
