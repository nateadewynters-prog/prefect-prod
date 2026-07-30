import json
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone

import pyodbc
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template

load_dotenv()
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)

# --- DOCID CACHE ENGINE ---------------------------------------------------
# The cache lives in a small SQLite file, NOT in a Python dict.
#
# Why: this app runs under `gunicorn --workers 4`, i.e. four separate OS
# processes. A module-level dict is private to one process, so "Resync" only
# ever refreshed whichever worker happened to answer that POST, and the
# browser's follow-up GET /api/docids could easily land on one of the three
# workers still holding stale data. One SQLite file is shared by all four, so
# they always agree.
#
# Matches the SQLite pattern already used elsewhere in this repo
# (powerbi_refresher, powerbi_media_report_dispatcher). Stdlib only.
#
# CAVEAT: the file sits inside the container and is NOT bind-mounted, so it is
# lost on redeploy. This is not free: an empty cache and an expired one behave
# identically only while SQL is reachable. An expired cache still holds rows,
# so a SQL outage degrades to serving stale-but-usable IDs; an empty one has
# nothing to fall back on and returns 500. Deploys and infrastructure work
# tend to land in the same change window, so the fallback is unavailable
# exactly when it is most likely to be needed. Bind-mounting the file (as the
# sibling dispatchers do with ./data) would fix this. See ANALYSIS.md H8/L5.
CACHE_TTL_SECONDS = 1800  # 30 minutes

# After a failed refresh, wait this long before trying SQL again. Without it,
# every request retries on failure: with 4 sync workers and a 10s login
# timeout, an unreachable SQL server takes the whole service down rather than
# degrading it, because even GET / queues behind the blocked workers.
FAILED_REFRESH_BACKOFF_SECONDS = 60

# Bound query execution as well as login. pyodbc's connect(timeout=) sets
# SQL_ATTR_LOGIN_TIMEOUT only; without conn.timeout a slow SELECT hangs the
# worker until gunicorn's --timeout 120 SIGKILLs it mid-refresh.
SQL_LOGIN_TIMEOUT_SECONDS = 10
SQL_QUERY_TIMEOUT_SECONDS = 30

REQUIRED_ENV_VARS = ("SQL_SERVER", "SQL_USERNAME_BILOGIN", "SQL_PASSWORD_BILOGIN")

DB_PATH = os.getenv(
    "DOCID_CACHE_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "docid_cache.db"),
)


def get_db_conn() -> sqlite3.Connection:
    # timeout=10: if another worker is mid-write, wait for it instead of
    # raising "database is locked".
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the cache tables if they do not exist yet.

    Note `closing(...)` as well as `with conn`: sqlite3's connection context
    manager commits the transaction but does NOT close the connection, so
    without this every call leaks a file descriptor.
    """
    with closing(get_db_conn()) as conn, conn:
        # WAL so a writing worker does not block the other three readers.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS docid_cache ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "  # single row, always id=1
            "payload TEXT NOT NULL, "                  # the table rows, as JSON
            "updated_at DATETIME NOT NULL)"
        )
        # Tracked separately from docid_cache because a failed refresh may have
        # no payload to write alongside it — including on a cold start.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS refresh_state ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "
            "last_attempt_at DATETIME NOT NULL)"
        )


def read_cache() -> tuple[list, datetime | None]:
    """Return (rows, updated_at). updated_at is None when nothing is cached.

    A corrupt payload is discarded rather than raised. read_cache() runs before
    the code can decide to refresh, so letting a bad row raise would make the
    failure self-perpetuating — the repair path would never be reached and the
    tool would stay down until someone deleted the file by hand.
    """
    with closing(get_db_conn()) as conn:
        row = conn.execute(
            "SELECT payload, updated_at FROM docid_cache WHERE id = 1"
        ).fetchone()
    if not row:
        return [], None
    try:
        return json.loads(row["payload"]), datetime.fromisoformat(row["updated_at"])
    except (ValueError, TypeError) as e:
        log.error("Discarding corrupt DocID cache: %s", e)
        clear_cache()
        return [], None


def write_cache(rows: list) -> None:
    """Overwrite the shared cache with a fresh set of rows."""
    with closing(get_db_conn()) as conn, conn:
        conn.execute(
            "INSERT INTO docid_cache (id, payload, updated_at) VALUES (1, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "payload = excluded.payload, updated_at = excluded.updated_at",
            (json.dumps(rows), _now().isoformat(timespec="seconds")),
        )


def clear_cache() -> None:
    """Drop the cached payload so the next request rebuilds it from SQL."""
    with closing(get_db_conn()) as conn, conn:
        conn.execute("DELETE FROM docid_cache WHERE id = 1")


def read_last_attempt() -> datetime | None:
    """Return when a refresh was last attempted, successfully or not."""
    with closing(get_db_conn()) as conn:
        row = conn.execute(
            "SELECT last_attempt_at FROM refresh_state WHERE id = 1"
        ).fetchone()
    if not row:
        return None
    try:
        return datetime.fromisoformat(row["last_attempt_at"])
    except (ValueError, TypeError):
        return None


def write_last_attempt() -> None:
    with closing(get_db_conn()) as conn, conn:
        conn.execute(
            "INSERT INTO refresh_state (id, last_attempt_at) VALUES (1, ?) "
            "ON CONFLICT(id) DO UPDATE SET last_attempt_at = excluded.last_attempt_at",
            (_now().isoformat(timespec="seconds"),),
        )


def _now() -> datetime:
    """UTC, timezone-aware.

    Stored timestamps carry a +00:00 offset so they stay unambiguous if TZ is
    ever set on the container: with naive local time, the BST->GMT shift makes
    now jump backwards an hour and the TTL comparison goes negative, silently
    treating the cache as fresh for an extra hour.
    """
    return datetime.now(timezone.utc)


def _as_int(value) -> int:
    """Coerce a SQL ID to an int, raising ValueError if it cannot be.

    NOTE: NULL, 0 and '' all still collapse to 0, which is the pre-existing
    behaviour and a known data-integrity defect — 0 is a legal ID, so NULL rows
    can merge with real ones and an all-NULL row is published as (0, 0, 0).
    Fixing that changes the API payload and the UI, so it is left for a
    deliberate decision. See ANALYSIS.md C2.
    """
    if value is None:
        return 0
    text = str(value).strip()
    if text == "":
        return 0
    return int(text)


try:
    init_db()
except sqlite3.Error:
    # Log and carry on rather than killing the worker at import: the app can
    # still serve the page, and gunicorn would otherwise enter a restart loop
    # with nothing serving at all. This becomes reachable the moment the
    # container stops running as root or DB_PATH points somewhere unwritable.
    log.exception("Could not initialise the DocID cache at %s", DB_PATH)


def update_docid_cache() -> tuple[bool, str]:
    """Fetch every row from SQL and cache the distinct ones.

    Rows are deduplicated on the FULL displayed tuple, not on the ID triple.
    Keying on (ShowId, TheatreId, DocumentTypeId) alone silently dropped every
    row that shared an ID triple but carried a different ShowName/TheatreName/
    DocumentName — the common case for a documents-and-venues join — and the
    survivor was whichever row SQL Server happened to return first, so the name
    shown against a given ID triple could change between refreshes with no
    change to the data. Users validate their ID choice by reading that name.
    """
    write_last_attempt()

    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        # Without this the connection string reads SERVER=None;UID=None and the
        # failure only ever surfaces as a per-request ODBC error.
        message = f"Missing environment variables: {', '.join(missing)}"
        log.error(message)
        return False, message

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        "DATABASE=TicketingDS;"
        f"UID={os.getenv('SQL_USERNAME_BILOGIN')};"
        f"PWD={os.getenv('SQL_PASSWORD_BILOGIN')};"
        "TrustServerCertificate=yes;"
    )

    try:
        with pyodbc.connect(conn_str, timeout=SQL_LOGIN_TIMEOUT_SECONDS) as conn:
            conn.timeout = SQL_QUERY_TIMEOUT_SECONDS
            cursor = conn.cursor()
            # The query remains the same to get all necessary display and ID columns
            # Total ordering, not just ShowName: a sort on one non-unique
            # column leaves ties in an order SQL Server does not guarantee, so
            # the display order shifted between refreshes for no reason.
            cursor.execute(
                "SELECT ShowName, TheatreName, DocumentName, ShowId, TheatreId, DocumentTypeId "
                "FROM [dbo].[DocumentsAndVenues] "
                "ORDER BY ShowName, TheatreName, DocumentName, ShowId, TheatreId, DocumentTypeId"
            )
            rows = cursor.fetchall()
    except pyodbc.Error as e:
        # Logged in full here; the caller returns a generic message to the
        # browser, because ODBC error text carries the DB username, driver
        # version and sometimes the server hostname.
        log.exception("SQL error fetching DocIDs")
        return False, str(e)

    fresh_data = []
    seen_rows = set()      # Track fully-distinct rows, IDs *and* names
    seen_combos = set()    # ID triples only — counted for visibility, not used to drop
    multi_name_triples = set()
    skipped = 0

    for r in rows:
        # Parse per row: a single malformed value used to raise out of the loop
        # and discard every good row with it, taking the tool offline until
        # someone fixed the source data.
        try:
            show_id = _as_int(r[3])
            theatre_id = _as_int(r[4])
            doc_type_id = _as_int(r[5])
        except (TypeError, ValueError):
            skipped += 1
            log.warning(
                "Skipping DocID row with unparseable IDs: show=%r theatre=%r doctype=%r",
                r[3], r[4], r[5],
            )
            continue

        record = [
            r[0] if r[0] else "N/A",  # ShowName
            r[1] if r[1] else "N/A",  # TheatreName
            r[2] if r[2] else "N/A",  # DocumentName
            show_id,
            theatre_id,
            doc_type_id,
        ]

        combo = (show_id, theatre_id, doc_type_id)
        if combo in seen_combos:
            multi_name_triples.add(combo)
        seen_combos.add(combo)

        # Drop only exact repeats — same IDs *and* same names, which carry no
        # information. Anything that differs in any column is kept.
        key = tuple(record)
        if key not in seen_rows:
            seen_rows.add(key)
            fresh_data.append(record)

    # Refuse to publish an empty result. A successful query returning no rows
    # (permissions change, source mid-reload) would otherwise replace a good
    # cache with an empty one and report success — destroying the stale-data
    # fallback that the failure path depends on.
    if not fresh_data:
        message = "Refusing to overwrite cache: SQL returned no usable rows"
        log.error("%s (%d row(s) read, %d skipped)", message, len(rows), skipped)
        return False, message

    log.info(
        "DocID cache updated: %d row(s) cached from %d read "
        "(%d exact duplicate(s) dropped, %d unparseable skipped); "
        "%d distinct ID triple(s), %d of which appear under more than one name",
        len(fresh_data), len(rows), len(rows) - len(fresh_data) - skipped, skipped,
        len(seen_combos), len(multi_name_triples),
    )
    write_cache(fresh_data)
    return True, "Cache updated successfully."


def _should_refresh(updated_at: datetime | None) -> bool:
    """True when the cache is missing or past its TTL, and not backing off."""
    if updated_at is not None:
        age = (_now() - updated_at).total_seconds()
        if age <= CACHE_TTL_SECONDS:
            return False

    last_attempt = read_last_attempt()
    if last_attempt is not None:
        if _now() - last_attempt < timedelta(seconds=FAILED_REFRESH_BACKOFF_SECONDS):
            return False
    return True


# --- ROUTES ---

@app.route('/')
def index():
    """Serves the frontend UI directly at the root of this microservice."""
    return render_template('docid.html')


@app.route('/health')
def health():
    """Liveness plus cache state, for HEALTHCHECK and for debugging.

    Deliberately reports the cache, not just the process: the failure mode this
    service actually had was "process up, every API request 500ing", which a
    check against / could not see.
    """
    try:
        rows, updated_at = read_cache()
    except sqlite3.Error as e:
        log.exception("Health check could not read cache")
        return jsonify({"status": "error", "detail": str(e)}), 500

    age = None if updated_at is None else int((_now() - updated_at).total_seconds())
    return jsonify({
        "status": "ok",
        "rows": len(rows),
        "updated_at": None if updated_at is None else updated_at.isoformat(),
        "age_seconds": age,
        "stale": age is None or age > CACHE_TTL_SECONDS,
    })


@app.route('/api/docids')
def api_docids():
    """Serves the cached SQL data, refreshing it if older than CACHE_TTL_SECONDS."""
    rows, updated_at = read_cache()

    if _should_refresh(updated_at):
        success, msg = update_docid_cache()
        if success:
            rows, updated_at = read_cache()
        else:
            # Serve stale data rather than nothing, but say so in the log —
            # the browser cannot currently tell fresh data from days-old data.
            log.warning("DocID refresh failed, falling back to cache: %s", msg)

    if updated_at is None:
        # Never successfully populated. This also covers the case where a
        # failed attempt put us in backoff, so no refresh was tried at all —
        # an explicit error beats an empty table the user reads as "no match".
        return jsonify({"error": "Failed to fetch data"}), 503

    return jsonify(rows)


@app.route('/api/docids/refresh', methods=['POST'])
def force_refresh_docids():
    """Forces an immediate refresh of the SQL data."""
    success, _ = update_docid_cache()
    if success:
        return jsonify({"status": "success", "message": "Database resynced!"})
    # Generic message by design: the detail is in the service log.
    return jsonify({
        "status": "error",
        "message": "Refresh failed — check the service logs.",
    }), 500


if __name__ == '__main__':
    # Runs on 8002 internally, which Docker maps to 8003 externally
    app.run(host='0.0.0.0', port=8002)
