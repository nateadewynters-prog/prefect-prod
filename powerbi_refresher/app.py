"""
Power BI Dataset Refresher — a small internal web tool.

Lets a user pick a Power BI report from a list and trigger a refresh of the
dataset behind it, streaming progress back to the browser over SSE.

Layout of this file (top to bottom):
    1. Config          — environment variables, validated at import
    2. HTTP session    — shared requests.Session with retries
    3. Auth            — MSAL client-credentials token
    4. Database        — SQLite helpers (state, logs, locks, report cache)
    5. Discovery       — ask the Power BI API which reports exist
    6. Refresh         — trigger a refresh and poll it to completion
    7. Routes          — Flask endpoints

Debugging notes:
    * Everything logs to stdout, so `docker logs -f powerbi-refresher` is the
      first place to look.
    * Set LOG_LEVEL=DEBUG in .env for per-request API detail.
    * All timestamps are stored in the database as UTC ISO-8601 strings ending
      in "Z". The browser converts them to local time for display. Never store
      local time — it breaks silently when the clocks change.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import msal
import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, stream_with_context

# Retry is re-exported by requests, so we do not need urllib3 as a direct dependency.
from requests.adapters import HTTPAdapter, Retry

# ---------------------------------------------------------------------------
# 1. CONFIG
# ---------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s [%(threadName)s] %(message)s",
)
log = logging.getLogger("refresher")

# Fail fast on missing credentials. Without this the app starts happily and
# then fails with a confusing "Auth failed: None" the first time someone
# clicks Refresh — by which point they think the tool is broken, not the config.
REQUIRED_ENV_VARS = ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET")
_missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
if _missing:
    raise SystemExit(
        f"Refusing to start — missing environment variable(s): {', '.join(_missing)}. "
        "Check that /opt/prefect/prod/.env exists and is mounted into the container."
    )

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

PBI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
PBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"

# Path to the SQLite file. In production this points at /app/data/, which is a
# mounted volume — otherwise every container rebuild would wipe the refresh
# history. See the powerbi-refresher block in docker-compose.yml.
DB_PATH = os.getenv("DB_PATH", "refresher_state.db")

HTTP_TIMEOUT = 30  # seconds; applied to every single Power BI API call

# These three values form a deliberate hierarchy — keep them in this order:
#   POLL_TIMEOUT_SECONDS  we stop watching a refresh
#   LOCK_TTL_SECONDS      a lock is assumed stale and can be taken over
#   gunicorn --timeout    the worker is killed (set in the Dockerfile)
# If the worker were killed first, the `finally` that releases the lock would
# never run and the dataset would stay locked until someone restarted the
# container. The TTL is the safety net for exactly that case.
POLL_TIMEOUT_SECONDS = int(os.getenv("POLL_TIMEOUT_SECONDS", "1800"))  # 30 min
LOCK_TTL_SECONDS = int(os.getenv("LOCK_TTL_SECONDS", "2100"))  # 35 min

SYNC_RETRY_SECONDS = 300  # after a failed report sync, retry in 5 minutes
LOG_RETENTION_DAYS = 7

app = Flask(__name__)

# SQL snippet for "now, as a UTC ISO-8601 string". Used everywhere so that all
# stored timestamps have an identical fixed-width format, which means plain
# string comparison (`timestamp >= ?`) is also correct chronological ordering.
SQL_UTC_NOW = "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"


def utc_now_iso() -> str:
    """Current UTC time in the same format the database uses."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# 2. HTTP SESSION
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    """
    One shared Session with automatic retries for throttling and transient
    server errors. Power BI returns 429 with a Retry-After header when the
    tenant is being hammered; urllib3 honours that header for us.

    Note: Retry's default `allowed_methods` deliberately excludes POST, so the
    refresh trigger is NEVER retried automatically. That is what we want — a
    retried POST would start a second refresh on the same dataset.
    """
    retry = Retry(
        total=3,
        backoff_factor=1,  # 0s, 1s, 2s between attempts
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


http = build_session()


# ---------------------------------------------------------------------------
# 3. AUTH
# ---------------------------------------------------------------------------
_msal_app = None
_msal_lock = threading.Lock()


def get_msal_app() -> msal.ConfidentialClientApplication:
    """
    Build the MSAL client on first use, not at import.

    Creating it contacts Azure to discover the tenant's OIDC configuration, so
    doing it at import would mean the app cannot even start when the network is
    unavailable — and a container that will not start is much harder to
    diagnose than one that starts and reports an auth error.

    The lock keeps two threads from building it at the same time.
    """
    global _msal_app
    with _msal_lock:
        if _msal_app is None:
            _msal_app = msal.ConfidentialClientApplication(
                CLIENT_ID,
                authority=f"https://login.microsoftonline.com/{TENANT_ID}",
                client_credential=CLIENT_SECRET,
            )
    return _msal_app


def get_token() -> str:
    """
    Fetch an access token. MSAL caches tokens in memory and only calls Azure
    again when the cached one is close to expiry, so calling this often is
    cheap — call it inside long loops rather than holding one token for an
    hour, or a slow refresh will start failing with 401s halfway through.
    """
    result = get_msal_app().acquire_token_for_client(scopes=PBI_SCOPE)
    token = result.get("access_token")
    if not token:
        raise RuntimeError(result.get("error_description") or "unknown MSAL error")
    return token


def pbi_headers() -> dict:
    return {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# 4. DATABASE
# ---------------------------------------------------------------------------
@contextmanager
def db():
    """
    Open a connection, commit on success, roll back on error, always close.

    The `with conn:` block handles the transaction; the `finally` handles the
    connection itself. sqlite3's own context manager does NOT close the
    connection, which is a very easy leak to write by accident.
    """
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db() -> None:
    with db() as conn:
        # WAL lets readers and the writer work at the same time instead of
        # blocking each other; busy_timeout makes a brief clash wait rather
        # than immediately raising "database is locked".
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")

        conn.execute(
            """CREATE TABLE IF NOT EXISTS locks (
                   dataset_id TEXT PRIMARY KEY,
                   is_locked  INTEGER NOT NULL DEFAULT 0,
                   locked_at  TEXT
               )"""
        )
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS logs (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg       TEXT NOT NULL,
                    type      TEXT NOT NULL,
                    timestamp TEXT NOT NULL DEFAULT ({SQL_UTC_NOW})
                )"""
        )
        # The UI only ever reads the last 30 minutes of logs, so without this
        # index that filter becomes a full scan of an ever-growing table.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp)")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS last_refresh (
                   dataset_id   TEXT PRIMARY KEY,
                   refreshed_at TEXT
               )"""
        )
        # The report list is cached here rather than in a module-level variable
        # so that it survives a restart (the UI is populated instantly on boot)
        # and so every process sees the same data.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS reports (
                   report_id      TEXT PRIMARY KEY,
                   report_name    TEXT NOT NULL,
                   workspace_id   TEXT NOT NULL,
                   workspace_name TEXT NOT NULL,
                   dataset_id     TEXT NOT NULL,
                   dashboard_url  TEXT NOT NULL
               )"""
        )
        # Simple key/value store for sync bookkeeping (last_sync_at, last_sync_error).
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")

        # Any lock still set at boot is left over from a previous run.
        conn.execute("UPDATE locks SET is_locked = 0")

    log.info("Database ready at %s", DB_PATH)


def db_log(msg: str, msg_type: str = "info") -> None:
    """Write a line to the shared activity log shown in the UI."""
    with db() as conn:
        conn.execute("INSERT INTO logs (msg, type) VALUES (?, ?)", (msg, msg_type))


def prune_logs() -> None:
    """Drop old log rows. Called after each report sync so it happens daily."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOG_RETENTION_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    with db() as conn:
        before = conn.total_changes
        conn.execute("DELETE FROM logs WHERE timestamp < ?", (cutoff,))
        deleted = conn.total_changes - before
    if deleted:
        log.info("Pruned %d log rows older than %d days", deleted, LOG_RETENTION_DAYS)


def meta_set(key: str, value) -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def meta_get(key: str):
    with db() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


# --- locks -----------------------------------------------------------------
def try_acquire_lock(dataset_id: str) -> bool:
    """
    Take the lock for a dataset, returning True only if we actually got it.

    This is one atomic statement on purpose. Checking "is it locked?" and then
    separately setting the lock leaves a gap where two clicks a few
    milliseconds apart both pass the check and both trigger a refresh.

    A lock older than LOCK_TTL_SECONDS is treated as stale and can be taken
    over — that covers the case where a worker was killed before it could
    release its lock.
    """
    with db() as conn:
        before = conn.total_changes
        conn.execute(
            f"""INSERT INTO locks (dataset_id, is_locked, locked_at)
                VALUES (?, 1, {SQL_UTC_NOW})
                ON CONFLICT(dataset_id) DO UPDATE
                    SET is_locked = 1, locked_at = {SQL_UTC_NOW}
                    WHERE locks.is_locked = 0
                       OR locks.locked_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?)""",
            (dataset_id, f"-{LOCK_TTL_SECONDS} seconds"),
        )
        # total_changes only moves if a row was actually inserted or updated,
        # which tells us whether the WHERE clause above let us in.
        return conn.total_changes > before


def release_lock(dataset_id: str) -> None:
    with db() as conn:
        conn.execute(
            "UPDATE locks SET is_locked = 0, locked_at = NULL WHERE dataset_id = ?",
            (dataset_id,),
        )


def locked_dataset_ids() -> list:
    """Dataset IDs currently refreshing, ignoring locks that have gone stale."""
    with db() as conn:
        rows = conn.execute(
            "SELECT dataset_id FROM locks WHERE is_locked = 1 "
            "AND locked_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?)",
            (f"-{LOCK_TTL_SECONDS} seconds",),
        ).fetchall()
    return [row["dataset_id"] for row in rows]


# --- report cache ----------------------------------------------------------
def save_reports(reports: list) -> None:
    """Replace the cached report list in a single transaction."""
    with db() as conn:
        conn.execute("DELETE FROM reports")
        conn.executemany(
            """INSERT INTO reports
                   (report_id, report_name, workspace_id, workspace_name,
                    dataset_id, dashboard_url)
               VALUES (:report_id, :report_name, :workspace_id, :workspace_name,
                       :dataset_id, :dashboard_url)""",
            reports,
        )


def get_reports() -> list:
    """All cached reports, sorted so the UI grouping is stable between syncs."""
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM reports ORDER BY workspace_name COLLATE NOCASE, "
            "report_name COLLATE NOCASE"
        ).fetchall()
    return [dict(row) for row in rows]


def get_report(report_id: str):
    """One report by its Power BI report ID, or None."""
    with db() as conn:
        row = conn.execute("SELECT * FROM reports WHERE report_id = ?", (report_id,)).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# 5. DISCOVERY
# ---------------------------------------------------------------------------
def fetch_all_workspaces(headers: dict) -> list:
    """
    Fetch every workspace the service principal can see, one page at a time.

    /groups caps how many it returns in a single response, so without paging
    the extra workspaces simply never appear in the UI — and there is no error
    to tell you that happened.
    """
    workspaces = []
    page_size = 100
    skip = 0
    while True:
        resp = http.get(
            f"{PBI_BASE_URL}/groups",
            headers=headers,
            params={"$top": page_size, "$skip": skip},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        page = resp.json().get("value", [])
        workspaces.extend(page)
        log.debug("Fetched %d workspaces (skip=%d)", len(page), skip)
        if len(page) < page_size:
            return workspaces
        skip += page_size


def discover_reports() -> list:
    """
    Build the full report list: two API calls per workspace, not one per report.

    Only reports whose dataset is both visible to us and refreshable are
    included — anything else would give the user a button that cannot work.

    (The per-workspace reports and datasets endpoints return complete lists and
    take no paging parameters, so only /groups above needs paging.)
    """
    headers = pbi_headers()
    reports = []

    for workspace in fetch_all_workspaces(headers):
        ws_id, ws_name = workspace["id"], workspace["name"]

        reports_resp = http.get(
            f"{PBI_BASE_URL}/groups/{ws_id}/reports", headers=headers, timeout=HTTP_TIMEOUT
        )
        if reports_resp.status_code != 200:
            log.warning(
                "Skipping workspace %r — reports returned HTTP %d",
                ws_name,
                reports_resp.status_code,
            )
            continue

        datasets_resp = http.get(
            f"{PBI_BASE_URL}/groups/{ws_id}/datasets", headers=headers, timeout=HTTP_TIMEOUT
        )
        refreshable_dataset_ids = set()
        if datasets_resp.status_code == 200:
            for dataset in datasets_resp.json().get("value", []):
                if dataset.get("isRefreshable"):
                    refreshable_dataset_ids.add(dataset["id"])
        else:
            log.warning(
                "No datasets visible in workspace %r (HTTP %d) — its reports will be hidden",
                ws_name,
                datasets_resp.status_code,
            )

        for report in reports_resp.json().get("value", []):
            dataset_id = report.get("datasetId")
            if dataset_id not in refreshable_dataset_ids:
                continue
            reports.append(
                {
                    "report_id": report["id"],
                    "report_name": report.get("name") or "(unnamed report)",
                    "workspace_id": ws_id,
                    "workspace_name": ws_name,
                    "dataset_id": dataset_id,
                    "dashboard_url": f"https://app.powerbi.com/groups/{ws_id}/reports/{report['id']}",
                }
            )

    return reports


def sync_reports() -> bool:
    """
    Refresh the cached report list. Returns True on success.

    On failure the previous cache is left untouched — a stale list is far more
    useful than an empty one — and the error is recorded so the UI can warn
    that what it is showing might be out of date.
    """
    log.info("Syncing report list from the Power BI API")
    db_log("🔃 Syncing report list from Power BI API...", "info")
    try:
        reports = discover_reports()
    except Exception as exc:
        log.exception("Report sync failed")
        meta_set("last_sync_error", str(exc))
        db_log(f"❌ Report sync failed: {exc}", "error")
        return False

    save_reports(reports)
    meta_set("last_sync_at", utc_now_iso())
    meta_set("last_sync_error", "")
    log.info("Report sync complete — %d refreshable reports", len(reports))
    db_log(f"✅ Report list synced — {len(reports)} reports found.", "success")
    prune_logs()
    return True


def seconds_until_midnight() -> float:
    now = datetime.now()
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (midnight - now).total_seconds()


def background_sync_loop() -> None:
    """
    Sync now, then once a day at midnight — retrying sooner after a failure so
    a single blip does not leave the list stale for 24 hours.

    This runs in a background thread rather than at import so a slow API call
    can never stop the app from starting up and serving the cached list.
    """
    while True:
        succeeded = sync_reports()
        time.sleep(seconds_until_midnight() if succeeded else SYNC_RETRY_SECONDS)


# ---------------------------------------------------------------------------
# 6. REFRESH
# ---------------------------------------------------------------------------
def parse_pbi_time(value):
    """Parse a Power BI timestamp, returning None if it is missing or odd."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        log.debug("Could not parse Power BI timestamp %r", value)
        return None


def find_our_refresh(rows: list, request_id, triggered_after: datetime):
    """
    Pick our own refresh out of the recent refresh history.

    This matters more than it looks. Power BI queues refreshes, so for the
    first second or two after triggering, the newest entry in the history is
    still the PREVIOUS refresh — normally "Completed". Reading that as our
    result reports instant success on a dataset that never refreshed, which is
    the worst possible failure for this tool: nobody notices, and stale numbers
    go out to a client.

    So we match on the RequestId that Power BI returned when we triggered, and
    fall back to "started after we asked" only if that header was absent.
    Returning None means "not in the queue yet — keep waiting".
    """
    if request_id:
        return next((row for row in rows if row.get("requestId") == request_id), None)

    for row in rows:
        started = parse_pbi_time(row.get("startTime"))
        if started and started >= triggered_after:
            return row
    return None


def describe_failure(row: dict) -> str:
    """Pull something actionable out of a failed refresh row."""
    detail = row.get("serviceExceptionJson") or ""
    try:
        parsed = json.loads(detail)
        detail = parsed.get("errorDescription") or parsed.get("error") or detail
    except (ValueError, AttributeError):
        pass
    return str(detail)[:400] or "no detail returned by Power BI"


def run_refresh(report: dict, msg):
    """
    Trigger the refresh and poll until it finishes, yielding SSE lines.

    `msg` is a callback that logs the text and formats it as an SSE line.
    """
    dataset_id = report["dataset_id"]
    refresh_url = (
        f"{PBI_BASE_URL}/groups/{report['workspace_id']}/datasets/{dataset_id}/refreshes"
    )

    yield msg(f"========== NEW REFRESH: {report['report_name'].upper()} ==========", "separator")
    yield msg(f"🚀 Starting dataset refresh for {report['report_name']}...")
    yield msg("🔑 Requesting Azure AD token...")

    try:
        headers = pbi_headers()
    except RuntimeError as exc:
        yield msg(f"❌ Auth failed: {exc}", "error")
        return

    yield msg(f"🔄 Triggering Dataset ID: {dataset_id}...")

    # Record the time just before triggering, for the fallback match below.
    triggered_after = datetime.now(timezone.utc).replace(microsecond=0)

    try:
        trigger = http.post(refresh_url, headers=headers, json={}, timeout=HTTP_TIMEOUT)
        trigger.raise_for_status()
    except requests.RequestException as exc:
        log.exception("Failed to trigger refresh for dataset %s", dataset_id)
        yield msg(f"❌ Could not start refresh: {exc}", "error")
        return

    # Power BI returns the ID of the refresh it just queued in this header.
    # It is how we tell our refresh apart from any other run on this dataset.
    request_id = trigger.headers.get("RequestId") or trigger.headers.get("requestid")
    log.info("Triggered refresh for dataset %s (RequestId=%s)", dataset_id, request_id)
    if not request_id:
        log.warning("No RequestId header returned — falling back to start-time matching")

    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    last_status = None

    while time.monotonic() < deadline:
        time.sleep(5)
        try:
            # $top=5 rather than 1, so a concurrent or scheduled refresh on the
            # same dataset cannot push ours out of view.
            poll = http.get(
                f"{refresh_url}?$top=5", headers=pbi_headers(), timeout=HTTP_TIMEOUT
            )
            poll.raise_for_status()
            rows = poll.json().get("value", [])
        except requests.RequestException as exc:
            # A transient blip should not fail the whole refresh — the retry
            # adapter already tried a few times, so just report and keep going.
            log.warning("Poll failed for dataset %s: %s", dataset_id, exc)
            yield msg(f"⚠️ Could not read status ({exc}) — still waiting...", "info")
            continue

        row = find_our_refresh(rows, request_id, triggered_after)
        if row is None:
            yield msg("⏳ Status: Queued in Power BI...")
            continue

        status = row.get("status", "Unknown")
        if status != last_status:
            yield msg(f"⏳ Status: {status}...")
            last_status = status

        if status == "Completed":
            with db() as conn:
                conn.execute(
                    f"""INSERT INTO last_refresh (dataset_id, refreshed_at)
                        VALUES (?, {SQL_UTC_NOW})
                        ON CONFLICT(dataset_id) DO UPDATE
                            SET refreshed_at = excluded.refreshed_at""",
                    (dataset_id,),
                )
            log.info("Refresh completed for dataset %s", dataset_id)
            yield msg("✅ Refresh completed.", "success")
            return

        if status == "Failed":
            detail = describe_failure(row)
            log.error("Refresh failed for dataset %s: %s", dataset_id, detail)
            yield msg(f"❌ Refresh failed in Power BI: {detail}", "error")
            return

    # Out of time. The refresh may well still be running in Power BI; we have
    # simply stopped watching it, so say that rather than claiming failure.
    log.warning("Stopped watching dataset %s after %ds", dataset_id, POLL_TIMEOUT_SECONDS)
    yield msg(
        f"⚠️ Still running after {POLL_TIMEOUT_SECONDS // 60} minutes — "
        "no longer tracking it here. Check Power BI directly.",
        "error",
    )


# ---------------------------------------------------------------------------
# 7. ROUTES
# ---------------------------------------------------------------------------
def sse(payload: dict) -> str:
    """Format a dict as one Server-Sent Event."""
    return f"data: {json.dumps(payload)}\n\n"


SSE_DONE = "data: [DONE]\n\n"


@app.route("/")
def dispatcher():
    return render_template(
        "dispatcher.html",
        reports=get_reports(),
        synced_at=meta_get("last_sync_at") or "",
        sync_error=meta_get("last_sync_error") or "",
    )


@app.route("/healthz")
def healthz():
    """Liveness check for Docker. Confirms the database is actually reachable."""
    try:
        with db() as conn:
            report_count = conn.execute("SELECT COUNT(*) AS n FROM reports").fetchone()["n"]
    except sqlite3.Error as exc:
        log.exception("Health check failed")
        return jsonify({"status": "error", "detail": str(exc)}), 503
    return jsonify(
        {
            "status": "ok",
            "reports": report_count,
            "last_sync_at": meta_get("last_sync_at"),
            "last_sync_error": meta_get("last_sync_error") or None,
        }
    )


@app.route("/api/state")
def get_state():
    """Everything the page polls for: active locks, recent logs, timestamps."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with db() as conn:
        logs = [
            dict(row)
            for row in conn.execute(
                "SELECT msg, type, timestamp FROM logs WHERE timestamp >= ? "
                "ORDER BY timestamp ASC, id ASC",
                (cutoff,),
            ).fetchall()
        ]
        last_refreshes = {
            row["dataset_id"]: row["refreshed_at"]
            for row in conn.execute("SELECT dataset_id, refreshed_at FROM last_refresh").fetchall()
        }
    return jsonify(
        {
            "locks": locked_dataset_ids(),
            "logs": logs,
            "last_refreshes": last_refreshes,
            "synced_at": meta_get("last_sync_at"),
            "sync_error": meta_get("last_sync_error") or "",
        }
    )


@app.route("/api/reload-reports", methods=["POST"])
def reload_reports():
    """Manual 'Sync reports' button — kicks off a sync without blocking."""
    threading.Thread(target=sync_reports, name="manual-sync", daemon=True).start()
    return jsonify({"status": "sync started"})


@app.route("/stream/<report_id>")
def stream_refresh(report_id):
    """
    Refresh the dataset behind `report_id`, streaming progress as SSE.

    The URL carries the real Power BI report ID rather than a position in a
    list. Positional IDs re-shuffle every time the report list is synced, so a
    page left open could send an index that now points at a different report —
    and refresh the wrong client's dataset.
    """
    report = get_report(report_id)

    def reject(text):
        log.warning("Rejected refresh for report_id=%s: %s", report_id, text)
        yield sse({"msg": text, "type": "error"})
        yield SSE_DONE

    if report is None:
        return Response(
            stream_with_context(reject("❌ Error: Report not found. Try syncing the list.")),
            mimetype="text/event-stream",
        )

    if not try_acquire_lock(report["dataset_id"]):
        return Response(
            stream_with_context(reject("❌ System busy: this dataset is already refreshing.")),
            mimetype="text/event-stream",
        )

    def generate():
        def msg(text, msg_type="info"):
            db_log(text, msg_type)
            return sse({"msg": text, "type": msg_type})

        try:
            yield from run_refresh(report, msg)
        except Exception as exc:  # noqa: BLE001 — last resort, so the UI always hears back
            log.exception("Unexpected error refreshing dataset %s", report["dataset_id"])
            yield msg(f"❌ Unexpected error: {exc}", "error")
        finally:
            release_lock(report["dataset_id"])

        # Sent on every path (success, failure, timeout) so the browser can
        # tell "finished" apart from "connection dropped".
        yield SSE_DONE

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


# ---------------------------------------------------------------------------
# STARTUP
# ---------------------------------------------------------------------------
init_db()

# Set DISABLE_BACKGROUND_SYNC=1 to start the app without contacting Power BI.
# Useful for working on the UI locally, and for tests.
if os.getenv("DISABLE_BACKGROUND_SYNC") == "1":
    log.warning("DISABLE_BACKGROUND_SYNC=1 — report list will not be synced automatically")
else:
    threading.Thread(target=background_sync_loop, name="report-sync", daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8004)
