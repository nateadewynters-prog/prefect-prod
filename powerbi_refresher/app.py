import os, time, requests, msal, json, sqlite3, threading
from datetime import datetime, timedelta
from flask import Flask, render_template, Response, stream_with_context, jsonify
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

TENANT_ID     = os.getenv("AZURE_TENANT_ID")
CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

PBI_SCOPE    = ["https://analysis.windows.net/powerbi/api/.default"]
PBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"

# ---------------------------------------------------------------------------
# MSAL helper — one shared instance is fine for client-credentials flow
# ---------------------------------------------------------------------------
_msal_app = None

def get_msal_app():
    global _msal_app
    if _msal_app is None:
        _msal_app = msal.ConfidentialClientApplication(
            CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{TENANT_ID}",
            client_credential=CLIENT_SECRET,
        )
    return _msal_app

def get_token():
    result = get_msal_app().acquire_token_for_client(scopes=PBI_SCOPE)
    token = result.get("access_token")
    if not token:
        raise RuntimeError(f"Auth failed: {result.get('error_description')}")
    return token

def pbi_headers():
    return {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

# ---------------------------------------------------------------------------
# REPORT DISCOVERY  (replaces get_report_ids.py)
# ---------------------------------------------------------------------------
_reports_lock   = threading.Lock()
_reports_config = []          # list of dicts, rebuilt on each sync
_reports_synced_at = None     # datetime of last successful sync

def discover_reports():
    """
    Fetch all workspaces the service principal can see, then — per workspace —
    fetch reports and datasets in parallel (2 calls per workspace, not N).
    Only reports whose dataset is visible AND refreshable are returned.
    """
    headers = pbi_headers()

    ws_resp = requests.get(f"{PBI_BASE_URL}/groups", headers=headers, timeout=30)
    ws_resp.raise_for_status()
    workspaces = ws_resp.json().get("value", [])

    reports = []
    for ws in workspaces:
        ws_id, ws_name = ws["id"], ws["name"]

        rep_resp = requests.get(
            f"{PBI_BASE_URL}/groups/{ws_id}/reports",
            headers=headers, timeout=30,
        )
        if rep_resp.status_code != 200:
            continue

        # Fetch all datasets for this workspace in one call.
        # If the service principal lacks access, the response is a 403 or empty —
        # in either case we get no refreshable dataset IDs and all reports are skipped.
        ds_resp = requests.get(
            f"{PBI_BASE_URL}/groups/{ws_id}/datasets",
            headers=headers, timeout=30,
        )
        refreshable_ids = set()
        if ds_resp.status_code == 200:
            for ds in ds_resp.json().get("value", []):
                if ds.get("isRefreshable"):
                    refreshable_ids.add(ds["id"])

        for rep in rep_resp.json().get("value", []):
            dataset_id = rep.get("datasetId")
            if dataset_id not in refreshable_ids:
                continue
            reports.append({
                "id":             str(len(reports)),
                "workspace_name": ws_name,
                "workspace_id":   ws_id,
                "report_name":    rep.get("name"),
                "report_id":      rep.get("id"),
                "dataset_id":     dataset_id,
                "dashboard_url":  f"https://app.powerbi.com/groups/{ws_id}/reports/{rep.get('id')}",
            })

    return reports

def sync_reports(log=True):
    """Thread-safe wrapper around discover_reports(); updates global state."""
    global _reports_config, _reports_synced_at
    try:
        if log:
            db_log("🔃 Syncing report list from Power BI API...", "info")
        fresh = discover_reports()
        with _reports_lock:
            # Re-index ids sequentially after rebuild
            for i, r in enumerate(fresh):
                r["id"] = str(i)
            _reports_config  = fresh
            _reports_synced_at = datetime.now()
        if log:
            db_log(f"✅ Report list synced — {len(fresh)} reports found.", "success")
    except Exception as e:
        if log:
            db_log(f"❌ Report sync failed: {e}", "error")

def _seconds_until_midnight():
    now = datetime.now()
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (midnight - now).total_seconds()

def _background_sync_loop():
    """Sync once per day at midnight."""
    while True:
        time.sleep(_seconds_until_midnight())
        sync_reports()

# ---------------------------------------------------------------------------
# DATABASE  (shared state: locks + logs)
# ---------------------------------------------------------------------------
def get_db_conn():
    conn = sqlite3.connect("refresher_state.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS locks (dataset_id TEXT PRIMARY KEY, is_locked INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT, type TEXT, timestamp DATETIME DEFAULT (datetime('now', 'localtime')))")
        conn.execute("CREATE TABLE IF NOT EXISTS last_refresh (dataset_id TEXT PRIMARY KEY, refreshed_at DATETIME)")
        conn.execute("UPDATE locks SET is_locked = 0")

def set_lock(dataset_id, locked):
    with get_db_conn() as conn:
        conn.execute(
            "INSERT INTO locks (dataset_id, is_locked) VALUES (?, ?) ON CONFLICT(dataset_id) DO UPDATE SET is_locked = ?",
            (dataset_id, int(locked), int(locked)),
        )

def is_locked(dataset_id):
    with get_db_conn() as conn:
        row = conn.execute("SELECT is_locked FROM locks WHERE dataset_id = ?", (dataset_id,)).fetchone()
        return row["is_locked"] == 1 if row else False

def db_log(msg, msg_type="info"):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO logs (msg, type) VALUES (?, ?)", (msg, msg_type))

# ---------------------------------------------------------------------------
# STARTUP
# ---------------------------------------------------------------------------
init_db()
sync_reports(log=False)                          # initial blocking sync
threading.Thread(target=_background_sync_loop, daemon=True).start()

# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------
@app.route("/")
def dispatcher():
    with _reports_lock:
        reports = list(_reports_config)
    synced_at = _reports_synced_at.strftime("%d %b %Y %H:%M") if _reports_synced_at else "never"
    return render_template("dispatcher.html", reports=reports, synced_at=synced_at)

@app.route("/api/state")
def get_state():
    with get_db_conn() as conn:
        locks = [row["dataset_id"] for row in conn.execute("SELECT dataset_id FROM locks WHERE is_locked = 1").fetchall()]
        thirty_mins_ago = (datetime.now() - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        logs = [dict(row) for row in conn.execute(
            "SELECT msg, type, timestamp FROM logs WHERE timestamp >= ? ORDER BY timestamp ASC",
            (thirty_mins_ago,),
        ).fetchall()]
    with get_db_conn() as conn:
        last_refreshes = {
            row["dataset_id"]: row["refreshed_at"]
            for row in conn.execute("SELECT dataset_id, refreshed_at FROM last_refresh").fetchall()
        }
    synced_at = _reports_synced_at.strftime("%d %b %Y %H:%M") if _reports_synced_at else None
    return jsonify({"locks": locks, "logs": logs, "synced_at": synced_at, "last_refreshes": last_refreshes})

@app.route("/api/reload-reports", methods=["POST"])
def reload_reports():
    """Manual trigger to re-discover reports immediately."""
    threading.Thread(target=sync_reports, daemon=True).start()
    return jsonify({"status": "sync started"})

@app.route("/stream/<report_index>")
def stream_logs(report_index):
    with _reports_lock:
        config = next((r for r in _reports_config if r["id"] == report_index), None)

    def reject(msg_text):
        yield f'data: {json.dumps({"msg": msg_text, "type": "error"})}\n\n'
        yield "data: [DONE]\n\n"

    if not config:
        return Response(stream_with_context(reject("❌ Error: Report not found")), mimetype="text/event-stream")
    if not config["dataset_id"]:
        return Response(stream_with_context(reject("❌ Error: No underlying dataset.")), mimetype="text/event-stream")
    if is_locked(config["dataset_id"]):
        return Response(stream_with_context(reject("❌ System busy: Dataset is refreshing.")), mimetype="text/event-stream")

    set_lock(config["dataset_id"], True)

    def generate():
        def msg(text, msg_type="info"):
            db_log(text, msg_type)
            return f'data: {json.dumps({"msg": text, "type": msg_type})}\n\n'

        try:
            yield msg(f"========== NEW REFRESH: {config['report_name'].upper()} ==========", "separator")
            yield msg(f"🚀 Starting dataset refresh for {config['report_name']}...")
            yield msg("🔑 Requesting Azure AD token...")

            try:
                headers = pbi_headers()
            except RuntimeError as e:
                yield msg(f"❌ Auth failed: {e}", "error")
                return

            refresh_url = f"{PBI_BASE_URL}/groups/{config['workspace_id']}/datasets/{config['dataset_id']}/refreshes"
            yield msg(f"🔄 Triggering Dataset ID: {config['dataset_id']}...")

            try:
                requests.post(refresh_url, headers=headers, json={}).raise_for_status()

                status_url = f"{refresh_url}?$top=1"
                while True:
                    poll = requests.get(status_url, headers=headers, timeout=30)
                    poll.raise_for_status()
                    status = poll.json().get("value", [{}])[0].get("status", "Unknown")
                    yield msg(f"⏳ Status: {status}...")
                    if status == "Completed":
                        with get_db_conn() as conn:
                            conn.execute(
                                "INSERT INTO last_refresh (dataset_id, refreshed_at) VALUES (?, datetime('now', 'localtime')) ON CONFLICT(dataset_id) DO UPDATE SET refreshed_at = excluded.refreshed_at",
                                (config["dataset_id"],),
                            )
                        yield msg("✅ Refresh completed.", "success")
                        break
                    elif status == "Failed":
                        yield msg("❌ Refresh failed in Power BI.", "error")
                        return
                    time.sleep(5)

            except Exception as e:
                yield msg(f"❌ API error: {e}", "error")
                return

            yield "data: [DONE]\n\n"

        finally:
            set_lock(config["dataset_id"], False)

    return Response(stream_with_context(generate()), mimetype="text/event-stream")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8004)