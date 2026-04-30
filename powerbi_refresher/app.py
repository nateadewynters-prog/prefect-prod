import os, time, requests, msal, json, sqlite3
from datetime import datetime, timedelta
from flask import Flask, render_template, Response, stream_with_context, jsonify
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

# --- LOAD CONFIGURATION ---
with open("PowerBI_Report_IDs.json", "r") as f:
    raw_reports = json.load(f)

REPORTS_CONFIG = []
for idx, r in enumerate(raw_reports):
    REPORTS_CONFIG.append({
        "id": str(idx),
        "workspace_name": r["Workspace Name"],
        "workspace_id": r["Workspace ID"],
        "report_name": r["Report Name"],
        "report_id": r["Report ID"],
        "dataset_id": r["Underlying Dataset ID"],
        "dashboard_url": f"https://app.powerbi.com/groups/{r['Workspace ID']}/reports/{r['Report ID']}"
    })

# --- SHARED STATE DATABASE ---
def get_db_conn():
    conn = sqlite3.connect('refresher_state.db', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS locks (dataset_id TEXT PRIMARY KEY, is_locked INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT, type TEXT, timestamp DATETIME DEFAULT (datetime('now', 'localtime')))")
        conn.execute("UPDATE locks SET is_locked = 0")
init_db()

def set_lock(dataset_id, locked):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO locks (dataset_id, is_locked) VALUES (?, ?) ON CONFLICT(dataset_id) DO UPDATE SET is_locked = ?", (dataset_id, int(locked), int(locked)))

def is_locked(dataset_id):
    with get_db_conn() as conn:
        row = conn.execute("SELECT is_locked FROM locks WHERE dataset_id = ?", (dataset_id,)).fetchone()
        return row['is_locked'] == 1 if row else False

def db_log(msg, msg_type="info"):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO logs (msg, type) VALUES (?, ?)", (msg, msg_type))

class LiveReportingEngine:
    def __init__(self):
        self.authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        self.msal_app = msal.ConfidentialClientApplication(CLIENT_ID, authority=self.authority, client_credential=CLIENT_SECRET)
    
    def get_token(self, scopes):
        return self.msal_app.acquire_token_for_client(scopes=scopes).get("access_token")

@app.route('/')
def dispatcher():
    return render_template('dispatcher.html', reports=REPORTS_CONFIG)

@app.route('/api/state')
def get_state():
    with get_db_conn() as conn:
        locks = [row['dataset_id'] for row in conn.execute("SELECT dataset_id FROM locks WHERE is_locked = 1").fetchall()]
        thirty_mins_ago = (datetime.now() - timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
        logs = [dict(row) for row in conn.execute("SELECT msg, type, timestamp FROM logs WHERE timestamp >= ? ORDER BY timestamp ASC", (thirty_mins_ago,)).fetchall()]
    return jsonify({"locks": locks, "logs": logs})

@app.route('/stream/<report_index>')
def stream_logs(report_index):
    config = next((r for r in REPORTS_CONFIG if r["id"] == report_index), None)
    
    def reject(msg_text):
        yield f'data: {json.dumps({"msg": msg_text, "type": "error"})}\n\n'
        yield "data: [DONE]\n\n"

    if not config: return Response(stream_with_context(reject("❌ Error: Report not found")), mimetype='text/event-stream')
    if not config["dataset_id"]: return Response(stream_with_context(reject("❌ Error: No underlying dataset.")), mimetype='text/event-stream')
    if is_locked(config["dataset_id"]): return Response(stream_with_context(reject("❌ System busy: Dataset is refreshing.")), mimetype='text/event-stream')

    set_lock(config["dataset_id"], True)

    def generate():
        def msg(text, msg_type="info"): 
            db_log(text, msg_type) 
            return f'data: {json.dumps({"msg": text, "type": msg_type})}\n\n'
            
        try:
            yield msg(f"========== NEW REFRESH: {config['report_name'].upper()} ==========", "separator")
            yield msg(f"🚀 Starting dataset refresh for {config['report_name']}...")
            
            engine = LiveReportingEngine()
            yield msg("🔑 Requesting Azure AD Token...")
            pbi_token = engine.get_token(["https://analysis.windows.net/powerbi/api/.default"])
            
            if not pbi_token:
                yield msg("❌ Auth Failed.", "error")
                return

            pbi_headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
            yield msg(f"🔄 Triggering Dataset ID: {config['dataset_id']}...")
            
            try:
                refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{config['workspace_id']}/datasets/{config['dataset_id']}/refreshes"
                requests.post(refresh_url, headers=pbi_headers, json={}).raise_for_status()
                
                status_url = f"{refresh_url}?$top=1"
                while True:
                    poll_req = requests.get(status_url, headers=pbi_headers)
                    poll_req.raise_for_status() 
                    status = poll_req.json().get('value', [{}])[0].get('status', 'Unknown')
                    
                    yield msg(f"⏳ Status: {status}...")
                    if status == "Completed":
                        yield msg("✅ Refresh Completed.", "success")
                        break
                    elif status == "Failed":
                        yield msg("❌ Refresh Failed in Power BI.", "error")
                        return
                    time.sleep(5)
            except Exception as e:
                yield msg(f"❌ API Error: {str(e)}", "error")
                return
            yield "data: [DONE]\n\n"
        finally:
            set_lock(config["dataset_id"], False)

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8004)
