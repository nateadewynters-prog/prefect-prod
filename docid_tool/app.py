import os, json, sqlite3, pyodbc
from datetime import datetime
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

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
# lost on redeploy. That is harmless — an empty cache behaves exactly like an
# expired one, and the next request refetches from SQL.
CACHE_TTL_SECONDS = 1800  # 30 minutes (unchanged behaviour)

DB_PATH = os.getenv(
    "DOCID_CACHE_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "docid_cache.db"),
)

def get_db_conn():
    # timeout=10: if another worker is mid-write, wait for it instead of
    # raising "database is locked".
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Create the one-row cache table if it does not exist yet."""
    with get_db_conn() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS docid_cache ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "  # single row, always id=1
            "payload TEXT NOT NULL, "                  # the table rows, as JSON
            "updated_at DATETIME NOT NULL)"
        )

def read_cache():
    """Return (rows, updated_at). updated_at is None when nothing is cached."""
    with get_db_conn() as conn:
        row = conn.execute(
            "SELECT payload, updated_at FROM docid_cache WHERE id = 1"
        ).fetchone()
    if not row:
        return [], None
    return json.loads(row["payload"]), datetime.fromisoformat(row["updated_at"])

def write_cache(rows):
    """Overwrite the shared cache with a fresh set of rows."""
    with get_db_conn() as conn:
        conn.execute(
            "INSERT INTO docid_cache (id, payload, updated_at) VALUES (1, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "payload = excluded.payload, updated_at = excluded.updated_at",
            (json.dumps(rows), datetime.now().isoformat(timespec="seconds")),
        )

init_db()

def update_docid_cache():
    """Fetches data from SQL and keeps only unique (ShowId, TheatreId, DocTypeId) combinations."""
    try:
        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE=TicketingDS;UID={os.getenv('SQL_USERNAME_BILOGIN')};PWD={os.getenv('SQL_PASSWORD_BILOGIN')};TrustServerCertificate=yes;"
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            # The query remains the same to get all necessary display and ID columns
            cursor.execute("SELECT ShowName, TheatreName, DocumentName, ShowId, TheatreId, DocumentTypeId FROM [dbo].[DocumentsAndVenues] ORDER BY ShowName")
            rows = cursor.fetchall()
            
            fresh_data = []
            seen_combos = set() # Track unique combinations of IDs
            
            for r in rows:
                # Safely parse the IDs
                show_id = int(r[3]) if r[3] else 0
                theatre_id = int(r[4]) if r[4] else 0
                doc_type_id = int(r[5]) if r[5] else 0
                
                # Create a unique key for this record
                combo = (show_id, theatre_id, doc_type_id)
                
                # Only add if we haven't seen this specific ID set yet
                if combo not in seen_combos:
                    seen_combos.add(combo)
                    fresh_data.append([
                        r[0] if r[0] else "N/A", # ShowName
                        r[1] if r[1] else "N/A", # TheatreName
                        r[2] if r[2] else "N/A", # DocumentName[cite: 1]
                        show_id,
                        theatre_id,
                        doc_type_id
                    ])
                
            with CACHE_LOCK:
                DOCID_CACHE["data"] = fresh_data
                DOCID_CACHE["last_updated"] = datetime.now()
                
            return True, "Cache updated successfully."
    except Exception as e:
        print(f"SQL Error fetching DocIDs: {e}")
        return False, str(e)

# --- ROUTES ---

@app.route('/')
def docid():
    """Serves the frontend UI directly at the root of this microservice."""
    return render_template('docid.html')

@app.route('/api/docids')
def api_docids():
    """Serves the SQL data from memory, updating it if it's older than 30 mins."""
    now = datetime.now()
    needs_update = False
    
    with CACHE_LOCK:
        if DOCID_CACHE["last_updated"] is None:
            needs_update = True
        elif (now - DOCID_CACHE["last_updated"]).total_seconds() > 1800: # 30 mins
            needs_update = True
            
    if needs_update:
        success, msg = update_docid_cache()
        if not success and not DOCID_CACHE["data"]:
            return jsonify({"error": "Failed to fetch data"}), 500

    return jsonify(DOCID_CACHE["data"])

@app.route('/api/docids/refresh', methods=['POST'])
def force_refresh_docids():
    """Forces an immediate refresh of the SQL data."""
    success, msg = update_docid_cache()
    if success:
        return jsonify({"status": "success", "message": "Database resynced!"})
    else:
        return jsonify({"status": "error", "message": msg}), 500

if __name__ == '__main__':
    # Runs on 8002 internally, which Docker maps to 8003 externally
    app.run(host='0.0.0.0', port=8002)