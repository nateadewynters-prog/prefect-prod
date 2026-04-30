import os, pyodbc, threading
from datetime import datetime
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

# --- DOCID CACHE ENGINE ---
DOCID_CACHE = {
    "data": [],
    "last_updated": None
}
CACHE_LOCK = threading.Lock()

def update_docid_cache():
    """Fetches data from SQL and keeps only unique (ShowId, TheatreId, DocTypeId) combinations."""
    try:
        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE=TicketingDS;UID={os.getenv('SQL_USERNAME')};PWD={os.getenv('SQL_PASSWORD')};TrustServerCertificate=yes;"
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