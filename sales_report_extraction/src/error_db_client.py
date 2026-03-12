import sqlite3
import os

# Dynamically find the path so it works perfectly across host and container
APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRACKING_DIR = os.path.join(APP_ROOT, "data", "error_tracking")
DB_PATH = os.path.join(TRACKING_DIR, "dataops_tracking.db")

def init_db():
    """Creates the SQLite database, forcing max permissions so Docker doesn't block it."""
    if not os.path.exists(TRACKING_DIR):
        os.makedirs(TRACKING_DIR, exist_ok=True)
        
    # 🚀 THE FIX: Force max permissions on the directory from INSIDE the container
    try:
        os.chmod(TRACKING_DIR, 0o777)
    except Exception:
        pass # Ignore if it complains, we just want to try forcing it
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lookup_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_name TEXT,
            venue_name TEXT,
            show_id TEXT,
            venue_id TEXT,
            missing_code TEXT,
            msg_id TEXT,
            status TEXT DEFAULT 'Pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    
    # 🚀 THE FIX: Force read/write permissions on the file itself
    if os.path.exists(DB_PATH):
        try:
            os.chmod(DB_PATH, 0o666)
        except Exception:
            pass

def log_lookup_failure(show_name, venue_name, show_id, venue_id, missing_code, msg_id):
    """Inserts a new pending failure into the database."""
    init_db()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if this exact missing code for this msg_id already exists to prevent duplicates
    cursor.execute('''
        SELECT id FROM lookup_failures 
        WHERE msg_id = ? AND missing_code = ? AND status = 'Pending'
    ''', (msg_id, missing_code))
    
    if not cursor.fetchone():
        cursor.execute('''
            INSERT INTO lookup_failures 
            (show_name, venue_name, show_id, venue_id, missing_code, msg_id, status)
            VALUES (?, ?, ?, ?, ?, ?, 'Pending')
        ''', (show_name, venue_name, show_id, venue_id, missing_code, msg_id))
        conn.commit()
        
    conn.close()

if __name__ == "__main__":
    init_db()
    print(f"✅ Database initialized at {DB_PATH}")
