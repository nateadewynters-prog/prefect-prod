"""
state.py — everything that touches SQLite: locks, logs and dispatch history.

Knows nothing about Flask. Callers get plain dicts and lists back, never
sqlite3.Row objects.

The lock is deliberately global rather than per-show: is_any_locked() counts
every locked row, and the dispatch refuses to start if any of them is set.
It lives in SQLite rather than in a Python global because gunicorn runs four
worker processes, which would each get their own copy of a module-level flag.
"""

import sqlite3
from datetime import datetime, timedelta

from config import DB_PATH


def get_db_conn():
    # check_same_thread=False because gunicorn's gthread workers hand a
    # connection to whichever thread is serving the request.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS locks (show_id TEXT PRIMARY KEY, is_locked INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT, type TEXT, stage TEXT, timestamp DATETIME DEFAULT (datetime('now', 'localtime')))")
        conn.execute("CREATE TABLE IF NOT EXISTS dispatch_history (id INTEGER PRIMARY KEY AUTOINCREMENT, show_name TEXT, duration_mins INTEGER, pdf_size_mb REAL, timestamp DATETIME DEFAULT (datetime('now', 'localtime')))")

        # Migration: add per-phase timing columns (seconds) if they don't already exist.
        # Existing rows get NULL, which renders as a blank in the history table.
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(dispatch_history)")}
        for col in ("refresh_secs", "sql_secs", "export_secs"):
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE dispatch_history ADD COLUMN {col} REAL")

        # Migration: add the stage column to logs for databases created before
        # the stepper UI existed. Existing rows get NULL stage (rendered as untagged).
        log_cols = {row[1] for row in conn.execute("PRAGMA table_info(logs)")}
        if "stage" not in log_cols:
            conn.execute("ALTER TABLE logs ADD COLUMN stage TEXT")

        conn.execute("UPDATE locks SET is_locked = 0")


# ---------------------------------------------------------------------------
# LOCKS
# ---------------------------------------------------------------------------
def set_lock(show_id, locked):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO locks (show_id, is_locked) VALUES (?, ?) ON CONFLICT(show_id) DO UPDATE SET is_locked = ?", (show_id, int(locked), int(locked)))


def is_any_locked():
    with get_db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) as active_locks FROM locks WHERE is_locked = 1").fetchone()
        return row['active_locks'] > 0


def get_active_locks():
    with get_db_conn() as conn:
        return [row['show_id'] for row in conn.execute("SELECT show_id FROM locks WHERE is_locked = 1").fetchall()]


# ---------------------------------------------------------------------------
# LOGS
# ---------------------------------------------------------------------------
def db_log(msg, msg_type="info", stage=None):
    with get_db_conn() as conn:
        conn.execute("INSERT INTO logs (msg, type, stage) VALUES (?, ?, ?)", (msg, msg_type, stage))


def get_recent_logs(minutes=30):
    """Oldest first, so the UI can replay a run it missed the start of."""
    with get_db_conn() as conn:
        cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
        return [dict(row) for row in conn.execute("SELECT msg, type, stage, timestamp FROM logs WHERE timestamp >= ? ORDER BY timestamp ASC", (cutoff,)).fetchall()]


# ---------------------------------------------------------------------------
# DISPATCH HISTORY
# ---------------------------------------------------------------------------
def get_history():
    """Returns the /api/history payload shape directly."""
    with get_db_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM dispatch_history").fetchone()[0]
        history = [dict(row) for row in conn.execute("SELECT show_name, duration_mins, pdf_size_mb, refresh_secs, sql_secs, export_secs, timestamp FROM dispatch_history ORDER BY timestamp DESC LIMIT 50").fetchall()]
    return {"total": total, "history": history}


def record_dispatch(show_name, duration_mins, pdf_size_mb, refresh_secs, sql_secs, export_secs):
    with get_db_conn() as conn:
        conn.execute(
            "INSERT INTO dispatch_history (show_name, duration_mins, pdf_size_mb, refresh_secs, sql_secs, export_secs) VALUES (?, ?, ?, ?, ?, ?)",
            (show_name, duration_mins, pdf_size_mb, refresh_secs, sql_secs, export_secs)
        )
        conn.commit()
