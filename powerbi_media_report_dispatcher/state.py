"""
state.py — the shared SQLite database (locks / logs / history).

Every bit of state the tool keeps between requests lives in one SQLite file
(config.DB_PATH). Three tables:

  locks            one row per show; is_locked=1 while a job is running.
                   The lock is GLOBAL in practice: is_any_locked() is checked
                   before starting any job, so only one runs at a time.
  logs             every pipeline message, tagged with a type and stage.
                   The UI polls these to rebuild the live progress view.
  dispatch_history one row per successful dispatch, shown in the History modal.

Same idea as the sales dispatcher, but a clean schema so there are no
migrations to reason about.
"""

import sqlite3
from datetime import datetime, timedelta

from config import DB_PATH


def get_db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the tables if missing. Called once at boot (see app.py)."""
    with get_db_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS locks "
                     "(show_id TEXT PRIMARY KEY, is_locked INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS logs "
                     "(id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT, type TEXT, "
                     "stage TEXT, timestamp DATETIME DEFAULT (datetime('now','localtime')))")
        conn.execute("CREATE TABLE IF NOT EXISTS dispatch_history "
                     "(id INTEGER PRIMARY KEY AUTOINCREMENT, show_name TEXT, "
                     "date_range TEXT, spend REAL, revenue REAL, roas REAL, "
                     "duration_secs INTEGER, pptx_size_mb REAL, "
                     "timestamp DATETIME DEFAULT (datetime('now','localtime')))")
        # On boot, clear any locks left over from a crash / restart.
        conn.execute("UPDATE locks SET is_locked = 0")


# ---------------------------------------------------------------------------
# LOCKS
# ---------------------------------------------------------------------------
def set_lock(show_id: str, locked: bool) -> None:
    with get_db_conn() as conn:
        conn.execute("INSERT INTO locks (show_id, is_locked) VALUES (?, ?) "
                     "ON CONFLICT(show_id) DO UPDATE SET is_locked = ?",
                     (show_id, int(locked), int(locked)))


def is_any_locked() -> bool:
    with get_db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM locks WHERE is_locked = 1").fetchone()
        return row["n"] > 0


def get_active_locks() -> list:
    """Show IDs currently locked — the /api/state 'locks' list the UI polls."""
    with get_db_conn() as conn:
        return [r["show_id"] for r in
                conn.execute("SELECT show_id FROM locks WHERE is_locked = 1").fetchall()]


# ---------------------------------------------------------------------------
# LOGS
# ---------------------------------------------------------------------------
def db_log(msg: str, msg_type: str = "info", stage: str | None = None) -> None:
    with get_db_conn() as conn:
        conn.execute("INSERT INTO logs (msg, type, stage) VALUES (?, ?, ?)",
                     (msg, msg_type, stage))


def get_recent_logs(minutes: int = 30) -> list:
    """Log rows from the last N minutes, oldest first — enough for the UI to
    rebuild the most recent run's progress view."""
    with get_db_conn() as conn:
        cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
        return [dict(r) for r in conn.execute(
            "SELECT msg, type, stage, timestamp FROM logs WHERE timestamp >= ? "
            "ORDER BY timestamp ASC", (cutoff,)).fetchall()]


# ---------------------------------------------------------------------------
# DISPATCH HISTORY
# ---------------------------------------------------------------------------
def get_history(limit: int = 50) -> dict:
    """The exact JSON shape /api/history returns: {"total": N, "history": [...]}."""
    with get_db_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM dispatch_history").fetchone()[0]
        history = [dict(row) for row in conn.execute(
            "SELECT show_name, date_range, spend, revenue, roas, duration_secs, "
            "pptx_size_mb, timestamp FROM dispatch_history "
            "ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()]
    return {"total": total, "history": history}


def record_dispatch(show_name: str, date_range: str, spend: float, revenue: float,
                    roas: float, duration_secs: int, pptx_size_mb: float) -> None:
    """Add one row after a successful dispatch (feeds the History modal)."""
    with get_db_conn() as conn:
        conn.execute(
            "INSERT INTO dispatch_history (show_name, date_range, spend, revenue, "
            "roas, duration_secs, pptx_size_mb) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (show_name, date_range, spend, revenue, roas, duration_secs, pptx_size_mb))
        conn.commit()
