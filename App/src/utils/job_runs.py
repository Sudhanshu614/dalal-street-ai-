import sqlite3
from pathlib import Path
try:
    from App.config import config
    DB_PATH = str(config.DB_PATH)
except Exception:
    DB_PATH = str(Path(__file__).resolve().parents[2] / "database" / "stock_market_new.db")

def _conn():
    return sqlite3.connect(DB_PATH)

def ensure_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS job_runs (
            job_id TEXT PRIMARY KEY,
            target_table TEXT,
            last_run TEXT,
            status TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()

def get_last_run(conn, job_id):
    cur = conn.cursor()
    cur.execute("SELECT last_run, updated_at FROM job_runs WHERE job_id=?", (job_id,))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)

def set_last_run(conn, job_id, target_table, last_run, status):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO job_runs(job_id, target_table, last_run, status, updated_at)
        VALUES(?,?,?,?,datetime('now'))
        ON CONFLICT(job_id) DO UPDATE SET
            target_table=excluded.target_table,
            last_run=excluded.last_run,
            status=excluded.status,
            updated_at=excluded.updated_at
        """,
        (job_id, target_table, last_run, status)
    )
    conn.commit()