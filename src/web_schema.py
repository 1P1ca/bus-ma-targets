"""Additional tables for the web app: communications, enrichments, edit log.

Layered on top of db.py's schema; safe to run repeatedly.
"""

from __future__ import annotations

from pathlib import Path

from db import DB_PATH, connect

WEB_SCHEMA = """
CREATE TABLE IF NOT EXISTS communications (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,      -- 'appel' | 'courriel' | 'rencontre' | 'visite' | 'autre'
    direction   TEXT,               -- 'entrant' | 'sortant'
    occurred_at TEXT NOT NULL,       -- ISO date or datetime
    contact     TEXT,               -- person at the operator
    subject     TEXT,
    notes       TEXT,
    next_step   TEXT,               -- prochaine étape
    author      TEXT,               -- user creating the entry
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_comm_op ON communications(operator_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS enrichments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id  INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    field        TEXT NOT NULL,     -- e.g. 'fleet_total', 'owner_age', 'neq', 'website',
                                    --      'shareholders', 'revenue_est', 'note'
    value        TEXT,
    source       TEXT,              -- e.g. 'appel', 'LinkedIn', 'REQ-manuel', 'site-web'
    source_url   TEXT,
    confidence   REAL,              -- 0..1 from the user
    author       TEXT,
    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_enr_op ON enrichments(operator_id, created_at DESC);

CREATE TABLE IF NOT EXISTS edit_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id  INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    field        TEXT NOT NULL,
    old_value    TEXT,
    new_value    TEXT,
    author       TEXT,
    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_edit_op ON edit_log(operator_id, created_at DESC);

-- Free-form profile notes + status flags on the operator.
-- Add columns only if missing.
"""


def _has_col(conn, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == col for row in cur.fetchall())


EXTRA_OP_COLS = [
    ("notes",           "TEXT"),
    ("status",          "TEXT"),        # 'nouveau'|'à contacter'|'en discussion'|'rejeté'|'acquis'
    ("owner_flag",      "INTEGER"),     # favorite
    ("last_contact_at", "TEXT"),
]


def upgrade(db_path: Path = DB_PATH) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(WEB_SCHEMA)
        for name, ctype in EXTRA_OP_COLS:
            if not _has_col(conn, "operators", name):
                conn.execute(f"ALTER TABLE operators ADD COLUMN {name} {ctype}")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    upgrade()
    print("Web schema upgraded.")
