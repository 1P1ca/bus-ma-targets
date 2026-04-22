"""SQLite schema + helpers for the bus_ma project.

Source of truth lives at data/operators.db. All enrichment stages read/write here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Union

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "operators.db"

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS groups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT UNIQUE NOT NULL,
    kind        TEXT,               -- 'consolidator' | 'family' | 'shared-address' | ...
    description TEXT,
    is_excluded INTEGER DEFAULT 0   -- 1 => excluded from Top Targets
);

CREATE TABLE IF NOT EXISTS operators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fta_no          TEXT UNIQUE,
    name            TEXT NOT NULL,
    legal_name      TEXT,
    neq             TEXT,
    address         TEXT,
    city            TEXT,
    postal          TEXT,
    province        TEXT,
    phone           TEXT,
    fax             TEXT,
    email           TEXT,
    website         TEXT,
    president       TEXT,
    delegue_votant  TEXT,
    lat             REAL,
    lng             REAL,
    place_id        TEXT,
    group_id        INTEGER REFERENCES groups(id),
    group_reason    TEXT,            -- why assigned to this group
    notes           TEXT,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_operators_email   ON operators(email);
CREATE INDEX IF NOT EXISTS idx_operators_address ON operators(address, city);
CREATE INDEX IF NOT EXISTS idx_operators_pres    ON operators(president);
CREATE INDEX IF NOT EXISTS idx_operators_neq     ON operators(neq);
CREATE INDEX IF NOT EXISTS idx_operators_group   ON operators(group_id);

CREATE TABLE IF NOT EXISTS permits (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id        INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    permit_type        TEXT,        -- scolaire | nolise | interurbain | adapte | taxi | ...
    permit_no          TEXT,
    declared_vehicles  INTEGER,
    status             TEXT,
    issued             TEXT,
    expires            TEXT,
    source_url         TEXT,
    raw                TEXT,        -- JSON dump of source row
    fetched_at         TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_permits_op ON permits(operator_id);

CREATE TABLE IF NOT EXISTS fleet (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id       INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    source            TEXT,         -- 'ctq' | 'website' | 'media' | 'satellite_est'
    buses_scolaire    INTEGER,
    buses_coach       INTEGER,
    buses_adapte      INTEGER,
    buses_urbain      INTEGER,
    other             INTEGER,
    total             INTEGER,
    as_of_date        TEXT,
    confidence        REAL,
    notes             TEXT,
    fetched_at        TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fleet_op ON fleet(operator_id);

CREATE TABLE IF NOT EXISTS facility (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id           INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    parcel_area_m2        REAL,
    parking_area_m2       REAL,
    building_area_m2      REAL,
    bus_capacity_est      INTEGER,
    satellite_image_path  TEXT,
    zoom                  INTEGER,
    estimation_method     TEXT,     -- 'opencv-threshold' | 'claude-vision' | 'manual'
    confidence            REAL,
    notes                 TEXT,
    fetched_at            TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_facility_op ON facility(operator_id);

CREATE TABLE IF NOT EXISTS ownership (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id  INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    entity_name  TEXT,
    entity_type  TEXT,              -- 'person' | 'company'
    role         TEXT,              -- 'actionnaire' | 'administrateur' | 'president' | ...
    pct          REAL,
    address      TEXT,
    neq          TEXT,
    source       TEXT,              -- 'req' | 'website' | 'media'
    source_url   TEXT,
    fetched_at   TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_own_op    ON ownership(operator_id);
CREATE INDEX IF NOT EXISTS idx_own_ent   ON ownership(entity_name);
CREATE INDEX IF NOT EXISTS idx_own_neq   ON ownership(neq);

CREATE TABLE IF NOT EXISTS media_mentions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id  INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    url          TEXT,
    title        TEXT,
    date         TEXT,
    snippet      TEXT,
    relevance    REAL,
    tags         TEXT,              -- 'succession' | 'sale' | 'fleet' | 'award' | ...
    fetched_at   TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_media_op ON media_mentions(operator_id);

CREATE TABLE IF NOT EXISTS scores (
    operator_id         INTEGER PRIMARY KEY REFERENCES operators(id) ON DELETE CASCADE,
    size_score          REAL,
    independence_score  REAL,
    clarity_score       REAL,
    succession_score    REAL,
    ma_fit_score        REAL,
    rank                INTEGER,
    rationale           TEXT,
    scored_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


def connect(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(db_path: Path | str = DB_PATH) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def upsert_operator(conn: sqlite3.Connection, row: dict) -> int:
    """Insert or update an operator keyed on fta_no. Returns operator id."""
    cols = [
        "fta_no", "name", "legal_name", "neq", "address", "city", "postal",
        "province", "phone", "fax", "email", "website", "president",
        "delegue_votant",
    ]
    vals = [row.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    colnames = ",".join(cols)
    updates = ",".join(f"{c}=excluded.{c}" for c in cols if c != "fta_no")
    conn.execute(
        f"INSERT INTO operators ({colnames}) VALUES ({placeholders}) "
        f"ON CONFLICT(fta_no) DO UPDATE SET {updates}, updated_at=CURRENT_TIMESTAMP",
        vals,
    )
    cur = conn.execute("SELECT id FROM operators WHERE fta_no = ?", (row.get("fta_no"),))
    return cur.fetchone()["id"]


def upsert_group(conn: sqlite3.Connection, name: str, kind: str = None,
                 description: str = None, is_excluded: int = 0) -> int:
    conn.execute(
        "INSERT INTO groups (name, kind, description, is_excluded) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(name) DO UPDATE SET "
        "kind = COALESCE(excluded.kind, groups.kind), "
        "description = COALESCE(excluded.description, groups.description), "
        "is_excluded = MAX(groups.is_excluded, excluded.is_excluded)",
        (name, kind, description, is_excluded),
    )
    cur = conn.execute("SELECT id FROM groups WHERE name = ?", (name,))
    return cur.fetchone()["id"]


def assign_group(conn: sqlite3.Connection, operator_id: int, group_id: int,
                 reason: str = None) -> None:
    conn.execute(
        "UPDATE operators SET group_id = ?, group_reason = ?, "
        "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (group_id, reason, operator_id),
    )


if __name__ == "__main__":
    init_db()
    print(f"Initialized DB at {DB_PATH}")
