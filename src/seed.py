"""Stage 1: load FTA member roster from TSV into the operators table."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from db import DB_PATH, connect, init_db, upsert_operator

ROOT = Path(__file__).resolve().parent.parent
TSV = ROOT / "data" / "raw" / "fta_members.tsv"


def _clean(v: str | None) -> str | None:
    if v is None:
        return None
    v = v.strip()
    return v or None


def load_tsv(tsv_path: Path = TSV) -> list[dict]:
    rows: list[dict] = []
    with tsv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            rows.append({
                "fta_no":         _clean(r.get("no_membre")),
                "name":           _clean(r.get("Nom")),
                "legal_name":     _clean(r.get("Nom")),
                "address":        _clean(r.get("Adresse")),
                "city":           _clean(r.get("Ville")),
                "postal":         _clean(r.get("Code_postal")),
                "province":       _clean(r.get("Province")) or "QC",
                "phone":          _clean(r.get("Telephone")),
                "fax":            _clean(r.get("Fax")),
                "delegue_votant": _clean(r.get("Delegue_votant")),
                "president":      _clean(r.get("President")),
                "email":          _clean(r.get("Courriel")),
                "website":        _clean(r.get("Site_Internet")),
                "neq":            None,
            })
    return rows


def seed(db_path: Path = DB_PATH, tsv_path: Path = TSV) -> int:
    init_db(db_path)
    rows = load_tsv(tsv_path)
    conn = connect(db_path)
    try:
        for r in rows:
            if not r["fta_no"] or not r["name"]:
                continue
            upsert_operator(conn, r)
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM operators").fetchone()[0]
    finally:
        conn.close()
    return n


if __name__ == "__main__":
    n = seed()
    print(f"Seeded {n} operators into {DB_PATH}")
