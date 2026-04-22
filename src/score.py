"""Stage 7: M&A fit scoring.

Computes four sub-scores per operator and a composite `ma_fit_score`.

  size_score         normalized(0..1) from declared fleet + parking m²
                     + permit-breadth (distinct permit types)
  independence_score 1.0 if not in a consolidator group, 0.2 otherwise
                     (shared-signal clusters get 0.7 — candidate, not rejected)
  clarity_score      ownership clarity: 1.0 if we have NEQ + ≤3 shareholders
                     with single-family surname, falls off with complexity
                     or missing data
  succession_score   placeholder (0.5) until agent_enrich fills it from web
                     signals (owner age, "à vendre", generational transition)

  ma_fit_score       = independence * (0.5*size + 0.3*clarity + 0.2*succession)

Operators ranked by ma_fit_score DESC, ties broken by size DESC.
"""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Optional

from db import DB_PATH, connect, init_db


def _norm(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def _size_score(conn: sqlite3.Connection, op_id: int,
                fleet_p95: float, parking_p95: float) -> tuple[float, dict]:
    fleet_total = conn.execute(
        "SELECT COALESCE(MAX(total), 0) FROM fleet WHERE operator_id = ?", (op_id,)
    ).fetchone()[0]
    parking_m2 = conn.execute(
        "SELECT COALESCE(MAX(parking_area_m2), 0) FROM facility WHERE operator_id = ?",
        (op_id,),
    ).fetchone()[0]
    permit_types = conn.execute(
        "SELECT COUNT(DISTINCT permit_type) FROM permits WHERE operator_id = ?",
        (op_id,),
    ).fetchone()[0]

    s_fleet   = _norm(fleet_total, 0, fleet_p95 or 50)
    s_parking = _norm(parking_m2,  0, parking_p95 or 10000)
    s_breadth = _norm(permit_types, 0, 4)

    size = 0.5 * s_fleet + 0.3 * s_parking + 0.2 * s_breadth
    return size, {"fleet_total": fleet_total, "parking_m2": parking_m2,
                  "permit_types": permit_types}


def _independence_score(conn: sqlite3.Connection, op_id: int) -> tuple[float, dict]:
    row = conn.execute(
        "SELECT g.name, g.kind, g.is_excluded FROM operators o "
        "LEFT JOIN groups g ON o.group_id = g.id WHERE o.id = ?", (op_id,)
    ).fetchone()
    if row is None or row["name"] is None:
        return 1.0, {"group": None}
    if row["is_excluded"]:
        return 0.2, {"group": row["name"], "kind": row["kind"]}
    # emergent shared-signal cluster — candidate, needs review
    return 0.7, {"group": row["name"], "kind": row["kind"]}


def _clarity_score(conn: sqlite3.Connection, op_id: int,
                   president: str = None) -> tuple[float, dict]:
    op = conn.execute(
        "SELECT neq, president FROM operators WHERE id = ?", (op_id,)
    ).fetchone()
    has_neq = bool(op and op["neq"])
    shareholders = conn.execute(
        "SELECT entity_name, pct FROM ownership "
        "WHERE operator_id = ? AND role = 'actionnaire'",
        (op_id,),
    ).fetchall()
    n_sh = len(shareholders)

    score = 0.0
    if has_neq:
        score += 0.4

    if n_sh == 0:
        # no REQ data yet — partial credit based on single-president signal
        if president:
            score += 0.3
    elif n_sh == 1:
        score += 0.5
    elif n_sh <= 3:
        score += 0.4
    elif n_sh <= 6:
        score += 0.2

    # single-family signal: shareholder surname matches president surname
    if president and shareholders:
        pres_last = president.strip().split()[-1].lower()
        if any(pres_last in (sh["entity_name"] or "").lower() for sh in shareholders):
            score += 0.1

    return min(1.0, score), {"n_shareholders": n_sh, "has_neq": has_neq}


def _succession_score(conn: sqlite3.Connection, op_id: int) -> tuple[float, dict]:
    # TODO: wire agent_enrich / media_mentions tags ('succession', 'sale')
    hits = conn.execute(
        "SELECT COUNT(*) FROM media_mentions WHERE operator_id = ? "
        "AND tags LIKE '%succession%'",
        (op_id,),
    ).fetchone()[0]
    if hits > 0:
        return 0.9, {"media_hits": hits}
    return 0.5, {"media_hits": 0}


def compute_all(db_path: Path = DB_PATH) -> int:
    init_db(db_path)
    conn = connect(db_path)
    try:
        # Percentiles for normalization
        fleet_p95 = conn.execute(
            "SELECT total FROM fleet WHERE total IS NOT NULL "
            "ORDER BY total DESC LIMIT 1 OFFSET "
            "(SELECT CAST(COUNT(*) * 0.05 AS INT) FROM fleet WHERE total IS NOT NULL)"
        ).fetchone()
        parking_p95 = conn.execute(
            "SELECT parking_area_m2 FROM facility WHERE parking_area_m2 IS NOT NULL "
            "ORDER BY parking_area_m2 DESC LIMIT 1 OFFSET "
            "(SELECT CAST(COUNT(*) * 0.05 AS INT) FROM facility "
            "WHERE parking_area_m2 IS NOT NULL)"
        ).fetchone()
        fleet_p95_v   = (fleet_p95[0] if fleet_p95 else None) or 50
        parking_p95_v = (parking_p95[0] if parking_p95 else None) or 10000

        ops = conn.execute(
            "SELECT id, name, president FROM operators ORDER BY id"
        ).fetchall()
        conn.execute("DELETE FROM scores")

        scored = []
        for op in ops:
            size, size_det   = _size_score(conn, op["id"], fleet_p95_v, parking_p95_v)
            indep, indep_det = _independence_score(conn, op["id"])
            clar, clar_det   = _clarity_score(conn, op["id"], op["president"])
            succ, succ_det   = _succession_score(conn, op["id"])

            fit = indep * (0.5 * size + 0.3 * clar + 0.2 * succ)
            rationale_bits = []
            if indep_det.get("group"):
                rationale_bits.append(f"group={indep_det['group']}")
            rationale_bits.append(f"fleet={size_det['fleet_total']}")
            rationale_bits.append(f"parking_m2={int(size_det['parking_m2'])}")
            rationale_bits.append(f"permits={size_det['permit_types']}")
            rationale_bits.append(f"sh={clar_det['n_shareholders']}")
            rationale = " | ".join(rationale_bits)

            scored.append((op["id"], size, indep, clar, succ, fit, rationale))

        scored.sort(key=lambda r: (-r[5], -r[1]))
        for rank, row in enumerate(scored, 1):
            op_id, size, indep, clar, succ, fit, rationale = row
            conn.execute(
                "INSERT INTO scores (operator_id, size_score, independence_score, "
                "clarity_score, succession_score, ma_fit_score, rank, rationale) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (op_id, size, indep, clar, succ, fit, rank, rationale),
            )
        conn.commit()
        return len(scored)
    finally:
        conn.close()


if __name__ == "__main__":
    n = compute_all()
    print(f"Scored {n} operators")
    conn = connect(DB_PATH)
    print("\n--- Top 30 by ma_fit_score ---")
    rows = conn.execute(
        "SELECT s.rank, o.name, o.city, "
        "ROUND(s.ma_fit_score, 3) fit, "
        "ROUND(s.size_score, 3) size, "
        "ROUND(s.independence_score, 2) indep, "
        "s.rationale FROM scores s JOIN operators o ON s.operator_id = o.id "
        "ORDER BY s.rank LIMIT 30"
    ).fetchall()
    for r in rows:
        print(f"{r['rank']:>3}. {(r['name'] or '')[:40]:<40} "
              f"{(r['city'] or '')[:18]:<18} fit={r['fit']} "
              f"size={r['size']} indep={r['indep']} | {r['rationale']}")
    conn.close()
