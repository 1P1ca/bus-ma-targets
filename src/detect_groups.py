"""Stage 2: rollup / consolidator detection.

Two passes over the seeded operators table:

  1. **Known-consolidator dictionary** — hard-coded name/email-domain/president
     patterns for Transdev, busbusbus (Desmarais), Groupe Autocar Jeannois,
     Keolis/Orléans Express, FirstGroup/Transco, Groupe Renaud, Gaudreault,
     Guévin, Viens, Verreault, Maheux, Chevrette, Ouellet, Hélie, Dion,
     Bell-Horizon, Moreau, Lachaine, Landry, Bellemare.  These get
     `is_excluded = 1` so they're dropped from Top-Targets.

  2. **Emergent shared-signal clusters** — operators not matched by #1 but
     sharing an email domain, street address, president, or délégué votant
     with ≥2 siblings.  Flagged `kind='shared-address'` etc., NOT excluded —
     they surface on the Groups sheet as candidate mini-rollups for review.

Pass 3 (after the REQ scraper runs) will extend this with shared NEQ
actionnaires.  That pass lives in a separate function so we can re-run it
after ownership enrichment.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

from db import DB_PATH, assign_group, connect, init_db, upsert_group

# ---------------------------------------------------------------------------
# Known consolidator dictionary
# ---------------------------------------------------------------------------
# Each entry: (canonical group name, kind, is_excluded, [patterns])
# Patterns are matched case-insensitively against: name, legal_name, email,
# website, president, delegue_votant.  Any match assigns the operator to the
# group.
# ---------------------------------------------------------------------------

KNOWN_CONSOLIDATORS: list[tuple[str, str, int, list[str]]] = [
    ("Transdev Québec", "consolidator", 1, [
        r"\btransdev\b", r"@transdev\.",
    ]),
    ("Keolis / Orléans Express", "consolidator", 1, [
        r"\bkeolis\b", r"orl[ée]ans express", r"@keolis", r"@orleansexpress",
    ]),
    ("FirstGroup / Transco (First Student)", "consolidator", 1, [
        r"\btransco\b", r"first student", r"firstgroup", r"@firstgroup",
        r"@firststudent", r"@transco",
    ]),
    ("Groupe Autocar Jeannois", "consolidator", 1, [
        r"autocar jeannois", r"groupe jeannois", r"@jeannois",
        r"@autocarjeannois",
    ]),
    ("busbusbus (Desmarais)", "consolidator", 1, [
        r"busbusbus", r"@busbusbus",
    ]),
    ("Groupe Renaud", "consolidator", 1, [
        r"groupe renaud", r"autobus renaud", r"@grouperenaud",
        r"@autobusrenaud", r"@renaud",
    ]),
    ("Groupe Gaudreault", "consolidator", 1, [
        r"groupe[- ]?gaudreault", r"autobus gaudreault", r"gaudreault\b",
        r"@gaudreault",
    ]),
    ("Groupe Guévin", "consolidator", 1, [
        r"groupe gu[ée]vin", r"autobus gu[ée]vin", r"@gu[ée]vin",
        r"gu[ée]vin\b",
    ]),
    ("Groupe Viens", "consolidator", 1, [
        r"groupe l?viens", r"autobus viens", r"@groupel?viens",
        r"@autobusviens", r"viens\.ca", r"viens\.com",
    ]),
    ("Groupe Verreault", "consolidator", 1, [
        r"groupe verreault", r"autobus verreault", r"@verreault",
        r"verreault\.(ca|com|net)",
    ]),
    ("Groupe Maheux", "consolidator", 1, [
        r"groupe maheux", r"autocars? maheux", r"autobus maheux",
        r"@(autobus|autocar|groupe)?maheux", r"maheux\.(ca|com|qc\.ca)",
    ]),
    ("Groupe Chevrette", "consolidator", 1, [
        r"groupe chevrette", r"autobus chevrette", r"@chevrette",
        r"chevrette\.(ca|com)",
    ]),
    ("Groupe Ouellet", "consolidator", 1, [
        r"groupe ouellet", r"autobus ouellet", r"@(groupe|autobus)?ouellet",
        r"ouellet\.(ca|com)",
    ]),
    ("Groupe Hélie", "consolidator", 1, [
        r"groupe h[ée]lie", r"autobus h[ée]lie", r"@(groupe|autobus)?h[ée]lie",
        r"h[ée]lie\.(ca|com)",
    ]),
    ("Groupe Bell-Horizon", "consolidator", 1, [
        r"bell[-\s]?horizon", r"@bellhorizon", r"bellhorizon\.",
    ]),
    ("Groupe Dion", "consolidator", 1, [
        r"groupe dion", r"autobus dion", r"@(groupe|autobus)?dion",
        r"autobusdion\.", r"groupedion\.",
    ]),
    ("Groupe Moreau", "consolidator", 1, [
        r"groupe moreau", r"groupe gilles moreau", r"autobus moreau",
        r"@(groupe|autobus)?moreau", r"groupegillesmoreau\.",
    ]),
    ("Groupe Lachaine", "consolidator", 1, [
        r"groupe lachaine", r"autobus lachaine", r"@.*lachaine",
        r"lachaine\.(ca|com)",
    ]),
    ("Groupe Landry", "consolidator", 1, [
        r"groupe landry", r"autobus[- ]?landry", r"@(groupe|autobus)?[-]?landry",
        r"autobus-?landry\.",
    ]),
    ("Groupe Bellemare", "consolidator", 1, [
        r"groupe bellemare", r"transport bellemare", r"@bellemare",
        r"bellemare\.(ca|com)",
    ]),
]


def _match_consolidator(op: dict) -> tuple[str, str, int, str] | None:
    """Return (group_name, kind, is_excluded, reason) or None."""
    blob_parts = [
        op.get("name"), op.get("legal_name"), op.get("email"),
        op.get("website"), op.get("president"), op.get("delegue_votant"),
    ]
    blob = " | ".join(p for p in blob_parts if p).lower()
    for name, kind, excluded, patterns in KNOWN_CONSOLIDATORS:
        for pat in patterns:
            m = re.search(pat, blob, re.IGNORECASE)
            if m:
                return (name, kind, excluded, f"matched /{pat}/ in '{m.group(0)}'")
    return None


# ---------------------------------------------------------------------------
# Shared-signal clustering
# ---------------------------------------------------------------------------

_GENERIC_EMAIL_DOMAINS = {
    # webmail
    "gmail.com", "hotmail.com", "hotmail.fr", "hotmail.ca", "outlook.com",
    "outlook.fr", "yahoo.ca", "yahoo.com", "yahoo.fr",
    "live.ca", "live.com", "me.com", "icloud.com", "aol.com", "gmx.com",
    "protonmail.com", "proton.me",
    # ISP / telco (Québec)
    "videotron.ca", "videotron.qc.ca", "bell.net", "bellnet.ca",
    "sympatico.ca", "globetrotter.net", "cgocable.ca", "cgocable.net",
    "telus.net", "cooptel.qc.ca", "cooptel.net", "axion.ca", "sogetel.net",
    "sogetel.ca", "derytele.com", "xittel.net", "xittel.ca",
    "courriel.com", "ccapcable.com", "tlb.sympatico.ca",
}


def _email_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    d = email.rsplit("@", 1)[-1].strip().lower()
    if d in _GENERIC_EMAIL_DOMAINS:
        return None
    return d


def _norm_address(addr: str | None, city: str | None) -> str | None:
    if not addr:
        return None
    a = re.sub(r"\s+", " ", addr.strip().lower())
    a = re.sub(r"[.,]", "", a)
    c = (city or "").strip().lower()
    return f"{a}|{c}" if c else a


def _norm_person(name: str | None) -> str | None:
    if not name:
        return None
    n = re.sub(r"\s+", " ", name.strip().lower())
    # drop "jr.", "sr." suffixes for matching
    n = re.sub(r"\b(jr|sr)\.?\b", "", n).strip()
    return n or None


def detect_groups(db_path: Path = DB_PATH) -> dict:
    init_db(db_path)
    conn = connect(db_path)
    stats = {"consolidator_hits": 0, "shared_signal_groups": 0, "operators_grouped": 0}
    try:
        ops = [dict(r) for r in conn.execute("SELECT * FROM operators").fetchall()]

        # --- Pass 1: known-consolidator dictionary ---
        for op in ops:
            m = _match_consolidator(op)
            if not m:
                continue
            gname, kind, excluded, reason = m
            gid = upsert_group(conn, gname, kind=kind, is_excluded=excluded,
                               description="Known consolidator / rollup parent")
            assign_group(conn, op["id"], gid, reason=reason)
            stats["consolidator_hits"] += 1

        conn.commit()

        # --- Pass 2: emergent shared-signal clusters ---
        # Refresh: only cluster operators not yet assigned a group.
        unassigned = [dict(r) for r in conn.execute(
            "SELECT * FROM operators WHERE group_id IS NULL"
        ).fetchall()]

        by_email_domain: dict[str, list[dict]] = defaultdict(list)
        by_address:      dict[str, list[dict]] = defaultdict(list)
        by_president:    dict[str, list[dict]] = defaultdict(list)
        by_delegue:      dict[str, list[dict]] = defaultdict(list)

        for op in unassigned:
            if d := _email_domain(op.get("email")):
                by_email_domain[d].append(op)
            if a := _norm_address(op.get("address"), op.get("city")):
                by_address[a].append(op)
            if p := _norm_person(op.get("president")):
                by_president[p].append(op)
            if dv := _norm_person(op.get("delegue_votant")):
                by_delegue[dv].append(op)

        def _apply_cluster(bucket: dict[str, list[dict]], signal_label: str,
                           kind: str) -> None:
            for key, members in bucket.items():
                if len(members) < 2:
                    continue
                # skip if all members already assigned in a prior sub-pass
                members = [m for m in members if _current_group_id(conn, m["id"]) is None]
                if len(members) < 2:
                    continue
                gname = f"[{kind}] {signal_label}: {key[:80]}"
                gid = upsert_group(conn, gname, kind=kind, is_excluded=0,
                                   description=f"Auto-clustered by shared {signal_label}")
                for op in members:
                    assign_group(conn, op["id"], gid,
                                 reason=f"shared {signal_label} = {key!r}")
                    stats["operators_grouped"] += 1
                stats["shared_signal_groups"] += 1

        # Order matters: strongest signal first.
        _apply_cluster(by_email_domain, "email-domain",  "shared-email")
        _apply_cluster(by_address,      "address",       "shared-address")
        _apply_cluster(by_president,    "président",     "shared-president")
        _apply_cluster(by_delegue,      "délégué-votant", "shared-delegue")

        conn.commit()
    finally:
        conn.close()
    return stats


def _current_group_id(conn, operator_id: int):
    row = conn.execute(
        "SELECT group_id FROM operators WHERE id = ?", (operator_id,)
    ).fetchone()
    return row["group_id"] if row else None


def summary(db_path: Path = DB_PATH) -> list[tuple]:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT g.name, g.kind, g.is_excluded, COUNT(o.id) AS n "
            "FROM operators o JOIN groups g ON o.group_id = g.id "
            "GROUP BY g.id ORDER BY n DESC, g.name"
        ).fetchall()
        return [tuple(r) for r in rows]
    finally:
        conn.close()


if __name__ == "__main__":
    stats = detect_groups()
    print(f"Group detection complete: {stats}")
    print()
    print(f"{'Group':<60} {'Kind':<20} {'Excl':>5} {'N':>5}")
    print("-" * 95)
    for name, kind, excluded, n in summary():
        print(f"{(name or '')[:60]:<60} {(kind or '')[:20]:<20} {excluded:>5} {n:>5}")
