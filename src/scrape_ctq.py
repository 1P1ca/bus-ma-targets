"""Stage 4: scrape CTQ (Commission des transports du Québec) permits + fleet.

Target: https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient

The CTQ "Dossier d'une entreprise" search form is:
    method:  POST (JSF / Spring Webflow)
    action:  /pes2/mvc/dossierclient;jsessionid=.../?execution=eNsM
    fields:  mainForm:personneMorale   (company name)
             mainForm:personnePhysique (natural person)
             mainForm:municipalite     (municipality)
             mainForm:neq              (NEQ, 10 digits)
             mainForm:numeroPermis     (permit number)
             mainForm:numeroDossier    (CTQ file #)
             javax.faces.ViewState     (required token)
    guard:   Google reCAPTCHA v2/v3 invoked via `executeRecaptcha(buttonId)`
             on button click — no valid token => server returns empty results.

IMPLICATIONS
------------
Pure-requests scraping won't pass the reCAPTCHA guard. Two paths:

  A) Interactive / Playwright with reCAPTCHA solver  (CTQ_SOLVER=playwright)
     - Uses playwright + optional 2captcha integration (env: TWOCAPTCHA_KEY)
     - Works for the full 400-operator batch at ~$0.003/solve ≈ $1.20 total
     - Recommended for automation.

  B) Manual CSV / HTML dump (CTQ_SOLVER=manual)
     - Operator runs the search in a real browser, saves result HTML into
       data/raw/ctq_html/<neq_or_fta>.html
     - This module then parses those files into permits/fleet tables.
     - Viable for the top-30 acquisition targets.

This file implements (B) in full (parser is what matters long-term) and
leaves a clear Playwright hook for (A).

The RSS decision feed gives only *new* decisions, not the historical permit
inventory — useful as a delta feed, not for initial backfill.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, Optional

from db import DB_PATH, connect, init_db

ROOT = Path(__file__).resolve().parent.parent
HTML_DIR = ROOT / "data" / "raw" / "ctq_html"
HTML_DIR.mkdir(parents=True, exist_ok=True)

CTQ_BASE = "https://www.pes.ctq.gouv.qc.ca"
CTQ_SEARCH_URL = f"{CTQ_BASE}/pes2/mvc/dossierclient?voletContexte=RECHERCHE_GLOBAL_MENU"

# Keep permit-type normalisation central so scoring can count by type.
PERMIT_TYPE_NORMALIZE = {
    "transport par autobus - scolaire": "scolaire",
    "autobus - scolaire": "scolaire",
    "transport par autobus - nolisé": "nolise",
    "autobus - nolisé": "nolise",
    "transport par autobus - interurbain": "interurbain",
    "autobus - interurbain": "interurbain",
    "transport par autobus - urbain": "urbain",
    "transport adapté": "adapte",
    "autobus - adapté": "adapte",
    "taxi": "taxi",
}


def _norm_permit_type(raw: str) -> str:
    if not raw:
        return ""
    k = raw.strip().lower()
    return PERMIT_TYPE_NORMALIZE.get(k, k)


# ---------------------------------------------------------------------------
# Parser for saved dossier HTML
# ---------------------------------------------------------------------------

def parse_dossier_html(html: str) -> dict:
    """Return {'neq', 'permits': [...], 'vehicles_declared': int|None, 'raw_text': str}

    The CTQ dossier page uses nested <table> layouts with French labels. We
    grep for known labels and take the adjacent cell's text. `bs4` is used
    if available, else a regex fallback.
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except ImportError:
        BeautifulSoup = None  # type: ignore

    out: dict = {"neq": None, "permits": [], "vehicles_declared": None,
                 "raw_text": ""}

    if BeautifulSoup is None:
        # regex-only fallback (coarse)
        neq = re.search(r"NEQ\s*[:\s]*\s*(\d{10})", html)
        if neq:
            out["neq"] = neq.group(1)
        veh = re.search(r"Nombre de v[ée]hicules[^\d]{0,30}(\d+)", html, re.I)
        if veh:
            out["vehicles_declared"] = int(veh.group(1))
        # permit rows: "Autobus - Scolaire", "Autobus - Nolisé", etc.
        for m in re.finditer(
            r"(Autobus\s*-\s*(?:Scolaire|Nolis[ée]|Interurbain|Urbain|Adapt[ée])"
            r"|Transport\s+adapt[ée]|Taxi)"
            r"[\s\S]{0,400}?Permis[^\d]{0,20}(P-\d{3,}(?:-\d+)?)?"
            r"[\s\S]{0,400}?(\d+)\s*v[ée]hicules?",
            html, re.I,
        ):
            out["permits"].append({
                "permit_type": _norm_permit_type(m.group(1)),
                "permit_no": m.group(2),
                "declared_vehicles": int(m.group(3)) if m.group(3) else None,
                "status": None,
            })
        return out

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    out["raw_text"] = text

    # NEQ
    m = re.search(r"NEQ\s*[:\s]+(\d{10})", text)
    if m:
        out["neq"] = m.group(1)

    # Total declared vehicles (top of dossier)
    m = re.search(r"Nombre (?:total )?de v[ée]hicules[^\d]{0,20}(\d+)", text, re.I)
    if m:
        out["vehicles_declared"] = int(m.group(1))

    # Permits table: look for rows where first cell is a known permit type.
    for tbl in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower()
                   for th in tbl.find_all("th")]
        if not headers:
            continue
        want = {"type", "numéro", "numero", "véhicules", "vehicules", "statut"}
        if not any(w in h for h in headers for w in want):
            continue
        for tr in tbl.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue
            ptype = _norm_permit_type(cells[0])
            if not ptype:
                continue
            permit = {
                "permit_type": ptype,
                "permit_no": next((c for c in cells if re.match(r"P-?\d", c)), None),
                "declared_vehicles": None,
                "status": None,
            }
            for c in cells:
                m = re.fullmatch(r"(\d+)", c)
                if m and permit["declared_vehicles"] is None:
                    permit["declared_vehicles"] = int(c)
                if c.lower() in {"actif", "en vigueur", "suspendu", "révoqué",
                                 "revoque", "expiré", "expire"}:
                    permit["status"] = c
            out["permits"].append(permit)

    return out


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_dossier(conn: sqlite3.Connection, operator_id: int,
                    parsed: dict, source_url: str = None) -> int:
    """Write permits + fleet rows for one operator. Returns # permits written."""
    if parsed.get("neq"):
        conn.execute(
            "UPDATE operators SET neq = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE id = ? AND (neq IS NULL OR neq = '')",
            (parsed["neq"], operator_id),
        )

    # Clear prior CTQ permits to avoid duplicates on re-run
    conn.execute(
        "DELETE FROM permits WHERE operator_id = ? AND source_url LIKE ?",
        (operator_id, f"{CTQ_BASE}%"),
    )

    total_veh_from_permits = 0
    for p in parsed.get("permits", []):
        conn.execute(
            "INSERT INTO permits (operator_id, permit_type, permit_no, "
            "declared_vehicles, status, source_url, raw) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (operator_id, p.get("permit_type"), p.get("permit_no"),
             p.get("declared_vehicles"), p.get("status"),
             source_url or CTQ_SEARCH_URL, json.dumps(p, ensure_ascii=False)),
        )
        total_veh_from_permits += p.get("declared_vehicles") or 0

    # Upsert a fleet row from the CTQ declaration
    declared = parsed.get("vehicles_declared") or total_veh_from_permits or None
    if declared is not None:
        by_type: dict = {}
        for p in parsed.get("permits", []):
            by_type[p.get("permit_type")] = (
                by_type.get(p.get("permit_type"), 0) + (p.get("declared_vehicles") or 0)
            )
        conn.execute(
            "DELETE FROM fleet WHERE operator_id = ? AND source = 'ctq'",
            (operator_id,),
        )
        conn.execute(
            "INSERT INTO fleet (operator_id, source, buses_scolaire, buses_coach, "
            "buses_adapte, buses_urbain, other, total, as_of_date) "
            "VALUES (?, 'ctq', ?, ?, ?, ?, ?, ?, DATE('now'))",
            (operator_id,
             by_type.get("scolaire"),
             (by_type.get("nolise") or 0) + (by_type.get("interurbain") or 0) or None,
             by_type.get("adapte"),
             by_type.get("urbain"),
             sum(v for k, v in by_type.items()
                 if k not in {"scolaire", "nolise", "interurbain",
                              "adapte", "urbain"} and v) or None,
             declared),
        )
    conn.commit()
    return len(parsed.get("permits", []))


# ---------------------------------------------------------------------------
# Manual-mode orchestration: read saved HTML files from data/raw/ctq_html
# ---------------------------------------------------------------------------

def ingest_manual_dumps(db_path: Path = DB_PATH) -> dict:
    """Ingest every *.html in data/raw/ctq_html/ and map to an operator.

    Filename convention:
        <fta_no>.html         -> matches operators.fta_no
        neq_<10digits>.html   -> matches operators.neq (or updates it)
    """
    init_db(db_path)
    conn = connect(db_path)
    stats = {"files": 0, "ingested": 0, "unmatched": []}
    try:
        for path in sorted(HTML_DIR.glob("*.html")):
            stats["files"] += 1
            html = path.read_text(encoding="utf-8", errors="replace")
            parsed = parse_dossier_html(html)

            op_row = None
            stem = path.stem
            if stem.startswith("neq_") and stem[4:].isdigit():
                op_row = conn.execute(
                    "SELECT id FROM operators WHERE neq = ?", (stem[4:],)
                ).fetchone()
            else:
                op_row = conn.execute(
                    "SELECT id FROM operators WHERE fta_no = ?", (stem,)
                ).fetchone()
            if not op_row and parsed.get("neq"):
                op_row = conn.execute(
                    "SELECT id FROM operators WHERE neq = ?", (parsed["neq"],)
                ).fetchone()

            if not op_row:
                stats["unmatched"].append(path.name)
                continue
            n = persist_dossier(conn, op_row["id"], parsed,
                                source_url=f"{CTQ_BASE}/pes2/mvc/dossierclient")
            stats["ingested"] += 1
            print(f"  {path.name}: {n} permits, NEQ={parsed.get('neq')}, "
                  f"declared={parsed.get('vehicles_declared')}")
        return stats
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Playwright hook (for mode A). Skeleton — implement when Playwright is in deps.
# ---------------------------------------------------------------------------

def scrape_one_playwright(name: str = None, neq: str = None,
                          municipality: str = None, out_path: Path = None):
    """Drive the CTQ search via Playwright, solve reCAPTCHA, save HTML.

    Not implemented yet. Implementation sketch:
      1. playwright.chromium.launch(headless=False)  # reCAPTCHA hates headless
      2. goto CTQ_SEARCH_URL
      3. fill mainForm:personneMorale / municipalite / neq
      4. solve recaptcha (2captcha via TWOCAPTCHA_KEY env) OR pause for user
      5. click btnRechercherGlobal
      6. click each result row -> capture dossier HTML
      7. save to HTML_DIR / f"{neq or fta_no}.html"
    """
    raise NotImplementedError(
        "Playwright solver not yet wired. For now use ingest_manual_dumps() "
        "after saving dossier HTML files to data/raw/ctq_html/."
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "ingest"
    if cmd == "ingest":
        stats = ingest_manual_dumps()
        print(f"\nCTQ ingest: {stats['ingested']}/{stats['files']} files matched "
              f"({len(stats['unmatched'])} unmatched)")
        if stats["unmatched"]:
            for u in stats["unmatched"][:20]:
                print(f"  unmatched: {u}")
    elif cmd == "test-parse" and len(sys.argv) > 2:
        html = Path(sys.argv[2]).read_text(encoding="utf-8", errors="replace")
        print(json.dumps(parse_dossier_html(html), indent=2, ensure_ascii=False))
    else:
        print("Usage: scrape_ctq.py ingest | test-parse <file.html>")
