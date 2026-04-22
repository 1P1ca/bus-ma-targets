"""Stage 3: REQ (Registre des entreprises du Québec) ownership scraper.

Target: https://www.registreentreprises.gouv.qc.ca/RQAnonymeGR/GR/GR03/
        GR03A2_19A_PIU_RechEnt_PC/PageRechSimple.aspx

REQ is behind Cloudflare "Just a moment..." challenge — plain HTTP returns
HTTP 403 with a JS challenge page. Real browser required (Playwright).

Like scrape_ctq.py this module ships in two modes:

  (A) Playwright solver  [skeleton; uncomment + pip install playwright]
  (B) Manual dump ingest  — save "État de renseignements" PDF/HTML into
      data/raw/req_html/<fta_no>.html or <neq>.html and run
      `python scrape_req.py ingest`

Fields we want:
    - NEQ
    - Dénomination / forme juridique
    - Date de constitution
    - Adresse du siège
    - Liste des actionnaires (noms, % si divulgué — Québec exige la
      divulgation des 3 principaux actionnaires si corp. non cotée)
    - Liste des administrateurs / président / sec.-trésorier
    - Liste des établissements (peut révéler des filiales)

Parsed rows land in the `ownership` table. Shareholders with pct >= 25 (or
single-majority-family-surname) drive the `ownership_clarity` score.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Optional

from db import DB_PATH, connect, init_db

ROOT = Path(__file__).resolve().parent.parent
HTML_DIR = ROOT / "data" / "raw" / "req_html"
HTML_DIR.mkdir(parents=True, exist_ok=True)

REQ_BASE = "https://www.registreentreprises.gouv.qc.ca"
REQ_SEARCH_URL = (
    f"{REQ_BASE}/RQAnonymeGR/GR/GR03/GR03A2_19A_PIU_RechEnt_PC/PageRechSimple.aspx"
)


# ---------------------------------------------------------------------------
# Parser for saved "État de renseignements" HTML
# ---------------------------------------------------------------------------

def parse_etat_html(html: str) -> dict:
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except ImportError:
        BeautifulSoup = None  # type: ignore

    out: dict = {
        "neq": None, "legal_name": None, "legal_form": None,
        "date_constitution": None, "hq_address": None,
        "administrators": [], "shareholders": [], "establishments": [],
        "president_name": None,
    }

    if BeautifulSoup is None:
        m = re.search(r"NEQ\s*[:\s]+(\d{10})", html)
        if m:
            out["neq"] = m.group(1)
        return out

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    m = re.search(r"Num[ée]ro d'entreprise[^\d]{0,20}(\d{10})", text) \
        or re.search(r"\bNEQ\b[^\d]{0,10}(\d{10})", text)
    if m:
        out["neq"] = m.group(1)

    m = re.search(r"D[ée]nomination[^\n:]{0,30}[:\n]\s*([^\n]{3,160})", text)
    if m:
        out["legal_name"] = m.group(1).strip()

    m = re.search(r"Forme juridique[^\n:]{0,20}[:\n]\s*([^\n]{3,80})", text)
    if m:
        out["legal_form"] = m.group(1).strip()

    m = re.search(r"Date de (?:constitution|d[ée]but)[^\n:]{0,20}[:\n]\s*([^\n]{6,40})", text)
    if m:
        out["date_constitution"] = m.group(1).strip()

    m = re.search(r"Adresse du (?:domicile|si[èe]ge)[^\n:]{0,20}[:\n]\s*([^\n]{5,200})", text)
    if m:
        out["hq_address"] = m.group(1).strip()

    # Administrators: section "Liste des administrateurs" -> rows of names
    m = re.search(r"Liste des administrateurs([\s\S]{0,4000}?)(?=Liste des|$)", text, re.I)
    if m:
        for name_m in re.finditer(r"Nom\s*[:\n]\s*([^\n]{2,80})", m.group(1)):
            out["administrators"].append(name_m.group(1).strip())

    # President
    m = re.search(r"Pr[ée]sident[^\n:]{0,20}[:\n]\s*([^\n]{2,80})", text)
    if m:
        out["president_name"] = m.group(1).strip()

    # Shareholders: "Liste des actionnaires" — format varies; best-effort
    m = re.search(r"Liste des actionnaires([\s\S]{0,6000}?)(?=Liste des|$)", text, re.I)
    if m:
        sh_block = m.group(1)
        for row in re.finditer(
            r"Nom\s*[:\n]\s*([^\n]{2,120})"
            r"(?:[\s\S]{0,400}?Pourcentage[^\d]{0,10}(\d{1,3})\s*%)?",
            sh_block,
        ):
            out["shareholders"].append({
                "name": row.group(1).strip(),
                "pct": int(row.group(2)) if row.group(2) else None,
            })

    # Establishments
    m = re.search(r"Liste des [ée]tablissements([\s\S]{0,6000}?)(?=Liste des|$)", text, re.I)
    if m:
        for row in re.finditer(r"Adresse[^\n:]{0,20}[:\n]\s*([^\n]{5,200})", m.group(1)):
            out["establishments"].append(row.group(1).strip())

    return out


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_etat(conn: sqlite3.Connection, operator_id: int, parsed: dict,
                 source_url: str = None) -> int:
    if parsed.get("neq"):
        conn.execute(
            "UPDATE operators SET neq = COALESCE(operators.neq, ?), "
            "legal_name = COALESCE(?, operators.legal_name), "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (parsed["neq"], parsed.get("legal_name"), operator_id),
        )

    conn.execute(
        "DELETE FROM ownership WHERE operator_id = ? AND source = 'req'",
        (operator_id,),
    )
    n = 0
    for sh in parsed.get("shareholders", []):
        conn.execute(
            "INSERT INTO ownership (operator_id, entity_name, entity_type, "
            "role, pct, source, source_url) "
            "VALUES (?, ?, 'unknown', 'actionnaire', ?, 'req', ?)",
            (operator_id, sh["name"], sh.get("pct"), source_url or REQ_SEARCH_URL),
        )
        n += 1
    for adm in parsed.get("administrators", []):
        conn.execute(
            "INSERT INTO ownership (operator_id, entity_name, entity_type, "
            "role, source, source_url) "
            "VALUES (?, ?, 'person', 'administrateur', 'req', ?)",
            (operator_id, adm, source_url or REQ_SEARCH_URL),
        )
        n += 1
    if parsed.get("president_name"):
        conn.execute(
            "INSERT INTO ownership (operator_id, entity_name, entity_type, "
            "role, source, source_url) "
            "VALUES (?, ?, 'person', 'president', 'req', ?)",
            (operator_id, parsed["president_name"], source_url or REQ_SEARCH_URL),
        )
        n += 1
    conn.commit()
    return n


# ---------------------------------------------------------------------------
# Manual ingest
# ---------------------------------------------------------------------------

def ingest_manual_dumps(db_path: Path = DB_PATH) -> dict:
    init_db(db_path)
    conn = connect(db_path)
    stats = {"files": 0, "ingested": 0, "unmatched": []}
    try:
        for path in sorted(HTML_DIR.glob("*.html")):
            stats["files"] += 1
            html = path.read_text(encoding="utf-8", errors="replace")
            parsed = parse_etat_html(html)

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
            n = persist_etat(conn, op_row["id"], parsed,
                             source_url=f"{REQ_BASE}/...{path.name}")
            stats["ingested"] += 1
            print(f"  {path.name}: NEQ={parsed.get('neq')}, "
                  f"{len(parsed.get('shareholders', []))} shareholders, "
                  f"{len(parsed.get('administrators', []))} admins")
        return stats
    finally:
        conn.close()


def scrape_one_playwright(name: str = None, neq: str = None,
                          out_path: Path = None):
    """Not implemented yet. See scrape_ctq.scrape_one_playwright docstring.

    Same pattern: headful Chromium, let Cloudflare JS challenge resolve,
    search by NEQ (preferred) or name, click "État de renseignements",
    save the full HTML. NEQ is the ideal key because REQ search-by-name
    is a disambig UI.
    """
    raise NotImplementedError(
        "Playwright REQ scraper not yet wired. Use ingest_manual_dumps() "
        "after saving État-de-renseignements HTML to data/raw/req_html/."
    )


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "ingest"
    if cmd == "ingest":
        stats = ingest_manual_dumps()
        print(f"\nREQ ingest: {stats['ingested']}/{stats['files']} files matched")
        for u in stats["unmatched"][:20]:
            print(f"  unmatched: {u}")
    elif cmd == "test-parse" and len(sys.argv) > 2:
        html = Path(sys.argv[2]).read_text(encoding="utf-8", errors="replace")
        print(json.dumps(parse_etat_html(html), indent=2, ensure_ascii=False))
    else:
        print("Usage: scrape_req.py ingest | test-parse <file.html>")
