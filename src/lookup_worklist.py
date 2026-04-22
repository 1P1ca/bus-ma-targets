"""Build a prioritized manual-lookup worklist for CTQ + REQ.

Since Maps enrichment is still blocked (API not enabled), we prioritize on
the signals we already have:
  1. Independent (not in a consolidator group, is_excluded = 0 OR group is null)
  2. Has a website / email (indicates active operating company, easier to
     disambiguate in REQ search)
  3. Has a president listed (REQ lookup target)
  4. Prefers names suggesting standalone ops ("Autobus <surname>",
     "Transport <surname>", "Entreprises <surname>") — these are the
     generational-transition targets the brief wants.

Output: output/lookup_worklist.csv  with columns:
    priority, fta_no, name, city, postal, phone, email, website, president,
    group_hint,
    ctq_search_url  (pre-filled name search against CTQ)
    req_search_url  (pre-filled name search against REQ)
    ctq_dump_filename, req_dump_filename

Then:
  - Open each `ctq_search_url` in a real browser, solve reCAPTCHA, open the
    dossier, File > Save Page As > save into data/raw/ctq_html/<filename>
  - Same for REQ -> data/raw/req_html/<filename>
  - Run `python3 run.py req ctq score export` to ingest + re-score + re-export
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from urllib.parse import quote_plus

sys.path.insert(0, str(Path(__file__).parent))
from db import DB_PATH, connect, init_db

ROOT = Path(__file__).resolve().parent.parent
OUT_CSV = ROOT / "output" / "lookup_worklist.csv"

CTQ_BASE = "https://www.pes.ctq.gouv.qc.ca/pes2/mvc/dossierclient?voletContexte=RECHERCHE_GLOBAL_MENU"
REQ_BASE = ("https://www.registreentreprises.gouv.qc.ca/RQAnonymeGR/GR/GR03/"
            "GR03A2_19A_PIU_RechEnt_PC/PageRechSimple.aspx")


def _slug(fta_no: str) -> str:
    # filename-safe, keeps fta_no as the primary key
    return "".join(c for c in str(fta_no) if c.isalnum()) or "unknown"


def build(limit: int = 60, db_path: Path = DB_PATH, out_path: Path = OUT_CSV) -> Path:
    init_db(db_path)
    conn = connect(db_path)
    try:
        rows = conn.execute("""
            SELECT o.fta_no, o.name, o.city, o.postal, o.phone, o.email,
                   o.website, o.president, o.delegue_votant,
                   g.name AS group_name, g.is_excluded,
                   s.independence_score, s.ma_fit_score
            FROM operators o
            LEFT JOIN groups g ON o.group_id = g.id
            LEFT JOIN scores s ON s.operator_id = o.id
            WHERE (g.is_excluded IS NULL OR g.is_excluded = 0)
            ORDER BY
                /* independents first, then emergent clusters */
                CASE WHEN o.group_id IS NULL THEN 0 ELSE 1 END,
                /* then "name signals a family business" */
                CASE
                  WHEN o.name LIKE 'Autobus %'       THEN 0
                  WHEN o.name LIKE 'Transport %'     THEN 1
                  WHEN o.name LIKE 'Entreprises %'   THEN 2
                  WHEN o.name LIKE 'Autocars %'      THEN 3
                  ELSE 9
                END,
                /* prefer ones with an actual website (active, findable) */
                CASE WHEN o.website IS NULL OR o.website = '' THEN 1 ELSE 0 END,
                o.name
            LIMIT ?
        """, (limit,)).fetchall()

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "priority", "fta_no", "name", "city", "postal", "phone",
                "email", "website", "president", "group_hint",
                "ctq_search_url", "ctq_dump_filename",
                "req_search_url", "req_dump_filename",
            ])
            for i, r in enumerate(rows, 1):
                fn = _slug(r["fta_no"]) + ".html"
                q = quote_plus(r["name"] or "")
                w.writerow([
                    i, r["fta_no"], r["name"], r["city"], r["postal"],
                    r["phone"], r["email"], r["website"], r["president"],
                    r["group_name"] or "",
                    CTQ_BASE,                      # form needs filling by hand
                    fn,
                    REQ_BASE + f"?Mode=Simple&Texte={q}",
                    fn,
                ])
        return out_path
    finally:
        conn.close()


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    p = build(limit=n)
    print(f"Wrote {p}  (top {n} manual-lookup candidates)")
    print()
    print("Workflow:")
    print("  1. Open output/lookup_worklist.csv.")
    print("  2. For each row, open ctq_search_url and req_search_url in your browser.")
    print("  3. Search by name (or NEQ if you find it on one site, then paste on the other).")
    print(f"  4. File > Save As > save page HTML to:")
    print(f"       data/raw/ctq_html/<ctq_dump_filename>")
    print(f"       data/raw/req_html/<req_dump_filename>")
    print("  5. When a batch is saved, run:")
    print("       python3 run.py req ctq score export")
