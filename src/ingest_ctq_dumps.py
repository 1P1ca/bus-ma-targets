#!/usr/bin/env python3
"""Ingest CTQ HTML dumps from data/raw/ctq_*.html and populate fleet table."""

import sys
from pathlib import Path
import sqlite3

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from scrape_ctq import parse_dossier_html, persist_dossier
from db import DB_PATH, connect, init_db

def ingest_ctq_dumps():
    """Process all ctq_*.html files and populate fleet table."""
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    ctq_files = sorted(raw_dir.glob("ctq_*.html"))

    print(f"Found {len(ctq_files)} CTQ HTML files\n")

    init_db(DB_PATH)
    conn = connect(DB_PATH)

    stats = {
        "processed": 0,
        "success": 0,
        "errors": 0,
        "permits": 0,
        "vehicles_total": 0,
    }

    try:
        for fpath in ctq_files:
            try:
                # Extract operator ID from filename (ctq_107.html -> 107)
                op_id = int(fpath.stem.split('_')[1])

                # Read and parse HTML
                html = fpath.read_text(encoding='utf-8', errors='replace')
                parsed = parse_dossier_html(html)

                # Persist to fleet table
                n_permits = persist_dossier(conn, op_id, parsed,
                                           source_url="https://www.pes.ctq.gouv.qc.ca")

                veh = parsed.get('vehicles_declared', 0) or 0
                stats["success"] += 1
                stats["permits"] += n_permits
                stats["vehicles_total"] += veh

                if n_permits > 0 or veh > 0:
                    print(f"  ✓ ID {op_id:3d}: {n_permits:2d} permits, {veh:4d} vehicles, NEQ={parsed.get('neq')}")

            except Exception as e:
                stats["errors"] += 1
                print(f"  ✗ {fpath.name}: {str(e)[:60]}")

            stats["processed"] += 1

        print(f"\n{'='*70}")
        print(f"Total processed: {stats['success']}/{stats['processed']}")
        print(f"Permits found: {stats['permits']}")
        print(f"Total vehicles declared: {stats['vehicles_total']}")
        print(f"Errors: {stats['errors']}")

        # Show vehicle count by type
        rows = conn.execute("""
            SELECT
                SUM(buses_scolaire) as scolaire,
                SUM(buses_coach) as coach,
                SUM(buses_adapte) as adapte,
                SUM(buses_urbain) as urbain,
                SUM(total) as total
            FROM fleet WHERE source = 'ctq'
        """).fetchone()

        print(f"\nFleet by type (from CTQ permits):")
        print(f"  School buses: {rows[0] or 0}")
        print(f"  Coach/Intercity: {rows[1] or 0}")
        print(f"  Adapted transit: {rows[2] or 0}")
        print(f"  Urban transit: {rows[3] or 0}")
        print(f"  TOTAL: {rows[4] or 0}")

        return stats

    finally:
        conn.close()

if __name__ == "__main__":
    ingest_ctq_dumps()
