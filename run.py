"""Orchestrator for the bus_ma pipeline.

Stages:
  1. seed              (TSV -> operators)
  2. detect_groups     (consolidator + shared-signal clustering)
  3. scrape_req        (REQ ownership)    — manual-ingest mode by default
  4. scrape_ctq        (CTQ permits)      — manual-ingest mode by default
  5. enrich_maps       (geocode + satellite + parking estimate)
  6. score             (M&A fit composite)
  7. export_excel      (output/operators_ma.xlsx)

Usage:
  python run.py                         # full pipeline (non-destructive)
  python run.py seed groups             # specific stages only
  python run.py score export            # re-score + re-export
  python run.py --reset                 # blow away DB then seed+groups
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from db import DB_PATH, init_db
import detect_groups
import enrich_maps
import export_excel
import score
import scrape_ctq
import scrape_req
import seed

STAGES = {
    "seed":     lambda: seed.seed(),
    "groups":   lambda: detect_groups.detect_groups(),
    "req":      lambda: scrape_req.ingest_manual_dumps(),
    "ctq":      lambda: scrape_ctq.ingest_manual_dumps(),
    "maps":     lambda: enrich_maps.run(),
    "score":    lambda: score.compute_all(),
    "export":   lambda: export_excel.export(),
}

DEFAULT_ORDER = ["seed", "groups", "req", "ctq", "maps", "score", "export"]


def main(argv: list[str]) -> int:
    if "--reset" in argv:
        if DB_PATH.exists():
            DB_PATH.unlink()
            print(f"Reset: removed {DB_PATH}")
        argv = [a for a in argv if a != "--reset"]

    stages = [a for a in argv if a in STAGES] or DEFAULT_ORDER
    unknown = [a for a in argv if a and a not in STAGES]
    if unknown:
        print(f"Unknown stage(s): {unknown}. Known: {list(STAGES)}")
        return 2

    init_db()
    for name in stages:
        print(f"\n=== stage: {name} ===")
        result = STAGES[name]()
        print(f"    -> {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
