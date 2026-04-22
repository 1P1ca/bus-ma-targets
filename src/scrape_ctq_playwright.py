#!/usr/bin/env python3
"""
Playwright-based CTQ scraper with manual CAPTCHA solving.

Usage:
    python3 scrape_ctq_playwright.py [--limit 30] [--headless false]

Flow:
    1. For each operator in the worklist
    2. Navigate to CTQ search page
    3. Fill in operator name/NEQ
    4. PAUSE: Wait for user to manually solve reCAPTCHA + click search
    5. Capture the dossier HTML
    6. Save to data/raw/ctq_<operator_id>.html
    7. Parse and load into database
"""

import asyncio
import sys
import time
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright, Page, Browser, BrowserContext
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright")
    sys.exit(1)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from scrape_ctq import parse_dossier_html, persist_dossier
from db import DB_PATH, connect, init_db
import sqlite3

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CTQ_BASE = "https://www.pes.ctq.gouv.qc.ca"
CTQ_SEARCH_URL = f"{CTQ_BASE}/pes2/mvc/dossierclient?voletContexte=RECHERCHE_GLOBAL_MENU"


async def scrape_operator(page: Page, operator_id: int, name: str, neq: Optional[str] = None) -> bool:
    """Scrape one operator's dossier.

    Returns True if successful, False if skipped or errored.
    """
    try:
        # Navigate to search page
        await page.goto(CTQ_SEARCH_URL, wait_until="networkidle")
        print(f"\n{'='*70}")
        print(f"Operator {operator_id}: {name[:50]}")
        print(f"NEQ: {neq}")
        print(f"URL: {page.url}")

        # Wait for form to load
        await page.wait_for_selector('input[id*="neq"], input[id*="personneMorale"]', timeout=10000)

        # Fill in search: try NEQ first, then name
        if neq:
            try:
                neq_field = page.locator('input#mainForm\\:neq, input[id*="neq"]')
                await neq_field.first.fill(neq, timeout=5000)
                print(f"✓ Filled NEQ: {neq}")
            except Exception as e:
                print(f"  Note: Could not fill NEQ: {e}")

        try:
            name_field = page.locator('input#mainForm\\:personneMorale, input[id*="personneMorale"]')
            if (await name_field.count()) > 0:
                await name_field.first.fill(name, timeout=5000)
                print(f"✓ Filled name: {name[:40]}")
        except Exception as e:
            print(f"  Note: Could not fill name: {e}")

        # Wait for reCAPTCHA and user to solve
        print(f"\n⏸️  MANUAL STEP REQUIRED:")
        print(f"   1. Solve the reCAPTCHA on the page")
        print(f"   2. Click the 'Rechercher' / 'Search' button")
        print(f"   3. Wait for results to load")
        print(f"\n   (Waiting for navigation... press Ctrl+C to skip this operator)")

        # Wait for user to solve CAPTCHA and click search
        # The page will navigate to show results
        try:
            # In Playwright v1.30+, wait_for_navigation() is deprecated.
            # Wait for the page URL to change or load state to complete
            await page.wait_for_url("**/*", timeout=120000)  # 2 minute timeout
        except Exception as e:
            print(f"   ⚠️  Navigation timeout or cancelled: {e}")
            return False

        # Capture the dossier HTML
        html = await page.content()

        # Parse to verify we got real data
        parsed = parse_dossier_html(html)

        if not parsed.get("permits") and not parsed.get("vehicles_declared"):
            print(f"   ⚠️  No permit data found on results page")
            return False

        # Save HTML dump
        out_file = RAW_DIR / f"ctq_{operator_id}.html"
        out_file.write_text(html, encoding="utf-8")
        print(f"   ✓ Saved: {out_file.name}")

        # Parse and load into database
        init_db(DB_PATH)
        conn = connect(DB_PATH)
        try:
            n_permits = persist_dossier(conn, operator_id, parsed,
                                       source_url=CTQ_SEARCH_URL)
            veh = parsed.get("vehicles_declared", 0) or 0
            print(f"   ✓ Loaded: {n_permits} permits, {veh} vehicles")
            return True
        finally:
            conn.close()

    except Exception as e:
        print(f"   ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main(limit: int = 30, headless: bool = False):
    """Scrape CTQ dossiers for top operators."""

    # Get operator list from database
    init_db(DB_PATH)
    conn = connect(DB_PATH)

    try:
        # Get top-N independents (non-consolidated)
        rows = conn.execute("""
            SELECT o.id, o.name, o.neq, o.fta_no
            FROM operators o
            LEFT JOIN scores s ON o.id = s.operator_id
            WHERE o.group_id IS NULL
            ORDER BY COALESCE(s.ma_fit_score, 0) DESC
            LIMIT ?
        """, (limit,)).fetchall()

        print(f"Found {len(rows)} independent operators to scrape")

        # Launch browser
        async with async_playwright() as p:
            # Run in headed mode so user can see and interact
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = await context.new_page()

            success = 0
            failed = 0

            for row in rows:
                op_id, name, neq, fta_no = row
                result = await scrape_operator(page, op_id, name, neq)
                if result:
                    success += 1
                    time.sleep(2)  # Rate limit between operators
                else:
                    failed += 1

            await context.close()
            await browser.close()

        print(f"\n{'='*70}")
        print(f"SUMMARY:")
        print(f"  Success: {success}/{len(rows)}")
        print(f"  Failed/Skipped: {failed}/{len(rows)}")

        # Show fleet stats
        conn = connect(DB_PATH)
        rows = conn.execute("""
            SELECT
                SUM(buses_scolaire) as scolaire,
                SUM(buses_coach) as coach,
                SUM(buses_adapte) as adapte,
                SUM(buses_urbain) as urbain,
                SUM(total) as total
            FROM fleet WHERE source = 'ctq'
        """).fetchone()

        if rows[4]:
            print(f"\nFleet data loaded:")
            print(f"  School buses: {rows[0] or 0}")
            print(f"  Coach/Intercity: {rows[1] or 0}")
            print(f"  Adapted transit: {rows[2] or 0}")
            print(f"  Urban transit: {rows[3] or 0}")
            print(f"  TOTAL: {rows[4] or 0}")

        conn.close()

    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30, help="Number of operators to scrape")
    parser.add_argument("--headless", type=bool, default=False, help="Run in headless mode")
    args = parser.parse_args()

    print("Starting CTQ Playwright scraper with manual CAPTCHA solving...")
    print("Browser will open. Solve each CAPTCHA manually and click Search.\n")

    asyncio.run(main(limit=args.limit, headless=args.headless))
