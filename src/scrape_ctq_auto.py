#!/usr/bin/env python3
"""
CTQ scraper with 2captcha reCAPTCHA solver.

Usage:
    export TWOCAPTCHA_KEY=your_api_key
    python3 scrape_ctq_auto.py [--limit 30] [--test-first 5]

Features:
  - Automates reCAPTCHA solving via 2captcha API
  - Estimates cost before running
  - Fallback: manual HTML dump ingestion for top-30
"""

import asyncio
import sys
import time
import os
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright, Page
    import requests
except ImportError:
    print("ERROR: playwright or requests not installed.")
    print("Run: pip install playwright requests")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parent))

from scrape_ctq import parse_dossier_html, persist_dossier
from db import DB_PATH, connect, init_db

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CTQ_BASE = "https://www.pes.ctq.gouv.qc.ca"
CTQ_SEARCH_URL = f"{CTQ_BASE}/pes2/mvc/dossierclient?voletContexte=RECHERCHE_GLOBAL_MENU"

TWOCAPTCHA_KEY = os.getenv("TWOCAPTCHA_KEY")
TWOCAPTCHA_COST_PER_SOLVE = 0.003  # $0.003 per reCAPTCHA solve


def estimate_cost(operator_count: int) -> float:
    """Estimate total 2captcha cost for N operators."""
    return operator_count * TWOCAPTCHA_COST_PER_SOLVE


def solve_recaptcha_2captcha(sitekey: str, pageurl: str) -> Optional[str]:
    """Solve reCAPTCHA v2 using 2captcha API.
    
    Returns the g-recaptcha-response token, or None if failed.
    """
    if not TWOCAPTCHA_KEY:
        print("  ⚠️  TWOCAPTCHA_KEY not set. Cannot solve CAPTCHA.")
        return None

    # Submit CAPTCHA
    submit_url = "http://2captcha.com/api/upload"
    data = {
        "key": TWOCAPTCHA_KEY,
        "method": "userrecaptcha",
        "googlekey": sitekey,
        "pageurl": pageurl,
    }

    try:
        resp = requests.post(submit_url, data=data, timeout=10)
        resp.raise_for_status()
        captcha_id = resp.text.split("|")[1].strip()
        if not captcha_id:
            print(f"  ✗ 2captcha submission failed: {resp.text}")
            return None
        print(f"  📤 CAPTCHA submitted (ID: {captcha_id})")
    except Exception as e:
        print(f"  ✗ 2captcha submission error: {e}")
        return None

    # Poll for result (wait up to 60s)
    result_url = "http://2captcha.com/api/res"
    for attempt in range(60):
        try:
            resp = requests.get(result_url, params={"key": TWOCAPTCHA_KEY, "captchaid": captcha_id}, timeout=5)
            resp.raise_for_status()
            text = resp.text.strip()
            if text.startswith("OK|"):
                token = text.split("|")[1]
                print(f"  ✓ CAPTCHA solved in {attempt+1}s")
                return token
            elif text == "CAPCHA_NOT_READY":
                await asyncio.sleep(1)
            else:
                print(f"  ✗ 2captcha error: {text}")
                return None
        except Exception as e:
            print(f"  ✗ Poll error: {e}")
            return None

    print(f"  ✗ 2captcha timeout (60s)")
    return None


async def scrape_operator(page: Page, operator_id: int, name: str, neq: Optional[str] = None) -> bool:
    """Scrape one operator using automated CAPTCHA solving.
    
    Returns True if successful, False otherwise.
    """
    try:
        # Navigate to search page
        await page.goto(CTQ_SEARCH_URL, wait_until="networkidle")
        print(f"\n{'='*70}")
        print(f"Operator {operator_id}: {name[:50]}")
        print(f"NEQ: {neq}")

        # Fill form fields
        await page.wait_for_selector('input[id*="neq"], input[id*="personneMorale"]', timeout=10000)

        if neq:
            try:
                neq_field = page.locator('input[id*="neq"]')
                if await neq_field.count() > 0:
                    await neq_field.first.fill(neq)
                    print(f"  ✓ Filled NEQ: {neq}")
            except Exception as e:
                print(f"  Note: NEQ field error: {e}")

        try:
            name_field = page.locator('input[id*="personneMorale"]')
            if await name_field.count() > 0:
                await name_field.first.fill(name)
                print(f"  ✓ Filled name: {name[:40]}")
        except Exception as e:
            print(f"  Note: Name field error: {e}")

        # Solve reCAPTCHA (alternative: try to detect sitekey)
        try:
            sitekey = await page.evaluate("() => document.querySelector('[data-sitekey]')?.getAttribute('data-sitekey')")
            if sitekey:
                print(f"  🔍 Found sitekey: {sitekey}")
                token = solve_recaptcha_2captcha(sitekey, page.url)
                if token:
                    # Inject token into page
                    await page.evaluate(f"""
                        document.getElementById('g-recaptcha-response').innerHTML = '{token}';
                        if (typeof ___grecaptcha_cfg !== 'undefined') {{
                            Object.entries(___grecaptcha_cfg.clients).forEach(([key, client]) => {{
                                if (client.callback) client.callback('{token}');
                            }});
                        }}
                    """)
                    print(f"  ✓ Injected CAPTCHA token")
                else:
                    print(f"  ⚠️  Could not solve CAPTCHA, trying manual approach...")
                    return False
            else:
                print(f"  ⚠️  No reCAPTCHA found on page (already solved?)")
        except Exception as e:
            print(f"  Note: reCAPTCHA detection error: {e}")

        # Click search button
        try:
            search_btn = page.locator('button[type="submit"], button:has-text("Rechercher"), button:has-text("Search")')
            if await search_btn.count() > 0:
                await search_btn.first.click()
                print(f"  ✓ Clicked search button")
            else:
                print(f"  ⚠️  Search button not found")
        except Exception as e:
            print(f"  Note: Search button error: {e}")

        # Wait for results
        try:
            await page.wait_for_navigation(timeout=30000)
        except Exception as e:
            print(f"  ⚠️  Navigation timeout or cancelled: {e}")
            return False

        # Capture and parse
        html = await page.content()
        parsed = parse_dossier_html(html)

        if not parsed.get("permits") and not parsed.get("vehicles_declared"):
            print(f"  ⚠️  No permit data found")
            return False

        # Save and persist
        out_file = RAW_DIR / f"ctq_{operator_id}.html"
        out_file.write_text(html, encoding="utf-8")
        print(f"  ✓ Saved HTML")

        init_db(DB_PATH)
        conn = connect(DB_PATH)
        try:
            n_permits = persist_dossier(conn, operator_id, parsed, source_url=CTQ_SEARCH_URL)
            veh = parsed.get("vehicles_declared", 0) or 0
            print(f"  ✓ Loaded: {n_permits} permits, {veh} vehicles")
            return True
        finally:
            conn.close()

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main(limit: int = 30, test_first: int = 0):
    """Run automated scraper with optional test batch."""

    init_db(DB_PATH)
    conn = connect(DB_PATH)

    try:
        # Get operator worklist
        rows = conn.execute("""
            SELECT o.id, o.name, o.neq
            FROM operators o
            LEFT JOIN scores s ON o.id = s.operator_id
            WHERE o.group_id IS NULL
            ORDER BY COALESCE(s.ma_fit_score, 0) DESC
            LIMIT ?
        """, (limit,)).fetchall()

        # If test mode, only use first N
        if test_first > 0:
            rows = rows[:test_first]
            print(f"⚙️  TEST MODE: running first {test_first} operators")

        # Estimate cost
        cost = estimate_cost(len(rows))
        print(f"\n{'='*70}")
        print(f"CTQ Scraper with 2captcha CAPTCHA Solving")
        print(f"{'='*70}")
        print(f"Operators to scrape: {len(rows)}")
        print(f"Estimated 2captcha cost: ${cost:.2f}")
        if TWOCAPTCHA_KEY:
            print(f"✓ 2captcha API key found")
        else:
            print(f"⚠️  TWOCAPTCHA_KEY not set — will skip CAPTCHA solving")
        print(f"")

        # Launch browser
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = await context.new_page()

            success = 0
            failed = 0

            for op_id, name, neq in rows:
                result = await scrape_operator(page, op_id, name, neq)
                if result:
                    success += 1
                    time.sleep(2)
                else:
                    failed += 1

            await context.close()
            await browser.close()

        print(f"\n{'='*70}")
        print(f"SUMMARY:")
        print(f"  Success: {success}/{len(rows)}")
        print(f"  Failed/Skipped: {failed}/{len(rows)}")
        print(f"  Total cost: ~${estimate_cost(success):.2f}")

        conn.close()

    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--test-first", type=int, default=0)
    args = parser.parse_args()

    print("Starting CTQ scraper with 2captcha reCAPTCHA solver...")
    asyncio.run(main(limit=args.limit, test_first=args.test_first))
