#!/usr/bin/env python3
"""
CTQ scraper with 2captcha reCAPTCHA solver integration.

Usage:
    export TWOCAPTCHA_KEY=your_api_key
    python3 scrape_ctq_with_solver.py [--limit 30] [--test-first 5]

Cost: ~$0.003 per CAPTCHA solve = $0.90 for 300 operators
"""

import asyncio
import sys
import time
import os
import requests
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright, Page
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright")
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
TWOCAPTCHA_COST = 0.003  # ~$0.003 per solve


def solve_recaptcha_2captcha(sitekey: str, pageurl: str, timeout: int = 60) -> Optional[str]:
    """Solve reCAPTCHA v2 using 2captcha service.
    
    Returns the g-recaptcha-response token, or None if failed.
    Cost: ~$0.003 per solve.
    """
    if not TWOCAPTCHA_KEY:
        return None

    print(f"    → 2captcha: submitting reCAPTCHA (sitekey: {sitekey[:12]}...)")

    # 1. Submit CAPTCHA task
    try:
        resp = requests.post(
            "http://2captcha.com/api/upload",
            data={
                "key": TWOCAPTCHA_KEY,
                "method": "userrecaptcha",
                "googlekey": sitekey,
                "pageurl": pageurl,
            },
            timeout=10
        )
        resp.raise_for_status()

        if not resp.text.startswith("OK|"):
            print(f"    ✗ 2captcha submission failed: {resp.text}")
            return None

        captcha_id = resp.text.split("|")[1].strip()
        print(f"    → 2captcha: ID {captcha_id} (polling...)")
    except Exception as e:
        print(f"    ✗ 2captcha submission error: {e}")
        return None

    # 2. Poll for result
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            resp = requests.get(
                "http://2captcha.com/api/res",
                params={
                    "key": TWOCAPTCHA_KEY,
                    "captchaid": captcha_id,
                    "json": 1
                },
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") == 1:
                token = data.get("request")
                elapsed = int(time.time() - start_time)
                print(f"    ✓ 2captcha solved in {elapsed}s (cost: ${TWOCAPTCHA_COST:.3f})")
                return token
            elif data.get("status") == 0:
                # Not ready yet
                time.sleep(1)
                continue
            else:
                error = data.get("error_text", data.get("error"))
                print(f"    ✗ 2captcha error: {error}")
                return None

        except Exception as e:
            print(f"    ✗ 2captcha poll error: {e}")
            return None

    print(f"    ✗ 2captcha timeout ({timeout}s)")
    return None


async def scrape_operator(page: Page, operator_id: int, name: str, neq: Optional[str]) -> bool:
    """Scrape one operator's CTQ dossier with auto CAPTCHA solving."""
    try:
        print(f"\n{'='*70}")
        print(f"Operator {operator_id}: {name[:50]}")
        if neq:
            print(f"  NEQ: {neq}")

        # Navigate
        print(f"  → Loading CTQ search page...")
        await page.goto(CTQ_SEARCH_URL, wait_until="networkidle")

        # Wait for form
        await page.wait_for_selector('input[id*="neq"], input[id*="personneMorale"]', timeout=10000)

        # Fill NEQ
        if neq:
            try:
                neq_field = page.locator('input[id*="neq"]')
                if await neq_field.count() > 0:
                    await neq_field.first.fill(neq, timeout=5000)
                    print(f"  ✓ Filled NEQ")
            except Exception as e:
                print(f"  ~ NEQ field skipped: {e}")

        # Fill name
        try:
            name_field = page.locator('input[id*="personneMorale"]')
            if await name_field.count() > 0:
                await name_field.first.fill(name, timeout=5000)
                print(f"  ✓ Filled company name")
        except Exception as e:
            print(f"  ~ Name field skipped: {e}")

        # Detect and solve reCAPTCHA
        print(f"  → Checking for reCAPTCHA...")
        try:
            # Try to find reCAPTCHA sitekey
            sitekey = await page.evaluate("""
                () => {
                    // Try data-sitekey attribute
                    const elem = document.querySelector('[data-sitekey]');
                    if (elem) return elem.getAttribute('data-sitekey');
                    
                    // Try javascript variable __INITIAL_STATE__
                    if (window.__INITIAL_STATE__ && window.__INITIAL_STATE__.sitekey)
                        return window.__INITIAL_STATE__.sitekey;
                    
                    return null;
                }
            """)

            if sitekey:
                print(f"  ✓ Found reCAPTCHA (sitekey: {sitekey[:12]}...)")
                token = solve_recaptcha_2captcha(sitekey, page.url)
                if token:
                    # Inject token
                    await page.evaluate(f"""
                        () => {{
                            // Set response field
                            const responseField = document.getElementById('g-recaptcha-response');
                            if (responseField) {{
                                responseField.innerHTML = '{token}';
                                responseField.value = '{token}';
                            }}
                            
                            // Trigger callback if exists
                            if (window.___grecaptcha_cfg) {{
                                Object.entries(window.___grecaptcha_cfg.clients || {{}}).forEach(([_, client]) => {{
                                    if (client.callback) client.callback('{token}');
                                }});
                            }}
                            
                            return true;
                        }}
                    """)
                    print(f"  ✓ Injected CAPTCHA token")
                else:
                    print(f"  ✗ CAPTCHA solve failed")
                    return False
            else:
                print(f"  ~ No reCAPTCHA detected (may be pre-solved or cached)")
        except Exception as e:
            print(f"  ~ reCAPTCHA detection failed: {e}")

        # Click search
        print(f"  → Clicking search...")
        try:
            # Try various search button selectors
            btn = page.locator(
                'button:has-text("Rechercher"), '
                'button:has-text("Search"), '
                'button[type="submit"]'
            )
            if await btn.count() > 0:
                await btn.first.click()
                print(f"  ✓ Clicked search")
            else:
                print(f"  ~ Search button not found, trying JS click...")
                await page.evaluate("document.querySelector('button[type=submit]')?.click()")
        except Exception as e:
            print(f"  ~ Search button error: {e}")

        # Wait for results
        print(f"  → Waiting for results...")
        try:
            await page.wait_for_navigation(timeout=60000)
        except Exception as e:
            print(f"  ⚠️  Navigation timeout: {e}")
            return False

        # Parse results
        print(f"  → Parsing dossier...")
        html = await page.content()
        parsed = parse_dossier_html(html)

        if not parsed.get("permits") and not parsed.get("vehicles_declared"):
            print(f"  ⚠️  No permit data on results page")
            return False

        # Save & persist
        out_file = RAW_DIR / f"ctq_{operator_id}.html"
        out_file.write_text(html, encoding="utf-8")

        init_db(DB_PATH)
        conn = connect(DB_PATH)
        try:
            n_permits = persist_dossier(conn, operator_id, parsed, source_url=CTQ_SEARCH_URL)
            veh = parsed.get("vehicles_declared", 0) or 0
            print(f"  ✓ SUCCESS: {n_permits} permits, {veh} vehicles")
            return True
        finally:
            conn.close()

    except Exception as e:
        print(f"  ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main(limit: int = 30, test_first: int = 0):
    """Main scraper runner."""

    init_db(DB_PATH)
    conn = connect(DB_PATH)

    try:
        # Get top-N independents by fit score
        rows = conn.execute("""
            SELECT o.id, o.name, o.neq
            FROM operators o
            LEFT JOIN scores s ON o.id = s.operator_id
            WHERE o.group_id IS NULL
            ORDER BY COALESCE(s.ma_fit_score, 0) DESC
            LIMIT ?
        """, (limit,)).fetchall()

        if test_first > 0:
            rows = rows[:test_first]

        # Header
        print(f"\n{'='*70}")
        print(f"CTQ Scraper with 2captcha reCAPTCHA Solver")
        print(f"{'='*70}")
        print(f"Operators to scrape: {len(rows)}")
        print(f"Estimated cost: ${len(rows) * TWOCAPTCHA_COST:.2f}")
        if TWOCAPTCHA_KEY:
            print(f"2captcha API: ✓ configured")
        else:
            print(f"2captcha API: ⚠️  TWOCAPTCHA_KEY not set")
            print(f"             → Set it: export TWOCAPTCHA_KEY=your_key")
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
                    time.sleep(2)  # Rate limit
                else:
                    failed += 1

            await context.close()
            await browser.close()

        # Summary
        print(f"\n{'='*70}")
        print(f"SUMMARY:")
        print(f"  ✓ Successful: {success}/{len(rows)}")
        print(f"  ✗ Failed: {failed}/{len(rows)}")
        print(f"  Total cost: ~${success * TWOCAPTCHA_COST:.2f}")

    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="CTQ scraper with 2captcha reCAPTCHA solver"
    )
    parser.add_argument(
        "--limit", type=int, default=30,
        help="Number of operators to scrape (default: 30)"
    )
    parser.add_argument(
        "--test-first", type=int, default=0,
        help="Test mode: scrape only first N operators (default: 0 = all)"
    )
    args = parser.parse_args()

    print("CTQ Scraper starting...")
    print(f"To use 2captcha, set: export TWOCAPTCHA_KEY=your_api_key")
    print(f"Cost estimate: ${args.limit * TWOCAPTCHA_COST:.2f} for {args.limit} operators\n")

    asyncio.run(main(limit=args.limit, test_first=args.test_first))
