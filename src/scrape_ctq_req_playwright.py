#!/usr/bin/env python3
"""
Playwright-based scraper for CTQ & REQ operator lookups.
Fetches permit/ownership data for all operators in lookup_worklist.csv
"""

import asyncio
import csv
import os
import sys
import time
from pathlib import Path
from urllib.parse import unquote

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("❌ Playwright not installed. Run: pip install playwright")
    print("   Then: playwright install chromium")
    sys.exit(1)

# Paths
REPO_ROOT = Path(__file__).parent.parent
WORKLIST_CSV = REPO_ROOT / "output" / "lookup_worklist.csv"
RAW_DATA_DIR = REPO_ROOT / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Config
TIMEOUT_MS = 30000  # 30 seconds for page load
DELAY_BETWEEN_REQUESTS = 2.0  # CTQ rate limit: 2s between requests
HEADLESS = True


async def fetch_ctq_data(page, operator_fta_no, ctq_url, dump_filename):
    """Fetch CTQ permit/vehicle data"""
    output_file = RAW_DATA_DIR / f"ctq_{dump_filename}"
    
    try:
        print(f"  📄 CTQ (FTA#{operator_fta_no})...", end=" ", flush=True)
        
        # Navigate to CTQ search URL
        await page.goto(ctq_url, wait_until="networkidle", timeout=TIMEOUT_MS)
        
        # Wait for content to render
        await page.wait_for_timeout(2000)
        
        # Save HTML
        html = await page.content()
        output_file.write_text(html, encoding='utf-8')
        
        print(f"✓ saved {output_file.name}")
        return True
        
    except asyncio.TimeoutError:
        print(f"⏱️ timeout")
        return False
    except Exception as e:
        print(f"❌ error: {e}")
        return False


async def fetch_req_data(page, operator_fta_no, operator_name, req_url, dump_filename):
    """Fetch REQ ownership data"""
    output_file = RAW_DATA_DIR / f"req_{dump_filename}"
    
    try:
        print(f"  📊 REQ ({operator_name})...", end=" ", flush=True)
        
        # Navigate to REQ search URL
        await page.goto(req_url, wait_until="networkidle", timeout=TIMEOUT_MS)
        
        # Wait for content to render
        await page.wait_for_timeout(2000)
        
        # Save HTML
        html = await page.content()
        output_file.write_text(html, encoding='utf-8')
        
        print(f"✓ saved {output_file.name}")
        return True
        
    except asyncio.TimeoutError:
        print(f"⏱️ timeout")
        return False
    except Exception as e:
        print(f"❌ error: {e}")
        return False


async def scrape_operator(page, row):
    """Scrape one operator's CTQ & REQ data"""
    fta_no = row['fta_no']
    name = row['name']
    ctq_url = row['ctq_search_url']
    ctq_filename = row['ctq_dump_filename']
    req_url = row['req_search_url']
    req_filename = row['req_dump_filename']
    
    print(f"\n{fta_no:4s} | {name[:40]:40s}")
    
    # Fetch CTQ data
    ctq_ok = await fetch_ctq_data(page, fta_no, ctq_url, ctq_filename)
    
    # Rate limit
    await page.wait_for_timeout(int(DELAY_BETWEEN_REQUESTS * 1000))
    
    # Fetch REQ data
    req_ok = await fetch_req_data(page, fta_no, name, req_url, req_filename)
    
    # Rate limit
    await page.wait_for_timeout(int(DELAY_BETWEEN_REQUESTS * 1000))
    
    return ctq_ok and req_ok


async def main():
    """Main scraper loop"""
    
    # Read worklist
    if not WORKLIST_CSV.exists():
        print(f"❌ Worklist not found: {WORKLIST_CSV}")
        sys.exit(1)
    
    with open(WORKLIST_CSV, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    
    print(f"📋 Found {len(rows)} operators to scrape")
    print(f"💾 Saving to: {RAW_DATA_DIR}")
    print()
    
    # Launch browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        page = await browser.new_page()
        
        # Set user agent to avoid bot detection
        await page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        success_count = 0
        fail_count = 0
        
        try:
            for idx, row in enumerate(rows, 1):
                try:
                    ok = await scrape_operator(page, row)
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    print(f"  ❌ Scrape error: {e}")
                    fail_count += 1
                
                # Progress indicator
                if idx % 10 == 0:
                    print(f"\n  ⏳ Progress: {idx}/{len(rows)}")
        
        finally:
            await browser.close()
    
    # Summary
    print("\n" + "=" * 70)
    print(f"✅ Complete! Scraped {success_count} operators successfully")
    if fail_count > 0:
        print(f"⚠️  Failed: {fail_count} operators")
    print(f"💾 Data saved to: {RAW_DATA_DIR}")
    print("=" * 70)
    
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
