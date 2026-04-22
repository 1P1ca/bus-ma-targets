#!/usr/bin/env python3
"""
Direct HTTP POST scraper for CTQ permit data.

Bypasses Playwright/browser automation by making direct POST requests to the
CTQ search API. Uses cloudscraper to handle JS/CAPTCHA challenges if needed.

Usage:
    python3 scrape_ctq_direct.py [--limit 30]
"""

import sys
import json
import time
import re
from pathlib import Path
from typing import Optional, Dict

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)

try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    print("Warning: cloudscraper not installed. Install with: pip install cloudscraper")

sys.path.insert(0, str(Path(__file__).parent))

from scrape_ctq import parse_dossier_html, persist_dossier
from db import DB_PATH, connect, init_db

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CTQ_BASE = "https://www.pes.ctq.gouv.qc.ca"
CTQ_SEARCH_URL = f"{CTQ_BASE}/pes2/mvc/dossierclient"
CTQ_SEARCH_PAGE = f"{CTQ_BASE}/pes2/mvc/dossierclient?voletContexte=RECHERCHE_GLOBAL_MENU"


def create_session():
    """Create a requests session with retry strategy and proper headers."""
    session = requests.Session()
    
    # Retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Headers to mimic a real browser
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-CA,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    
    return session


def fetch_search_form(session) -> Optional[str]:
    """Fetch the initial search form to extract ViewState and other tokens."""
    print("  → Fetching search form...")
    try:
        resp = session.get(CTQ_SEARCH_PAGE, timeout=10)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"  ✗ Failed to fetch form: {e}")
        return None


def extract_form_state(html: str) -> Dict[str, str]:
    """Extract hidden form fields (ViewState, etc.) from HTML."""
    state = {}
    
    # Find javax.faces.ViewState
    viewstate_match = re.search(r'<input[^>]*name="javax\.faces\.ViewState"[^>]*value="([^"]*)"', html)
    if viewstate_match:
        state['javax.faces.ViewState'] = viewstate_match.group(1)
    
    # Find other hidden fields that might be needed
    for match in re.finditer(r'<input[^>]*type="hidden"[^>]*name="([^"]*)"[^>]*value="([^"]*)"', html):
        name, value = match.groups()
        if 'faces' in name.lower() or 'execution' in name.lower():
            state[name] = value
    
    return state


def search_operator(
    session,
    operator_id: int,
    name: str,
    neq: Optional[str] = None,
) -> Optional[str]:
    """Search for operator and return dossier HTML, or None if failed."""
    
    print(f"\n{'='*70}")
    print(f"Operator {operator_id}: {name[:50]}")
    if neq:
        print(f"  NEQ: {neq}")
    
    try:
        # 1. Fetch initial form to get ViewState
        form_html = fetch_search_form(session)
        if not form_html:
            return None
        
        form_state = extract_form_state(form_html)
        print(f"  ✓ Extracted form state ({len(form_state)} fields)")
        
        # 2. Build POST data
        post_data = {
            'mainForm:personneMorale': name.strip(),
            'mainForm:neq': neq.strip() if neq else '',
            'mainForm:j_idt26:filter': '',
            'mainForm:j_idt26_rppDD': '10',
            'mainForm:j_idt26:globalFilterValue': '',
            'mainForm:buttonRechercher': 'mainForm:buttonRechercher',
            'javax.faces.ViewState': form_state.get('javax.faces.ViewState', ''),
        }
        
        # Add other required hidden fields
        for key, value in form_state.items():
            if key not in post_data and 'faces' in key.lower():
                post_data[key] = value
        
        # 3. Try POST request
        print(f"  → Searching (POST)...")
        resp = session.post(
            CTQ_SEARCH_URL,
            data=post_data,
            timeout=15,
            allow_redirects=True
        )
        resp.raise_for_status()
        
        # 4. Check if we got results
        html = resp.text
        if 'Aucun' in html or 'No result' in html or len(html) < 5000:
            print(f"  ⚠️  No results found (likely blocked by CAPTCHA or no matching records)")
            # Save anyway to inspect
            out_file = RAW_DIR / f"ctq_{operator_id}_NO_RESULTS.html"
            out_file.write_text(html, encoding='utf-8')
            return None
        
        # 5. Try to parse
        parsed = parse_dossier_html(html)
        
        if parsed.get("permits") or parsed.get("vehicles_declared"):
            print(f"  ✓ Found data: {len(parsed.get('permits', []))} permits")
            return html
        else:
            print(f"  ~ Response received but no permit data extracted")
            # Save for debugging
            out_file = RAW_DIR / f"ctq_{operator_id}_UNPARSED.html"
            out_file.write_text(html, encoding='utf-8')
            return None
            
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None


def scrape_with_cloudscraper(operator_id: int, name: str, neq: Optional[str]) -> Optional[str]:
    """Try scraping with cloudscraper to handle reCAPTCHA."""
    if not HAS_CLOUDSCRAPER:
        return None
    
    try:
        print(f"  → Trying cloudscraper for reCAPTCHA bypass...")
        scraper = cloudscraper.create_scraper()
        
        search_params = {
            'mainForm:personneMorale': name.strip(),
            'mainForm:neq': neq.strip() if neq else '',
            'mainForm:buttonRechercher': 'mainForm:buttonRechercher',
        }
        
        resp = scraper.post(
            CTQ_SEARCH_URL,
            data=search_params,
            timeout=15
        )
        resp.raise_for_status()
        
        html = resp.text
        if len(html) > 5000 and 'Aucun' not in html:
            print(f"  ✓ cloudscraper succeeded")
            return html
        else:
            print(f"  ~ cloudscraper returned response but no data")
            return None
            
    except Exception as e:
        print(f"  ~ cloudscraper failed: {e}")
        return None


def main(limit: int = 30):
    """Run direct HTTP scraper."""
    
    init_db(DB_PATH)
    conn = connect(DB_PATH)
    
    try:
        # Get top-N independents
        rows = conn.execute("""
            SELECT o.id, o.name, o.neq
            FROM operators o
            LEFT JOIN scores s ON o.id = s.operator_id
            WHERE o.group_id IS NULL
            ORDER BY COALESCE(s.ma_fit_score, 0) DESC
            LIMIT ?
        """, (limit,)).fetchall()
        
        print(f"\n{'='*70}")
        print(f"CTQ Direct HTTP Scraper")
        print(f"{'='*70}")
        print(f"Operators to scrape: {len(rows)}")
        print(f"Method: Direct POST + cloudscraper fallback")
        print()
        
        # Create session
        session = create_session()
        
        success = 0
        failed = 0
        
        for op_id, name, neq in rows:
            # Try direct POST first
            html = search_operator(session, op_id, name, neq)
            
            # Fallback to cloudscraper if needed
            if not html and HAS_CLOUDSCRAPER:
                html = scrape_with_cloudscraper(op_id, name, neq)
            
            # Persist if we got results
            if html:
                parsed = parse_dossier_html(html)
                
                out_file = RAW_DIR / f"ctq_{op_id}.html"
                out_file.write_text(html, encoding='utf-8')
                
                init_db(DB_PATH)
                conn_inner = connect(DB_PATH)
                try:
                    n_permits = persist_dossier(conn_inner, op_id, parsed, source_url=CTQ_SEARCH_URL)
                    veh = parsed.get("vehicles_declared", 0) or 0
                    print(f"  ✓ Loaded: {n_permits} permits, {veh} vehicles")
                    success += 1
                finally:
                    conn_inner.close()
            else:
                failed += 1
            
            time.sleep(1)  # Rate limit
        
        print(f"\n{'='*70}")
        print(f"SUMMARY:")
        print(f"  ✓ Successful: {success}/{len(rows)}")
        print(f"  ✗ Failed: {failed}/{len(rows)}")
        
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Direct HTTP CTQ scraper")
    parser.add_argument("--limit", type=int, default=30)
    args = parser.parse_args()
    
    print("CTQ Direct HTTP Scraper")
    if not HAS_CLOUDSCRAPER:
        print("Note: cloudscraper not installed. Install for better CAPTCHA handling:")
        print("      pip install cloudscraper")
    print()
    
    main(limit=args.limit)
