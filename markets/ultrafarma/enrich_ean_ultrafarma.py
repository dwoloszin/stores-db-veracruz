"""
enrich_ean_ultrafarma.py — Fetch EAN for Ultrafarma products from product pages.

EAN is embedded in product detail HTML as:
    <b>EAN: </b>7896714200804

Run after the main scraper to populate the ean column for all products.

Usage:
    python -m markets.ultrafarma.enrich_ean_ultrafarma             # enrich all missing
    python -m markets.ultrafarma.enrich_ean_ultrafarma --workers 20
    python -m markets.ultrafarma.enrich_ean_ultrafarma --limit 500  # test
    python -m markets.ultrafarma.enrich_ean_ultrafarma --env .env
"""

import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL     = "https://www.ultrafarma.com.br"
GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

_thread_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent":      GOOGLEBOT_UA,
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })
        _thread_local.session = s
    return _thread_local.session


def _fetch_ean(product_id: str, product_url: str) -> Tuple[str, Optional[str]]:
    """
    Fetch a product page and extract the EAN.
    Returns (product_id, ean_or_None).
    """
    session = _get_session()
    for attempt in range(3):
        try:
            r = session.get(product_url, timeout=20)
            if r.status_code == 429:
                time.sleep(15)
                continue
            if r.status_code != 200:
                return product_id, None

            # <b>EAN: </b>7896714200804
            m = re.search(r'<b>EAN:\s*</b>\s*(\d{8,14})', r.text)
            if m:
                return product_id, m.group(1)
            return product_id, None

        except requests.RequestException:
            if attempt < 2:
                time.sleep(5)
    return product_id, None


def enrich(workers: int = 12, limit: Optional[int] = None, db=None) -> Dict:
    """
    Fetch EAN for all products missing it in the DB.
    Returns {"fetched": N, "found": N, "updated": N}.
    """
    if db is None:
        from db.db_manager import UltrafarmDB
        db = UltrafarmDB()

    missing = db.load_missing_eans()   # {product_id: product_url}
    if not missing:
        print("No products with missing EAN.")
        return {"fetched": 0, "found": 0, "updated": 0}

    if limit:
        items = list(missing.items())[:limit]
        missing = dict(items)

    print(f"Fetching EAN for {len(missing):,} products using {workers} workers ...")

    pending: Dict[str, str] = {}
    total_updated = 0
    fetched = 0
    found = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_ean, pid, url): pid
            for pid, url in missing.items()
        }
        for future in as_completed(futures):
            pid, ean = future.result()
            fetched += 1
            if ean:
                pending[pid] = ean
                found += 1
                if len(pending) >= 500:
                    total_updated += db.update_eans(pending)
                    pending.clear()
            if fetched % 500 == 0 or fetched == len(missing):
                print(f"  Fetched {fetched:,}/{len(missing):,}  found={found:,}")

    if pending:
        total_updated += db.update_eans(pending)

    updated = total_updated
    print(f"EAN enrichment done — fetched={fetched:,}  found={found:,}  updated={updated:,}")
    return {"fetched": fetched, "found": found, "updated": updated}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch EAN for Ultrafarma products from product pages."
    )
    parser.add_argument("--workers", type=int, default=12,    help="Parallel threads (default: 12)")
    parser.add_argument("--limit",   type=int, default=None,  help="Only enrich N products (test)")
    parser.add_argument("--env",     type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import UltrafarmDB, load_env
    load_env(args.env)

    db = UltrafarmDB()
    enrich(workers=args.workers, limit=args.limit, db=db)
    db.close()
