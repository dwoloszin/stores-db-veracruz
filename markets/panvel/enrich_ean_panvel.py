"""
enrich_ean_panvel.py — Back-fill EAN for Panvel offers via /api/v2/catalog/{id}

The main scraper (POST /api/v3/search) does not return EAN.
This script fetches the catalog detail endpoint for every offer that is
missing an EAN and writes the result back to the database.

Usage:
    python -m markets.panvel.enrich_ean_panvel              # enrich all missing
    python -m markets.panvel.enrich_ean_panvel --limit 500  # test: first 500
    python -m markets.panvel.enrich_ean_panvel --workers 20 # more threads
    python -m markets.panvel.enrich_ean_panvel --env .env.prod
"""

import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.panvel.com"
APP_TOKEN = "ZYkPuDaVJEiD"

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_local = threading.local()


def _get_session() -> tuple:
    """Thread-local requests session + sessionId."""
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent":      BROWSER_UA,
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })
        # Warm up cookies
        s.get(BASE_URL, timeout=15)
        sid = str(uuid.uuid4())
        s.cookies.set("sessionId", sid, domain="www.panvel.com", path="/")
        _local.session = s
        _local.session_id = sid
    return _local.session, _local.session_id


def _fetch_ean(product_id: str, attempt: int = 0) -> Optional[str]:
    """Return EAN string for the given panvelCode, or None on failure."""
    session, session_id = _get_session()
    try:
        r = session.get(
            f"{BASE_URL}/api/v2/catalog/{product_id}",
            headers={
                "Accept":     "application/json",
                "app-token":  APP_TOKEN,
                "sessionId":  session_id,
                "user-id":    "8601417",
                "client-ip":  "1",
            },
            timeout=15,
        )
        if r.status_code == 429:
            if attempt >= 5:
                return None
            time.sleep(10)
            return _fetch_ean(product_id, attempt + 1)
        if r.status_code != 200:
            return None
        ean = r.json().get("ean") or ""
        return str(ean).strip() or None
    except Exception:
        return None


def enrich(workers: int = 12, limit: Optional[int] = None, db=None) -> Dict:
    """
    Fetch EAN for all offers without one and write back to DB.
    Returns {"fetched": N, "updated": N, "failed": N}.
    """
    missing: Dict[str, str] = db.load_missing_eans()
    if limit:
        missing = dict(list(missing.items())[:limit])

    total   = len(missing)
    fetched = 0
    failed  = 0
    pending: Dict[str, str] = {}
    total_updated = 0

    print(f"EAN enrichment: {total:,} products missing EAN (workers={workers})")
    if total == 0:
        print("Nothing to enrich.")
        return {"fetched": 0, "updated": 0, "failed": 0}

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_ean, pid): pid for pid in missing}
        for i, fut in enumerate(as_completed(futures), 1):
            pid = futures[fut]
            ean = fut.result()
            if ean:
                pending[pid] = ean
                fetched += 1
                if len(pending) >= 500:
                    total_updated += db.update_eans(pending)
                    pending.clear()
            else:
                failed += 1

            if i % 500 == 0 or i == total:
                elapsed = time.time() - t0
                rate = i / elapsed
                eta  = (total - i) / rate if rate > 0 else 0
                print(
                    f"  {i:>6}/{total}  found={fetched}  failed={failed}  "
                    f"{rate:.1f}/s  ETA {eta/60:.1f}min"
                )

    if pending:
        total_updated += db.update_eans(pending)

    updated = total_updated
    print(f"Updated {updated:,} EAN rows in DB.")
    return {"fetched": fetched, "updated": updated, "failed": failed}


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Back-fill EAN for Panvel offers via /api/v2/catalog/{id}"
    )
    parser.add_argument("--limit",   type=int, default=None, help="Process only N offers (test)")
    parser.add_argument("--workers", type=int, default=12,   help="Thread count (default: 12)")
    parser.add_argument("--env",     type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import PanvelDB, load_env
    load_env(args.env)

    db    = PanvelDB()
    stats = enrich(workers=args.workers, limit=args.limit, db=db)
    db.close()

    print(f"\nDone.")
    print(f"  Fetched: {stats['fetched']:,}  Updated: {stats['updated']:,}  Failed: {stats['failed']:,}")
