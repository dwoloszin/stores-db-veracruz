"""
enrich_ean_drogaraia.py - Backfill EAN/barcode for Drogaria Raia products.

Drogaria Raia category listings (Algolia) do not expose EAN. It is available
on each product detail page at:
    __NEXT_DATA__ -> props.pageProps.productData.productEan

Usage:
    python -m markets.drogaraia.enrich_ean_drogaraia
    python -m markets.drogaraia.enrich_ean_drogaraia --limit 500
    python -m markets.drogaraia.enrich_ean_drogaraia --workers 20
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL = "https://www.drogaraia.com.br"
GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
    re.S,
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": GOOGLEBOT_UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


def _fetch_ean(session: requests.Session, product_id: str, url: str) -> Tuple[str, Optional[str]]:
    for attempt in range(2):
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 429:
                time.sleep(20)
                continue
            if r.status_code != 200:
                return product_id, None

            m = _NEXT_DATA_RE.search(r.text)
            if not m:
                return product_id, None

            data = json.loads(m.group(1))
            pp = data["props"]["pageProps"]
            prod = pp.get("productData") or {}
            ean = prod.get("productEan")
            if ean:
                return product_id, str(ean).strip()

            for attr in prod.get("custom_attributes") or []:
                if isinstance(attr, dict) and attr.get("attribute_code") == "ean":
                    vals = attr.get("value_string") or []
                    if vals:
                        return product_id, str(vals[0]).strip()

            return product_id, None
        except Exception:
            if attempt == 0:
                time.sleep(2)
    return product_id, None


def enrich(workers: int = 12, limit: Optional[int] = None, db=None) -> Dict[str, int]:
    from db.db_manager import DrogaraiaDB

    own_db = db is None
    if own_db:
        db = DrogaraiaDB()

    missing = db.load_missing_eans()
    total = len(missing)
    print(f"Products missing EAN: {total:,}")

    if total == 0:
        if own_db:
            db.close()
        return {"fetched": 0, "found": 0, "updated": 0, "skipped_no_url": 0}

    if limit:
        items = list(missing.items())[:limit]
        print(f"Limiting to {limit} products for this run.")
    else:
        items = list(missing.items())

    import threading

    _tl = threading.local()

    def _get_session():
        if not hasattr(_tl, "session"):
            _tl.session = _make_session()
        return _tl.session

    pending: Dict[str, str] = {}
    total_updated = 0
    fetched = 0
    found = 0
    errors = 0
    log_every = max(1, len(items) // 20)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_ean, _get_session(), pid, url): pid for pid, url in items}
        for future in as_completed(futures):
            pid, ean = future.result()
            fetched += 1
            if ean:
                pending[pid] = ean
                found += 1
                if len(pending) >= 500:
                    total_updated += db.update_eans(pending)
                    pending.clear()
            else:
                errors += 1

            if fetched % log_every == 0 or fetched == len(items):
                pct = fetched / len(items) * 100
                print(
                    f"  Progress: {fetched:>6}/{len(items)} ({pct:5.1f}%)  "
                    f"found={found}  errors={errors}"
                )

    if pending:
        total_updated += db.update_eans(pending)

    print(f"\nFetched {fetched:,} pages - EAN found for {found:,} products.")

    updated = total_updated
    print(f"DB updated: {updated:,} rows had EAN written.")

    if own_db:
        db.close()

    return {
        "fetched": fetched,
        "found": found,
        "updated": updated,
        "skipped_no_url": total - len(items) - errors,
    }


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Backfill EAN for Drogaria Raia products")
    parser.add_argument("--workers", type=int, default=12, help="Parallel HTTP threads (default: 12)")
    parser.add_argument("--limit", type=int, default=None, help="Max products to process (test mode)")
    parser.add_argument("--env", type=str, default=".env", help=".env file path (default: .env)")
    args = parser.parse_args()

    try:
        with open(args.env) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass

    stats = enrich(workers=args.workers, limit=args.limit)
    print("\nDone.")
    print(f"  Pages fetched:    {stats['fetched']:,}")
    print(f"  EANs found:       {stats['found']:,}")
    print(f"  DB rows updated:  {stats['updated']:,}")
