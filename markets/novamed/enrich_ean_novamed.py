"""
enrich_ean_novamed.py — Backfill EAN/barcode for Nova Medicamentos products.

Magento GraphQL does not expose the barcode. Each product page carries it in:
    <div class="product attribute Codigo de Barra do Produto">
        <strong class="type">Ref:</strong>
        <div class="value">7891234567890</div>
    </div>

This backfill fetches each product page missing an EAN and writes the value back.

Usage:
    python -m markets.novamed.enrich_ean_novamed
    python -m markets.novamed.enrich_ean_novamed --limit 100
    python -m markets.novamed.enrich_ean_novamed --workers 12
"""

import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL = "https://www.novamedicamentos.com.br"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# The barcode lives inside the "Codigo de Barra do Produto" attribute block.
_EAN_RE = re.compile(
    r'Codigo de Barra do Produto.*?<div class="value">\s*(\d{8,14})',
    re.S,
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


def _fetch_ean(session: requests.Session, product_id: str, url: str) -> Tuple[str, Optional[str]]:
    for attempt in range(3):
        try:
            r = session.get(url, timeout=25)
            if r.status_code == 429:
                time.sleep(15)
                continue
            if r.status_code != 200:
                return product_id, None
            m = _EAN_RE.search(r.text)
            if m:
                ean = m.group(1)
                if len(ean) in (8, 12, 13, 14):
                    return product_id, ean
            return product_id, None
        except requests.exceptions.RequestException:
            if attempt < 2:
                time.sleep(3)
    return product_id, None


def enrich(workers: int = 12, limit: Optional[int] = None, db=None) -> Dict[str, int]:
    from db.db_manager import NovamedDB

    own_db = db is None
    if own_db:
        db = NovamedDB()

    missing = db.load_missing_eans()
    total = len(missing)
    print(f"Products missing EAN: {total:,}")

    if total == 0:
        if own_db:
            db.close()
        return {"fetched": 0, "found": 0, "updated": 0, "skipped_no_url": 0}

    items = list(missing.items())
    if limit:
        items = items[:limit]
        print(f"Limiting to {limit} products for this run.")

    _tl = threading.local()

    def _get_session() -> requests.Session:
        if not hasattr(_tl, "session"):
            _tl.session = _make_session()
        return _tl.session

    def _task(pid_url: Tuple[str, str]) -> Tuple[str, Optional[str]]:
        pid, url = pid_url
        return _fetch_ean(_get_session(), pid, url)

    pending: Dict[str, str] = {}
    total_updated = fetched = found = errors = 0
    log_every = max(1, len(items) // 20)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_task, item) for item in items]
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
                    f"found={found}  no_ean={errors}"
                )

    if pending:
        total_updated += db.update_eans(pending)

    print(f"\nFetched {fetched:,} pages — EAN found for {found:,} products.")
    print(f"DB updated: {total_updated:,} rows had EAN written.")

    if own_db:
        db.close()

    return {
        "fetched": fetched,
        "found": found,
        "updated": total_updated,
        "skipped_no_url": total - len(items),
    }


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Backfill EAN for Nova Medicamentos products")
    parser.add_argument("--workers", type=int, default=12, help="Parallel HTTP threads (default: 12)")
    parser.add_argument("--limit", type=int, default=None, help="Max products to process (test mode)")
    parser.add_argument("--env", type=str, default=".env", help=".env file path (default: .env)")
    args = parser.parse_args()

    try:
        with open(args.env, encoding="latin-1") as f:
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
