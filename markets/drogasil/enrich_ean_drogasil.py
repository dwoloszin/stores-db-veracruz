"""
enrich_ean_drogasil.py — Backfill EAN/barcode for Drogasil products.

Drogasil category listings (Algolia) do not expose EAN. It is available only
on each product's detail page at:
    __NEXT_DATA__ → props.pageProps.productData.productEan

This script reads all offers from the DB that are missing EAN, fetches their
product pages in parallel (Googlebot UA, same bypass as the main scraper),
extracts productEan, and writes results back to the offers table.

After the first full run, only new products (added by subsequent scrapes) will
need enrichment — so repeat runs are fast.

Usage:
    python -m markets.drogasil.enrich_ean_drogasil               # full enrichment
    python -m markets.drogasil.enrich_ean_drogasil --limit 500   # test run
    python -m markets.drogasil.enrich_ean_drogasil --workers 20  # faster (more threads)
    python -m markets.drogasil.enrich_ean_drogasil --env .env

Performance (sequential comparison):
    ~26,000 products × 0.6 s/request / 12 workers ≈ 22 minutes
    Increase --workers to go faster (tested safe up to ~20).
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL     = "https://www.drogasil.com.br"
GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
    re.S,
)


# ──────────────────────────────────────────────────────────────────────────────
# Per-product EAN fetch
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      GOOGLEBOT_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


def _fetch_ean(session: requests.Session, product_id: str, url: str) -> Tuple[str, Optional[str]]:
    """
    Fetch one product detail page and return (product_id, ean_or_None).
    Retries once on 429 (rate limit). Returns None on any other error.
    """
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

            data     = json.loads(m.group(1))
            pp       = data["props"]["pageProps"]
            prod     = pp.get("productData") or {}
            ean      = prod.get("productEan")
            if ean:
                return product_id, str(ean).strip()

            # Fallback: scan custom_attributes for attribute_code == "ean"
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


# ──────────────────────────────────────────────────────────────────────────────
# Enrichment orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def enrich(
    workers: int = 12,
    limit:   Optional[int] = None,
    db=None,
) -> Dict[str, int]:
    """
    Fetch EAN for all Drogasil offers currently missing it and write to DB.

    Parameters
    ----------
    workers : int
        Number of parallel HTTP threads.
    limit : int, optional
        Max number of products to process (useful for testing).
    db : DrogasilDB, optional
        Pass an open DB instance to reuse a connection; otherwise a new one
        is created (and closed) internally.

    Returns
    -------
    dict with keys: fetched, found, updated, skipped_no_url
    """
    from db.db_manager import DrogasilDB

    own_db = db is None
    if own_db:
        db = DrogasilDB()

    missing = db.load_missing_eans()          # {product_id: product_url}
    total   = len(missing)
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

    # Use one session per thread via thread-local storage
    import threading
    _tl = threading.local()

    def _get_session():
        if not hasattr(_tl, "session"):
            _tl.session = _make_session()
        return _tl.session

    pending:       Dict[str, str] = {}
    total_updated: int            = 0
    fetched   = 0
    found     = 0
    errors    = 0
    log_every = max(1, len(items) // 20)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_ean, _get_session(), pid, url): pid
            for pid, url in items
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

    print(f"\nFetched {fetched:,} pages — EAN found for {found:,} products.")

    updated = total_updated
    print(f"DB updated: {updated:,} rows had EAN written.")

    if own_db:
        db.close()

    return {
        "fetched":         fetched,
        "found":           found,
        "updated":         updated,
        "skipped_no_url":  total - len(items) - errors,
    }


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Backfill EAN for Drogasil products")
    parser.add_argument("--workers", type=int, default=12,
                        help="Parallel HTTP threads (default: 12)")
    parser.add_argument("--limit",   type=int, default=None,
                        help="Max products to process (test mode)")
    parser.add_argument("--env",     type=str, default=".env",
                        help=".env file path (default: .env)")
    args = parser.parse_args()

    # Load env
    try:
        with open(args.env) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip(); v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass

    stats = enrich(workers=args.workers, limit=args.limit)

    print(f"\nDone.")
    print(f"  Pages fetched:    {stats['fetched']:,}")
    print(f"  EANs found:       {stats['found']:,}")
    print(f"  DB rows updated:  {stats['updated']:,}")
