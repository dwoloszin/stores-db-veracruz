"""
enrich_price_drogasil.py — Backfill the real promotional price for Drogasil.

The fast category listing only exposes `priceService`, which is the reference /
max price (PMC) — the real selling price is only on the product page:
    __NEXT_DATA__ → props.pageProps.productData.price_aux
        value_from  -> regular / reference price
        value_to    -> actual selling price (lower for ~2/3 of the catalogue)

stripeCode/is_discounted is unreliable (misses ~94% of real discounts), so this
pass fetches EVERY product page and writes:
    regular_price = value_from   (authoritative)
    promo_price   = value_to     (when value_to < value_from)
    discount_pct  = derived
Runs as a separate daily enrichment job (heavy: ~160k pages), decoupled from the
fast 8h listing scrape.

Usage:
    python -m markets.drogasil.enrich_price_drogasil               # full pass
    python -m markets.drogasil.enrich_price_drogasil --limit 500   # test run
    python -m markets.drogasil.enrich_price_drogasil --workers 20  # faster
"""

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

STORE_ID   = "drogasil"
GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', re.S
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      GOOGLEBOT_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _fetch_price(session: requests.Session, pid: str, url: str
                 ) -> Tuple[str, Optional[Tuple[float, Optional[float], Optional[float]]]]:
    """Return (pid, (regular, promo|None, discount_pct|None)) or (pid, None)."""
    for attempt in range(2):
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 429:
                time.sleep(20)
                continue
            if r.status_code != 200:
                return pid, None
            m = _NEXT_DATA_RE.search(r.text)
            if not m:
                return pid, None
            data = json.loads(m.group(1))
            prod = (data.get("props", {}).get("pageProps", {}) or {}).get("productData") or {}
            pa = prod.get("price_aux") or {}
            vf = pa.get("value_from")
            vt = pa.get("value_to")
            try:
                vf = float(vf) if vf is not None else None
                vt = float(vt) if vt is not None else None
            except (TypeError, ValueError):
                return pid, None
            if not vf or vf <= 0:
                return pid, None
            if vt is not None and 0 < vt < vf:
                disc = round((1 - vt / vf) * 100, 1)
                return pid, (vf, vt, disc)
            # no real discount → clear any stale promo, keep regular
            return pid, (vf, None, None)
        except Exception:
            if attempt == 0:
                time.sleep(2)
    return pid, None


def enrich(db, limit: Optional[int] = None, workers: int = 16, min_price: float = 1000.0) -> Dict[str, int]:
    targets = db.load_all_urls(min_price)        # high-value products only
    items = list(targets.items())
    if limit:
        items = items[:limit]
    total = len(items)
    print(f"Products to price-enrich (regular_price >= {min_price:g}): {total:,}")
    if not total:
        return {"fetched": 0, "with_promo": 0, "updated": 0}

    fetched = with_promo = updated = 0
    pending: Dict[str, tuple] = {}
    session = _make_session()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_price, session, pid, url): pid
                   for pid, url in items}
        for fut in as_completed(futures):
            pid, res = fut.result()
            fetched += 1
            if res is not None:
                reg, promo, disc = res
                pending[pid] = (reg, promo, disc)
                if promo is not None:
                    with_promo += 1
            if len(pending) >= 500:
                updated += db.update_promo_prices(pending)
                pending.clear()
            if fetched % 1000 == 0:
                print(f"  [{fetched:>6}/{total}] fetched | promos found: {with_promo:,} | updated: {updated:,}")

    if pending:
        updated += db.update_promo_prices(pending)

    print(f"\nFetched {fetched:,} pages — {with_promo:,} real promos found, {updated:,} rows updated.")
    return {"fetched": fetched, "with_promo": with_promo, "updated": updated}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill promotional prices for Drogasil")
    parser.add_argument("--limit",   type=int, default=None)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--min-price", type=float, default=1000.0, dest="min_price")
    parser.add_argument("--env",     type=str, default=".env")
    args = parser.parse_args()

    from db.db_manager import DrogasilDB, load_env
    load_env(args.env)
    db = DrogasilDB()
    stats = enrich(db, limit=args.limit, workers=args.workers, min_price=args.min_price)
    db.close()
    print(f"\nDone. Pages: {stats['fetched']:,}  promos: {stats['with_promo']:,}  updated: {stats['updated']:,}")
