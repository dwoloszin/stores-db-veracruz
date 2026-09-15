"""
scraper_drogasil.py — Scraper for Drogasil (https://www.drogasil.com.br)

Platform : Magento 2 + Algolia. The storefront sits behind Akamai Bot Manager, which
           now blocks EVERY non-browser client (requests AND curl_cffi get 403 "Access
           Denied" regardless of UA/IP — it needs the JS sensor cookie). So we DON'T
           touch the storefront: we query the public **Algolia** index directly
           (`{app}-dsn.algolia.net`), which is NOT behind Akamai.
Index    : drogasil-rd-product-index  (search-only key; app A7RGTHYMDQ)
Fields   : each hit already has everything — name, `eanCode` (EAN, ~96% filled → NO
           enrichment step needed), `price` (single selling price, no de/por), `url`
           (slug), `brand`, `status` (AVAILABLE), `type`, `sku`/`objectID`.
Coverage : the index holds the FULL catalogue (~290k objects incl. pack variants),
           far more than the old SSR scraper (~40k, capped at 2000/category by Algolia).
Pagination: Algolia caps retrievable results at 1000/query, so we slice the catalogue
           by PRICE range and recursively split any slice with >1000 hits (Pacheco trick).

Usage:
    python -m markets.drogasil.scraper_drogasil            # scrape -> DB
    python -m markets.drogasil.scraper_drogasil --limit 500
"""

import csv
import json
import sys
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL      = "https://www.drogasil.com.br"
STORE_ID      = "drogasil"
ALGOLIA_APP   = "A7RGTHYMDQ"
ALGOLIA_KEY   = "2136627307d7d5384f92cda9f7e5357c"   # public search-only key
ALGOLIA_INDEX = "drogasil-rd-product-index"
PER_QUERY     = 1000     # Algolia hard cap on retrievable hits per query
SLICE_MAX     = 1000     # subdivide a price slice above this many hits
PRICE_CEILING = 100000.0
MAX_TRIES     = 5


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "x-algolia-application-id": ALGOLIA_APP,
        "x-algolia-api-key":        ALGOLIA_KEY,
        "Content-Type":             "application/json",
        "Accept":                   "application/json",
    })
    return s


def _algolia(session: requests.Session, params: str) -> Optional[Dict]:
    url = f"https://{ALGOLIA_APP}-dsn.algolia.net/1/indexes/*/queries"
    body = json.dumps({"requests": [{"indexName": ALGOLIA_INDEX, "params": params}]})
    for attempt in range(MAX_TRIES):
        try:
            r = session.post(url, data=body, timeout=30)
        except requests.RequestException:
            time.sleep(min(3 * (attempt + 1), 15)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 15)); continue
        if r.status_code != 200:
            return None
        try:
            return r.json()["results"][0]
        except (ValueError, KeyError, IndexError):
            return None
    return None


def _nf(filters: List[str]) -> str:
    return "numericFilters=" + urllib.parse.quote(json.dumps(filters))


def _count(session: requests.Session, lo: float, hi: Optional[float]) -> int:
    f = [f"price>={lo}"] + ([f"price<{hi}"] if hi is not None else [])
    res = _algolia(session, "query=&hitsPerPage=0&" + _nf(f))
    return int(res.get("nbHits", 0)) if res else 0


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _clean_ean(raw: Any) -> str:
    e = str(raw or "").strip()
    return e if e.isdigit() and 8 <= len(e) <= 14 and not e.startswith(("000", "999")) else ""


def _standardize(h: Dict) -> Optional[Dict]:
    name = str(h.get("name") or "").strip()
    pid  = str(h.get("objectID") or h.get("sku") or "").strip()
    if not name or not pid:
        return None
    if str(h.get("type") or "").upper() == "SERVICE":
        return None
    price = _to_float(h.get("price"))
    if price is None or price <= 0:
        return None

    images = h.get("images") or []
    if isinstance(images, dict):
        images = list(images.values())
    image_url = ""
    if isinstance(images, list) and images:
        first = images[0]
        image_url = first if isinstance(first, str) else str(first.get("url", "") if isinstance(first, dict) else "")

    hcat = h.get("hierarchicalCategories") or {}
    cat_path = str(hcat.get("lvl2") or hcat.get("lvl1") or hcat.get("lvl0") or "").replace(" /// ", " > ") if isinstance(hcat, dict) else ""

    url = str(h.get("url") or "").strip()
    product_url = f"{BASE_URL}/{url}" if url and not url.startswith("http") else (url or f"{BASE_URL}/")

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(h.get("brand") or "").strip(),
        "category_path": cat_path,
        "ean":           _clean_ean(h.get("eanCode")),
        "regular_price": price,   # Algolia exposes a single price (no de/por)
        "promo_price":   None,
        "discount_pct":  None,
        "unit":          "",
        "is_available":  str(h.get("status") or "").upper() == "AVAILABLE",
        "stock":         None,
        "offer_tag":     "",
        "is_discounted": False,
        "product_url":   product_url,
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape — recursive price-range subdivision
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    session = _make_session()

    total = _count(session, 0, None)
    if total == 0:
        print("ERROR: Algolia returned 0 products (index/key issue) — aborting.")
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}
    print(f"Algolia index '{ALGOLIA_INDEX}': {total:,} priced objects. Slicing by price ...")

    # Build the list of price slices (each <= SLICE_MAX hits) via recursive halving.
    slices: List[tuple] = []

    def _subdivide(lo: float, hi: Optional[float]) -> None:
        n = _count(session, lo, hi)
        if n == 0:
            return
        if n <= SLICE_MAX or (hi is not None and (hi - lo) < 0.02):
            slices.append((lo, hi))
            return
        if hi is None:
            hi = PRICE_CEILING          # bound the open-ended top slice, then split
        mid = round((lo + hi) / 2, 2)
        if mid <= lo or mid >= hi:
            slices.append((lo, hi)); return
        _subdivide(lo, mid)
        _subdivide(mid, hi)

    _subdivide(0.0, None)
    print(f"  {len(slices)} price slices to fetch")

    total_upserted = total_history = total_skipped = total_saved = 0
    seen: set = set()
    batch: List[Dict] = []
    BATCH_SIZE = 500
    processed = 0

    def _flush() -> None:
        nonlocal total_saved, total_upserted, total_history, total_skipped
        if not batch:
            return
        stats = db.save(batch, verbose=False)
        total_saved    += stats["upserted"]
        total_upserted += stats["upserted"]
        total_history  += stats["history_inserted"]
        total_skipped  += stats["skipped_zero"]
        print(f"    -> saved {stats['upserted']} | price changes {stats['history_inserted']} | cumul {total_saved}")
        batch.clear()

    for i, (lo, hi) in enumerate(slices, 1):
        f = [f"price>={lo}"] + ([f"price<{hi}"] if hi is not None else [])
        res = _algolia(session, f"query=&hitsPerPage={PER_QUERY}&" + _nf(f))
        hits = (res or {}).get("hits", [])
        new = 0
        for h in hits:
            pid = str(h.get("objectID") or h.get("sku") or "").strip()
            if not pid or pid in seen:
                continue
            offer = _standardize(h)
            if offer:
                seen.add(pid)
                batch.append(offer)
                new += 1
                processed += 1
        if new or i % 25 == 0:
            print(f"  [{i:>4}/{len(slices)}] price[{lo},{hi}] +{new:<4} unique={len(seen)}")
        if len(batch) >= BATCH_SIZE:
            _flush()
        if limit and processed >= limit:
            print(f"Limit {limit} reached — stopping.")
            break

    _flush()
    print(f"\nFinished: {processed:,} products parsed.")
    return {"upserted": total_upserted, "history_inserted": total_history,
            "skipped_zero": total_skipped, "total_unique": total_saved}


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Drogasil (Algolia) -> PostgreSQL")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--env",   type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import DrogasilDB, load_env
    load_env(args.env)

    db    = DrogasilDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  history: {stats['history_inserted']:,}  "
          f"skipped: {stats['skipped_zero']:,}")

    if stats["upserted"] == 0:
        print("ERROR: 0 products scraped — treating as failure.")
        sys.exit(1)
