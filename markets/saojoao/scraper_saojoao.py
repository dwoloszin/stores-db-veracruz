"""
scraper_saojoao.py — Scraper for São João Farmácias (https://www.saojoaofarmacias.com.br)

Platform  : VTEX — but LOCKED DOWN against bulk listing: the catalog `fq`
            (C:/... and P:[...]) is ignored (every query returns the full
            catalogue count), and every pagination method (_from/_to, IS
            from/page) caps at ~2550 products. So a normal listing scrape can
            only see ~11% of the ~21.7k catalogue.
Strategy  : sitemap -> per-product API.
            1) /sitemap.xml -> product-0.xml … product-N.xml : ALL product URLs
               (each ends `/{slug}/p`).
            2) Per product, the by-slug catalog API returns ONE clean product:
                   /api/catalog_system/pub/products/search/{slug}/p
               with items[0].ean, sellers[0].commertialOffer.{ListPrice, Price,
               IsAvailable, AvailableQuantity}, images, brand.
Promo     : ListPrice = "de", Price = "por". promo_price = Price when Price <
            ListPrice (~40% of the catalogue is on promo). PAY ATTENTION: this is
            the only place the promo shows — the product-page HTML __STATE__ is
            messy and even carries wrong EANs (e.g. "00000002"); the by-slug API
            is authoritative.
EAN       : items[0].ean; kept only when 8–14 digits (kit SKUs carry junk).

One request per product (~21.7k) with a thread pool — same shape as Campea/Nissei.

Usage:
    python -m markets.saojoao.scraper_saojoao              # scrape -> DB
    python -m markets.saojoao.scraper_saojoao --limit 300  # test run -> DB
"""

import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.saojoaofarmacias.com.br"
STORE_ID  = "saojoao"
SITEMAP   = f"{BASE_URL}/sitemap.xml"
PRODUCT_API = f"{BASE_URL}/api/catalog_system/pub/products/search"
WORKERS   = 12
MAX_TRIES = 5

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC  = re.compile(r'<loc>\s*([^<\s]+)\s*</loc>')
_RE_SLUG = re.compile(r'/([^/]+)/p/?$')


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str, as_json: bool, diag: bool = False):
    last = None
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException as exc:
            last = f"exc {exc.__class__.__name__}"
            time.sleep(min(2 * (attempt + 1), 12))
            continue
        last = f"HTTP {r.status_code}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 20))
            continue
        if r.status_code not in (200, 206):
            if diag:
                print(f"  [get] {url} -> {last}")
            return None
        if as_json:
            try:
                return r.json()
            except ValueError:
                return None
        return r.text
    if diag:
        print(f"  [get] {url} -> gave up after {MAX_TRIES} tries (last: {last})")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Discovery — sitemap -> all product URLs
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    idx = _get(session, SITEMAP, as_json=False, diag=True)
    if idx is None:
        print("ERROR: could not fetch sitemap index.")
        return []
    subs = [u for u in _RE_LOC.findall(idx) if "product" in u.lower()]
    print(f"  product sub-sitemaps: {len(subs)}")
    urls: List[str] = []
    seen: set = set()
    for sm in subs:
        xml = _get(session, sm, as_json=False, diag=True)
        if not xml:
            continue
        for u in _RE_LOC.findall(xml):
            if u.endswith("/p") and u not in seen:
                seen.add(u)
                urls.append(u)
    print(f"  Product URLs in sitemap: {len(urls):,}")
    return urls


# ──────────────────────────────────────────────────────────────────────────────
# Per-product API -> offer
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
    return e if e.isdigit() and 8 <= len(e) <= 14 else ""


def _standardize(url: str, p: Dict) -> Optional[Dict]:
    name = str(p.get("productName") or "").strip()
    pid  = str(p.get("productId") or "").strip()
    if not name or not pid:
        return None

    items = p.get("items") or []
    item0 = items[0] if items else {}
    sellers = item0.get("sellers") or []
    offer = (sellers[0].get("commertialOffer") or {}) if sellers else {}

    regular = _to_float(offer.get("ListPrice"))
    price   = _to_float(offer.get("Price"))
    if regular is None or regular <= 0:
        regular = price
    if regular is None or regular <= 0:
        return None

    promo_price = price if (price is not None and 0 < price < regular) else None
    discount_pct = (
        round((1 - promo_price / regular) * 100, 1) if promo_price else None
    )

    images = item0.get("images") or []
    image_url = images[0].get("imageUrl", "") if images else ""
    qty = offer.get("AvailableQuantity")
    available = bool(offer.get("IsAvailable")) or ((qty or 0) > 0)

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(p.get("brand") or "").strip(),
        "category_path": (p.get("categories") or [""])[0].strip("/"),
        "ean":           _clean_ean(item0.get("ean")),
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          str(item0.get("measurementUnit") or "").strip(),
        "is_available":  available,
        "stock":         qty,
        "offer_tag":     "",
        "is_discounted": promo_price is not None,
        "product_url":   url,
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None, workers: int = WORKERS) -> Dict:
    session = _make_session()

    print("Fetching product URLs from sitemap ...")
    urls = fetch_product_urls(session)
    if not urls:
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}
    if limit:
        urls = urls[:limit]

    total_upserted = total_history = total_skipped = total_saved = 0
    BATCH_SIZE = 300
    batch: List[Dict] = []
    processed = failed = 0

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

    def _fetch_one(url: str) -> Optional[Dict]:
        m = _RE_SLUG.search(url)
        if not m:
            return None
        data = _get(session, f"{PRODUCT_API}/{m.group(1)}/p", as_json=True)
        if not isinstance(data, list) or not data:
            return None
        return _standardize(url, data[0])

    print(f"Fetching {len(urls):,} products via by-slug API with {workers} workers ...")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_one, u): u for u in urls}
        for fut in as_completed(futures):
            offer = fut.result()
            if offer is None:
                failed += 1
                continue
            batch.append(offer)
            processed += 1
            if processed % 1000 == 0:
                print(f"  [{processed:>6}/{len(urls)}] parsed  (failed={failed}, saved={total_saved})")
            if len(batch) >= BATCH_SIZE:
                _flush()
                batch.clear()

    _flush()
    batch.clear()

    print(f"\nFinished: {processed:,} products parsed  ({failed} failed/skipped).")
    return {"upserted": total_upserted, "history_inserted": total_history,
            "skipped_zero": total_skipped, "total_unique": total_saved}


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape São João Farmácias -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import SaoJoaoDB, load_env
    load_env(args.env)

    db    = SaoJoaoDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
