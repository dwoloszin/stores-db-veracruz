"""
scraper_minasbrasil.py — Scraper for Drogaria Minas Brasil (https://www.drogariaminasbrasil.com.br)

Platform  : Magento (sitemap under /media/sitemap/). Unlike many Magento stores,
            this one DOES expose the EAN in the product JSON-LD.
Discovery : /media/sitemap/drogariaminasbrasil.xml (index) -> 8 sub-sitemaps
            (drogariaminasbrasil-5-*.xml) with ~59k product URLs total.
Per page  : a JSON-LD Product block carries:
                name                                 -> product_name
                gtin13                               -> ean / barcode (EAN-13)
                sku                                  -> internal product id
                brand.name                           -> brand
                image[0]                             -> image_url
                offers.price                         -> selling price ("por")
                offers.priceSpecification[ListPrice] -> regular price ("de")
                offers.availability                  -> is_available (InStock)
Promo     : regular = ListPrice, promo = offers.price when price < ListPrice
            (this store lists almost everything at a discount vs ListPrice).
EAN       : first-class (`gtin13`); no enrichment needed.

Large catalogue (~59k) fetched with a thread pool, one request per product. The
Product JSON-LD may embed raw control chars in `description`, so json.loads is
retried after stripping them.

Usage:
    python -m markets.minasbrasil.scraper_minasbrasil              # scrape -> DB
    python -m markets.minasbrasil.scraper_minasbrasil --limit 300  # test run -> DB
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.drogariaminasbrasil.com.br"
STORE_ID   = "minasbrasil"
SITEMAP    = f"{BASE_URL}/media/sitemap/drogariaminasbrasil.xml"
WORKERS    = 14
MAX_TRIES  = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC     = re.compile(r'<loc>\s*([^<\s]+)\s*</loc>')
_RE_LDBLOCK = re.compile(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', re.S | re.I)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str, diag: bool = False) -> Optional[str]:
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
            time.sleep(min(3 * (attempt + 1), 18))
            continue
        if r.status_code != 200:
            if diag:
                print(f"  [get] {url} -> {last}")
            return None
        return r.text
    if diag:
        print(f"  [get] {url} -> gave up (last: {last})")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Discovery — sitemap index -> sub-sitemaps -> product URLs
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    idx = _get(session, SITEMAP, diag=True)
    if idx is None:
        print("ERROR: could not fetch sitemap index.")
        return []
    subs = [u for u in _RE_LOC.findall(idx) if u.endswith(".xml")]
    print(f"  sub-sitemaps: {len(subs)}")
    urls: List[str] = []
    seen: set = set()
    for sm in subs:
        xml = _get(session, sm, diag=True)
        if not xml:
            continue
        n0 = len(urls)
        for u in _RE_LOC.findall(xml):
            if not u.endswith(".xml") and u not in seen:
                seen.add(u)
                urls.append(u)
        print(f"    {sm.rsplit('/', 1)[-1]}: +{len(urls) - n0}")
    print(f"  Product URLs in sitemap: {len(urls):,}")
    return urls


# ──────────────────────────────────────────────────────────────────────────────
# Product page -> offer
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _product_node(html: str) -> Optional[Dict]:
    for blk in _RE_LDBLOCK.findall(html):
        if '"Product"' not in blk:
            continue
        try:
            d = json.loads(blk)
        except ValueError:
            try:
                d = json.loads(re.sub(r'[\x00-\x1f]+', ' ', blk))
            except ValueError:
                continue
        if isinstance(d, dict) and d.get("@type") == "Product":
            return d
    return None


def _list_price(offer: Dict) -> Optional[float]:
    for ps in (offer.get("priceSpecification") or []):
        if "ListPrice" in str(ps.get("priceType") or ""):
            return _to_float(ps.get("price"))
    return None


def _standardize(url: str, html: str) -> Optional[Dict]:
    d = _product_node(html)
    if not d:
        return None
    name = str(d.get("name") or "").strip()
    ean  = str(d.get("gtin13") or d.get("gtin") or "").strip()
    if not name:
        return None

    offer = d.get("offers") or {}
    if isinstance(offer, list):
        offer = offer[0] if offer else {}
    sell = _to_float(offer.get("price"))
    if sell is None or sell <= 0:
        return None

    list_price = _list_price(offer)
    if list_price and list_price > sell:
        regular, promo_price = list_price, sell
        discount_pct = round((1 - promo_price / regular) * 100, 1)
    else:
        regular, promo_price, discount_pct = sell, None, None

    available = "instock" in str(offer.get("availability") or "").lower()

    brand = d.get("brand")
    brand = str(brand.get("name") or "") if isinstance(brand, dict) else str(brand or "")
    image = d.get("image")
    image = image[0] if isinstance(image, list) and image else (image if isinstance(image, str) else "")
    sku = str(d.get("sku") or "").strip()
    product_id = sku or url.rstrip("/").rsplit("/", 1)[-1]

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         brand.strip(),
        "category_path": "",
        "ean":           ean,
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  available,
        "stock":         None,
        "offer_tag":     "",
        "is_discounted": promo_price is not None,
        "product_url":   url,
        "image_url":     str(image).strip(),
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
        html = _get(session, url)
        return _standardize(url, html) if html is not None else None

    print(f"Fetching {len(urls):,} product pages with {workers} workers ...")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_one, u): u for u in urls}
        for fut in as_completed(futures):
            offer = fut.result()
            if offer is None:
                failed += 1
                continue
            batch.append(offer)
            processed += 1
            if processed % 2000 == 0:
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

    parser = argparse.ArgumentParser(description="Scrape Drogaria Minas Brasil -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import MinasBrasilDB, load_env
    load_env(args.env)

    db    = MinasBrasilDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
