"""
scraper_redesuperpopular.py — Scraper for Rede Super Popular
(https://www.redesuperpopular.com.br)

Platform  : Fbits / Wake.
Discovery : /sitemap.xml — a flat urlset (~26k URLs); product URLs contain
            "/produto/" (~26,022 of them).
Per page  : a JSON-LD Product block carries name, gtin13 (EAN), sku,
            offers.price (the SELLING price), offers.availability, brand, image.
            The block often has unescaped quotes/HTML + raw control chars in its
            `description`, so fields are pulled with regex, NOT json.loads.
Promo     : the de/por is NOT in the JSON-LD — it lives in the GA4 dataLayer item
            (`price` = list price, `discount` = amount off; list - discount ==
            the JSON-LD sell price). ~42% of products are on promo. So:
                regular_price = dataLayer price (when discount > 0), else sell
                promo_price   = JSON-LD offers.price (the sell price) when on promo
EAN       : first-class (`gtin13`); no enrichment needed.

Large catalogue (~26k) fetched with a thread pool (one request per product).
Fbits/Wake sometimes bot-walls with HTTP 424 — treated as a retryable block.

Usage:
    python -m markets.redesuperpopular.scraper_redesuperpopular              # -> DB
    python -m markets.redesuperpopular.scraper_redesuperpopular --limit 300  # test
    python -m markets.redesuperpopular.scraper_redesuperpopular --min-price 1000
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

BASE_URL  = "https://www.redesuperpopular.com.br"
STORE_ID  = "redesuperpopular"
SITEMAP   = "https://redesuperpopular.com.br/sitemap.xml"
WORKERS   = 12
MAX_TRIES = 5

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC     = re.compile(r'<loc>\s*([^<\s]+)\s*</loc>')
_RE_LDBLOCK = re.compile(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', re.S | re.I)

# The Product JSON-LD often has unescaped quotes/HTML + raw control chars in its
# `description`, so json.loads can't parse it — pull fields with targeted regexes
# (all the fields we need sit outside `description`). Same approach as Campea.
_RE_NAME  = re.compile(r'"name"\s*:\s*"([^"]*)"')
_RE_GTIN  = re.compile(r'"gtin13"\s*:\s*"(\d{8,14})"')
_RE_SKU   = re.compile(r'"sku"\s*:\s*"([^"]*)"')
_RE_PRICE = re.compile(r'"price"\s*:\s*"([\d.]+)"')
_RE_AVAIL = re.compile(r'"availability"\s*:\s*"([^"]+)"')
_RE_BRAND = re.compile(r'"brand"\s*:\s*\{[^}]*?"name"\s*:\s*"([^"]+)"')
_RE_IMG   = re.compile(r'"image"\s*:\s*\[\s*"([^"]+)"')


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str, diag: bool = False) -> Optional[requests.Response]:
    last = None
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException as exc:
            last = f"exc {exc.__class__.__name__}"
            time.sleep(min(2 * (attempt + 1), 12))
            continue
        last = f"HTTP {r.status_code}"
        # 424 = Fbits/Wake bot-wall; back off and retry
        if r.status_code in (424, 429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 20))
            continue
        if r.status_code != 200:
            if diag:
                print(f"  [get] {url} -> {last}")
            return None
        return r
    if diag:
        print(f"  [get] {url} -> gave up after {MAX_TRIES} tries (last: {last})")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Discovery
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    r = _get(session, SITEMAP, diag=True)
    if r is None:
        print("ERROR: could not fetch sitemap.xml.")
        return []
    locs = _RE_LOC.findall(r.text)
    urls = [u for u in locs if "/produto/" in u]
    print(f"  sitemap URLs: {len(locs):,}  |  product URLs: {len(urls):,}")
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


def _dl_list_discount(html: str, url: str) -> "tuple[Optional[float], Optional[float]]":
    """Promos are NOT in the JSON-LD — they live in the GA4 dataLayer item, whose
    `price` is the LIST price and `discount` the amount off (list - discount ==
    the JSON-LD sell price). Match the MAIN product's item by the URL's trailing
    id to avoid picking up related-product items."""
    m = re.search(r'-(\d+)/?$', url.rstrip("/"))
    if not m:
        return None, None
    item = re.search(r'\{item_id:' + re.escape(m.group(1)) + r'\b[^}]*\}', html)
    if not item:
        return None, None
    blk = item.group(0)
    p = re.search(r'(?:^|,)\s*price:([\d.]+)', blk)
    d = re.search(r'(?:^|,)\s*discount:([\d.]+)', blk)
    return (_to_float(p.group(1)) if p else None,
            _to_float(d.group(1)) if d else None)


def _product_block(html: str) -> str:
    """Return the JSON-LD <script> text holding the Product (has "Product")."""
    for blk in _RE_LDBLOCK.findall(html):
        if '"Product"' in blk:
            return blk
    return ""


def _first(rx: "re.Pattern", s: str) -> str:
    m = rx.search(s)
    return m.group(1).strip() if m else ""


def _standardize(url: str, html: str) -> Optional[Dict]:
    blk = _product_block(html)
    if not blk:
        return None
    name = _first(_RE_NAME, blk)      # first "name" in the block = product name
    gtin = _first(_RE_GTIN, blk)
    if not name:
        return None

    sell = _to_float(_first(_RE_PRICE, blk))   # JSON-LD price = actual selling price
    if sell is None or sell <= 0:
        return None

    # Fold in the promo (de/por) from the dataLayer (not in the JSON-LD).
    list_price, discount = _dl_list_discount(html, url)
    if list_price and discount and discount > 0 and list_price > sell:
        regular, promo_price = list_price, sell
        discount_pct = round((1 - promo_price / regular) * 100, 1)
    else:
        regular, promo_price, discount_pct = sell, None, None

    available = "instock" in _first(_RE_AVAIL, blk).lower()
    brand = _first(_RE_BRAND, blk)
    image = _first(_RE_IMG, blk)
    sku = _first(_RE_SKU, blk)
    product_id = sku or url.rstrip("/").rsplit("-", 1)[-1]

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         brand.strip(),
        "category_path": "",
        "ean":           gtin,
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

def scrape(db, limit: Optional[int] = None, workers: int = WORKERS,
           min_price: float = 0.0) -> Dict:
    session = _make_session()

    if min_price and min_price > 0:
        urls = list(db.load_all_urls(min_price=min_price).values())
        print(f"High-value mode: {len(urls):,} products with regular_price >= {min_price:.0f} (from DB)")
        if not urls:
            print("No products at/above the threshold yet — run a full scrape first to seed prices.")
            return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}
    else:
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
        r = _get(session, url)
        return _standardize(url, r.text) if r is not None else None

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
            if processed % 500 == 0:
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

    parser = argparse.ArgumentParser(description="Scrape Rede Super Popular -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--min-price", type=float, default=0.0, dest="min_price",
                        help="High-value refresh: only re-fetch DB products with regular_price >= this")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import RedeSuperPopularDB, load_env
    load_env(args.env)

    db    = RedeSuperPopularDB()
    stats = scrape(db, limit=args.limit, workers=args.workers, min_price=args.min_price)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
