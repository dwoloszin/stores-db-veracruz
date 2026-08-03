"""
scraper_sampharma.py — Scraper for Sampharma (https://www.sampharma.com.br)

Platform  : Irroba / OpenCart (img.irroba.com.br). High-cost specialty pharmacy
            (transplant immunosuppressants, oncology) — small catalogue (~200),
            but exactly the >= R$1000 segment.
Discovery : /sitemap_products.xml (flat urlset, ~203 product URLs).
Per page  : a JSON-LD block (CollectionPage → mainEntity.itemListElement[]) nests
            an `@type: Product`. Parses cleanly with json.loads. Fields:
                name                    -> product_name
                sku                     -> ean / barcode (13-digit EAN, GS1)
                offers.price            -> regular price
                offers.availability     -> is_available (schema.org InStock)
                brand.name              -> brand ("_" placeholder -> blank)
                image[0]                -> image_url
EAN       : the `sku` field IS the EAN-13; no enrichment needed.

Small catalogue fetched with a thread pool (~200 pages, ~1 min). Plain HTTP 200.

Usage:
    python -m markets.sampharma.scraper_sampharma               # scrape -> DB
    python -m markets.sampharma.scraper_sampharma --limit 50    # test run -> DB
    python -m markets.sampharma.scraper_sampharma --min-price 1000  # high-value refresh
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.sampharma.com.br"
STORE_ID  = "sampharma"
SITEMAP   = f"{BASE_URL}/sitemap_products.xml"
WORKERS   = 10
MAX_TRIES = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_XML_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
_RE_LDBLOCK = re.compile(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', re.S | re.I)


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
            time.sleep(min(2 * (attempt + 1), 10))
            continue
        last = f"HTTP {r.status_code}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 15))
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
        print("ERROR: could not fetch sitemap_products.xml.")
        return []
    try:
        xml = ET.fromstring(r.content)
    except ET.ParseError:
        print(f"  sitemap not XML (bytes={len(r.content)})")
        return []
    urls = [loc.text.strip() for loc in xml.findall(".//sm:loc", _XML_NS) if loc.text]
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
    """The Product is nested inside a CollectionPage/ItemList JSON-LD block."""
    for block in _RE_LDBLOCK.findall(html):
        if '"Product"' not in block:
            continue
        try:
            data = json.loads(block)
        except ValueError:
            continue
        stack = [data]
        while stack:
            o = stack.pop()
            if isinstance(o, dict):
                if o.get("@type") == "Product":
                    return o
                stack.extend(o.values())
            elif isinstance(o, list):
                stack.extend(o)
    return None


def _standardize(url: str, html: str) -> Optional[Dict]:
    p = _product_node(html)
    if not p:
        return None
    name = str(p.get("name") or "").strip()
    ean  = str(p.get("sku") or "").strip()
    if not name:
        return None

    offers = p.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    regular = _to_float(offers.get("price"))
    if regular is None or regular <= 0:
        return None

    available = "instock" in str(offers.get("availability") or "").lower()

    brand = p.get("brand")
    brand = brand if isinstance(brand, str) else str((brand or {}).get("name") or "")
    if brand.strip() == "_":
        brand = ""

    image = p.get("image")
    image = image[0] if isinstance(image, list) and image else (image or "")

    product_id = url.rstrip("/").rsplit("/", 1)[-1]

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(brand).strip(),
        "category_path": "",
        "ean":           ean,
        "regular_price": regular,
        "promo_price":   None,
        "discount_pct":  None,
        "unit":          "",
        "is_available":  available,
        "stock":         None,
        "offer_tag":     "",
        "is_discounted": False,
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
            if len(batch) >= 200:
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

    parser = argparse.ArgumentParser(description="Scrape Sampharma -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--min-price", type=float, default=0.0, dest="min_price",
                        help="High-value refresh: only re-fetch DB products with regular_price >= this")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import SampharmaDB, load_env
    load_env(args.env)

    db    = SampharmaDB()
    stats = scrape(db, limit=args.limit, workers=args.workers, min_price=args.min_price)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
