"""
scraper_singular.py — Scraper for Singular Medicamentos
(https://www.singularmedicamentos.com.br)

Platform  : Fbits / Wake (fbitsstatic.net) — but NOT bot-walled (plain HTTP works).
Discovery : /sitemap.xml lists /produto/{slug} URLs (~569).
Per page  : each product page has a clean JSON-LD Product block:
                name                 -> product_name
                gtin13               -> ean / barcode
                sku                  -> internal id (product_id)
                brand.name           -> brand
                offers.price         -> price (single selling price)
                offers.availability  -> is_available (schema.org/InStock)
                image                -> image_url
EAN       : first-class (gtin13) in JSON-LD; no enrichment step needed.

Single selling price (the de/por original isn't cleanly structured), so
regular_price = the JSON-LD offers.price and promo_price is left null.

Usage:
    python -m markets.singular.scraper_singular              # scrape -> DB
    python -m markets.singular.scraper_singular --limit 100  # test run -> DB
    python -m markets.singular.scraper_singular --csv        # scrape -> DB + CSV
"""

import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.singularmedicamentos.com.br"
STORE_ID   = "singular"
SITEMAP    = f"{BASE_URL}/sitemap.xml"
WORKERS    = 10
DELAY      = 0.05
MAX_TRIES  = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC = re.compile(r'<loc>\s*([^<]+?)\s*</loc>')
_RE_LD  = re.compile(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', re.S | re.I)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> Optional[str]:
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException:
            time.sleep(min(2 * (attempt + 1), 10))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 15))
            continue
        if r.status_code != 200:
            return None
        return r.text
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Product URL discovery
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    xml = _get(session, SITEMAP)
    if not xml:
        print("ERROR: could not fetch sitemap.")
        return []
    urls: List[str] = []
    seen: set = set()
    for loc in _RE_LOC.findall(xml):
        u = re.sub(r'(?<!:)//', '/', loc.strip())
        if "/produto/" in u.lower() and u not in seen:
            seen.add(u)
            urls.append(u)
    print(f"  Product URLs in sitemap: {len(urls)}")
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


_EAN_RE = re.compile(r"^\d{8,14}$")


def _product_ld(html: str) -> Optional[Dict]:
    for m in _RE_LD.findall(html):
        try:
            data = json.loads(m)
        except ValueError:
            continue
        blocks = data if isinstance(data, list) else [data]
        for b in blocks:
            if isinstance(b, dict) and "Product" in str(b.get("@type", "")):
                return b
    return None


def _standardize(url: str, html: str) -> Optional[Dict]:
    ld = _product_ld(html)
    if not ld:
        return None

    name = str(ld.get("name") or "").strip()
    sku  = str(ld.get("sku") or "").strip()
    if not name:
        return None

    offers = ld.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price = _to_float(offers.get("price"))
    if price is None or price <= 0:
        return None

    gtin = str(ld.get("gtin13") or ld.get("gtin") or "").strip()
    ean = gtin if _EAN_RE.match(gtin) else ""

    brand = ld.get("brand") or {}
    if isinstance(brand, dict):
        brand = str(brand.get("name") or "").strip()
    else:
        brand = str(brand).strip()

    image = ld.get("image")
    if isinstance(image, list):
        image = image[0] if image else ""
    image_url = str(image or "").strip()

    available = "InStock" in str(offers.get("availability") or "")
    product_id = sku or url.rstrip("/").rsplit("/", 1)[-1]

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         brand,
        "category_path": "",
        "ean":           ean,
        "regular_price": price,
        "promo_price":   None,
        "discount_pct":  None,
        "unit":          "",
        "is_available":  available,
        "stock":         None,
        "offer_tag":     "",
        "is_discounted": False,
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
    batch: List[Dict] = []
    processed = 0
    failed = 0

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
        if not html:
            return None
        return _standardize(url, html)

    print(f"Fetching {len(urls)} product pages with {workers} workers ...")
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
# CSV export (optional)
# ──────────────────────────────────────────────────────────────────────────────

CSV_FIELDS = [
    "product_id", "store_id", "product_name", "brand", "category_path",
    "ean", "regular_price", "promo_price", "discount_pct",
    "unit", "is_available", "stock", "offer_tag",
    "product_url", "image_url", "scraped_at",
]


def save_csv(offers: List[Dict], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(offers)
    print(f"Saved {len(offers):,} rows -> {path}")


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Singular Medicamentos -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--csv",     action="store_true",       help="Also export a CSV file after scrape")
    parser.add_argument("--output",  type=str, default=None,    help="CSV path (implies --csv)")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import SingularDB, load_env
    load_env(args.env)

    db    = SingularDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = SingularDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
