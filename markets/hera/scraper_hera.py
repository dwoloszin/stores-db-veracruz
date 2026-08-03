"""
scraper_hera.py — Scraper for Hera Medicamentos (https://www.heraonline.com.br)

Platform  : Fox / ConB2C
Discovery : the homepage links 15 specialty group pages (/grupo/{name}/{id}),
            each of which SERVER-RENDERS all its product cards (no pagination).
            Collect the product .html URLs across all groups (~348 unique).
Per page  : each product page is server-rendered (plain HTTP, no bot wall) with:
                <strong>EAN:</strong> <ean>                       -> ean / barcode
                <meta property="product:price:amount" content=X>  -> price
                itemprop="availability" href=".../InStock"        -> is_available
                <title> / itemprop name                           -> product_name
EAN       : first-class field on the product page; no enrichment step needed.

Prices are a single selling price (the visible "R$ x.xxx,xx" secondary value is
the 3x card installment, NOT a promo), so regular_price = the meta price and
promo_price is left null.

Usage:
    python -m markets.hera.scraper_hera              # scrape -> DB
    python -m markets.hera.scraper_hera --limit 100  # test run -> DB
    python -m markets.hera.scraper_hera --csv        # scrape -> DB + CSV
"""

import csv
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.heraonline.com.br"
STORE_ID   = "hera"
WORKERS    = 10
DELAY      = 0.05
MAX_TRIES  = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_GROUP = re.compile(r'/grupo/([a-z0-9\-]+)/(\d+)')
_RE_PRODLINK = re.compile(r'href="([^"]*\.html)"')
_RE_EAN   = re.compile(r'EAN:?\s*</strong>\s*(\d{8,14})', re.I)
_RE_PRICE = re.compile(r'product:price:amount"\s*content="([\d.]+)"')
_RE_NAME  = re.compile(r'itemprop="name"[^>]*>\s*([^<]+)')
_RE_TITLE = re.compile(r'<title>\s*([^<|]+)')
_RE_BRAND = re.compile(r'itemprop="brand"[^>]*>\s*([^<]+)')
_RE_AVAIL = re.compile(r'itemprop="availability"[^>]*href="[^"]*InStock"', re.I)
_RE_IMG   = re.compile(r'<meta property="og:image" content="([^"]+)"')


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
# Product URL discovery via group pages
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    home = _get(session, BASE_URL + "/")
    if not home:
        print("ERROR: could not fetch homepage.")
        return []
    groups = list(dict.fromkeys(_RE_GROUP.findall(home)))
    print(f"  Specialty groups: {len(groups)}")

    seen: set = set()
    urls: List[str] = []
    for name, gid in groups:
        html = _get(session, f"{BASE_URL}/grupo/{name}/{gid}")
        if not html:
            continue
        new = 0
        for href in _RE_PRODLINK.findall(html):
            if "/grupo/" in href or "/marca/" in href:
                continue
            u = href if href.startswith("http") else f"{BASE_URL}/{href.lstrip('/')}"
            if u not in seen:
                seen.add(u)
                urls.append(u)
                new += 1
        print(f"    {name[:24]:<24} +{new}  (total {len(urls)})")
        time.sleep(DELAY)
    print(f"  Product URLs: {len(urls)}")
    return urls


# ──────────────────────────────────────────────────────────────────────────────
# Product page -> offer
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v))
    except (ValueError, TypeError):
        return None


def _standardize(url: str, html: str) -> Optional[Dict]:
    mp = _RE_PRICE.search(html)
    price = _to_float(mp.group(1)) if mp else None
    if price is None or price <= 0:
        return None

    name = ""
    m = _RE_NAME.search(html)
    if m:
        name = m.group(1).strip()
    if not name:
        mt = _RE_TITLE.search(html)
        if mt:
            name = mt.group(1).strip()
    if not name:
        return None

    ean = ""
    me = _RE_EAN.search(html)
    if me:
        ean = me.group(1).strip()

    mb = _RE_BRAND.search(html)
    brand = mb.group(1).strip() if mb else ""

    mi = _RE_IMG.search(html)
    image_url = mi.group(1).strip() if mi else ""

    code_m = re.search(r'-(\d+)\.html$', url)
    product_id = code_m.group(1) if code_m else url.rstrip("/").rsplit("/", 1)[-1]

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
        "is_available":  bool(_RE_AVAIL.search(html)),
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

    print("Discovering product URLs from group pages ...")
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
        description="Scrape Hera Medicamentos -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--csv",     action="store_true",       help="Also export a CSV file after scrape")
    parser.add_argument("--output",  type=str, default=None,    help="CSV path (implies --csv)")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import HeraDB, load_env
    load_env(args.env)

    db    = HeraDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = HeraDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
