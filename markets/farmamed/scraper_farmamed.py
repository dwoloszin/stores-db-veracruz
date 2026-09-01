"""
scraper_farmamed.py — Scraper for Farmamed (https://www.farmamed.com.br)

Platform  : DCG "core" commerce (server "SecurityCore"; Linx-adjacent). NOT VTEX.
Discovery : sitemap index /sitemap/index -> /sitemap/products/1..N (100 URLs each,
            ~114 sitemaps, ~11.4k products). Product URL: /{mpn}-{slug}-p{sku}.
Per page  : each product page carries a JSON-LD Product block AND a big embedded
            product-state JSON. We read (hybrid, most reliable of each):
                JSON-LD Product : name, image, offers.price (SELLING price), sku,
                                  offers.availability (schema.org/InStock)
                embedded state  : "UPC" -> ean (EAN-13), "ListPrice" -> regular ("de")
Promo     : regular = ListPrice, selling = JSON-LD price. A real promo exists when
            ListPrice > selling (store also exposes "IsPromotion":true) -> promo_price
            = selling; otherwise regular = selling and no promo.
EAN       : first-class via "UPC" (EAN-13). One request per product (~21.4k), threaded.

Usage:
    python -m markets.farmamed.scraper_farmamed              # scrape -> DB
    python -m markets.farmamed.scraper_farmamed --limit 300  # test run -> DB
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

BASE_URL     = "https://www.farmamed.com.br"
STORE_ID     = "farmamed"
WORKERS      = 16
MAX_TRIES    = 4
MAX_SITEMAPS = 260   # /sitemap/products/1.. ; stop when one is empty/404

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC     = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
_RE_LD      = re.compile(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', re.S | re.I)
_RE_UPC     = re.compile(r'"UPC"\s*:\s*"(\d{8,14})"')
_RE_LIST    = re.compile(r'"ListPrice"\s*:\s*([\d.]+)')
_RE_URLSKU  = re.compile(r'-p(\d+)/?$')


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> Optional[requests.Response]:
    last = None
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException as exc:
            last = exc.__class__.__name__
            time.sleep(min(2 * (attempt + 1), 12))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 18))
            continue
        if r.status_code != 200:
            return None
        return r
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Discovery
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session, workers: int = WORKERS) -> List[str]:
    # The sitemap index lists every /sitemap/products/N file — grab it, then fetch
    # those product sitemaps in parallel (215 files -> ~15s instead of ~3min serial).
    idx = _get(session, f"{BASE_URL}/sitemap/index")
    product_sitemaps: List[str] = []
    if idx is not None:
        product_sitemaps = [u for u in _RE_LOC.findall(idx.text) if "/sitemap/products/" in u]
    if not product_sitemaps:  # fallback: walk by number
        product_sitemaps = [f"{BASE_URL}/sitemap/products/{n}" for n in range(1, MAX_SITEMAPS + 1)]
    print(f"  {len(product_sitemaps)} product sitemaps to read ...")

    def _read(sm: str) -> List[str]:
        r = _get(session, sm)
        if r is None or "<loc" not in r.text.lower():
            return []
        return [u for u in _RE_LOC.findall(r.text) if "/sitemap/" not in u]

    seen: set = set()
    urls: List[str] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for locs in ex.map(_read, product_sitemaps):
            for u in locs:
                if u not in seen:
                    seen.add(u)
                    urls.append(u)
    print(f"  Product URLs in sitemap: {len(urls):,}")
    return urls


# ──────────────────────────────────────────────────────────────────────────────
# Product page -> offer
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None


def _clean_ean(raw: Optional[str]) -> str:
    e = (raw or "").strip()
    return e if e.isdigit() and 8 <= len(e) <= 14 and not e.startswith(("000", "999")) else ""


def _ld_product(html: str) -> Dict:
    """Return the JSON-LD Product block as a dict (or {})."""
    for blob in _RE_LD.findall(html):
        try:
            j = json.loads(blob)
        except ValueError:
            continue
        if isinstance(j, dict) and j.get("@type") == "Product":
            return j
    return {}


def _standardize(url: str, html: str) -> Optional[Dict]:
    ld = _ld_product(html)
    if not ld:
        return None

    name = str(ld.get("name") or "").strip()
    if not name:
        return None

    offers = ld.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}

    selling = _to_float(offers.get("price"))
    if selling is None or selling <= 0:
        return None

    # regular price ("de") from the embedded state; fall back to selling
    m = _RE_LIST.search(html)
    regular = _to_float(m.group(1)) if m else None
    if regular is None or regular < selling:
        regular = selling

    # regular ("de") > selling ("por") == a real promo; ListPrice/IsPromotion agree.
    if regular > selling:
        promo_price = selling
        discount_pct = round((1 - promo_price / regular) * 100, 1)
    else:
        promo_price, discount_pct = None, None

    ean = _clean_ean(_RE_UPC.search(html).group(1) if _RE_UPC.search(html) else "")

    avail_raw = str(offers.get("availability") or "").lower()
    available = "instock" in avail_raw or "in_stock" in avail_raw

    image = ld.get("image") or ""
    if isinstance(image, list):
        image = image[0] if image else ""

    m_sku = _RE_URLSKU.search(url)
    product_id = str(ld.get("sku") or (m_sku.group(1) if m_sku else "") or offers.get("sku") or "").strip()
    if not product_id:
        product_id = url.rstrip("/").rsplit("/", 1)[-1]

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(ld.get("brand", {}).get("name", "") if isinstance(ld.get("brand"), dict) else (ld.get("brand") or "")).strip(),
        "category_path": str(ld.get("category") or "").strip(),
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
        "image_url":     str(image),
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None, workers: int = WORKERS) -> Dict:
    session = _make_session()

    print("Fetching product URLs from sitemap ...")
    urls = fetch_product_urls(session, workers=workers)
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

    parser = argparse.ArgumentParser(description="Scrape Farmamed -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import FarmamedDB, load_env
    load_env(args.env)

    db    = FarmamedDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
