"""
scraper_maxxieconomica.py — Scraper for Maxxi Econômica (https://maxxieconomica.com)

Platform  : custom site (server LiteSpeed; no VTEX/Woo/API/JSON-LD). Products live
            at /detalhe-produto/{slug}.
Discovery : flat /sitemap.xml (~19k locs) -> keep the /detalhe-produto/ URLs (~18.9k).
Per page  : the MAIN product (a page also shows a related-products carousel, so we
            take the FIRST of each main-product signal):
                name    -> <title> (clean, no chain suffix)
                ean     -> first /storage/photos/1/Products/ean/{EAN}.jpg  (main photo)
                de/por  -> inside the `prodPrices` block:
                             priceOfMaxxy -> regular ("de", only present when on promo)
                             priceByMaxxi -> selling ("por", always present)
Promo     : regular = priceOfMaxxy if present else priceByMaxxi; selling = priceByMaxxi.
            A real promo exists when priceOfMaxxy > priceByMaxxi -> promo_price = selling.
EAN       : first-class via the product photo filename. One request per product, threaded.

Usage:
    python -m markets.maxxieconomica.scraper_maxxieconomica              # scrape -> DB
    python -m markets.maxxieconomica.scraper_maxxieconomica --limit 300  # test run -> DB
"""

import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL = "https://maxxieconomica.com"
STORE_ID = "maxxieconomica"
WORKERS  = 32   # ~18.8k pages, ~62% dead/dup -> more workers to stay well under 60min
MAX_TRIES = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC   = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
_RE_EAN   = re.compile(r"/Products/ean/(\d{8,14})\.jpg")
_RE_TITLE = re.compile(r"<title>\s*(.*?)\s*</title>", re.S | re.I)
_RE_PRICES = re.compile(r'prodPrices"?\s*>(.*?)</div>', re.S)
_RE_OF    = re.compile(r'priceOfMaxxy[^>]*>\s*R\$\s*([\d.,]+)', re.I)
_RE_BY    = re.compile(r'priceByMaxxi[^>]*>\s*R\$\s*([\d.,]+)', re.I)
_RE_OUT   = re.compile(r'(esgotado|indispon[ií]vel|sem estoque|produto\s+indispon)', re.I)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str) -> Optional[requests.Response]:
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException:
            time.sleep(min(2 * (attempt + 1), 12))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 18))
            continue
        if r.status_code != 200:
            return None
        return r
    return None


def _br_float(v: Optional[str]) -> Optional[float]:
    if not v:
        return None
    s = v.strip().replace(".", "").replace(",", ".")  # BR: 1.234,56 -> 1234.56
    try:
        return float(s)
    except ValueError:
        return None


def _clean_ean(raw: Optional[str]) -> str:
    e = (raw or "").strip()
    return e if e.isdigit() and 8 <= len(e) <= 14 and not e.startswith(("000", "999")) else ""


# ──────────────────────────────────────────────────────────────────────────────
# Discovery
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    r = _get(session, f"{BASE_URL}/sitemap.xml")
    if r is None:
        return []
    urls = [u for u in _RE_LOC.findall(r.text) if "/detalhe-produto/" in u]
    # de-dup, keep order
    seen: set = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    print(f"  Product URLs in sitemap: {len(out):,}")
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Product page -> offer
# ──────────────────────────────────────────────────────────────────────────────

def _standardize(url: str, html: str) -> Optional[Dict]:
    m_title = _RE_TITLE.search(html)
    name = (m_title.group(1).strip() if m_title else "")
    # strip a trailing chain suffix if any ("Nome - Maxxi Econômica")
    name = re.split(r"\s+[-|]\s+Maxxi", name, maxsplit=1)[0].strip()
    if not name:
        return None

    # main product's price block
    m_block = _RE_PRICES.search(html)
    block = m_block.group(1) if m_block else html
    selling = _br_float(_RE_BY.search(block).group(1) if _RE_BY.search(block) else None)
    if selling is None or selling <= 0:
        return None
    regular = _br_float(_RE_OF.search(block).group(1) if _RE_OF.search(block) else None)
    if regular is None or regular < selling:
        regular = selling

    if regular > selling:
        promo_price = selling
        discount_pct = round((1 - promo_price / regular) * 100, 1)
    else:
        promo_price, discount_pct = None, None

    ean = _clean_ean(_RE_EAN.search(html).group(1) if _RE_EAN.search(html) else "")
    available = not bool(_RE_OUT.search(html))
    image = f"{BASE_URL}/storage/photos/1/Products/ean/{ean}.jpg" if ean else ""

    return {
        "product_id":    ean or url.rstrip("/").rsplit("/", 1)[-1],
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         "",
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
        "image_url":     image,
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
    seen_ids: set = set()   # different slugs can share one EAN -> dedup by product_id
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
            if offer["product_id"] in seen_ids:
                continue
            seen_ids.add(offer["product_id"])
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

    parser = argparse.ArgumentParser(description="Scrape Maxxi Econômica -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import MaxxiEconomicaDB, load_env
    load_env(args.env)

    db    = MaxxiEconomicaDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
