"""
scraper_redefarmaprime.py — Scraper for Rede Farma Prime (https://www.redefarmaprime.com.br)

Platform  : custom ASP/.NET e-commerce (URLs like /{slug}~{id}~2~1~MEDICAMENTO~...).
Discovery : /sitemap.xml (CDATA-wrapped, iso-8859-1). Product URLs have >=3 `~`
            segments (e.g. .../~465~2~1~MEDICAMENTO~Medicamento); institutional ones
            have a single `~` (e.g. /3~Seguranca). ~298 products.
Per page  : product data is server-rendered as Schema.org microdata:
                name   -> <meta itemprop="name"|"description"> / og:title
                ean    -> shown next to a barcode icon: fa-barcode ... {EAN-13}
                price  -> <meta itemprop="price" content="X"> (single price; the
                          store exposes NO de/por, so regular=price, no promo)
                avail  -> <meta itemprop="availability" .../InStock|OutOfStock>
EAN       : ~92% (a few product pages omit the barcode line). One request/product.

Usage:
    python -m markets.redefarmaprime.scraper_redefarmaprime              # scrape -> DB
    python -m markets.redefarmaprime.scraper_redefarmaprime --limit 100  # test -> DB
"""

import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.redefarmaprime.com.br"
STORE_ID  = "redefarmaprime"
WORKERS   = 12
MAX_TRIES = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_SMURL = re.compile(r"<loc><!\[CDATA\[(.*?)\]\]></loc>")
# barcode line: fa-barcode"></i><font size="1" color="#c2c2c2"> {EAN}</font>.
# Skip ahead (size="1"/#c2c2c2 aren't 8+ digit runs) to the first 8-14 digit run.
_RE_EAN   = re.compile(r"fa-barcode.{0,80}?(\d{8,14})", re.S)
_RE_PRICE = re.compile(r'itemprop="price"\s+content="([\d.,]+)"')
_RE_AVAIL = re.compile(r'itemprop="availability"[^>]*content="[^"]*/(\w+)"', re.I)
_RE_NAME1 = re.compile(r'itemprop="name"\s+content="([^"]+)"')
_RE_NAME2 = re.compile(r'og:title"\s+content="([^"]+)"')
_RE_NAME3 = re.compile(r'itemprop="description"\s+content="([^"]+)"')
_RE_IMG   = re.compile(r'og:image"\s+content="([^"]+)"')


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


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).strip().replace(",", "."))
    except (ValueError, TypeError):
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
    urls: List[str] = []
    seen: set = set()
    for u in _RE_SMURL.findall(r.text):
        u = u.strip()
        # product URLs carry >=3 '~' segments; institutional pages have exactly one.
        if u.count("~") >= 3 and u not in seen:
            seen.add(u)
            urls.append(u)
    print(f"  Product URLs in sitemap: {len(urls):,}")
    return urls


# ──────────────────────────────────────────────────────────────────────────────
# Product page -> offer
# ──────────────────────────────────────────────────────────────────────────────

def _standardize(url: str, html: str) -> Optional[Dict]:
    price = _to_float(_RE_PRICE.search(html).group(1) if _RE_PRICE.search(html) else None)
    if price is None or price <= 0:
        return None

    m_name = _RE_NAME1.search(html) or _RE_NAME2.search(html) or _RE_NAME3.search(html)
    name = (m_name.group(1).strip() if m_name else "")
    if not name:
        return None

    ean = _clean_ean(_RE_EAN.search(html).group(1) if _RE_EAN.search(html) else "")

    m_av = _RE_AVAIL.search(html)
    available = bool(m_av) and "outofstock" not in m_av.group(1).lower()

    m_img = _RE_IMG.search(html)
    image = m_img.group(1).strip() if m_img else ""

    # product_id: the numeric id in the URL (…~{id}~2~1~…) or the produto=NNN link
    m_id = re.search(r"~(\d+)~\d+~\d+~", url) or re.search(r"produto=(\d+)", html)
    product_id = (m_id.group(1) if m_id else url.rstrip("/").rsplit("/", 1)[-1])

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         "",
        "category_path": "",
        "ean":           ean,
        "regular_price": price,   # store exposes a single price (no de/por)
        "promo_price":   None,
        "discount_pct":  None,
        "unit":          "",
        "is_available":  available,
        "stock":         None,
        "offer_tag":     "",
        "is_discounted": False,
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
    BATCH_SIZE = 200
    batch: List[Dict] = []
    seen_ids: set = set()
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

    parser = argparse.ArgumentParser(description="Scrape Rede Farma Prime -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import RedeFarmaPrimeDB, load_env
    load_env(args.env)

    db    = RedeFarmaPrimeDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
