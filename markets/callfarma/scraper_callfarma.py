"""
scraper_callfarma.py — Scraper for Callfarma (https://www.callfarma.com.br)

Platform  : Next.js/React SPA. Each product page server-renders ONE product
            object inside the flight payload (escaped JSON, exactly one per page).
Discovery : sitemap index lists /public/sitemap-N.xml (those 404); the real files
            are /sitemap-1.xml … /sitemap-23.xml (~2000 product URLs each, ~46k).
Per page  : the embedded product object carries (keys, after un-escaping \" -> "):
                CODIGO   -> product_id (= the URL id)
                NOME     -> product_name
                BARRA    -> ean / barcode (EAN-13; fallback BAR1)
                PRECO    -> regular price ("de")
                PREPRO   -> promo price ("por"); with INIPRO/FIMPRO validity window
                ESTOQUE  -> stock (is_available = > 0)
                FOTO     -> image
Promo     : promo_price = PREPRO when 0 < PREPRO < PRECO AND today is within
            [INIPRO, FIMPRO] (the store ships the promo window).
EAN       : first-class (BARRA/BAR1); ~94% real GS1 coverage.

One request per product (~46k), threaded.

Usage:
    python -m markets.callfarma.scraper_callfarma              # scrape -> DB
    python -m markets.callfarma.scraper_callfarma --limit 300  # test run -> DB
"""

import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.callfarma.com.br"
STORE_ID  = "callfarma"
WORKERS   = 14
MAX_TRIES = 4
MAX_SITEMAPS = 30   # iterate /sitemap-1.xml.. until empty/404

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC = re.compile(r'<loc>\s*([^<\s]+)\s*</loc>')
_RE_URLID = re.compile(r'/produto/(\d+)-')


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
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 18))
            continue
        if r.status_code != 200:
            if diag:
                print(f"  [get] {url} -> {last}")
            return None
        return r
    if diag:
        print(f"  [get] {url} -> gave up (last: {last})")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Discovery
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    urls: List[str] = []
    seen: set = set()
    for n in range(1, MAX_SITEMAPS + 1):
        r = _get(session, f"{BASE_URL}/sitemap-{n}.xml")
        if r is None or "<loc>" not in r.text:
            if n > 1:
                break            # ran past the last sitemap
            continue
        added = 0
        for u in _RE_LOC.findall(r.text):
            if "/produto/" in u and u not in seen:
                seen.add(u)
                urls.append(u)
                added += 1
        print(f"  sitemap-{n}.xml: +{added}")
        if added == 0 and n > 1:
            break
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


def _num(html: str, key: str) -> Optional[str]:
    m = re.search(r'"%s"\s*:\s*(\d[\d.]*)' % key, html)
    return m.group(1) if m else None


def _str(html: str, key: str) -> str:
    m = re.search(r'"%s"\s*:\s*"([^"]*)"' % key, html)
    return m.group(1).strip() if m else ""


def _clean_ean(raw: Optional[str]) -> str:
    e = (raw or "").strip()
    return e if e.isdigit() and 8 <= len(e) <= 14 and not e.startswith(("000", "999")) else ""


def _promo_active(html: str) -> bool:
    ini = _str(html, "INIPRO")
    fim = _str(html, "FIMPRO")
    if not ini or not fim:
        return True   # no window given -> trust PREPRO
    try:
        now = datetime.now(timezone.utc)
        d0 = datetime.fromisoformat(ini.replace("Z", "+00:00"))
        d1 = datetime.fromisoformat(fim.replace("Z", "+00:00"))
        # Some INIPRO/FIMPRO come without an offset (naive) -> assume UTC so we
        # never compare naive vs aware datetimes.
        if d0.tzinfo is None:
            d0 = d0.replace(tzinfo=timezone.utc)
        if d1.tzinfo is None:
            d1 = d1.replace(tzinfo=timezone.utc)
        return d0 <= now <= d1
    except ValueError:
        return True


def _standardize(url: str, raw_html: str) -> Optional[Dict]:
    # Un-escape the flight payload so the single product object reads as plain JSON.
    html = raw_html.replace('\\"', '"')
    if '"BARRA"' not in html and '"PRECO"' not in html:
        return None

    name = _str(html, "NOME")
    if not name:
        return None
    regular = _to_float(_num(html, "PRECO"))
    if regular is None or regular <= 0:
        return None

    prepro = _to_float(_num(html, "PREPRO"))
    if prepro and 0 < prepro < regular and _promo_active(html):
        promo_price = prepro
        discount_pct = round((1 - promo_price / regular) * 100, 1)
    else:
        promo_price, discount_pct = None, None

    ean = _clean_ean(_num(html, "BARRA")) or _clean_ean(_num(html, "BAR1"))
    stock = _num(html, "ESTOQUE")
    available = (int(stock) > 0) if (stock and stock.isdigit()) else True

    foto = _str(html, "FOTO")
    image = foto if foto.startswith("http") else (f"{BASE_URL}/{foto}" if foto else "")
    m = _RE_URLID.search(url)
    product_id = (m.group(1) if m else _str(html, "CODIGO")) or url.rstrip("/").rsplit("/", 1)[-1]

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         _str(html, "LABOR"),
        "category_path": _str(html, "NOMSUBC"),
        "ean":           ean,
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  available,
        "stock":         int(stock) if (stock and stock.isdigit()) else None,
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

    parser = argparse.ArgumentParser(description="Scrape Callfarma -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import CallfarmaDB, load_env
    load_env(args.env)

    db    = CallfarmaDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
