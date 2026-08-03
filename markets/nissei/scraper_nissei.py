"""
scraper_nissei.py — Scraper for Farmácias Nissei (https://www.farmaciasnissei.com.br)

Platform  : custom Django storefront (csrfmiddlewaretoken + csrftoken cookie).
Discovery : /sitemap.xml -> produtos-1.xml ... produtos-4.xml (~30–40k product URLs).
Per page  : server-rendered JSON-LD Product block carries everything except stock:
                name    -> product_name
                gtin    -> ean / barcode (13-digit; sometimes 14 w/ leading 0)
                sku     -> internal product id (product_id)  [offers.sku]
                price   -> regular price                     [offers.price]
                brand   -> brand
                image   -> image_url ("/media/..." -> absolute)
            EAN is also printed in plain text ("EAN: 0789...") as a backup.
Stock     : NOT in the page — the SPA calls POST /buscar/estoque with produto_id
            + the Django CSRF token. Response `lista_estoque` = branches that have
            the item. is_available = status is True AND lista_estoque is non-empty.

Large catalogue fetched with a thread pool; two requests per product (page GET +
estoque POST). Plain HTTP; JSON-LD has raw control chars in `description`, so
fields are pulled with targeted regexes rather than json.loads (same as Campea).

Usage:
    python -m markets.nissei.scraper_nissei                 # full scrape -> DB
    python -m markets.nissei.scraper_nissei --limit 300     # test run -> DB
    python -m markets.nissei.scraper_nissei --min-price 1000 # high-value refresh
    python -m markets.nissei.scraper_nissei --no-stock      # skip estoque calls
"""

import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.farmaciasnissei.com.br"
STORE_ID  = "nissei"
SITEMAP   = f"{BASE_URL}/sitemap.xml"
ESTOQUE   = f"{BASE_URL}/buscar/estoque"
WORKERS   = 12
MAX_TRIES = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_XML_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Field extractors. Anchor near "gtin" so we read the Product block, not the
# Organization/Breadcrumb JSON-LD that also carry a "name".
_RE_GTIN  = re.compile(r'"gtin"\s*:\s*"(\d{8,14})"')
_RE_EAN_T = re.compile(r'EAN:\s*(\d{8,14})')            # plain-text backup
_RE_NAME  = re.compile(r'"name"\s*:\s*"([^"]+)"')
_RE_SKU   = re.compile(r'"sku"\s*:\s*"([^"]+)"')
_RE_PRICE = re.compile(r'"price"\s*:\s*"?([\d.]+)"?')
_RE_LIST  = re.compile(r'"lowPrice"\s*:\s*"?([\d.]+)"?')
_RE_IMG   = re.compile(r'"image"\s*:\s*"([^"]+)"')
_RE_BRAND = re.compile(r'"brand"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"')
_RE_CSRF  = re.compile(r"csrfmiddlewaretoken'\s*:\s*'([^']+)'")


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
# Product URL discovery
# ──────────────────────────────────────────────────────────────────────────────

def _snippet(resp: requests.Response) -> str:
    ct = resp.headers.get("Content-Type", "?")
    body = (resp.text or "")[:160].replace("\n", " ").replace("\r", " ")
    return f"ct={ct} bytes={len(resp.content)} head={body!r}"


def fetch_product_urls(session: requests.Session) -> List[str]:
    root = _get(session, SITEMAP, diag=True)
    if root is None:
        print("ERROR: could not fetch sitemap index.")
        return []
    try:
        idx = ET.fromstring(root.content)
    except ET.ParseError:
        print(f"  sitemap index is not XML ({_snippet(root)})")
        return []

    sub_maps = [
        loc.text.strip()
        for loc in idx.findall(".//sm:loc", _XML_NS)
        if loc.text and "produto" in loc.text.lower()
    ]
    print(f"  product sub-sitemaps: {len(sub_maps)}")

    urls: List[str] = []
    seen: set = set()
    for sm_url in sub_maps:
        r = _get(session, sm_url, diag=True)
        if r is None:
            continue
        try:
            xml = ET.fromstring(r.content)
        except ET.ParseError:
            print(f"  sub-sitemap not XML: {sm_url} ({_snippet(r)})")
            continue
        n0 = len(urls)
        for loc in xml.findall(".//sm:loc", _XML_NS):
            u = (loc.text or "").strip()
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
        print(f"    {sm_url.rsplit('/', 1)[-1]}: +{len(urls) - n0} URLs")
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


def _first(rx: "re.Pattern", html: str) -> str:
    m = rx.search(html)
    return m.group(1).strip() if m else ""


def _norm_ean(raw: str) -> str:
    # Store carries EANs as 14 chars with a leading 0; normalise real EAN-13.
    raw = (raw or "").strip()
    if len(raw) == 14 and raw.startswith("0"):
        return raw[1:]
    return raw


def _fetch_csrf(session: requests.Session, urls: List[str]) -> str:
    """Grab a Django CSRF token (stable per session) from a product page."""
    for u in urls[:5]:
        r = _get(session, u)
        if r and _RE_CSRF.search(r.text):
            return _RE_CSRF.search(r.text).group(1)
    return ""


def _check_stock(session: requests.Session, produto_id: str, csrf: str) -> Optional[bool]:
    """POST /buscar/estoque; True/False if the answer is trustworthy, else None."""
    if not produto_id or not csrf:
        return None
    for attempt in range(3):
        try:
            r = session.post(
                ESTOQUE,
                data={"csrfmiddlewaretoken": csrf, "produto_id": produto_id,
                      "cliente_id": "", "parceiro_id": ""},
                headers={"Referer": BASE_URL + "/", "X-Requested-With": "XMLHttpRequest",
                         "X-CSRFToken": csrf},
                timeout=25,
            )
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code != 200:
            time.sleep(2 * (attempt + 1))
            continue
        try:
            data = r.json()
        except ValueError:
            return None
        return bool(data.get("status")) and bool(data.get("lista_estoque"))
    return None


_RE_LDBLOCK = re.compile(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', re.S | re.I)


def _product_block(html: str) -> str:
    """Return the JSON-LD <script> text that holds the Product (has "gtin").
    Fields (name, gtin, brand, offers.price) live in one block but are far apart
    because `description` is huge — so we scope to the whole block, not a window."""
    for b in _RE_LDBLOCK.findall(html):
        if '"gtin"' in b or '"Product"' in b:
            return b
    return ""


def _standardize(url: str, html: str) -> Optional[Dict]:
    block = _product_block(html)
    if not block:
        return None

    gtin = _norm_ean(_first(_RE_GTIN, block) or _first(_RE_EAN_T, html))
    name = _first(_RE_NAME, block)          # first "name" in the block = product name
    if not name:
        return None

    regular = _to_float(_first(_RE_PRICE, block))
    if regular is None or regular <= 0:
        return None
    list_price = _to_float(_first(_RE_LIST, block))
    promo_price = None
    if list_price and list_price > regular:
        regular, promo_price = list_price, regular  # price is the sale price

    discount_pct = (
        round((1 - promo_price / regular) * 100, 1) if promo_price else None
    )

    sku = _first(_RE_SKU, block)
    product_id = sku or url.rstrip("/").rsplit("/", 1)[-1]
    image = _first(_RE_IMG, block)
    if image.startswith("/"):
        image = BASE_URL + image

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         _first(_RE_BRAND, block),
        "category_path": "",
        "ean":           gtin,
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  True,      # set by the estoque call (see scrape)
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

def scrape(db, limit: Optional[int] = None, workers: int = WORKERS,
           min_price: float = 0.0, check_stock: bool = True) -> Dict:
    session = _make_session()

    if min_price and min_price > 0:
        # High-value refresh: re-fetch only products the DB already knows are
        # >= min_price (Nissei is per-page, so this trims a full ~35k-page run to
        # the expensive items). New >= min_price products only appear via a full
        # scrape (price is unknown until a page is fetched) — run one periodically.
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

    csrf = _fetch_csrf(session, urls) if check_stock else ""
    if check_stock and not csrf:
        print("  WARN: no CSRF token — availability will default to True (stock check skipped).")

    total_upserted = total_history = total_skipped = total_saved = 0
    BATCH_SIZE = 300
    batch: List[Dict] = []
    processed = failed = 0
    no_stock = [0]  # list so worker threads can bump it (loose stat, races OK)

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
        if r is None:
            return None
        offer = _standardize(url, r.text)
        if offer is None:
            return None
        if check_stock and csrf:
            avail = _check_stock(session, offer["product_id"], csrf)
            if avail is None:
                no_stock[0] += 1   # couldn't confirm — keep listed-with-price as available
            else:
                offer["is_available"] = avail
        return offer

    print(f"Fetching {len(urls):,} product pages with {workers} workers "
          f"(stock check: {'on' if check_stock and csrf else 'off'}) ...")
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
                print(f"  [{processed:>6}/{len(urls)}] parsed  (failed={failed}, "
                      f"unconfirmed_stock={no_stock[0]}, saved={total_saved})")
            if len(batch) >= BATCH_SIZE:
                _flush()
                batch.clear()

    _flush()
    batch.clear()

    print(f"\nFinished: {processed:,} products parsed  ({failed} failed/skipped, "
          f"{no_stock[0]} with unconfirmed stock).")
    return {"upserted": total_upserted, "history_inserted": total_history,
            "skipped_zero": total_skipped, "total_unique": total_saved}


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Farmácias Nissei -> PostgreSQL"
    )
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--min-price", type=float, default=0.0, dest="min_price",
                        help="High-value refresh: only re-fetch DB products with regular_price >= this")
    parser.add_argument("--no-stock", action="store_true",
                        help="Skip the per-product /buscar/estoque availability call")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import NisseiDB, load_env
    load_env(args.env)

    db    = NisseiDB()
    stats = scrape(db, limit=args.limit, workers=args.workers,
                   min_price=args.min_price, check_stock=not args.no_stock)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
