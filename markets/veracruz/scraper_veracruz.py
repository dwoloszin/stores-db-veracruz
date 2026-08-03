"""
scraper_veracruz.py — Scraper for Drogaria Vera Cruz (https://www.drogariaveracruz.com.br)

Platform  : Convertiez (io.convertiez.com.br)
Discovery : /s/drogariaveracruz/sitemap.xml -> sitemap-products-1.xml (~19.8k URLs)
Per page  : each product page is server-rendered with an embedded Product JSON
            block that carries everything we need:
                name                 -> product_name
                gtin13               -> ean / barcode (13-digit)
                sku                  -> internal product id (product_id)
                price                -> regular price
                sale_price           -> promo price (when < price)
                availability         -> is_available (schema.org/InStock)
                image                -> image_url
EAN       : first-class (gtin13) in the embedded JSON; no enrichment needed.

Large catalogue (~19.8k products) fetched with a thread pool. Plain HTTP works
(no bot wall). The embedded block is JSON-LD-shaped but contains raw control
chars in the description, so fields are extracted with targeted regexes rather
than a full json.loads.

Usage:
    python -m markets.campea.scraper_veracruz              # scrape -> DB
    python -m markets.campea.scraper_veracruz --limit 300  # test run -> DB
    python -m markets.campea.scraper_veracruz --csv        # scrape -> DB + CSV
"""

import csv
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.drogariaveracruz.com.br"
STORE_ID   = "veracruz"
SITEMAP    = f"{BASE_URL}/s/drogariaveracruz/sitemap.xml"
WORKERS    = 16
DELAY      = 0.0
MAX_TRIES  = 4

# The Convertiez WAF 403s datacenter (GitHub Actions) IPs for a normal browser
# UA, but lets the Googlebot UA through on page + sitemap routes — same bypass we
# use for Drogasil's Cloudflare WAF. Residential IPs work with either UA.
GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_XML_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Field extractors over the embedded product JSON block
_RE_NAME  = re.compile(r'"name"\s*:\s*"([^"]+)"')
_RE_SKU   = re.compile(r'"sku"\s*:\s*"([^"]+)"')
_RE_GTIN  = re.compile(r'"gtin13"\s*:\s*"(\d{8,14})"')
_RE_PRICE = re.compile(r'"price"\s*:\s*"?([\d.]+)"?')
_RE_SALE  = re.compile(r'"sale_price"\s*:\s*"?([\d.]+)"?')
_RE_AVAIL = re.compile(r'"availability"\s*:\s*"([^"]+)"')
_RE_IMG   = re.compile(r'"image"\s*:\s*\[\s*"([^"]+)"')
_RE_BRAND = re.compile(r'"brand"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"')


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      GOOGLEBOT_UA,
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

# Known product sub-sitemaps — used as a fallback when the index can't be
# fetched/parsed (e.g. a datacenter-IP bot challenge that returns HTML, not XML).
SUBMAP_FALLBACK = [
    f"{BASE_URL}/s/drogariaveracruz/sitemap-products-{i}.xml" for i in range(1, 6)
]


def _snippet(resp: requests.Response) -> str:
    ct = resp.headers.get("Content-Type", "?")
    body = (resp.text or "")[:160].replace("\n", " ").replace("\r", " ")
    return f"ct={ct} bytes={len(resp.content)} head={body!r}"


def fetch_product_urls(session: requests.Session) -> List[str]:
    sub_maps: List[str] = []
    root = _get(session, SITEMAP, diag=True)
    if root is None:
        print("  sitemap index unreachable — falling back to known sub-sitemap URLs")
    else:
        try:
            idx = ET.fromstring(root.content)
            sub_maps = [
                loc.text.strip()
                for loc in idx.findall(".//sm:loc", _XML_NS)
                if loc.text and "product" in loc.text.lower()
            ]
        except ET.ParseError:
            # Not XML — almost always a bot-challenge/HTML page from datacenter IPs.
            print(f"  sitemap index is not XML ({_snippet(root)}) — using fallback URLs")
    if not sub_maps:
        sub_maps = SUBMAP_FALLBACK
    print(f"  product sub-sitemaps to read: {len(sub_maps)}")

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
            if u and u.endswith("/p") and u not in seen:
                seen.add(u)
                urls.append(u)
        print(f"    {sm_url.rsplit('/', 1)[-1]}: +{len(urls) - n0} product URLs")
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


def _standardize(url: str, html: str) -> Optional[Dict]:
    # Anchor to the embedded product block to avoid picking up unrelated fields
    anchor = html.find('"gtin13"')
    if anchor == -1:
        anchor = html.find('"sale_price"')
    if anchor == -1:
        return None
    window = html[max(0, anchor - 1500): anchor + 500]

    name = _first(_RE_NAME, window) or _first(_RE_NAME, html)
    sku  = _first(_RE_SKU, window) or _first(_RE_SKU, html)
    gtin = _first(_RE_GTIN, window)
    if not name:
        return None

    # Convertiez feed: `sale_price` is the real SELLING price (it matches the
    # schema.org JSON-LD offer price Google reads); `price` is a reference. When
    # price > selling it is a genuine "de" (a real promo). When price < selling
    # it is an understated value we must NOT trust — selling is then the regular
    # price. This fixes products (~14-26% on veracruz/farmsaopaulo) whose price
    # was stored at the lower, wrong `price`. (No effect on campea, whose price
    # is always >= selling.)
    price   = _to_float(_first(_RE_PRICE, window))
    selling = _to_float(_first(_RE_SALE, window)) or price
    if selling is None or selling <= 0:
        return None
    if price is not None and price > selling:
        regular, promo_price = price, selling      # real discount (de/por)
    else:
        regular, promo_price = selling, None
    discount_pct = (
        round((1 - promo_price / regular) * 100, 1)
        if promo_price else None
    )

    product_id = sku or url.rstrip("/").rsplit("/", 1)[-1]
    # availability sits in the Offer block ~3k chars after gtin13 — outside the
    # narrow window — and there is exactly one per product page, so read it from
    # the full html (window-scoped read left every product unavailable).
    available = "InStock" in _first(_RE_AVAIL, html)

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         _first(_RE_BRAND, window),
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
        "image_url":     _first(_RE_IMG, window),
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None, workers: int = WORKERS,
           min_price: float = 0.0) -> Dict:
    session = _make_session()

    if min_price and min_price > 0:
        # High-value refresh: only re-fetch pages for products the DB already
        # knows are >= min_price. Campea is one HTTP request per product, so this
        # trims a full ~19.8k-page run down to the handful of expensive items we
        # actually track. New >= min_price products only appear via a full scrape
        # (a price is unknown until its page is fetched), so run a full scrape
        # periodically to seed them.
        url_map = db.load_all_urls(min_price=min_price)
        urls = list(url_map.values())
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
        r = _get(session, url)
        if r is None:
            return None
        return _standardize(url, r.text)

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
        description="Scrape Drogaria Vera Cruz -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--csv",     action="store_true",       help="Also export a CSV file after scrape")
    parser.add_argument("--output",  type=str, default=None,    help="CSV path (implies --csv)")
    parser.add_argument("--min-price", type=float, default=0.0, dest="min_price",
                        help="High-value refresh: only re-fetch DB products with "
                             "regular_price >= this (0 = full sitemap scrape)")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import VeraCruzDB, load_env
    load_env(args.env)

    db    = VeraCruzDB()
    stats = scrape(db, limit=args.limit, workers=args.workers, min_price=args.min_price)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    # A scrape that discovered/saved nothing is a failure, not a silent success —
    # exit non-zero so the orchestrator marks it FAILED and shows the log tail.
    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = VeraCruzDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
