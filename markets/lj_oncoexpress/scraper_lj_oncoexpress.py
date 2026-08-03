"""
scraper_lj_oncoexpress.py — Scraper for Onco Express (loja virtual)
(https://lojavirtual.oncoexpress.com.br)

Platform  : WooCommerce / WordPress (same as Onco Expresso)
API       : /wp-json/wc/store/v1/products  (public WooCommerce Store API, no auth)
Pagination: per_page (max 100) + page; X-WP-TotalPages header gives page count
Prices    : Store API returns integer minor units (cents) — divide by
            currency_minor_unit (2) to get BRL. regular_price / sale_price / price.
EAN       : the product `sku` holds the barcode (13-digit EAN) for most products.

Note: same operator as Onco Expresso (oncoexpresso.com.br); catalogue overlaps
heavily but ~37% of shared items are priced differently (separate price channel),
so it is tracked as its own store.

Usage:
    python -m markets.lj_oncoexpress.scraper_lj_oncoexpress              # scrape -> DB
    python -m markets.lj_oncoexpress.scraper_lj_oncoexpress --limit 200  # test run -> DB
    python -m markets.lj_oncoexpress.scraper_lj_oncoexpress --csv        # scrape -> DB + CSV
"""

import csv
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://lojavirtual.oncoexpress.com.br"
STORE_ID   = "lj_oncoexpress"
API        = f"{BASE_URL}/wp-json/wc/store/v1/products"
PER_PAGE   = 100    # WooCommerce Store API hard cap
DELAY      = 0.25   # seconds between page requests
MAX_TRIES  = 6

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ──────────────────────────────────────────────────────────────────────────────
# Session / fetch
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _fetch_page(session: requests.Session, page: int) -> Optional[requests.Response]:
    """GET one products page with back-off on 429/5xx. Returns the Response or None."""
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(API, params={"per_page": PER_PAGE, "page": page}, timeout=30)
        except requests.RequestException:
            time.sleep(min(3 * (attempt + 1), 20))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(5 * (attempt + 1), 30))
            continue
        if r.status_code != 200:
            return None
        return r
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _price(raw: Any, minor_unit: int) -> Optional[float]:
    if raw in (None, ""):
        return None
    try:
        return int(raw) / (10 ** minor_unit)
    except (ValueError, TypeError):
        try:
            return float(raw)
        except (ValueError, TypeError):
            return None


_EAN_RE = re.compile(r"^\d{8,14}$")


def _standardize(p: Dict) -> Optional[Dict]:
    name = str(p.get("name") or "").strip()
    pid  = str(p.get("id") or "").strip()
    if not name or not pid:
        return None

    prices = p.get("prices") or {}
    minor  = int(prices.get("currency_minor_unit") or 2)

    regular = _price(prices.get("regular_price"), minor)
    sale    = _price(prices.get("sale_price"), minor)
    price   = _price(prices.get("price"), minor)

    # variable products expose a price_range instead of a single price
    if regular is None:
        pr = prices.get("price_range") or {}
        regular = _price(pr.get("min_amount"), minor) or price

    if regular is None or regular <= 0:
        if price and price > 0:
            regular = price
        else:
            return None

    on_sale = bool(p.get("on_sale"))
    if on_sale and sale is not None and 0 < sale < regular:
        promo_price = sale
    else:
        promo_price = None

    discount_pct = (
        round((1 - promo_price / regular) * 100, 1)
        if promo_price and regular > 0 else None
    )

    sku = str(p.get("sku") or "").strip()
    ean = sku if _EAN_RE.match(sku) else ""

    cats = p.get("categories") or []
    category_path = " > ".join(str(c.get("name") or "").strip() for c in cats if c.get("name"))

    images = p.get("images") or []
    image_url = str(images[0].get("src") or "").strip() if images else ""

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         "",
        "category_path": category_path,
        "ean":           ean,
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  bool(p.get("is_in_stock", True)),
        "stock":         p.get("low_stock_remaining"),
        "offer_tag":     sku if not ean else "",
        "is_discounted": promo_price is not None,
        "product_url":   str(p.get("permalink") or "").strip(),
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    session = _make_session()

    total_upserted = total_history = total_skipped = total_saved = 0
    seen: set = set()

    first = _fetch_page(session, 1)
    if first is None:
        print("ERROR: could not fetch products page 1 — aborting.")
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}

    total_products = int(first.headers.get("X-WP-Total") or 0)
    total_pages    = int(first.headers.get("X-WP-TotalPages") or 1)
    print(f"WooCommerce Store API: {total_products:,} products across {total_pages} pages")

    def _handle(resp: requests.Response) -> List[Dict]:
        offers: List[Dict] = []
        try:
            products = resp.json()
        except ValueError:
            return offers
        for p in products:
            pid = str(p.get("id") or "").strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            offer = _standardize(p)
            if offer:
                offers.append(offer)
        return offers

    def _flush(offers: List[Dict]) -> None:
        nonlocal total_saved, total_upserted, total_history, total_skipped
        if not offers:
            return
        stats = db.save(offers, verbose=False)
        total_saved    += stats["upserted"]
        total_upserted += stats["upserted"]
        total_history  += stats["history_inserted"]
        total_skipped  += stats["skipped_zero"]
        print(f"    -> saved {stats['upserted']} | price changes {stats['history_inserted']} | cumul {total_saved}")

    page = 1
    resp = first
    while True:
        offers = _handle(resp)
        _flush(offers)
        print(f"  page {page}/{total_pages}: {len(offers)} products  (cumul {total_saved})")

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached — stopping.")
            break
        page += 1
        if page > total_pages:
            break
        time.sleep(DELAY)
        resp = _fetch_page(session, page)
        if resp is None:
            print(f"  WARNING: page {page} failed after retries — catalogue may be incomplete")
            break

    print(f"\nFinished: {len(seen):,} unique products seen.")
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
        description="Scrape Onco Express (loja virtual) -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import LjOncoexpressDB, load_env
    load_env(args.env)

    db    = LjOncoexpressDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = LjOncoexpressDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
