"""
scraper_eualiria.py — Scraper for Alíria Medicamentos Especiais (https://eualiria.com.br)

Platform   : WordPress + WooCommerce (high-cost / specialty medications).
Data source : Public WooCommerce Store API
                https://eualiria.com.br/wp-json/wc/store/v1/products?per_page=100&page=N
              Plain HTTP GET, no auth, no browser — GitHub Actions friendly.
              Pagination via the X-WP-TotalPages response header.
Prices     : prices.regular_price / prices.sale_price are strings in MINOR units
             (cents); currency_minor_unit=2 → divide by 100.
EAN        : Not a first-class field. A minority of products embed it in the
             description HTML as "ean: 7891234567890". Extracted where present.
             Most products only list the ANVISA registration number (not a barcode).

Usage:
    python -m markets.eualiria.scraper_eualiria              # scrape -> DB
    python -m markets.eualiria.scraper_eualiria --limit 50   # test run -> DB
    python -m markets.eualiria.scraper_eualiria --csv        # scrape -> DB + CSV
"""

import csv
import re
import sys
import time
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://eualiria.com.br"
STORE_ID  = "eualiria"
API_URL   = f"{BASE_URL}/wp-json/wc/store/v1/products"
PAGE_SIZE = 100    # WooCommerce Store API hard cap
DELAY     = 0.3    # seconds between requests

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# EAN embedded in description as "ean: 7891234567890" (label required so the
# ANVISA registration number on the preceding line is never mistaken for a barcode).
_EAN_RE = re.compile(r"\bean\s*[:\-]?\s*(\d{8,14})", re.IGNORECASE)


# ──────────────────────────────────────────────────────────────────────────────
# Session
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Page fetcher
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_page(
    session: requests.Session,
    page:    int,
    attempt: int = 0,
) -> tuple[List[Dict], int]:
    """
    Fetch one Store-API page. Returns (products, total_pages).
    total_pages comes from the X-WP-TotalPages header (0 on error).
    """
    try:
        r = session.get(
            API_URL,
            params={"per_page": PAGE_SIZE, "page": page},
            timeout=45,
        )
    except requests.exceptions.RequestException:
        if attempt >= 5:
            print(f"    Network error on page {page} — giving up")
            return [], 0
        print(f"    Network error on page {page} — retrying in 15s")
        time.sleep(15)
        return _fetch_page(session, page, attempt + 1)

    if r.status_code == 429:
        if attempt >= 5:
            print(f"    Rate limited 5x on page {page} — giving up")
            return [], 0
        print("    Rate limited — sleeping 15s")
        time.sleep(15)
        return _fetch_page(session, page, attempt + 1)

    if r.status_code not in (200, 206):
        print(f"    HTTP {r.status_code} for page {page}")
        return [], 0

    try:
        products = r.json()
    except ValueError:
        return [], 0

    total_pages = int(r.headers.get("X-WP-TotalPages") or 0)
    return (products if isinstance(products, list) else []), total_pages


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _price_from_minor(value: Any, minor_unit: int) -> Optional[float]:
    """WooCommerce Store API prices are integer strings in minor units (cents)."""
    if value is None or value == "":
        return None
    try:
        return round(int(value) / (10 ** minor_unit), 2)
    except (ValueError, TypeError):
        return None


def _extract_ean(*texts: str) -> Optional[str]:
    for t in texts:
        if not t:
            continue
        m = _EAN_RE.search(t)
        if m:
            ean = m.group(1)
            if len(ean) in (8, 12, 13, 14):
                return ean
    return None


def _standardize(raw: Dict) -> Optional[Dict]:
    pid  = str(raw.get("id") or "").strip()
    name = unescape(str(raw.get("name") or "").strip())
    # Product names are wrapped in <b> tags in a few entries — strip markup.
    name = re.sub(r"<[^>]+>", "", name).strip()
    if not pid or not name:
        return None

    prices     = raw.get("prices") or {}
    minor      = int(prices.get("currency_minor_unit") or 2)
    regular    = _price_from_minor(prices.get("regular_price"), minor)
    sale       = _price_from_minor(prices.get("sale_price"), minor)
    on_sale    = bool(raw.get("on_sale"))

    if regular is None:
        regular = _price_from_minor(prices.get("price"), minor)

    promo_price = None
    if on_sale and sale is not None and regular is not None and sale < regular:
        promo_price = sale

    if regular is None or regular <= 0:
        return None

    discount_pct = (
        round((1 - promo_price / regular) * 100, 1)
        if promo_price and regular > 0 else None
    )

    # Category path from the WooCommerce category list (drops laboratory-only nodes last).
    cats = raw.get("categories") or []
    cat_names = [unescape(str(c.get("name") or "").strip()) for c in cats if c.get("name")]
    category_path = " > ".join(cat_names)

    # Images
    images = raw.get("images") or []
    image_url = str(images[0].get("src")).strip() if images and images[0].get("src") else ""

    # EAN from description / short_description
    ean = _extract_ean(raw.get("description") or "", raw.get("short_description") or "")

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         cat_names[-1] if cat_names else "",
        "category_path": category_path,
        "ean":           ean or "",
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  bool(raw.get("is_in_stock", True)),
        "stock":         raw.get("low_stock_remaining"),
        "offer_tag":     str(raw.get("sku") or "").strip(),
        "product_url":   str(raw.get("permalink") or "").strip(),
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    """
    Paginate the WooCommerce Store API and save in per-page batches.
    Deduplicates by product id in-memory. Returns cumulative stats dict.
    """
    session  = _make_session()
    seen_ids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0
    ean_count = 0

    page = 1
    total_pages = None
    while True:
        raw_products, tp = _fetch_page(session, page)
        if total_pages is None and tp:
            total_pages = tp
            print(f"Store API reports {total_pages} page(s) of up to {PAGE_SIZE} products.")
        if not raw_products:
            break

        batch: List[Dict] = []
        for raw in raw_products:
            pid = str(raw.get("id") or "")
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            offer = _standardize(raw)
            if offer:
                if offer["ean"]:
                    ean_count += 1
                batch.append(offer)

        if batch:
            stats = db.save(batch, verbose=False)
            total_saved    += stats["upserted"]
            total_upserted += stats["upserted"]
            total_history  += stats["history_inserted"]
            total_skipped  += stats["skipped_zero"]
            print(
                f"  page {page:>2}/{total_pages or '?'}  "
                f"got={len(raw_products)}  saved={stats['upserted']}  "
                f"cumul={total_saved}  with_ean={ean_count}"
            )

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached — stopping.")
            break
        if total_pages and page >= total_pages:
            break
        if len(raw_products) < PAGE_SIZE:
            break

        page += 1
        time.sleep(DELAY)

    print(
        f"\nFinished: {total_saved:,} products saved, "
        f"{ean_count:,} with EAN ({(100 * ean_count // total_saved) if total_saved else 0}%)."
    )
    return {"upserted": total_upserted, "history_inserted": total_history,
            "skipped_zero": total_skipped, "total_unique": total_saved}


# ──────────────────────────────────────────────────────────────────────────────
# CSV export
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Alíria (eualiria.com.br) -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test mode)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path (default: .env)")
    args = parser.parse_args()

    from db.db_manager import EualiriaDB, load_env
    load_env(args.env)

    db    = EualiriaDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = EualiriaDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
