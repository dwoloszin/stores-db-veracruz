"""
scraper_justmedicamentos.py — Scraper for Just Medicamentos
(https://novo.justmedicamentos.com.br)

Platform   : WordPress + WooCommerce (specialty / high-cost medications).
Data source : Public WooCommerce Store API
                https://novo.justmedicamentos.com.br/wp-json/wc/store/v1/products
              Plain HTTP GET, no auth, no browser — GitHub Actions friendly.
              Pagination via the X-WP-TotalPages header; orderby=title for stable paging.
EAN        : First-class — the WooCommerce `sku` field IS the product's GTIN-13
             (100% coverage, checksum-validated). No enrichment step needed.
Prices     : prices.regular_price / prices.sale_price are strings in MINOR units
             (cents); currency_minor_unit=2 → divide by 100.

Usage:
    python -m markets.justmedicamentos.scraper_justmedicamentos              # scrape -> DB
    python -m markets.justmedicamentos.scraper_justmedicamentos --limit 50   # test run -> DB
    python -m markets.justmedicamentos.scraper_justmedicamentos --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://novo.justmedicamentos.com.br"
STORE_ID  = "justmedicamentos"
API_URL   = f"{BASE_URL}/wp-json/wc/store/v1/products"
PAGE_SIZE = 100
DELAY     = 0.3

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


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
# EAN helper
# ──────────────────────────────────────────────────────────────────────────────

def _valid_gtin(code: str) -> bool:
    """EAN-8/12/13/14 checksum validation."""
    if not code.isdigit() or len(code) not in (8, 12, 13, 14):
        return False
    digits = [int(c) for c in code]
    check = digits[-1]
    body = digits[:-1][::-1]
    total = sum(d * (3 if i % 2 == 0 else 1) for i, d in enumerate(body))
    return (10 - total % 10) % 10 == check


# ──────────────────────────────────────────────────────────────────────────────
# Page fetcher
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_page(
    session: requests.Session,
    page:    int,
    attempt: int = 0,
) -> tuple[List[Dict], int]:
    """Fetch one Store-API page. Returns (products, total_pages)."""
    try:
        r = session.get(
            API_URL,
            params={"per_page": PAGE_SIZE, "page": page, "orderby": "title", "order": "asc"},
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
    if value is None or value == "":
        return None
    try:
        return round(int(value) / (10 ** minor_unit), 2)
    except (ValueError, TypeError):
        return None


def _standardize(raw: Dict) -> Optional[Dict]:
    pid  = str(raw.get("id") or "").strip()
    name = unescape(str(raw.get("name") or "").strip())
    if not pid or not name:
        return None

    prices  = raw.get("prices") or {}
    minor   = int(prices.get("currency_minor_unit") or 2)
    regular = _price_from_minor(prices.get("regular_price"), minor)
    sale    = _price_from_minor(prices.get("sale_price"), minor)
    on_sale = bool(raw.get("on_sale"))
    if regular is None:
        regular = _price_from_minor(prices.get("price"), minor)
    if regular is None or regular <= 0:
        return None

    promo_price = None
    if on_sale and sale is not None and sale < regular:
        promo_price = sale

    discount_pct = (
        round((1 - promo_price / regular) * 100, 1)
        if promo_price and regular > 0 else None
    )

    # The WooCommerce SKU is the product's GTIN-13; keep it only if valid.
    sku = str(raw.get("sku") or "").strip()
    ean = sku if _valid_gtin(sku) else ""

    brands = raw.get("brands") or []
    brand = unescape(str(brands[0].get("name")).strip()) if brands and brands[0].get("name") else ""

    cats = raw.get("categories") or []
    category_path = " > ".join(unescape(str(c.get("name")).strip()) for c in cats if c.get("name"))

    images = raw.get("images") or []
    image_url = str(images[0].get("src")).strip() if images and images[0].get("src") else ""

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         brand,
        "category_path": category_path,
        "ean":           ean,
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  bool(raw.get("is_in_stock", True)),
        "stock":         raw.get("low_stock_remaining"),
        "offer_tag":     "",
        "product_url":   str(raw.get("permalink") or "").strip(),
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    """Paginate the WooCommerce Store API and save in per-page batches."""
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
            print(f"  page {page:>2}/{total_pages or '?'}  got={len(raw_products)}  saved={stats['upserted']}  cumul={total_saved}  with_ean={ean_count}")

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached — stopping.")
            break
        if total_pages and page >= total_pages:
            break
        if not total_pages and len(raw_products) < PAGE_SIZE:
            break

        page += 1
        time.sleep(DELAY)

    pct = (100 * ean_count // total_saved) if total_saved else 0
    print(f"\nFinished: {total_saved:,} products saved, {ean_count:,} with EAN ({pct}%).")
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
        description="Scrape Just Medicamentos (novo.justmedicamentos.com.br) -> PostgreSQL"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test mode)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path (default: .env)")
    args = parser.parse_args()

    from db.db_manager import JustMedicamentosDB, load_env
    load_env(args.env)

    db    = JustMedicamentosDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = JustMedicamentosDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
