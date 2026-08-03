"""
scraper_levitta.py — Scraper for Levitta Medicamentos (https://www.levittamedicamentos.com.br)

Platform   : Tray Commerce (Brazilian e-commerce platform).
Data source : Public Tray Store API
                https://www.levittamedicamentos.com.br/web_api/products?limit=50&page=N
              Plain HTTP GET, no auth, no browser — GitHub Actions friendly.
              Each list item is wrapped as {"Product": {...}}.
              Pagination via paging.total; maxLimit is 50 per page.
EAN        : First-class API field ("ean"), ~96% coverage — no enrichment needed.
Prices     : "price" / "promotional_price" are decimal strings in BRL.
             promotional_price == "0" means no active promotion.

Usage:
    python -m markets.levitta.scraper_levitta              # scrape -> DB
    python -m markets.levitta.scraper_levitta --limit 200  # test run -> DB
    python -m markets.levitta.scraper_levitta --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.levittamedicamentos.com.br"
STORE_ID  = "levitta"
API_URL   = f"{BASE_URL}/web_api/products"
PAGE_SIZE = 50     # Tray maxLimit
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
# Page fetcher
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_page(
    session: requests.Session,
    page:    int,
    attempt: int = 0,
) -> tuple[List[Dict], int]:
    """Fetch one Tray API page. Returns (unwrapped_products, total_count)."""
    try:
        r = session.get(API_URL, params={"limit": PAGE_SIZE, "page": page}, timeout=45)
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
        data = r.json()
    except ValueError:
        return [], 0

    items = data.get("Products") or []
    products = [it.get("Product", it) for it in items]
    total = int((data.get("paging") or {}).get("total") or 0)
    return products, total


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _clean_ean(v: Any) -> str:
    ean = str(v or "").strip()
    if ean and ean != "0" and ean.isdigit() and len(ean) in (8, 12, 13, 14):
        return ean
    return ""


def _standardize(raw: Dict) -> Optional[Dict]:
    pid  = str(raw.get("id") or "").strip()
    name = str(raw.get("name") or "").strip()
    if not pid or not name:
        return None

    regular = _to_float(raw.get("price"))
    promo   = _to_float(raw.get("promotional_price"))
    if regular is None or regular <= 0:
        return None

    promo_price = None
    if promo is not None and 0 < promo < regular:
        promo_price = promo

    discount_pct = (
        round((1 - promo_price / regular) * 100, 1)
        if promo_price and regular > 0 else None
    )

    slug = str(raw.get("slug") or "").strip()
    category_path = ""
    if "/" in slug:
        category_path = slug.split("/")[0].replace("-", " ").title()

    url_field = raw.get("url")
    if isinstance(url_field, dict):
        product_url = str(url_field.get("https") or url_field.get("http") or "").strip()
    else:
        product_url = str(url_field or "").strip()

    image_url = ""
    imgs = raw.get("ProductImage")
    if isinstance(imgs, list) and imgs and isinstance(imgs[0], dict):
        image_url = str(imgs[0].get("https") or imgs[0].get("http") or "").strip()

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(raw.get("brand") or "").strip(),
        "category_path": category_path,
        "ean":           _clean_ean(raw.get("ean")),
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  str(raw.get("available")) == "1",
        "stock":         None,
        "offer_tag":     str(raw.get("model") or "").strip(),
        "product_url":   product_url,
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    """Paginate the Tray Store API and save in per-page batches."""
    session  = _make_session()
    seen_ids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0
    ean_count = 0

    page = 1
    total = None
    while True:
        products, tot = _fetch_page(session, page)
        if total is None and tot:
            total = tot
            pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
            print(f"Tray API reports {total:,} products across ~{pages} pages of {PAGE_SIZE}.")
        if not products:
            break

        batch: List[Dict] = []
        for raw in products:
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
            print(f"  page {page:>3}  got={len(products)}  saved={stats['upserted']}  cumul={total_saved}  with_ean={ean_count}")

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached — stopping.")
            break
        if total and page * PAGE_SIZE >= total:
            break
        if len(products) < PAGE_SIZE:
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
        description="Scrape Levitta Medicamentos (levittamedicamentos.com.br) -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test mode)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path (default: .env)")
    args = parser.parse_args()

    from db.db_manager import LevittaDB, load_env
    load_env(args.env)

    db    = LevittaDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = LevittaDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
