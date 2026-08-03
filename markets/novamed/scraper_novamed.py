"""
scraper_novamed.py — Scraper for Nova Medicamentos (https://www.novamedicamentos.com.br)

Platform   : Magento 2 (specialty / high-cost medications).
Data source : Public Magento GraphQL storefront endpoint
                POST https://www.novamedicamentos.com.br/graphql
              Plain HTTP POST, no auth, no browser — GitHub Actions friendly.
              Paginated via page_info.total_pages.
EAN        : NOT exposed via GraphQL. It lives on each product page inside
                <div class="product attribute Codigo de Barra do Produto">
                    ... <div class="value">7891234567890</div>
             Populated by enrich_ean_novamed.py after scraping.
Prices     : price_range.minimum_price values are already in BRL (not cents).

Usage:
    python -m markets.novamed.scraper_novamed              # scrape -> DB
    python -m markets.novamed.scraper_novamed --limit 100  # test run -> DB
    python -m markets.novamed.scraper_novamed --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.novamedicamentos.com.br"
STORE_ID   = "novamed"
GRAPHQL_URL = f"{BASE_URL}/graphql"
PAGE_SIZE  = 200
DELAY      = 0.3

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_QUERY = """
{
  products(search: "", pageSize: %(size)d, currentPage: %(page)d) {
    total_count
    page_info { current_page total_pages }
    items {
      sku
      name
      url_key
      stock_status
      image { url }
      categories { name }
      price_range {
        minimum_price {
          regular_price { value }
          final_price { value }
          discount { percent_off }
        }
      }
    }
  }
}
"""


# ──────────────────────────────────────────────────────────────────────────────
# Session
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json",
        "Content-Type":    "application/json",
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
    """Fetch one GraphQL page. Returns (items, total_pages)."""
    body = {"query": _QUERY % {"size": PAGE_SIZE, "page": page}}
    try:
        r = session.post(GRAPHQL_URL, json=body, timeout=45)
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

    if r.status_code != 200:
        print(f"    HTTP {r.status_code} for page {page}")
        return [], 0

    try:
        data = r.json()
    except ValueError:
        return [], 0

    products = (((data.get("data") or {}).get("products")) or {})
    items = products.get("items") or []
    total_pages = int(((products.get("page_info") or {}).get("total_pages")) or 0)
    return items, total_pages


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _standardize(raw: Dict) -> Optional[Dict]:
    sku  = str(raw.get("sku") or "").strip()
    name = str(raw.get("name") or "").strip()
    if not sku or not name:
        return None

    pr = ((raw.get("price_range") or {}).get("minimum_price")) or {}
    regular = _to_float((pr.get("regular_price") or {}).get("value"))
    final   = _to_float((pr.get("final_price") or {}).get("value"))
    if regular is None or regular <= 0:
        return None

    promo_price = None
    if final is not None and 0 < final < regular:
        promo_price = final

    discount_pct = _to_float((pr.get("discount") or {}).get("percent_off"))
    if discount_pct == 0:
        discount_pct = None

    cats = raw.get("categories") or []
    cat_names = [str(c.get("name") or "").strip() for c in cats if c.get("name")]
    category_path = " > ".join(cat_names)

    url_key = str(raw.get("url_key") or "").strip()
    product_url = f"{BASE_URL}/{url_key}" if url_key else ""

    image_url = str(((raw.get("image") or {}).get("url")) or "").strip()

    return {
        "product_id":    sku,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         "",
        "category_path": category_path,
        "ean":           "",   # enriched from product page later
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  str(raw.get("stock_status")) == "IN_STOCK",
        "stock":         None,
        "offer_tag":     "",
        "product_url":   product_url,
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    """Paginate the Magento GraphQL endpoint and save in per-page batches."""
    session  = _make_session()
    seen_ids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0

    page = 1
    total_pages = None
    while True:
        items, tp = _fetch_page(session, page)
        if total_pages is None and tp:
            total_pages = tp
            print(f"GraphQL reports {total_pages} page(s) of up to {PAGE_SIZE} products.")
        if not items:
            break

        batch: List[Dict] = []
        for raw in items:
            sku = str(raw.get("sku") or "")
            if not sku or sku in seen_ids:
                continue
            seen_ids.add(sku)
            offer = _standardize(raw)
            if offer:
                batch.append(offer)

        if batch:
            stats = db.save(batch, verbose=False)
            total_saved    += stats["upserted"]
            total_upserted += stats["upserted"]
            total_history  += stats["history_inserted"]
            total_skipped  += stats["skipped_zero"]
            print(f"  page {page}/{total_pages or '?'}  got={len(items)}  saved={stats['upserted']}  cumul={total_saved}")

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached — stopping.")
            break
        if total_pages and page >= total_pages:
            break
        if len(items) < PAGE_SIZE:
            break

        page += 1
        time.sleep(DELAY)

    print(f"\nFinished: {total_saved:,} products saved (EAN enrichment runs separately).")
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
        description="Scrape Nova Medicamentos (novamedicamentos.com.br) -> PostgreSQL"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test mode)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path (default: .env)")
    args = parser.parse_args()

    from db.db_manager import NovamedDB, load_env
    load_env(args.env)

    db    = NovamedDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = NovamedDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
