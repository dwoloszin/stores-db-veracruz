"""
scraper_panvel.py — Scraper for Panvel (https://www.panvel.com)

Platform  : Angular SPA with custom REST API (panvel-ecommerce-bff)
API       : POST /api/v3/search
Auth      : app-token + sessionId (UUID) + user-id headers
            Session cookies obtained from homepage visit
Pagination: currentPage (1-indexed), 24 items/page
            totalItems and totalPages reliable in response

Category note:
    Top-level departments (e.g. /panvel/medicamentos/c-35206) return all 10k
    products regardless of categoryId — filtering is done via slug-based
    descricao_da_categoria_N filters, not by categoryId.
    Use 2nd-level subcategories from /api/v1/category/menu (100 total).
    No subcategory exceeds 10k products (largest: beleza/cabelo ~5,421).

Prices:
    price.originalPrice -> regular_price
    price.dealPrice     -> promo_price (only when dealPrice < originalPrice)

EAN:
    NOT available in search API results. The product detail page HTML has
    EAN in a specifications table (data-cy="specifications-value-Código de Barras").
    Run enrich_ean_panvel.py to back-fill EAN via product pages.

Usage:
    python -m markets.panvel.scraper_panvel              # scrape -> DB
    python -m markets.panvel.scraper_panvel --limit 500  # test run -> DB
    python -m markets.panvel.scraper_panvel --csv        # scrape -> DB + CSV
"""

import csv
import re
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.panvel.com"
STORE_ID  = "panvel"
PAGE_SIZE = 24
APP_TOKEN = "ZYkPuDaVJEiD"
DELAY     = 0.25  # seconds between requests

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# These user-id / client-ip values are hardcoded in the Angular bundle
_USER_ID   = "8601417"
_CLIENT_IP = "1"


# ──────────────────────────────────────────────────────────────────────────────
# Session — requires homepage visit to obtain Azion CDN cookies
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> Tuple[requests.Session, str]:
    """
    Returns (session, session_id).
    Visits the homepage to collect CDN cookies, then generates a random
    UUID as the sessionId (replicated from the Angular client behaviour).
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    s.get(BASE_URL, timeout=20)

    session_id = str(uuid.uuid4())
    s.cookies.set("sessionId", session_id, domain="www.panvel.com", path="/")
    s.cookies.set("UF", "RS", domain="www.panvel.com", path="/")
    return s, session_id


def _api_headers(session_id: str) -> Dict[str, str]:
    return {
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "app-token":    APP_TOKEN,
        "sessionId":    session_id,
        "user-id":      _USER_ID,
        "client-ip":    _CLIENT_IP,
        "search-new":   "A",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Category tree — from /api/v1/category/menu
# ──────────────────────────────────────────────────────────────────────────────

def fetch_category_nodes(
    session: requests.Session,
    session_id: str,
) -> List[Dict]:
    """
    Returns a flat list of 2nd-level subcategories, each with:
      cat_id, dept_slug, sub_slug, full_path
    """
    r = session.get(
        f"{BASE_URL}/api/v1/category/menu",
        headers=_api_headers(session_id),
        timeout=20,
    )
    r.raise_for_status()
    menu = r.json()

    nodes: List[Dict] = []
    for dept in menu.get("categories") or []:
        dept_link = dept.get("link", "")
        parts = dept_link.strip("/").split("/")
        dept_slug = parts[1] if len(parts) >= 2 else ""

        for sub in dept.get("secondLevel") or []:
            sub_link = sub.get("link", "")
            sub_parts = sub_link.strip("/").split("/")
            if len(sub_parts) < 3:
                continue
            sub_slug = sub_parts[2] if len(sub_parts) >= 4 else sub_parts[-1]
            nodes.append({
                "cat_id":    int(sub["id"]),
                "dept_slug": dept_slug,
                "sub_slug":  sub_slug,
                "full_path": f"{dept.get('description', dept_slug)}/{sub.get('description', sub_slug)}",
            })
    return nodes


# ──────────────────────────────────────────────────────────────────────────────
# Page fetcher
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _fetch_page(
    session:    requests.Session,
    session_id: str,
    dept_slug:  str,
    sub_slug:   str,
    cat_id:     int,
    page:       int,
    attempt:    int = 0,
) -> Tuple[List[Dict], int, int]:
    """Returns (items, total_items, total_pages). Handles rate-limit retry."""
    body = {
        "filters": [
            {"name": "descricao_da_categoria_1", "values": [dept_slug]},
            {"name": "descricao_da_categoria_2", "values": [sub_slug]},
        ],
        "categoryId":  cat_id,
        "searchType":  "category",
        "currentPage": page,
        "itemsPerPage": PAGE_SIZE,
        "assortment":  "mais relevantes",
    }
    r = session.post(
        f"{BASE_URL}/api/v3/search",
        json=body,
        params={"type": "CSR", "uf": "RS"},
        headers=_api_headers(session_id),
        timeout=30,
    )
    if r.status_code == 429:
        if attempt >= 5:
            print("    Rate limited 5x — giving up on this page")
            return [], 0, 0
        print("    Rate limited — sleeping 15s")
        time.sleep(15)
        return _fetch_page(session, session_id, dept_slug, sub_slug, cat_id, page, attempt + 1)
    if r.status_code not in (200,):
        return [], 0, 0

    try:
        data = r.json()
    except ValueError:
        return [], 0, 0

    items       = data.get("items") or []
    total_items = data.get("totalItems") or 0
    total_pages = data.get("totalPages") or 0
    return items, total_items, total_pages


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _standardize(raw: Dict, cat_label: str) -> Optional[Dict]:
    name = str(raw.get("name") or "").strip()
    pid  = str(raw.get("panvelCode") or "").strip()
    if not name or not pid:
        return None

    price     = raw.get("price") or {}
    regular   = _to_float(price.get("originalPrice"))
    deal      = _to_float(price.get("dealPrice"))
    if not regular or regular <= 0:
        return None

    promo = deal if (deal and deal < regular) else None
    discount_pct = (
        round((1 - promo / regular) * 100, 1)
        if promo and regular > 0 else None
    )

    event    = raw.get("event") or {}
    cat_path = "/".join(filter(None, [
        event.get("itemCategory1"),
        event.get("itemCategory2"),
        event.get("itemCategory3"),
    ])) or cat_label

    tag_obj  = raw.get("tag") or {}
    offer_tag = str(tag_obj.get("description") or "").strip()

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(raw.get("brandName") or "").strip(),
        "category_path": cat_path,
        "ean":           "",
        "regular_price": regular,
        "promo_price":   promo,
        "discount_pct":  discount_pct,
        "unit":          str(raw.get("presentationTitle") or "").strip(),
        "is_available":  True,
        "stock":         None,
        "offer_tag":     offer_tag,
        "product_url":   str(raw.get("link") or "").strip(),
        "image_url":     str(raw.get("image") or "").strip(),
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    """
    Scrape all 2nd-level subcategories and save to DB after each one.
    Returns cumulative stats dict.
    """
    import gc

    session, session_id = _make_session()
    seen_ids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0

    print("Fetching category menu...")
    leaves = fetch_category_nodes(session, session_id)
    print(f"Found {len(leaves)} subcategories to scrape.")

    for cat in leaves:
        dept_slug = cat["dept_slug"]
        sub_slug  = cat["sub_slug"]
        cat_id    = cat["cat_id"]
        cat_label = cat["full_path"]
        cat_offers: List[Dict] = []

        page = 1
        total_pages_known = None
        total_items_known = None

        while True:
            items, total_items, total_pages = _fetch_page(
                session, session_id, dept_slug, sub_slug, cat_id, page
            )

            if total_items_known is None and total_items:
                total_items_known = total_items
                total_pages_known = total_pages
                if total_items >= 10000:
                    print(
                        f"  WARNING: {cat_label} total={total_items} "
                        f"(hit API cap — some products may be missed)"
                    )

            if not items:
                break

            new_this_page = 0
            for raw in items:
                pid = str(raw.get("panvelCode") or "").strip()
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                offer = _standardize(raw, cat_label)
                if offer:
                    cat_offers.append(offer)
                    new_this_page += 1

            if new_this_page > 0 or page == 1:
                print(
                    f"  {cat_label[:50]:<50}  p={page:>3}/{total_pages_known or '?':>3}  "
                    f"got={len(items)}  new={new_this_page}  "
                    f"total={total_items_known or '?':>5}  "
                    f"saved={total_saved}"
                )

            if page >= (total_pages_known or 1):
                break
            if len(items) < PAGE_SIZE:
                break

            page += 1
            time.sleep(DELAY)

            if limit and total_saved + len(cat_offers) >= limit:
                break

        # Save this category's batch
        if cat_offers:
            stats = db.save(cat_offers, verbose=False)
            total_saved    += stats["upserted"]
            total_upserted += stats["upserted"]
            total_history  += stats["history_inserted"]
            total_skipped  += stats["skipped_zero"]
            print(
                f"    -> saved {stats['upserted']} | "
                f"price changes {stats['history_inserted']} | cumul {total_saved}"
            )
            cat_offers.clear()
            gc.collect()

        time.sleep(DELAY)

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached — stopping.")
            break

    return {
        "upserted":         total_upserted,
        "history_inserted": total_history,
        "skipped_zero":     total_skipped,
        "total_unique":     total_saved,
    }


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
        description="Scrape Panvel -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",      type=int, default=None,  help="Stop after N products (test)")
    parser.add_argument("--enrich-ean", action="store_true",     help="Fetch EAN after scraping")
    parser.add_argument("--workers",    type=int, default=12,    help="EAN enrichment threads (default: 12)")
    parser.add_argument("--csv",        action="store_true",     help="Also export a CSV file after scrape")
    parser.add_argument("--output",     type=str, default=None,  help="CSV path (implies --csv)")
    parser.add_argument("--env",        type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import PanvelDB, load_env
    load_env(args.env)

    db    = PanvelDB()
    stats = scrape(db, limit=args.limit)

    if args.enrich_ean:
        from markets.panvel.enrich_ean_panvel import enrich
        enrich(workers=args.workers, limit=args.limit, db=db)

    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = PanvelDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
