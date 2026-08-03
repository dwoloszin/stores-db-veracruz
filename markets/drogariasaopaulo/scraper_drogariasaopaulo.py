"""
scraper_drogariasaopaulo.py — Scraper for Drogaria São Paulo (https://www.drogariasaopaulo.com.br)

Platform  : VTEX (same as Drogaleste)
API       : /api/catalog_system/pub/products/search/
            /api/catalog_system/pub/category/tree/5
Auth      : none (public VTEX catalog API, Googlebot UA for safety)
Pagination: _from / _to, 50 items per page, VTEX hard cap _to <= 2549 (2550 max per category)
EAN       : available inline at items[0].ean — 100% coverage

Category note:
    The site has ~1600 flat leaf categories accessible via short-form fq=C:/{id}/.
    Two parent categories exceed the VTEX 2550 cap:
      - Dermocosméticos (893):  ~5000 products → only 2550 retrieved (logged as WARNING)
      - Produto Pet (1177):     ~4800 products → only 2550 retrieved (logged as WARNING)
    All other categories are well under the cap.

Usage:
    python -m markets.drogariasaopaulo.scraper_drogariasaopaulo              # scrape -> DB
    python -m markets.drogariasaopaulo.scraper_drogariasaopaulo --limit 500  # test run -> DB
    python -m markets.drogariasaopaulo.scraper_drogariasaopaulo --csv        # scrape -> DB + CSV
"""

import csv
import json
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.drogariasaopaulo.com.br"
STORE_ID   = "drogariasaopaulo"
PAGE_SIZE  = 50     # items per VTEX page (_to - _from + 1)
PRICE_SENTINEL = 9_999_000  # VTEX "price not available" placeholder (real price is in Price)
VTEX_CAP   = 2550   # hard VTEX limit: _to cannot exceed 2549
DELAY      = 0.25   # seconds between requests

GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

# Categories known to exceed the VTEX 2550 cap (products only reachable via parent)
_CAPPED_CATEGORIES = {893, 1177}
# Internal/test categories to skip
_SKIP_CATEGORIES   = {804, 1229, 1230}


# ──────────────────────────────────────────────────────────────────────────────
# Session
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      GOOGLEBOT_UA,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Category tree
# ──────────────────────────────────────────────────────────────────────────────

def fetch_category_tree(session: requests.Session) -> List[Dict]:
    """
    Returns a flat list of categories to scrape, each with id, name, full_path.

    The VTEX tree is already mostly flat (depth-1). Two parent categories
    (Dermocosméticos, Produto Pet) aggregate products that are not individually
    queryable via their children, so we include the parent directly and accept
    the VTEX 2550 cap for those.
    """
    r = session.get(
        f"{BASE_URL}/api/catalog_system/pub/category/tree/5",
        timeout=20,
    )
    r.raise_for_status()
    raw_cats = r.json()

    targets: List[Dict] = []
    seen_ids: set = set()

    def _add(cat_id: int, name: str, full_path: str) -> None:
        if cat_id in seen_ids or cat_id in _SKIP_CATEGORIES:
            return
        seen_ids.add(cat_id)
        targets.append({"id": cat_id, "name": name, "full_path": full_path})

    for cat in raw_cats:
        cid      = cat["id"]
        name     = cat["name"]
        children = cat.get("children") or []

        if cid in _SKIP_CATEGORIES:
            continue

        if not children or cid in _CAPPED_CATEGORIES:
            # Leaf, or parent whose children are not individually queryable
            _add(cid, name, name)
        else:
            # Walk one level of children
            for child in children:
                _add(child["id"], child["name"], f"{name}/{child['name']}")

    return targets


# ──────────────────────────────────────────────────────────────────────────────
# Page fetcher
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_category_page(
    session:  requests.Session,
    cat_id:   int,
    from_:    int,
    attempt:  int = 0,
) -> Tuple[List[Dict], int]:
    """
    Returns (products, total_count).
    Uses short-form fq=C:/{id}/ which works on this VTEX instance.
    """
    to_ = min(from_ + PAGE_SIZE - 1, VTEX_CAP - 1)
    r = session.get(
        f"{BASE_URL}/api/catalog_system/pub/products/search/",
        params={"fq": f"C:/{cat_id}/", "_from": from_, "_to": to_},
        timeout=20,
    )

    if r.status_code == 429:
        if attempt >= 5:
            print("    Rate limited 5x — giving up on this page")
            return [], 0
        print("    Rate limited — sleeping 15s")
        time.sleep(15)
        return _fetch_category_page(session, cat_id, from_, attempt + 1)

    if r.status_code not in (200, 206):
        print(f"    HTTP {r.status_code} for cat={cat_id} from={from_}")
        return [], 0

    try:
        products = r.json()
    except ValueError:
        return [], 0

    resources = r.headers.get("resources", "")
    total = 0
    if resources and "/" in resources:
        try:
            total = int(resources.split("/")[-1])
        except ValueError:
            pass

    return products, total


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


def _standardize(raw: Dict, cat_label: str) -> Optional[Dict]:
    name = str(raw.get("productName") or "").strip()
    if not name:
        return None

    items   = raw.get("items") or []
    item0   = items[0] if items else {}
    sellers = item0.get("sellers") or []
    offer   = (sellers[0].get("commertialOffer") or {}) if sellers else {}

    regular = _to_float(offer.get("ListPrice"))
    promo   = _to_float(offer.get("Price"))
    # Some rows return a sentinel ListPrice (e.g. 9999876) with the real
    # price in Price; treat the sentinel as "no regular price" and promote
    # the real price to regular so no phantom discount is recorded.
    if regular and regular >= PRICE_SENTINEL:
        regular = promo
        promo = None
    if not regular or regular <= 0:
        return None
    if promo and promo >= regular:
        promo = None

    discount_pct = (
        round((1 - promo / regular) * 100, 1)
        if promo and regular and regular > 0 else None
    )

    images    = item0.get("images") or []
    image_url = images[0].get("imageUrl", "") if images else ""

    cats = raw.get("categories") or []
    cat_path = cats[0].strip("/") if cats else cat_label

    teasers   = offer.get("Teasers") or []
    offer_tag = teasers[0].get("Name", "") if teasers else ""

    return {
        "product_id":    str(raw.get("productId", "")).strip(),
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(raw.get("brand") or "").strip(),
        "category_path": cat_path,
        "ean":           str(item0.get("ean") or "").strip(),
        "regular_price": regular,
        "promo_price":   promo,
        "discount_pct":  discount_pct,
        "unit":          str(item0.get("measurementUnit") or "").strip(),
        "is_available":  bool(offer.get("IsAvailable", False)),
        "stock":         offer.get("AvailableQuantity"),
        "offer_tag":     offer_tag,
        "product_url":   f"{BASE_URL}/{raw.get('linkText', '')}/p",
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    """
    Scrape all categories and save to DB after each one.
    Flushes per-category offer list from memory after each save.
    Returns cumulative stats dict.
    """
    import gc

    session   = _make_session()
    seen_pids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0

    print("Fetching category tree ...")
    categories = fetch_category_tree(session)
    print(f"Categories to scrape: {len(categories)}")

    for cat in categories:
        cat_id    = cat["id"]
        cat_label = cat["full_path"]
        from_     = 0
        cat_total = None
        cat_offers: List[Dict] = []

        while True:
            if from_ >= VTEX_CAP:
                break

            page, total = _fetch_category_page(session, cat_id, from_)
            if cat_total is None and total:
                cat_total = total
            if not page:
                break

            new_this_page = 0
            for raw in page:
                pid = str(raw.get("productId", "")).strip()
                if not pid or pid in seen_pids:
                    continue
                seen_pids.add(pid)
                offer = _standardize(raw, cat_label)
                if offer:
                    cat_offers.append(offer)
                    new_this_page += 1

            if new_this_page > 0 or from_ == 0:
                print(
                    f"  {cat_label[:50]:<50}  from={from_:>5}  "
                    f"got={len(page)}  new={new_this_page}  "
                    f"total={cat_total or '?':>6}  "
                    f"saved={total_saved}"
                )

            from_ += len(page)

            if len(page) < PAGE_SIZE:
                break

            if from_ >= VTEX_CAP and cat_total and cat_total > VTEX_CAP:
                print(
                    f"  WARNING: {cat_label[:50]} has {cat_total} products "
                    f"but VTEX caps at {VTEX_CAP} — {cat_total - VTEX_CAP} products unreachable"
                )
                break

            time.sleep(DELAY)

            if limit and total_saved + len(cat_offers) >= limit:
                break

        # Save this category's batch and free memory
        if cat_offers:
            stats = db.save(cat_offers, verbose=False)
            total_saved    += stats["upserted"]
            total_upserted += stats["upserted"]
            total_history  += stats["history_inserted"]
            total_skipped  += stats["skipped_zero"]
            print(f"    -> saved {stats['upserted']} | price changes {stats['history_inserted']} | cumul {total_saved}")
            cat_offers.clear()
            gc.collect()

        time.sleep(DELAY)

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached — stopping.")
            break

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
        description="Scrape Drogaria São Paulo -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import DrogariaSaoPauloDB, load_env
    load_env(args.env)

    db    = DrogariaSaoPauloDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = DrogariaSaoPauloDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
