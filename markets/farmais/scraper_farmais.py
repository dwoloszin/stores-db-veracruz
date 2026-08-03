"""
scraper_farmais.py — Scraper for Farmais (https://www.farmais.com.br)

Platform  : VTEX hybrid (vtexassets + vteximg CDN)
API       : /api/catalog_system/pub/products/search/
            /api/catalog_system/pub/category/tree/5
Auth      : none (public VTEX catalog API, Googlebot UA)
Pagination: _from / _to, 50 items per page (0-indexed)
            VTEX hard cap: _to <= 2549 (max 2550 results per query)
EAN       : available inline at items[0].ean — full coverage

Category note:
    Short-form fq=C:/{id}/ only works at top-level departments (1, 2, 3...).
    Sub-categories require the FULL hierarchical path:
        fq=C:/1/8/31/  (dept/sub/leaf)
    One large catch-all leaf exceeds the VTEX cap:
        Category 28 "Medicamentos de A a Z" — ~4,396 products.
        Only 2,550 can be retrieved; a WARNING is printed for this category.

Usage:
    python -m markets.farmais.scraper_farmais              # scrape -> DB
    python -m markets.farmais.scraper_farmais --limit 500  # test run -> DB
    python -m markets.farmais.scraper_farmais --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.farmais.com.br"
STORE_ID  = "farmais"
PAGE_SIZE = 50    # VTEX max per request
VTEX_CAP  = 2549  # VTEX hard ceiling: _to cannot exceed this
DELAY     = 0.2   # seconds between requests

GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

_SKIP_IDS = {186}  # empty department (0 products)


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
# Category tree — full VTEX hierarchical paths required
# ──────────────────────────────────────────────────────────────────────────────

def fetch_category_nodes(session: requests.Session) -> List[Dict]:
    """
    Returns a flat list of ALL leaf nodes with full hierarchical fq paths.
    e.g. fq = 'C:/1/8/31/'

    Short-form fq=C:/{id}/ only works for top-level departments; sub-categories
    require the full parent chain.
    """
    r = session.get(
        f"{BASE_URL}/api/catalog_system/pub/category/tree/5",
        timeout=25,
    )
    r.raise_for_status()
    tree = r.json()

    nodes: List[Dict] = []

    def _walk(cat_list: List[Dict], id_path: List[int], name_path: str) -> None:
        for node in cat_list:
            nid = node["id"]
            if nid in _SKIP_IDS:
                continue
            new_ids  = id_path + [nid]
            new_name = f"{name_path}/{node['name']}" if name_path else node["name"]
            children = node.get("children") or []
            fq       = "C:/" + "/".join(str(i) for i in new_ids) + "/"

            if not children:
                nodes.append({
                    "id":        nid,
                    "name":      node["name"],
                    "fq":        fq,
                    "full_path": new_name,
                    "is_leaf":   True,
                })
            else:
                _walk(children, new_ids, new_name)

    _walk(tree, [], "")
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
    session: requests.Session,
    fq:      str,
    from_:   int,
    attempt: int = 0,
) -> Tuple[List[Dict], int]:
    """Returns (items, total_in_category). Handles rate-limit retry."""
    to_ = min(from_ + PAGE_SIZE - 1, VTEX_CAP)
    r = session.get(
        f"{BASE_URL}/api/catalog_system/pub/products/search/",
        params={"fq": fq, "_from": from_, "_to": to_},
        timeout=30,
    )
    if r.status_code == 429:
        if attempt >= 5:
            print("    Rate limited 5x — giving up on this page")
            return [], 0
        print("    Rate limited — sleeping 10s")
        time.sleep(10)
        return _fetch_page(session, fq, from_, attempt + 1)
    if r.status_code not in (200, 206):
        return [], 0

    resources = r.headers.get("resources", "")
    total = 0
    if resources and "/" in resources:
        try:
            total = int(resources.split("/")[-1])
        except ValueError:
            pass

    try:
        data = r.json()
    except ValueError:
        return [], 0
    return (data if isinstance(data, list) else []), total


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

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

    cats     = raw.get("categories") or []
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
    Scrape all leaf categories and save to DB after each one.
    Flushes per-category offer list from memory after each save.
    Returns cumulative stats dict.
    """
    import gc

    session  = _make_session()
    seen_ids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0

    print("Fetching category tree...")
    leaves = fetch_category_nodes(session)
    print(f"Found {len(leaves)} leaf categories to scrape.")

    for cat in leaves:
        fq        = cat["fq"]
        cat_label = cat["full_path"]
        from_     = 0
        cat_total = None
        cat_offers: List[Dict] = []

        while True:
            if from_ > VTEX_CAP:
                print(f"  WARNING: {cat_label} hit VTEX cap at {VTEX_CAP}")
                break

            page, total = _fetch_page(session, fq, from_)
            if cat_total is None and total:
                cat_total = total
                if total > VTEX_CAP:
                    print(
                        f"  WARNING: {cat_label} has {total} products "
                        f"but VTEX caps at {VTEX_CAP + 1} — "
                        f"{total - (VTEX_CAP + 1)} products will be missed."
                    )
            if not page:
                break

            new_this_page = 0
            for raw in page:
                pid = str(raw.get("productId", "")).strip()
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                offer = _standardize(raw, cat_label)
                if offer:
                    cat_offers.append(offer)
                    new_this_page += 1

            if new_this_page > 0 or from_ == 0:
                print(
                    f"  {cat_label[:50]:<50}  from={from_:>5}  "
                    f"got={len(page)}  new={new_this_page}  "
                    f"total={cat_total or '?':>5}  "
                    f"saved={total_saved}"
                )

            if len(page) < PAGE_SIZE:
                break

            from_ += PAGE_SIZE

            if from_ > VTEX_CAP and cat_total and cat_total > VTEX_CAP:
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
        description="Scrape Farmais -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--csv",    action="store_true",    help="Also export a CSV file after scrape")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import FarmaisDB, load_env
    load_env(args.env)

    db    = FarmaisDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = FarmaisDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
