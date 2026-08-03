"""
scraper_drogaleste.py — Scraper for Drogaleste (https://www.drogaleste.com.br)

Platform : VTEX
API      : /api/catalog_system/pub/products/search/
           /api/catalog_system/pub/category/tree/3
Auth     : none (public VTEX catalog API)
Pagination: _from / _to query params, 50 items per page (0-indexed)
            VTEX hard cap: _to <= 2549 (max 2550 results per query)
Barcodes : EAN available inline in items[0].ean — Tier 1

Category filter: must use the full category path, e.g. fq=C:/7/59/503/
                 short form (fq=C:/503/) returns 0 results on this store.

Usage:
    python -m markets.drogaleste.scraper_drogaleste               # full scrape -> DB
    python -m markets.drogaleste.scraper_drogaleste --limit 200   # test run -> DB
    python -m markets.drogaleste.scraper_drogaleste --csv         # scrape -> DB + CSV
    python -m markets.drogaleste.scraper_drogaleste --output f.csv  # scrape -> DB + named CSV
"""

import csv
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.drogaleste.com.br"
STORE_ID  = "drogaleste"
PAGE_SIZE = 50    # VTEX max per request
VTEX_CAP  = 2549  # VTEX hard ceiling: _to cannot exceed this
DELAY     = 0.15  # seconds between requests


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer":         BASE_URL,
    })
    return s


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = re.sub(r"[R$\s\xa0]", "", str(value)).strip()
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Category tree — build flat list of all nodes with full VTEX paths
# ──────────────────────────────────────────────────────────────────────────────

def fetch_category_nodes(session: requests.Session) -> List[Dict]:
    """
    Returns a flat list of ALL tree nodes, each with:
      id, name, path (e.g. "7/59/503"), is_leaf, full_path ("/7/59/503/")
    Products are only indexed under leaf categories on this store.
    """
    r = session.get(
        f"{BASE_URL}/api/catalog_system/pub/category/tree/3",
        timeout=25,
    )
    r.raise_for_status()
    tree = r.json()

    nodes: List[Dict] = []

    def _walk(cat_list: List[Dict], id_path: str = "") -> None:
        for node in cat_list:
            nid      = node["id"]
            new_path = f"{id_path}/{nid}" if id_path else str(nid)
            children = node.get("children") or []
            nodes.append({
                "id":        nid,
                "name":      node["name"],
                "path":      new_path,             # "7/59/503"
                "full_path": f"/{new_path}/",      # "/7/59/503/"
                "fq":        f"C:/{new_path}/",    # "C:/7/59/503/"
                "label":     node.get("url", "").replace(BASE_URL, "").strip("/"),
                "is_leaf":   len(children) == 0,
            })
            if children:
                _walk(children, new_path)

    _walk(tree)
    return nodes


# ──────────────────────────────────────────────────────────────────────────────
# Product fetching
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_page(
    session: requests.Session,
    fq: str,
    from_: int,
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
            print("    Rate limited 5x - giving up on this page")
            return [], 0
        print("    Rate limited - sleeping 10s")
        time.sleep(10)
        return _fetch_page(session, fq, from_, attempt + 1)
    if r.status_code not in (200, 206):
        return [], 0

    resources = r.headers.get("resources", "")
    total = 0
    if resources and "/" in resources:
        try:
            total = int(resources.split("/")[1])
        except ValueError:
            pass

    data = r.json()
    return (data if isinstance(data, list) else []), total


def _standardize(raw: Dict, cat_label: str) -> Optional[Dict]:
    name = str(raw.get("productName") or "").strip()
    if not name:
        return None

    items = raw.get("items") or []
    item0 = items[0] if items else {}
    sellers = item0.get("sellers") or []
    offer   = (sellers[0].get("commertialOffer") or {}) if sellers else {}

    regular = _to_float(offer.get("ListPrice"))
    promo   = _to_float(offer.get("Price"))
    if promo and regular and promo >= regular:
        promo = None

    images    = item0.get("images") or []
    image_url = images[0].get("imageUrl", "") if images else ""

    cats = raw.get("categories") or []
    cat_breadcrumb = cats[0].strip("/") if cats else cat_label

    teasers   = offer.get("Teasers") or []
    offer_tag = teasers[0].get("Name", "") if teasers else ""

    return {
        "product_id":    raw.get("productId", ""),
        "store_id":      "drogaleste",
        "product_name":  name,
        "brand":         str(raw.get("brand") or "").strip(),
        "category_path": cat_breadcrumb,
        "ean":           str(item0.get("ean") or "").strip(),
        "regular_price": regular,
        "promo_price":   promo,
        "discount_pct":  (
            round((1 - promo / regular) * 100, 1)
            if promo and regular and regular > 0 else None
        ),
        "unit":          str(item0.get("measurementUnit") or "").strip(),
        "is_available":  offer.get("IsAvailable", False),
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
    all_nodes = fetch_category_nodes(session)
    leaves = [n for n in all_nodes if n["is_leaf"]]
    print(f"Found {len(all_nodes)} total categories, {len(leaves)} leaves to scrape.")

    for cat in leaves:
        fq        = cat["fq"]
        cat_label = cat["label"] or cat["path"]
        from_     = 0
        cat_total = None
        cat_offers: List[Dict] = []

        while True:
            if from_ > VTEX_CAP:
                print(f"  WARNING: {cat_label} hit VTEX cap at {VTEX_CAP} — some products may be missed")
                break

            page, total = _fetch_page(session, fq, from_)
            if cat_total is None and total:
                cat_total = total
            if not page:
                break

            new_this_page = 0
            for raw in page:
                pid = raw.get("productId")
                if pid in seen_ids:
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
                    f"cat_total={cat_total or '?':>5}  "
                    f"saved={total_saved}"
                )

            if len(page) < PAGE_SIZE:
                break

            from_ += PAGE_SIZE
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
        description="Scrape Drogaleste -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test mode)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path (default: .env)")
    args = parser.parse_args()

    from db.db_manager import DrogalesteDB, load_env
    load_env(args.env)

    db    = DrogalesteDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        ts         = datetime.now().strftime("%Y%m%d_%H%M")
        output_dir = args.output or "."
        db2 = DrogalesteDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
