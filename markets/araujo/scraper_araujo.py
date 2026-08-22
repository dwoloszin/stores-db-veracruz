"""
scraper_araujo.py — Scraper for Drogaria Araujo (https://www.drogal.com.br)

Platform  : VTEX IO
API       : /api/catalog_system/pub/products/search/
            /api/catalog_system/pub/category/tree/5
Auth      : none (public VTEX catalog API, Googlebot UA)
Pagination: _from / _to, 50 items per page (0-indexed)
            VTEX hard cap: _to <= 2549 (max 2550 results per query)
EAN       : available inline at items[0].ean — 100% coverage

Category note:
    Short-form fq=C:/{id}/ only works at the top-level department (100, 200, ...).
    Sub-categories require the FULL hierarchical path:
        fq=C:/100/103/726/  (dept/sub/leaf)
    Category IDs 700, 800, 900 and the "SEM CATEGORIA" (id=1) return 0 products
    and are automatically skipped.
    No leaf category exceeds the VTEX 2550 cap (verified 2026-05-20).

Usage:
    python -m markets.paguemenos.scraper_araujo              # scrape -> DB
    python -m markets.paguemenos.scraper_araujo --limit 500  # test run -> DB
    python -m markets.paguemenos.scraper_araujo --csv        # scrape -> DB + CSV
"""

import csv
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.araujo.com.br"                 # storefront (for product_url); Akamai-blocked
API_HOST  = "https://araujo.vtexcommercestable.com.br"  # VTEX backend host — bypasses the storefront Akamai WAF
STORE_ID  = "araujo"
PAGE_SIZE = 50    # VTEX max per request
VTEX_CAP  = 2549  # VTEX hard ceiling: _to cannot exceed this
DELAY     = 0.2   # seconds between requests (per worker, between pages of one category)
WORKERS   = 8     # parallel leaf-category walkers (873 leaves -> ~20-30min vs ~161min sequential)

# One requests.Session per worker thread (Session isn't safe to share across threads).
_thread_local = threading.local()


def _worker_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = _make_session()
        _thread_local.session = s
    return s

GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

# Skip internal/empty categories
_SKIP_IDS: set = set()


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
    Returns a flat list of ALL leaf nodes, each with:
      id, name, fq (e.g. 'C:/100/103/726/'), full_path (human label), is_leaf=True

    Pague Menos requires the full hierarchical ID path for sub-categories.
    Short-form fq=C:/{id}/ only works for top-level departments (100, 200, ...).
    """
    r = session.get(
        f"{API_HOST}/api/catalog_system/pub/category/tree/5",
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
        f"{API_HOST}/api/catalog_system/pub/products/search/",
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

def _scrape_category(cat: Dict) -> List[Dict]:
    """
    Walk all pages of ONE leaf category (runs in a worker thread).
    Returns standardized offers with a per-category dedup only — global dedup and
    all DB writes happen single-threaded in scrape() below.
    """
    session   = _worker_session()
    fq        = cat["fq"]
    cat_label = cat["full_path"]
    from_     = 0
    cat_total = None
    local_seen: set = set()
    offers: List[Dict] = []

    while True:
        if from_ > VTEX_CAP:
            break
        page, total = _fetch_page(session, fq, from_)
        if cat_total is None and total:
            cat_total = total
        if not page:
            break

        for raw in page:
            pid = str(raw.get("productId", "")).strip()
            if not pid or pid in local_seen:
                continue
            local_seen.add(pid)
            offer = _standardize(raw, cat_label)
            if offer:
                offers.append(offer)

        if len(page) < PAGE_SIZE:
            break
        from_ += PAGE_SIZE
        if from_ > VTEX_CAP and cat_total and cat_total > VTEX_CAP:
            break
        time.sleep(DELAY)

    return offers


def scrape(db, limit: Optional[int] = None, workers: int = WORKERS) -> Dict:
    """
    Scrape all leaf categories in parallel (each worker walks one category's pages),
    while global dedup + batched DB saves stay on the main thread (crash-safe: every
    BATCH_SIZE products is committed via upsert, so a mid-run failure keeps what's saved).
    """
    session = _make_session()
    print("Fetching category tree...")
    leaves = fetch_category_nodes(session)
    print(f"Found {len(leaves)} leaf categories to scrape with {workers} workers.")

    seen_ids: set = set()
    batch: List[Dict] = []
    BATCH_SIZE = 500
    total_saved = total_upserted = total_history = total_skipped = 0

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
        batch.clear()

    done = 0
    ex = ThreadPoolExecutor(max_workers=workers)
    futures = {ex.submit(_scrape_category, cat): cat for cat in leaves}
    try:
        for fut in as_completed(futures):
            cat = futures[fut]
            done += 1
            try:
                cat_offers = fut.result()
            except Exception as exc:
                print(f"  ERROR {cat['full_path'][:50]}: {exc.__class__.__name__}: {exc}")
                continue

            new = 0
            for offer in cat_offers:
                pid = offer["product_id"]
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                batch.append(offer)
                new += 1

            if new or done % 25 == 0:
                print(f"  [{done:>3}/{len(leaves)}] {cat['full_path'][:45]:<45} +{new:<4} unique={len(seen_ids)}")

            if len(batch) >= BATCH_SIZE:
                _flush()

            if limit and len(seen_ids) >= limit:
                print(f"Limit {limit} reached — stopping.")
                break
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    _flush()

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
        description="Scrape Drogaria Araujo -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--csv",    action="store_true",    help="Also export a CSV file after scrape")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import AraujoDB, load_env
    load_env(args.env)

    db    = AraujoDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = AraujoDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
