"""
scraper_farmaciasapp.py — Scraper for Farmácias App (https://www.farmaciasapp.com.br)

Platform  : VTEX  (MIGRATED 2026-09 from the old mkplace/Typesense stack — the
            Typesense collection was decommissioned, which is why the old scraper
            started failing with 404 "Collection not found" / 530 on the
            search-*.main.mkplace.com.br hosts around 2026-09-17).
API       : /api/catalog_system/pub/products/search/  +  /category/tree/50
API host  : www.farmaciasapp.com.br (the storefront itself). NB the VTEX backend
            host lojafarmaciasapp.vtexcommercestable.com.br 429s HARD under load
            (like araujo's backend) — 6/6 rapid requests all 429 — while the
            storefront answers 6/6 clean 206s. So we hit the storefront directly;
            it returns plain catalog JSON (no WAF interstitial seen).
Auth      : none (public VTEX catalog API, Googlebot UA)
Pagination: _from/_to, 50/page, VTEX hard cap _to <= 2549 (2550 per fq)
EAN       : inline at items[0].ean
Promo     : commertialOffer ListPrice (regular) / Price (selling)

Big-category handling:
    ~74k products; the 10 top categories all blow the 2550 cap and products map
    to high category levels (deep leaves are mostly empty). We walk EVERY tree
    node (leaves + parents) and, for any node over the cap, subdivide by price
    range (fq=P:[lo TO hi]) recursively until each bucket is under the cap.
    Products are deduped globally by productId, so parent/child overlap is fine.

Usage:
    python -m markets.farmaciasapp.scraper_farmaciasapp              # scrape -> DB
    python -m markets.farmaciasapp.scraper_farmaciasapp --limit 500  # test run
    python -m markets.farmaciasapp.scraper_farmaciasapp --csv        # DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.farmaciasapp.com.br"   # storefront (product_url + catalog API)
API_HOST   = BASE_URL                            # backend vtexcommercestable host 429s hard -> use storefront
STORE_ID   = "farmaciasapp"
PAGE_SIZE  = 50
PRICE_SENTINEL = 9_999_000
VTEX_CAP   = 2550
PRICE_MAX  = 100_000     # upper bound for the top price bucket
DELAY      = 0.15

GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      GOOGLEBOT_UA,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


def _search(session: requests.Session, fqs: List[str], from_: int, to_: int,
            attempt: int = 0) -> Tuple[Optional[List[Dict]], int]:
    """GET products for a list of fq filters. Returns (products|None, subtree_total).

    NB: this VTEX edge REJECTS a `+`-encoded space in the price fq (P:[lo TO hi]) —
    requests' `params=` encodes space as `+`, which returns 0 hits when a category
    fq and a price fq are combined. So we build the query string manually and encode
    spaces as %20 (keeping :/[] literal, as the storefront expects).
    """
    q = "&".join([f"fq={quote(fq, safe=':/[]')}" for fq in fqs]
                 + [f"_from={from_}", f"_to={to_}"])
    try:
        r = session.get(f"{API_HOST}/api/catalog_system/pub/products/search?{q}",
                        timeout=30)
    except requests.RequestException:
        if attempt >= 4:
            return None, 0
        time.sleep(min(3 * (attempt + 1), 15))
        return _search(session, fqs, from_, to_, attempt + 1)
    if r.status_code == 429 or r.status_code >= 500:
        if attempt >= 5:
            return None, 0
        time.sleep(min(5 * (attempt + 1), 30))
        return _search(session, fqs, from_, to_, attempt + 1)
    if r.status_code not in (200, 206):
        return None, 0
    total = 0
    resources = r.headers.get("resources", "")
    if "/" in resources:
        try:
            total = int(resources.split("/")[-1])
        except ValueError:
            pass
    try:
        return r.json(), total
    except ValueError:
        return None, total


def _count(session: requests.Session, fqs: List[str]) -> int:
    _p, total = _search(session, fqs, 0, 1)
    return total


# ──────────────────────────────────────────────────────────────────────────────
# Category planning (walk tree + price subdivision for capped categories)
# ──────────────────────────────────────────────────────────────────────────────

def _price_buckets(session: requests.Session, base_fqs: List[str], lo: float, hi: float,
                   out: List[List[str]], depth: int = 0) -> None:
    """Recursively split price range [lo,hi] until each bucket holds <= VTEX_CAP."""
    fqs = base_fqs + [f"P:[{lo} TO {hi}]"]
    total = _count(session, fqs)
    time.sleep(DELAY)
    if total <= 0:
        return
    if total <= VTEX_CAP or depth >= 24 or (hi - lo) <= 0.02:
        out.append(fqs)
        return
    mid = round((lo + hi) / 2, 2)
    if mid <= lo or mid >= hi:
        out.append(fqs)
        return
    _price_buckets(session, base_fqs, lo, mid, out, depth + 1)
    _price_buckets(session, base_fqs, mid, hi, out, depth + 1)


def plan_targets(session: requests.Session) -> List[List[str]]:
    """Whole-catalogue plan: subdivide the FULL catalogue by price ONCE.

    Far cheaper than a per-category walk (~63 count calls vs ~1000): the store's
    ~74k products all sit under the top categories, so a single global price
    subdivision reaches every product. category_path comes from each product's own
    `categories` field, so we don't need the category tree at all. Datacenter
    latency makes count calls the dominant cost — minimising them is what keeps the
    GitHub run well under the 60min bar.
    """
    total = _count(session, [])
    print(f"  Catalogue total: {total:,} — subdividing by price ...")
    buckets: List[List[str]] = []
    _price_buckets(session, [], 0.0, float(PRICE_MAX), buckets)
    return buckets


# ──────────────────────────────────────────────────────────────────────────────
# Standardize (standard VTEX shape)
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
    import gc

    session = _make_session()
    seen_pids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0

    print("Planning price buckets over the whole catalogue ...")
    targets = plan_targets(session)
    print(f"  Price buckets to page: {len(targets)}")

    batch: List[Dict] = []

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
        gc.collect()

    def _scrape_target(fqs: List[str], label: str) -> None:
        from_ = 0
        while from_ < VTEX_CAP:
            to_ = min(from_ + PAGE_SIZE - 1, VTEX_CAP - 1)
            page, _total = _search(session, fqs, from_, to_)
            if not page:
                break
            for raw in page:
                pid = str(raw.get("productId", "")).strip()
                if not pid or pid in seen_pids:
                    continue
                seen_pids.add(pid)
                offer = _standardize(raw, label)
                if offer:
                    batch.append(offer)
            from_ += len(page)
            if len(page) < PAGE_SIZE:
                break
            time.sleep(DELAY)
            if limit and len(seen_pids) >= limit:
                break

    for ti, fqs in enumerate(targets, 1):
        _scrape_target(fqs, "")
        if len(batch) >= 300:
            _flush()
        if ti % 20 == 0:
            print(f"  [bucket {ti}/{len(targets)}] unique so far: {len(seen_pids):,}  saved: {total_saved:,}")
        if limit and len(seen_pids) >= limit:
            break

    _flush()
    print(f"\nFinished: {len(seen_pids):,} unique products seen.")
    if total_upserted == 0:
        print("ERROR: 0 products upserted — treating as failure.")
        sys.exit(1)
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
        description="Scrape Farmácias App (VTEX) -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import FarmaciasAppDB, load_env
    load_env(args.env)

    db    = FarmaciasAppDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = FarmaciasAppDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
