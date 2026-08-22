"""
scraper_moderna.py — Scraper for Drogaria Drogaria Moderna (https://www.drogariamoderna.com.br)

Platform  : VTEX (flat category tree, same shape as Drogaria São Paulo)
API       : /api/catalog_system/pub/products/search/
            /api/catalog_system/pub/category/tree/50
Auth      : none (public VTEX catalog API, Googlebot UA)
Pagination: _from / _to, 50 items/page, VTEX hard cap _to <= 2549 (2550 per fq)
EAN       : inline at items[0].ean — 100% coverage

Big-category handling:
    ~1,600 flat leaf categories via short-form fq=C:/{id}/. A few exceed the
    2550 cap with no children (e.g. Medicamentos ~11.5k). Those are subdivided
    by price range (fq=P:[lo TO hi]) recursively until every bucket is under the
    cap, so the whole catalogue is reachable. Products deduped globally by id.

Usage:
    python -m markets.pacheco.scraper_moderna              # scrape -> DB
    python -m markets.pacheco.scraper_moderna --limit 500  # test run -> DB
    python -m markets.pacheco.scraper_moderna --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.drogariamoderna.com.br"
STORE_ID   = "moderna"
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
    """GET products for a list of fq filters. Returns (products|None, subtree_total)."""
    params = [("fq", fq) for fq in fqs] + [("_from", from_), ("_to", to_)]
    try:
        r = session.get(f"{BASE_URL}/api/catalog_system/pub/products/search/",
                        params=params, timeout=30)
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
# Category planning (flat tree + price subdivision for capped categories)
# ──────────────────────────────────────────────────────────────────────────────

def _price_buckets(session: requests.Session, cat_fq: str, lo: float, hi: float,
                   out: List[List[str]], depth: int = 0) -> None:
    fqs = [cat_fq, f"P:[{lo} TO {hi}]"]
    total = _count(session, fqs)
    time.sleep(DELAY)
    if total <= 0:
        return
    if total <= VTEX_CAP or depth >= 20 or (hi - lo) <= 0.02:
        out.append(fqs)
        return
    mid = round((lo + hi) / 2, 2)
    if mid <= lo or mid >= hi:
        out.append(fqs)
        return
    _price_buckets(session, cat_fq, lo, mid, out, depth + 1)
    _price_buckets(session, cat_fq, mid, hi, out, depth + 1)


def flatten_categories(session: requests.Session) -> List[Tuple[int, str]]:
    """Return [(category_id, label)] from the VTEX tree (leaves + parents)."""
    tree = session.get(f"{BASE_URL}/api/catalog_system/pub/category/tree/50",
                       timeout=30).json()
    cats: List[Tuple[int, str]] = []
    seen_ids: set = set()

    def walk(node: Dict, path: str) -> None:
        cid = node["id"]
        name = node.get("name") or ""
        label = f"{path}/{name}" if path else name
        if cid not in seen_ids:
            seen_ids.add(cid)
            cats.append((cid, label))
        for ch in node.get("children") or []:
            walk(ch, label)

    for top in tree:
        walk(top, "")
    return cats


def category_targets(session: requests.Session, cid: int) -> List[List[str]]:
    """fq target(s) for one category — direct, or price-subdivided if over cap."""
    cat_fq = f"C:/{cid}/"
    total = _count(session, [cat_fq])
    time.sleep(DELAY)
    if total <= 0:
        return []
    if total <= VTEX_CAP:
        return [[cat_fq]]
    buckets: List[List[str]] = []
    _price_buckets(session, cat_fq, 0.0, float(PRICE_MAX), buckets)
    return buckets


# ──────────────────────────────────────────────────────────────────────────────
# Standardize (identical VTEX shape to drogariasaopaulo)
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

    print("Fetching category tree ...")
    cats = flatten_categories(session)
    print(f"  Categories: {len(cats)}")

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

    for ci, (cid, label) in enumerate(cats, 1):
        for fqs in category_targets(session, cid):
            _scrape_target(fqs, label)
            if len(batch) >= 300:
                _flush()
            if limit and len(seen_pids) >= limit:
                break
        if ci % 100 == 0:
            print(f"  [cat {ci}/{len(cats)}] unique so far: {len(seen_pids):,}  saved: {total_saved:,}")
        if limit and len(seen_pids) >= limit:
            break

    _flush()
    print(f"\nFinished: {len(seen_pids):,} unique products seen.")
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
        description="Scrape Drogaria Pacheco -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",  type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--csv",    action="store_true",    help="Also save a local CSV file")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env",    type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import ModernaDB, load_env
    load_env(args.env)

    db    = ModernaDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = ModernaDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
