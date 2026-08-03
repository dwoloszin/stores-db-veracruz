"""
scraper_qualidoc.py — Scraper for Qualidoc (https://www.qualidoc.com.br)

Platform    : deco.cx / Next.js storefront backed by a public Typesense search API.
Data source : POST https://search.qualidoc.com.br/multi_search
              Collection "products"; active/available products are status_id:=2.
              The search documents already include everything we need:
                  product_id            → product_id (canonical, stable)
                  id                    → SKU id (used in the product URL)
                  name                  → product_name
                  barcode               → ean / barcode  (first-class field)
                  brand                 → brand
                  price                 → regular price
                  sale_price            → promo price (== price when no discount)
                  discount_pct          → discount percentage
                  in_stock              → is_available
                  categories[]          → category_path
                  image.original        → image_url
                  slug + id             → product_url
                  requires_prescription → prescription
EAN         : First-class API field (barcode); no enrichment step needed.

Usage:
        python -m markets.qualidoc.scraper_qualidoc              # scrape -> DB
        python -m markets.qualidoc.scraper_qualidoc --limit 500  # test run -> DB
        python -m markets.qualidoc.scraper_qualidoc --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Set

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL            = "https://www.qualidoc.com.br"
STORE_ID            = "qualidoc"
TYPESENSE_URL       = "https://search.qualidoc.com.br"
TYPESENSE_API_KEY   = "Iduy6L32OonNSYdmgpu9EPBmt0Rmobas"
TYPESENSE_COLLECTION = "products"
ACTIVE_FILTER       = "status_id:=2"   # status_id 2 == active/published product
PER_PAGE            = 250               # Typesense per_page cap
DELAY               = 0.05             # seconds between search calls
WORKERS             = 8                # kept for CLI compatibility

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_TS_HEADERS = {
    "Content-Type":        "application/json",
    "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
}


# ──────────────────────────────────────────────────────────────────────────────
# Session
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json,text/plain,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Typesense search
# ──────────────────────────────────────────────────────────────────────────────

def _ts_post(session: requests.Session, payload: Dict) -> Dict:
    """POST to multi_search with automatic 429 back-off (max 8 tries)."""
    for _try in range(8):
        r = session.post(
            f"{TYPESENSE_URL}/multi_search",
            json=payload,
            timeout=60,
            headers=_TS_HEADERS,
        )
        if r.status_code == 429:
            print("    Rate limited — sleeping 15 s")
            time.sleep(15)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("multi_search: rate limited 8x — aborting")


def _search_page(session: requests.Session, page: int) -> Dict[str, Any]:
    payload = {
        "searches": [{
            "collection": TYPESENSE_COLLECTION,
            "q":          "*",
            "query_by":   "name",
            "filter_by":  ACTIVE_FILTER,
            "sort_by":    "product_id:asc",
            "per_page":   PER_PAGE,
            "page":       page,
        }]
    }
    result = _ts_post(session, payload)["results"][0]
    return {"found": int(result.get("found") or 0), "hits": result.get("hits") or []}


def _fetch_docs(
    session: requests.Session,
    limit: Optional[int] = None,
) -> Generator[Dict, None, None]:
    """Paginate the whole `products` collection (active products only)."""
    first = _search_page(session, 1)
    total = first["found"]
    total_pages = (total + PER_PAGE - 1) // PER_PAGE if total else 0
    print(f"  {total:,} active products  ->  {total_pages} pages")

    seen: Set[str] = set()
    yielded = 0

    def _emit(hits: List[Dict]) -> Generator[Dict, None, None]:
        nonlocal yielded
        for hit in hits:
            doc = hit.get("document") or {}
            pid = str(doc.get("product_id") or "").strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            yield doc
            yielded += 1

    for doc in _emit(first["hits"]):
        yield doc
        if limit and yielded >= limit:
            return

    for page in range(2, total_pages + 1):
        data = _search_page(session, page)
        if not data["hits"]:
            break
        for doc in _emit(data["hits"]):
            yield doc
            if limit and yielded >= limit:
                return
        if page % 10 == 0 or page == total_pages:
            print(f"  [page {page:>3}/{total_pages}]  unique products so far: {yielded:,}")
        time.sleep(DELAY)


# ──────────────────────────────────────────────────────────────────────────────
# Document normalization
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _category_path(doc: Dict) -> str:
    cats = doc.get("categories") or []
    names: List[str] = []
    for c in cats:
        name = str((c or {}).get("name") or "").strip()
        if name and name not in names:
            names.append(name)
    return " > ".join(names)


def _standardize_doc(doc: Dict) -> Optional[Dict]:
    name = str(doc.get("name") or "").strip()
    product_id = str(doc.get("product_id") or "").strip()
    if not name or not product_id:
        return None

    regular_price = _to_float(doc.get("price"))
    sale_price    = _to_float(doc.get("sale_price"))

    if regular_price is None or regular_price <= 0:
        # fall back to sale_price if the regular field is missing
        if sale_price and sale_price > 0:
            regular_price = sale_price
            sale_price = None
        else:
            return None

    # promo only when it's a genuine lower price
    if sale_price is not None and 0 < sale_price < regular_price:
        promo_price = sale_price
    else:
        promo_price = None

    discount_pct = _to_float(doc.get("discount_pct"))
    if promo_price is None:
        discount_pct = None
    elif not discount_pct:
        discount_pct = round((1 - promo_price / regular_price) * 100, 1)

    image = doc.get("image") or {}
    image_url = ""
    if isinstance(image, dict):
        image_url = str(image.get("original") or image.get("thumbnail") or "").strip()

    slug   = str(doc.get("slug") or "").strip()
    sku_id = str(doc.get("id") or "").strip()
    product_url = f"{BASE_URL}/{slug}/product/{sku_id}/" if slug and sku_id else ""

    return {
        "product_id":    product_id,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(doc.get("brand") or "").strip(),
        "category_path": _category_path(doc),
        "ean":           str(doc.get("barcode") or "").strip(),
        "regular_price": regular_price,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          str(doc.get("packaging") or "").strip(),
        "is_available":  bool(doc.get("in_stock", True)),
        "stock":         None,
        "offer_tag":     str(doc.get("sku") or "").strip(),
        "is_discounted": promo_price is not None,
        "prescription":  bool(doc.get("requires_prescription", False)),
        "product_url":   product_url,
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None, workers: int = WORKERS) -> Dict:
    """
    Paginate the entire Typesense `products` collection (active only),
    standardize each document, and upsert in batches.
    Returns cumulative stats dict.
    """
    session = _make_session()

    total_upserted = total_history = total_skipped = 0
    total_saved = 0
    BATCH_SIZE = 200
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
        print(
            f"    -> saved {stats['upserted']} | "
            f"price changes {stats['history_inserted']} | "
            f"cumul {total_saved}"
        )

    print("Paging Qualidoc products via Typesense...")
    processed = 0
    for doc in _fetch_docs(session, limit=limit):
        offer = _standardize_doc(doc)
        if not offer:
            continue

        batch.append(offer)
        processed += 1

        if processed % 500 == 0:
            print(f"  [{processed:>6}]  buffered={len(batch)}  saved={total_saved}")

        if len(batch) >= BATCH_SIZE:
            _flush()
            batch.clear()

    _flush()
    batch.clear()

    print(f"\nFinished: {processed:,} unique products processed.")
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
        description="Scrape Qualidoc -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--csv",     action="store_true",       help="Also export a CSV file after scrape")
    parser.add_argument("--output",  type=str, default=None,    help="CSV path (implies --csv)")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import QualidocDB, load_env
    load_env(args.env)

    db    = QualidocDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = QualidocDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
