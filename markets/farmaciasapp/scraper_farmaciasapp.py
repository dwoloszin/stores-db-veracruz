"""
scraper_farmaciasapp.py — Scraper for Farmácias App (https://www.farmaciasapp.com.br)

Platform   : Next.js storefront backed by a public Typesense search API.
Data source : POST https://search-lb.main.mkplace.com.br/multi_search
                         The search documents already include:
                             name            → product_name
                             slug            → product_id + product_url
                             ean             → ean / barcode
                             product.brand   → brand
                             thumbnail       → image_url
                             offer.price     → promo_price / current price
                             offer.originalPrice → regular_price / list price
                             offer.isAvailable → is_available
EAN        : First-class API field, no enrichment step needed.

Usage:
        python -m markets.farmaciasapp.scraper_farmaciasapp              # scrape -> DB
        python -m markets.farmaciasapp.scraper_farmaciasapp --limit 500  # test run -> DB
        python -m markets.farmaciasapp.scraper_farmaciasapp --csv        # scrape -> DB + CSV
"""

import csv
import math
import re
import sys
import time
from datetime import datetime
import unicodedata
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import requests
import xml.etree.ElementTree as ET

sys.stdout.reconfigure(line_buffering=True)

BASE_URL = "https://www.farmaciasapp.com.br"
STORE_ID = "farmaciasapp"
TYPESENSE_URL = "https://search-secondary.main.mkplace.com.br"
TYPESENSE_API_KEY = "9sUhPk3OEt7l3KJghC2YlaYF3zXw5kUD"
TYPESENSE_COLLECTION = "col-V-kS_pcI8C-V-kS_pcI8C-search"
PER_PAGE            = 250
MULTI_SEARCH_BATCH  = 8     # page-queries bundled into one multi_search HTTP call
DELAY               = 0.05  # seconds between multi_search calls
WORKERS             = 8     # kept for CLI compatibility

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
        "Accept":          "application/json,text/plain,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Typesense search
# ──────────────────────────────────────────────────────────────────────────────

_XML_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

def _typesense_search(
    session: requests.Session,
    *,
    department: str,
    page: int,
    attempt: int = 0,
) -> Dict[str, Any]:
    payload = {
        "searches": [{
            "q": "*",
            "query_by": "*",
            "per_page": PER_PAGE,
            "page": page,
            "filter_by": f"department:={department}",
            "collection": TYPESENSE_COLLECTION,
        }]
    }

    r = session.post(
        f"{TYPESENSE_URL}/multi_search",
        json=payload,
        timeout=45,
        headers={
            "Content-Type": "application/json",
            "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
        },
    )
    if r.status_code == 429:
        if attempt >= 5:
            print(f"    Rate limited 5x on {department} — giving up")
            return {"found": 0, "hits": []}
        print(f"    Rate limited on {department} — sleeping 10s")
        time.sleep(10)
        return _typesense_search(session, department=department, page=page, attempt=attempt + 1)
    r.raise_for_status()

    result = r.json()["results"][0]
    return {"found": int(result.get("found") or 0), "hits": result.get("hits") or []}


def _fetch_departments(session: requests.Session) -> List[str]:
    payload = {
        "searches": [{
            "q": "*",
            "query_by": "*",
            "per_page": 0,
            "page": 1,
            "facet_by": "department",
            "max_facet_values": 50,
            "collection": TYPESENSE_COLLECTION,
        }]
    }
    r = session.post(
        f"{TYPESENSE_URL}/multi_search",
        json=payload,
        timeout=45,
        headers={
            "Content-Type": "application/json",
            "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
        },
    )
    r.raise_for_status()
    facets = r.json()["results"][0].get("facet_counts") or []
    if not facets:
        return []

    departments: List[str] = []
    for facet in facets:
        if facet.get("field_name") != "department":
            continue
        for row in facet.get("counts") or []:
            value = str(row.get("value") or "").strip()
            if value and value[0].isupper():
                departments.append(value)
        break
    return departments


def _fetch_xml(session: requests.Session, url: str) -> Optional[ET.Element]:
    for attempt in range(2):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                print("    Rate limited (sitemap) — sleeping 10s")
                time.sleep(10)
                continue
            if r.status_code != 200:
                return None
            return ET.fromstring(r.content)
        except requests.exceptions.RequestException:
            if attempt == 0:
                time.sleep(5)
        except ET.ParseError:
            return None
    return None


def fetch_product_slugs(session: requests.Session) -> List[Tuple[str, str]]:
    """Walk the sitemap index and return deduplicated (slug, category_label)."""
    index = _fetch_xml(session, f"{BASE_URL}/api/sitemap/index.xml")
    if index is None:
        print("ERROR: Could not fetch sitemap index.")
        return []

    sitemap_urls = [
        loc.text.strip()
        for loc in index.findall(".//sm:loc", _XML_NS)
        if loc.text and "/category/" in loc.text
    ]
    print(f"  Found {len(sitemap_urls)} category sitemaps in index.")

    seen: Set[str] = set()
    results: List[Tuple[str, str]] = []

    for sitemap_url in sitemap_urls:
        m = re.search(r"/category/([^/]+)/\d+", sitemap_url)
        cat_label = m.group(1).replace("-", " ").title() if m else "Misc"

        page = 1
        while True:
            paged_url = re.sub(r"/\d+\.xml$", f"/{page}.xml", sitemap_url)
            xml = _fetch_xml(session, paged_url)
            if xml is None:
                break

            locs = [
                loc.text.strip()
                for loc in xml.findall(".//sm:loc", _XML_NS)
                if loc.text
            ]
            if not locs:
                break

            new = 0
            for product_url in locs:
                slug = product_url.rstrip("/").rsplit("/", 1)[-1]
                if slug and slug not in seen:
                    seen.add(slug)
                    results.append((slug, cat_label))
                    new += 1

            print(
                f"  {cat_label[:38]:<38} p{page}: "
                f"{len(locs)} urls, {new} new  (unique total: {len(results):,})"
            )
            time.sleep(DELAY)

            if len(locs) < 1000:
                break
            page += 1

    return results


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


def _slugify(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")


def _standardize_doc(doc: Dict) -> Optional[Dict]:
    slug = str(doc.get("slug") or "").strip()
    name = str(doc.get("name") or "").strip()
    if not name:
        return None
    if not slug:
        slug = _slugify(name)

    offer = doc.get("offer") or {}
    price = _to_float(offer.get("price"))
    original_price = _to_float(offer.get("originalPrice"))

    if price is None and original_price is None:
        return None

    if original_price is None and price is not None:
        regular_price = price
        promo_price = None
    elif price is None and original_price is not None:
        regular_price = original_price
        promo_price = None
    else:
        regular_price = max(float(original_price), float(price))
        promo_price = min(float(original_price), float(price)) if float(price) < float(original_price) else None

    if regular_price is None or regular_price <= 0:
        return None

    discount_pct = (
        round((1 - promo_price / regular_price) * 100, 1)
        if promo_price and regular_price > 0
        else None
    )

    product_info = doc.get("product") or {}
    brand = str(product_info.get("brand") or "").strip()
    if not brand:
        brand = str((doc.get("metadata") or {}).get("brandSlug") or "").strip()

    department = str(doc.get("department") or "").strip()
    category = str(doc.get("category") or "").strip()
    sub_category = str(doc.get("subCategory") or "").strip()
    category_path = " > ".join(part for part in [department, category, sub_category] if part)

    thumbnail = doc.get("thumbnail") or {}
    image_url = ""
    if isinstance(thumbnail, dict):
        image_url = str(thumbnail.get("default") or thumbnail.get("secondary") or "").strip()
    if not image_url:
        images = doc.get("images") or []
        if images:
            image_url = str(images[0]).strip()

    product_url = f"{BASE_URL}/{slug}"
    sku_id = str(doc.get("skuId") or doc.get("productId") or doc.get("id") or "").strip()
    stock_balance = offer.get("stockBalance")

    return {
        "product_id":    slug,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         brand,
        "category_path": category_path,
        "ean":           str(doc.get("ean") or "").strip(),
        "regular_price": regular_price,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        # Marketplace offer: only "available" when the seller flags it AND real
        # stock is present. Default the flag to False (not True) so a doc missing
        # the field is never assumed in-stock. Note: this reflects marketplace-wide
        # availability, not per-CEP deliverability (see _fetch_docs_by_departments).
        "is_available":  bool(offer.get("isAvailable", False))
                         and (stock_balance is None or (stock_balance or 0) > 0),
        "stock":         stock_balance,
        "offer_tag":     sku_id,
        "product_url":   product_url,
        "image_url":     image_url,
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _lookup_slug(session: requests.Session, slug: str) -> Optional[Dict]:
    def _query(payload: Dict[str, Any], attempt: int = 0) -> List[Dict]:
        r = session.post(
            f"{TYPESENSE_URL}/multi_search",
            json=payload,
            timeout=45,
            headers={
                "Content-Type": "application/json",
                "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
            },
        )
        if r.status_code == 429:
            if attempt >= 5:
                return []
            print(f"    Rate limited on {slug} — sleeping 10s")
            time.sleep(10)
            return _query(payload, attempt + 1)
        r.raise_for_status()
        return r.json()["results"][0].get("hits") or []

    exact_payload = {
        "searches": [{
            "q": "*",
            "query_by": "*",
            "per_page": 1,
            "page": 1,
            "filter_by": f"slug:={slug}",
            "collection": TYPESENSE_COLLECTION,
        }]
    }
    hits = _query(exact_payload)
    if hits:
        return hits[0].get("document") or None

    fallback_payload = {
        "searches": [{
            "q": slug.replace("-", " "),
            "query_by": "name,slug",
            "per_page": 20,
            "page": 1,
            "collection": TYPESENSE_COLLECTION,
        }]
    }
    for hit in _query(fallback_payload):
        doc = hit.get("document") or {}
        if str(doc.get("slug") or "").strip() == slug:
            return doc
        if _slugify(str(doc.get("name") or "")) == slug:
            return doc
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

_TS_HEADERS = {
    "Content-Type":       "application/json",
    "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
}


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


def _fetch_docs_by_departments(
    session: requests.Session,
    departments: List[str],
    limit: Optional[int] = None,
) -> Generator[Dict, None, None]:
    """
    Bulk-paginate every department using server-side deduplication:
      group_by=deduplicator + group_limit=1 + sort_by=offer.price:asc
    → one cheapest offer per unique product per request.

    Batches MULTI_SEARCH_BATCH page-queries per HTTP call to minimise
    total request count (~44 calls for the full FarmaciasApp catalogue
    vs ~4,773 calls in the legacy per-slug approach).
    """
    # ── Step 1: count unique products per dept (single multi_search call) ─────
    count_searches = [
        {
            "q": "*", "query_by": "*",
            "per_page": 0, "page": 1,
            "filter_by":  f"department:={dept}",
            "group_by":   "deduplicator",
            "group_limit": 1,
            "collection": TYPESENSE_COLLECTION,
        }
        for dept in departments
    ]
    data = _ts_post(session, {"searches": count_searches})
    dept_pages: List[Tuple[str, int]] = []
    for dept, result in zip(departments, data["results"]):
        unique_products = int(result.get("found") or 0)
        # Typesense caps grouped per_page at 100; use 100 for page math
        pages = math.ceil(unique_products / 100) if unique_products > 0 else 0
        dept_pages.append((dept, pages))
        print(f"  {dept:<28}  {unique_products:>7,} unique products  ->  {pages} pages")

    total_pages = sum(p for _, p in dept_pages)
    total_calls = math.ceil(total_pages / MULTI_SEARCH_BATCH)
    print(f"  Total pages: {total_pages}  |  multi_search calls needed: {total_calls}")

    # ── Step 2: flat list of all (dept, page) jobs ────────────────────────────
    jobs: List[Tuple[str, int]] = [
        (dept, pg)
        for dept, pages in dept_pages
        for pg in range(1, pages + 1)
    ]

    seen_slugs: Set[str] = set()
    yielded = 0
    batches_done = 0
    total_batches = math.ceil(len(jobs) / MULTI_SEARCH_BATCH)

    # ── Step 3: fetch in MULTI_SEARCH_BATCH-sized HTTP calls ──────────────────
    for i in range(0, len(jobs), MULTI_SEARCH_BATCH):
        chunk = jobs[i : i + MULTI_SEARCH_BATCH]
        searches = [
            {
                "q": "*", "query_by": "*",
                "per_page":    100,      # grouped per_page cap
                "page":        pg,
                "filter_by":   f"department:={dept}",
                "group_by":    "deduplicator",
                "group_limit": 1,
                "sort_by":     "offer.price:asc",
                "collection":  TYPESENSE_COLLECTION,
            }
            for dept, pg in chunk
        ]
        data = _ts_post(session, {"searches": searches})
        batches_done += 1

        for result in data["results"]:
            for group in result.get("grouped_hits") or []:
                hits = group.get("hits") or []
                if not hits:
                    continue
                doc = hits[0].get("document") or {}
                slug = str(doc.get("slug") or "").strip()
                if not slug or slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                yield doc
                yielded += 1
                if limit and yielded >= limit:
                    return

        if batches_done % 10 == 0 or batches_done == total_batches:
            print(
                f"  [{batches_done:>4}/{total_batches} calls]  "
                f"unique products so far: {yielded:,}"
            )
        time.sleep(DELAY)


def scrape(db, limit: Optional[int] = None, workers: int = WORKERS) -> Dict:
    """
    Bulk-paginate all capitalized departments from the Typesense index.
    Batches MULTI_SEARCH_BATCH page-queries per HTTP call (fast, low request count).
    Deduplicates by slug in-memory; keeps the cheapest offer per product.
    Skips product_ids already present in DB (resume-safe).
    Returns cumulative stats dict.
    """
    seen_ids: Set[str] = set()

    total_upserted = total_history = total_skipped = 0
    session = _make_session()

    print("Fetching department list from Typesense facets...")
    departments = _fetch_departments(session)
    if not departments:
        print("ERROR: No departments returned — aborting.")
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}
    print(f"Departments: {departments}\n")

    BATCH_SIZE = 200
    batch: List[Dict] = []
    total_saved = 0

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

    print("Bulk-paging departments via Typesense (multi_search batched)...")
    processed = 0
    for doc in _fetch_docs_by_departments(session, departments, limit=None):
        slug = str(doc.get("slug") or "").strip()
        if slug in seen_ids:
            continue

        offer = _standardize_doc(doc)
        if not offer:
            continue

        batch.append(offer)
        processed += 1

        if processed % 500 == 0:
            print(f"  [{processed:>6}]  buffered={len(batch)}  saved={total_saved}")

        if limit and processed >= limit:
            break

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
        description="Scrape Farmácias App -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--csv",     action="store_true",       help="Also export a CSV file after scrape")
    parser.add_argument("--output",  type=str, default=None,    help="CSV path (implies --csv)")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import FarmaciasAppDB, load_env
    load_env(args.env)

    db    = FarmaciasAppDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
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
