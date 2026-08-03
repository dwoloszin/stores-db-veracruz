"""
scraper_ultrafarma.py — Scraper for Ultrafarma (https://www.ultrafarma.com.br)

Platform  : LeanCommerce (Leanwork Tecnologia LTDA)
Data      : Products pre-rendered as data-product-* HTML attributes in category pages.
            Angular app loads additional data (old prices) dynamically — NOT scraped here.
Categories: 414 leaf categories parsed from homepage navigation.
Pagination: ?pg=N, 12 products per page. Stop when page returns 0 products.
EAN       : Available on product detail pages as <b>EAN: </b>XXXXXXXX.
            Run enrich_ean_ultrafarma.py after scraping to populate EAN column.
Prices    : Only current selling price (data-product-price) available from listing.
            Regular/list prices (PrecoDe) require per-product page fetch (enrichment).

Usage:
    python -m markets.ultrafarma.scraper_ultrafarma              # scrape -> DB
    python -m markets.ultrafarma.scraper_ultrafarma --limit 500  # test run -> DB
    python -m markets.ultrafarma.scraper_ultrafarma --csv        # scrape -> DB + CSV
"""

import csv
import re
import sys
import time
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.ultrafarma.com.br"
STORE_ID  = "ultrafarma"
PAGE_SIZE = 12    # products per category page
DELAY     = 0.4   # seconds between requests

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
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "sec-fetch-dest":  "document",
        "sec-fetch-mode":  "navigate",
        "sec-fetch-site":  "none",
    })
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Category tree
# ──────────────────────────────────────────────────────────────────────────────

def _parse_category_links(html: str) -> List[Dict]:
    raw_links = re.findall(r'href="(/categoria/[^"?#]+)"', html)
    seen: set = set()
    categories: List[Dict] = []
    for link in raw_links:
        link = link.rstrip("/")
        if link in seen:
            continue
        seen.add(link)
        parts = link.split("/")
        if len(parts) - 2 < 2:   # skip top-level /categoria/parent
            continue
        slug_parts = parts[2:]
        categories.append({
            "url_path":  link,
            "name":      slug_parts[-1].replace("-", " ").title(),
            "full_path": " > ".join(s.replace("-", " ").title() for s in slug_parts),
        })
    return categories


def _fetch_categories_from_sitemap(session: requests.Session) -> List[Dict]:
    """Fallback: extract /categoria/ paths from the XML sitemap."""
    import xml.etree.ElementTree as ET
    _NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    for url in (f"{BASE_URL}/sitemap.xml", f"{BASE_URL}/sitemap_index.xml"):
        try:
            r = session.get(url, timeout=25)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
            locs = [loc.text.strip() for loc in root.findall(".//sm:loc", _NS) if loc.text]
            links = []
            for loc in locs:
                path = loc.replace(BASE_URL, "")
                if "/categoria/" in path:
                    links.append(f'href="{path}"')
            if links:
                return _parse_category_links(" ".join(links))
        except Exception:
            continue
    return []


def fetch_category_tree(session: requests.Session) -> List[Dict]:
    """
    Build a flat list of leaf categories from the homepage navigation.
    Falls back to the XML sitemap if the homepage returns a non-200 status.
    """
    r = session.get(BASE_URL, timeout=25)
    if r.status_code == 200:
        categories = _parse_category_links(r.text)
        if categories:
            return categories

    print(f"  WARNING: homepage returned HTTP {r.status_code} — trying sitemap fallback.")
    categories = _fetch_categories_from_sitemap(session)
    if categories:
        print(f"  Sitemap fallback: found {len(categories)} categories.")
        return categories

    r.raise_for_status()   # re-raise original error if both sources failed
    return []


# ──────────────────────────────────────────────────────────────────────────────
# Page fetcher
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_category_page(
    session:  requests.Session,
    url_path: str,
    page_num: int,
    attempt:  int = 0,
) -> List[Dict]:
    """
    Fetches one category page and returns a list of raw product dicts.
    Returns [] on error or when the page has no products.
    """
    try:
        r = session.get(
            f"{BASE_URL}{url_path}",
            params={"pg": page_num},
            timeout=45,
        )
    except requests.exceptions.Timeout:
        if attempt >= 5:
            return []
        print(f"    Timeout on {url_path}?pg={page_num} — retrying in 15s")
        time.sleep(15)
        return _fetch_category_page(session, url_path, page_num, attempt + 1)
    except requests.exceptions.ConnectionError:
        if attempt >= 5:
            return []
        print(f"    Connection error on {url_path}?pg={page_num} — retrying in 20s")
        time.sleep(20)
        return _fetch_category_page(session, url_path, page_num, attempt + 1)

    if r.status_code == 429:
        if attempt >= 5:
            print("    Rate limited 5x — giving up on this page")
            return []
        print("    Rate limited — sleeping 15s")
        time.sleep(15)
        return _fetch_category_page(session, url_path, page_num, attempt + 1)

    if r.status_code not in (200, 206):
        print(f"    HTTP {r.status_code} for {url_path}?pg={page_num}")
        return []

    html = r.text

    # Extract product data attributes
    product_ids   = re.findall(r'data-product-id="([^"]+)"',       html)
    product_names = re.findall(r'data-product-name="([^"]+)"',     html)
    product_cats  = re.findall(r'data-product-category="([^"]+)"', html)
    product_brands= re.findall(r'data-product-brand="([^"]+)"',    html)
    product_prices= re.findall(r'data-product-price="([^"]+)"',    html)
    product_hrefs = re.findall(r'href="(/[^"]+)" ng-click="pdpItem', html)

    # Product images: inside div.product-image — the main product img has a title= attribute.
    # Badge/seal imgs inside the same div do NOT have title=.
    product_imgs = re.findall(
        r'class="product-image[^"]*"[^>]*>.*?<img[^>]+title="[^"]*"[^>]+src="([^"]+)"'
        r'|class="product-image[^"]*"[^>]*>.*?<img[^>]+src="([^"]+)"[^>]+title="[^"]*"',
        html, re.S
    )
    # Flatten tuples from alternation groups
    product_imgs = [g1 or g2 for g1, g2 in product_imgs]

    count = min(len(product_ids), len(product_names), len(product_prices), len(product_hrefs))
    if count == 0:
        return []

    results: List[Dict] = []
    for i in range(count):
        results.append({
            "product_id":    unescape(product_ids[i]    if i < len(product_ids)    else ""),
            "product_name":  unescape(product_names[i]  if i < len(product_names)  else ""),
            "category_path": unescape(product_cats[i]   if i < len(product_cats)   else ""),
            "brand":         unescape(product_brands[i] if i < len(product_brands) else ""),
            "price":         product_prices[i]           if i < len(product_prices) else "",
            "url_slug":      product_hrefs[i]            if i < len(product_hrefs)  else "",
            "image_url":     product_imgs[i]             if i < len(product_imgs)   else "",
        })

    return results


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


def _standardize(raw: Dict, cat_fallback: str) -> Optional[Dict]:
    pid  = str(raw.get("product_id", "")).strip()
    name = str(raw.get("product_name", "")).strip()
    if not pid or not name:
        return None

    price = _to_float(raw.get("price"))
    if price is None or price <= 0:
        return None

    url_slug = str(raw.get("url_slug", "")).strip()
    cat_path = str(raw.get("category_path", "")).strip() or cat_fallback

    # Normalize category separator: site uses " > ", keep as-is
    image_url = str(raw.get("image_url", "")).strip()
    if image_url and not image_url.startswith("http"):
        image_url = BASE_URL + image_url

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(raw.get("brand", "")).strip(),
        "category_path": cat_path,
        "ean":           "",           # enriched later via enrich_ean_ultrafarma.py
        "regular_price": price,        # current selling price (may be promotional)
        "promo_price":   None,
        "discount_pct":  None,
        "unit":          "",
        "is_available":  True,         # listed = available
        "stock":         None,
        "offer_tag":     "",
        "product_url":   f"{BASE_URL}{url_slug}" if url_slug else "",
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

    session   = _make_session()
    seen_pids: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0

    print("Fetching category tree from homepage ...")
    categories = fetch_category_tree(session)
    print(f"Categories to scrape: {len(categories)}")

    for cat in categories:
        url_path  = cat["url_path"]
        cat_label = cat["full_path"]
        page_num  = 1
        cat_offers: List[Dict] = []

        while True:
            page = _fetch_category_page(session, url_path, page_num)

            if not page:
                break

            new_this_page = 0
            for raw in page:
                pid = raw.get("product_id", "").strip()
                if not pid or pid in seen_pids:
                    continue
                seen_pids.add(pid)
                offer = _standardize(raw, cat_label)
                if offer:
                    cat_offers.append(offer)
                    new_this_page += 1

            if new_this_page > 0 or page_num == 1:
                print(
                    f"  {cat_label[:50]:<50}  p={page_num:>3}  "
                    f"got={len(page)}  new={new_this_page}  "
                    f"saved={total_saved}"
                )

            if len(page) < PAGE_SIZE:
                break

            page_num += 1
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
        description="Scrape Ultrafarma -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",      type=int, default=None,   help="Stop after N products (test)")
    parser.add_argument("--csv",        action="store_true",      help="Also export a CSV file after scrape")
    parser.add_argument("--output",     type=str, default=None,   help="CSV path (implies --csv)")
    parser.add_argument("--enrich-ean", action="store_true",      help="Fetch EAN from product pages after scraping")
    parser.add_argument("--workers",    type=int, default=12,     help="Parallel threads for EAN enrichment (default: 12)")
    parser.add_argument("--env",        type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import UltrafarmDB, load_env
    load_env(args.env)

    db    = UltrafarmDB()
    stats = scrape(db, limit=args.limit)

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped: {stats['skipped_zero']:,}")

    if args.enrich_ean:
        print("\nFetching EAN from product pages...")
        from markets.ultrafarma.enrich_ean_ultrafarma import enrich
        enrich(workers=args.workers, db=db)

    db.close()

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = UltrafarmDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
