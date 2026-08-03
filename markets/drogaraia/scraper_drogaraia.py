"""
scraper_drogaraia.py — Scraper for Drogaria Raia (https://www.drogaraia.com.br)

Platform : Next.js frontend + Magento 2 backend + Algolia search
Auth     : Googlebot User-Agent bypasses the Cloudflare WAF on page routes.
           Direct API calls (api/v2/...) are blocked; we fetch SSR HTML instead.
Data     : Extracted from __NEXT_DATA__ JSON embedded in each HTML page.
Pages    : /catalog/category/view/id/[id]?p=N  (45 products/page, Algolia 2000 cap)
Prices   : Only current selling price (priceService) available from listing pages.
           No EAN/barcode in category listings (would require per-product page fetch).
Pagination: ?p=N  (stops when products list is empty or p > 45 = 2000-product cap)

Usage:
    python -m markets.drogaraia.scraper_drogaraia                        # scrape -> DB
    python -m markets.drogaraia.scraper_drogaraia --limit 200            # test run -> DB
    python -m markets.drogaraia.scraper_drogaraia --csv                  # scrape -> DB + CSV
"""

import csv
import json
import re
import sys
import time
from datetime import datetime   
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.drogaraia.com.br"
STORE_ID   = "drogaraia"
PAGE_SIZE  = 48    # products per SSR page (Algolia default for Raia/Drogasil)
MAX_PAGES  = 50    # safety ceiling: 50 x 48 = 2400 (Algolia caps at ~2000 per query)
DELAY      = 0.35  # seconds between page requests (HTML is heavier than JSON)
RETRY_MAX  = 5
CATEGORY_CACHE_FILE = Path(__file__).with_name("drogaraia_category_tree_cache.json")

# Googlebot UA is allowed through the Cloudflare WAF on page routes
GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"


# ──────────────────────────────────────────────────────────────────────────────
# Session
# ──────────────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      GOOGLEBOT_UA,
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


def _http_get_with_retry(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    label: str = "request",
    retry_max: int = RETRY_MAX,
) -> requests.Response:
    """
    GET with retries for transient blocking / network errors.
    Retries: 403, 429, 5xx and requests RequestException.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, retry_max + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)

            # Akamai/edge access-denied pages should fail fast with a clear cause.
            denied = _extract_access_denied_reference(r)
            if denied:
                raise RuntimeError(
                    f"{label}: blocked by Akamai Access Denied "
                    f"(reference {denied}). Retry later or from another IP/network."
                )

            # Not-found is a valid terminal case for category pages.
            if r.status_code == 404:
                return r

            if r.status_code in (403, 429) or 500 <= r.status_code < 600:
                if attempt < retry_max:
                    sleep_s = min(5 * attempt, 20)
                    print(f"    {label}: HTTP {r.status_code} (attempt {attempt}/{retry_max}) — retry in {sleep_s}s")
                    time.sleep(sleep_s)
                    continue
            return r
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < retry_max:
                sleep_s = min(5 * attempt, 20)
                print(f"    {label}: network error on attempt {attempt}/{retry_max} ({type(exc).__name__}) — retry in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            raise

    # Defensive fallback if loop exits unexpectedly.
    if last_exc:
        raise last_exc
    raise RuntimeError(f"{label}: request failed without response")


def _extract_access_denied_reference(response: requests.Response) -> Optional[str]:
    """
    Detects Akamai Access Denied pages and extracts their reference token.
    Works for both HTTP 403 and edge pages that might render in 200 responses.
    """
    text = response.text or ""
    lower = text.lower()
    if "access denied" not in lower and "errors.edgesuite.net" not in lower:
        return None

    m = re.search(r"Reference\s*#\s*([A-Za-z0-9\.\-]+)", text)
    if m:
        return m.group(1)
    return "unknown"


def _save_category_cache(nodes: List[Dict]) -> None:
    try:
        with CATEGORY_CACHE_FILE.open("w", encoding="utf-8") as f:
            json.dump(nodes, f, ensure_ascii=False)
    except Exception:
        # Non-fatal: scraping should continue even if cache write fails.
        pass


def _load_category_cache() -> List[Dict]:
    try:
        if not CATEGORY_CACHE_FILE.exists():
            return []
        with CATEGORY_CACHE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict) and x.get("id")]
    except Exception:
        return []
    return []


# ──────────────────────────────────────────────────────────────────────────────
# Category tree
# ──────────────────────────────────────────────────────────────────────────────

def fetch_category_tree(session: requests.Session) -> List[Dict]:
    """
    Fetches /categorias and returns a flat list of ALL categories,
    each with: id (int), name, full_path (breadcrumb), url_path, is_leaf.
    IDs are decoded from VTEX-style base64 UIDs.
    """
    import base64

    r = _http_get_with_retry(
        session,
        f"{BASE_URL}/categorias",
        timeout=25,
        label="/categorias",
        retry_max=6,
    )
    r.raise_for_status()
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
        r.text, re.S,
    )
    if not m:
        raise RuntimeError("__NEXT_DATA__ not found on /categorias")

    raw_cats = json.loads(m.group(1))["props"]["pageProps"]["categories"]

    def _decode_uid(uid: str) -> int:
        try:
            return int(base64.b64decode(uid + "==").decode())
        except Exception:
            return 0

    nodes: List[Dict] = []

    def _walk(cat_list: List[Dict], parent_path: str = "") -> None:
        for node in cat_list:
            int_id   = _decode_uid(node.get("uid", ""))
            name     = node.get("name", "")
            url_path = node.get("url_path", node.get("url_key", ""))
            children = node.get("children") or []
            full_path = f"{parent_path}/{name}" if parent_path else name

            nodes.append({
                "id":        int_id,
                "name":      name,
                "full_path": full_path,
                "url_path":  url_path,
                "is_leaf":   len(children) == 0,
            })
            if children:
                _walk(children, full_path)

    _walk(raw_cats)
    _save_category_cache(nodes)
    return nodes


# ──────────────────────────────────────────────────────────────────────────────
# Page fetcher
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_category_page(
    session:  requests.Session,
    cat_id:   int,
    page_num: int,
) -> Tuple[List[Dict], int]:
    """
    Returns (products, total_count).
    Products is a list of raw product dicts from Algolia/Magento.
    total_count is 0 if no data is found.
    """
    url = f"{BASE_URL}/catalog/category/view/id/{cat_id}"
    r = _http_get_with_retry(
        session,
        url,
        params={"p": page_num},
        timeout=30,
        label=f"cat={cat_id} p={page_num}",
    )

    if r.status_code not in (200, 404):
        print(f"    HTTP {r.status_code} for cat={cat_id} p={page_num}")
        return [], 0

    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
        r.text, re.S,
    )
    if not m:
        return [], 0

    try:
        data     = json.loads(m.group(1))
        pp       = data["props"]["pageProps"]
        inner    = pp.get("pageProps") or {}
        results  = inner.get("results") or {}
        products = results.get("products") or []
        meta     = inner.get("metadata") or {}
        total    = int(meta.get("totalCount") or 0)
        return products, total
    except (KeyError, ValueError, json.JSONDecodeError):
        return [], 0


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _standardize(raw: Dict) -> Optional[Dict]:
    sku = str(raw.get("sku") or raw.get("objectID") or "").strip()
    if not sku:
        return None

    name = str(raw.get("name") or "").strip()
    if not name:
        return None

    price = raw.get("priceService")
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    if price is None or price <= 0:
        return None

    # Category breadcrumb: use deepest hierarchical level available
    hc = raw.get("hierarchicalCategories") or {}
    cat_path = (
        hc.get("lvl2") or hc.get("lvl1") or hc.get("lvl0")
        or raw.get("productGroup", "")
    )

    # Promo label (first one wins)
    labels   = raw.get("labels") or []
    offer_tag = labels[0].get("text", "") if labels else ""

    # Product URL
    url_slug = str(raw.get("url") or "").lstrip("/")
    product_url = f"{BASE_URL}/{url_slug}" if url_slug else ""

    image_src = ""
    img = raw.get("image") or {}
    if isinstance(img, dict):
        image_src = img.get("src", "")

    return {
        "product_id":     sku,
        "store_id":       STORE_ID,
        "product_name":   name,
        "brand":          str(raw.get("brand") or "").strip(),
        "category_path":  cat_path,
        "ean":            "",          # not available from listing pages
        "price":          price,
        "offer_tag":      offer_tag,
        "is_discounted":  int(raw.get("stripeCode") or 0) > 0,
        "is_generic":     bool(raw.get("isGeneric", False)),
        "prescription":   bool(raw.get("prescription", False)),
        "is_available":   True,        # listed = available
        "unit":           str(raw.get("amount") or "").strip(),
        "product_url":    product_url,
        "image_url":      image_src,
        "scraped_at":     datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
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
    seen_skus: set = set()
    total_saved = total_upserted = total_history = total_skipped = 0

    print("Fetching category tree from /categorias ...")
    try:
        all_nodes = fetch_category_tree(session)
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status in (403, 429, 500, 502, 503, 504):
            cached_nodes = _load_category_cache()
            if cached_nodes:
                print(f"  WARNING: /categorias returned HTTP {status}. Using cached category tree ({len(cached_nodes)} nodes).")
                all_nodes = cached_nodes
            else:
                print(f"  ERROR: /categorias returned HTTP {status} and no local cache is available.")
                raise
        else:
            raise

    leaves    = [n for n in all_nodes if n["is_leaf"]]
    print(f"Found {len(all_nodes)} total categories, {len(leaves)} leaves to scrape.")

    for cat in leaves:
        cat_id    = cat["id"]
        cat_label = cat["full_path"]
        if not cat_id:
            continue

        page_num   = 1
        cat_total  = None
        cat_offers: List[Dict] = []

        while True:
            if page_num > MAX_PAGES:
                if cat_total and cat_total > PAGE_SIZE * MAX_PAGES:
                    print(f"  WARNING: {cat_label[:50]} has {cat_total} products, "
                          f"capped at {PAGE_SIZE * MAX_PAGES} (Algolia limit)")
                break

            page, total = _fetch_category_page(session, cat_id, page_num)
            if cat_total is None and total:
                cat_total = total
            if not page:
                break

            new_this_page = 0
            for raw in page:
                sku = str(raw.get("sku") or raw.get("objectID") or "").strip()
                if not sku or sku in seen_skus:
                    continue
                seen_skus.add(sku)
                offer = _standardize(raw)
                if offer:
                    cat_offers.append(offer)
                    new_this_page += 1

            if new_this_page > 0 or page_num == 1:
                print(
                    f"  {cat_label[:50]:<50}  p={page_num:>3}  "
                    f"got={len(page)}  new={new_this_page}  "
                    f"cat_total={cat_total or '?':>6}  "
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
            stats = db.save(cat_offers, verbose=False, preserve_promo=True)
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
    "ean", "price", "offer_tag", "is_discounted",
    "is_generic", "prescription", "is_available", "unit",
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
        description="Scrape Drogaria Raia -> PostgreSQL (DB is always written; CSV is optional)"
    )
    parser.add_argument("--limit",      type=int, default=None,  help="Stop after N products (test)")
    parser.add_argument("--csv",        action="store_true",     help="Also save a local CSV file")
    parser.add_argument("--output",     type=str, default=None,  help="CSV path (implies --csv)")
    parser.add_argument("--env",        type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import DrogaraiaDB, load_env
    load_env(args.env)

    db    = DrogaraiaDB()
    stats = scrape(db, limit=args.limit)

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped: {stats['skipped_zero']:,}")

    db.close()

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = DrogaraiaDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
