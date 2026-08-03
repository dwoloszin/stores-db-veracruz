"""
scraper_farmaconde.py - Scraper for Farmaconde (https://www.farmaconde.com.br)

Platform: VTEX storefront with public catalog endpoint.
Data source:
    GET /api/catalog_system/pub/products/search?_from=N&_to=M

The endpoint already returns product name, category, product URL, image,
EAN (inside SKU items), and commercial offer prices.

Usage:
    python -m markets.farmaconde.scraper_farmaconde
    python -m markets.farmaconde.scraper_farmaconde --limit 500
    python -m markets.farmaconde.scraper_farmaconde --csv
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL = "https://www.farmaconde.com.br"
STORE_ID = "farmaconde"
SEARCH_ENDPOINT = f"{BASE_URL}/api/catalog_system/pub/products/search"
PAGE_SIZE = 50
DELAY = 0.12
MAX_EMPTY_PAGES = 2

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": BROWSER_UA,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "pt-BR,pt;q=0.9",
        }
    )
    return s


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _clean_path(parts: List[str]) -> str:
    out: List[str] = []
    for p in parts:
        p = str(p or "").strip().strip("/")
        if p:
            out.append(p)
    return " > ".join(out)


def _pick_sku(item: Dict[str, Any]) -> Dict[str, Any]:
    skus = item.get("items") or []
    if not skus:
        return {}

    # Prefer first available SKU if possible.
    for sku in skus:
        sellers = sku.get("sellers") or []
        for seller in sellers:
            offer = seller.get("commertialOffer") or {}
            if (offer.get("AvailableQuantity") or 0) > 0:
                return sku
    return skus[0]


def _pick_offer(sku: Dict[str, Any]) -> Dict[str, Any]:
    sellers = sku.get("sellers") or []
    if not sellers:
        return {}

    # Choose lowest positive price offer.
    best: Optional[Dict[str, Any]] = None
    best_price: Optional[float] = None
    for seller in sellers:
        offer = seller.get("commertialOffer") or {}
        price = _to_float(offer.get("Price"))
        if price is None or price <= 0:
            continue
        if best is None or best_price is None or price < best_price:
            best = offer
            best_price = price
    return best or (sellers[0].get("commertialOffer") or {})


def _standardize(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sku = _pick_sku(raw)
    offer = _pick_offer(sku)

    product_id = str(
        sku.get("itemId")
        or raw.get("productId")
        or raw.get("productReference")
        or ""
    ).strip()
    name = str(raw.get("productName") or "").strip()
    if not product_id or not name:
        return None

    price = _to_float(offer.get("Price"))
    list_price = _to_float(offer.get("ListPrice"))
    if price is None and list_price is None:
        return None

    if list_price is None and price is not None:
        regular_price = price
        promo_price = None
    elif price is None and list_price is not None:
        regular_price = list_price
        promo_price = None
    else:
        regular_price = max(float(list_price), float(price))
        promo_price = min(float(list_price), float(price)) if float(price) < float(list_price) else None

    if regular_price is None or regular_price <= 0:
        return None

    discount_pct = (
        round((1 - promo_price / regular_price) * 100, 1)
        if promo_price and regular_price > 0
        else None
    )

    categories = raw.get("categories") or []
    category_path = _clean_path(categories)
    if not category_path:
        category_path = str(raw.get("categoriesIds") or "").strip("/")

    link_text = str(raw.get("linkText") or "").strip()
    product_url = f"{BASE_URL}/{link_text}/p" if link_text else ""

    images = sku.get("images") or []
    image_url = ""
    if images:
        image_url = str(images[0].get("imageUrl") or images[0].get("imageLabel") or "").strip()

    ean = str(sku.get("ean") or "").strip()
    available_qty = _to_float(offer.get("AvailableQuantity"))
    is_available = bool(available_qty is None or available_qty > 0)

    brand = str(raw.get("brand") or "").strip()
    offer_tag = str(raw.get("clusterHighlights") or "").strip()

    return {
        "product_id": product_id,
        "store_id": STORE_ID,
        "product_name": name,
        "brand": brand,
        "category_path": category_path,
        "ean": ean,
        "regular_price": regular_price,
        "promo_price": promo_price,
        "discount_pct": discount_pct,
        "unit": "",
        "is_available": is_available,
        "stock": int(available_qty) if available_qty is not None else None,
        "offer_tag": offer_tag,
        "product_url": product_url,
        "image_url": image_url,
        "scraped_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _fetch_page(session: requests.Session, from_idx: int, to_idx: int) -> List[Dict[str, Any]]:
    for attempt in range(3):
        try:
            r = session.get(
                SEARCH_ENDPOINT,
                params={"_from": from_idx, "_to": to_idx},
                timeout=35,
            )
            if r.status_code == 429:
                sleep_s = 10 + attempt * 5
                print(f"  Rate limited on range {from_idx}-{to_idx}; sleeping {sleep_s}s")
                time.sleep(sleep_s)
                continue
            if r.status_code not in (200, 206):
                print(f"  HTTP {r.status_code} on range {from_idx}-{to_idx}")
                return []
            data = r.json()
            return data if isinstance(data, list) else []
        except requests.exceptions.RequestException as exc:
            if attempt == 2:
                print(f"  Request error on range {from_idx}-{to_idx}: {exc}")
                return []
            time.sleep(2 + attempt * 2)
        except ValueError:
            return []
    return []


def scrape(db, limit: Optional[int] = None) -> Dict[str, int]:
    seen_ids: set = set()

    session = _make_session()

    total_upserted = 0
    total_history = 0
    total_skipped = 0
    total_saved = 0

    batch: List[Dict[str, Any]] = []
    batch_size = 200

    def _flush() -> None:
        nonlocal total_upserted, total_history, total_skipped, total_saved
        if not batch:
            return
        stats = db.save(batch, verbose=False)
        total_upserted += stats["upserted"]
        total_history += stats["history_inserted"]
        total_skipped += stats["skipped_zero"]
        total_saved += stats["upserted"]
        print(
            f"    -> saved {stats['upserted']} | "
            f"price changes {stats['history_inserted']} | "
            f"cumul {total_saved}"
        )

    idx = 0
    empty_pages = 0
    scanned = 0

    while True:
        from_idx = idx
        to_idx = idx + PAGE_SIZE - 1
        page = _fetch_page(session, from_idx, to_idx)

        if not page:
            empty_pages += 1
            if empty_pages >= MAX_EMPTY_PAGES:
                break
            idx += PAGE_SIZE
            continue

        empty_pages = 0
        scanned += len(page)

        new_this_page = 0
        for raw in page:
            offer = _standardize(raw)
            if not offer:
                continue
            pid = str(offer.get("product_id") or "").strip()
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            batch.append(offer)
            new_this_page += 1

            if limit and total_saved + len(batch) >= limit:
                break

        print(
            f"  range={from_idx:>5}-{to_idx:<5} got={len(page):>3} "
            f"new={new_this_page:>3} buffered={len(batch):>3} saved={total_saved:>5}"
        )

        if len(batch) >= batch_size or (limit and total_saved + len(batch) >= limit):
            _flush()
            batch.clear()

        if limit and total_saved >= limit:
            print(f"Limit {limit} reached - stopping.")
            break

        idx += PAGE_SIZE
        time.sleep(DELAY)

    _flush()
    batch.clear()

    return {
        "upserted": total_upserted,
        "history_inserted": total_history,
        "skipped_zero": total_skipped,
        "total_unique": total_saved,
    }


CSV_FIELDS = [
    "product_id", "store_id", "product_name", "brand", "category_path",
    "ean", "regular_price", "promo_price", "discount_pct",
    "unit", "is_available", "stock", "offer_tag",
    "product_url", "image_url", "scraped_at",
]


def save_csv(offers: List[Dict[str, Any]], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(offers)
    print(f"Saved {len(offers):,} rows -> {path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Farmaconde -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit", type=int, default=None, help="Stop after N products (test)")
    parser.add_argument("--csv", action="store_true", help="Also export a CSV file after scrape")
    parser.add_argument("--output", type=str, default=None, help="CSV path (implies --csv)")
    parser.add_argument("--env", type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import FarmacondeDB, load_env

    load_env(args.env)

    db = FarmacondeDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print("\nDone.")
    print(
        f"  Upserted: {stats['upserted']:,}  "
        f"history: {stats['history_inserted']:,}  "
        f"skipped (zero): {stats['skipped_zero']:,}"
    )
