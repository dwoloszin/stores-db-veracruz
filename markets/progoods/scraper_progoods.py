"""
scraper_progoods.py — Scraper for ProGoods Medicamentos
(https://www.progoodsmedicamentos.com.br)

Platform  : Tray / CommerceSuite (Wake) — images.tcdn.com.br
API       : /web_api/products  (public Tray Web API, JSON, no auth for reads)
Pagination: ?limit=50&page=N  (maxLimit 50); paging.total gives the count (~432)
Fields    : id, name, ean (~100% coverage), price, promotional_price, available,
            brand, url.https / slug, ProductImage[0].
EAN       : first-class field; no enrichment step needed.

Usage:
    python -m markets.progoods.scraper_progoods              # scrape -> DB
    python -m markets.progoods.scraper_progoods --limit 100  # test run -> DB
    python -m markets.progoods.scraper_progoods --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://www.progoodsmedicamentos.com.br"
STORE_ID   = "progoods"
API        = f"{BASE_URL}/web_api/products"
PER_PAGE   = 50     # Tray Web API maxLimit
DELAY      = 0.20
MAX_TRIES  = 6

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get_page(session: requests.Session, page: int) -> Optional[Dict]:
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(API, params={"limit": PER_PAGE, "page": page}, timeout=30)
        except requests.RequestException:
            time.sleep(min(3 * (attempt + 1), 20))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(4 * (attempt + 1), 25))
            continue
        if r.status_code != 200:
            return None
        try:
            return r.json()
        except ValueError:
            return None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Standardize
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _product_url(p: Dict) -> str:
    url = p.get("url")
    if isinstance(url, dict):
        return str(url.get("https") or url.get("http") or "").strip()
    slug = str(p.get("slug") or "").strip()
    return f"{BASE_URL}/{slug}" if slug else ""


def _image_url(p: Dict) -> str:
    imgs = p.get("ProductImage") or []
    if isinstance(imgs, list) and imgs:
        first = imgs[0]
        if isinstance(first, dict):
            return str(first.get("https") or first.get("http") or "").strip()
    return ""


def _category_from_slug(p: Dict) -> str:
    slug = str(p.get("slug") or "").strip("/")
    parts = [s for s in slug.split("/") if s]
    cats = parts[:-1] if len(parts) > 1 else []
    return " > ".join(c.replace("-", " ") for c in cats)


def _standardize(p: Dict) -> Optional[Dict]:
    name = str(p.get("name") or "").strip()
    pid  = str(p.get("id") or "").strip()
    if not name or not pid:
        return None
    if name.upper() == "PRODUTO DE TESTE":
        return None

    regular = _to_float(p.get("price"))
    promo_raw = _to_float(p.get("promotional_price"))
    if regular is None or regular <= 0:
        return None

    promo_price = promo_raw if (promo_raw and 0 < promo_raw < regular) else None
    discount_pct = (
        round((1 - promo_price / regular) * 100, 1)
        if promo_price else None
    )

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         str(p.get("brand") or "").strip(),
        "category_path": _category_from_slug(p),
        "ean":           str(p.get("ean") or "").strip(),
        "regular_price": regular,
        "promo_price":   promo_price,
        "discount_pct":  discount_pct,
        "unit":          "",
        "is_available":  str(p.get("available")) == "1",
        "stock":         None,
        "offer_tag":     str(p.get("reference") or "").strip(),
        "is_discounted": promo_price is not None,
        "product_url":   _product_url(p),
        "image_url":     _image_url(p),
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None) -> Dict:
    session = _make_session()

    first = _get_page(session, 1)
    if first is None:
        print("ERROR: could not fetch /web_api/products page 1 — aborting.")
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}

    total = int((first.get("paging") or {}).get("total") or 0)
    total_pages = (total + PER_PAGE - 1) // PER_PAGE if total else 1
    print(f"Tray Web API: {total:,} products across {total_pages} pages")

    total_upserted = total_history = total_skipped = total_saved = 0
    seen: set = set()
    batch: List[Dict] = []
    processed = 0

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

    def _handle(data: Dict) -> None:
        nonlocal processed
        for pw in data.get("Products") or []:
            p = pw.get("Product") or pw
            pid = str(p.get("id") or "").strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            offer = _standardize(p)
            if offer:
                batch.append(offer)
                processed += 1

    _handle(first)
    print(f"  page 1/{total_pages}: {processed} products")

    for page in range(2, total_pages + 1):
        if limit and processed >= limit:
            break
        data = _get_page(session, page)
        if data is None:
            print(f"  WARNING: page {page} failed after retries — catalogue may be incomplete")
            break
        _handle(data)
        if len(batch) >= 200:
            _flush()
            batch.clear()
        if page % 5 == 0 or page == total_pages:
            print(f"  page {page}/{total_pages}: {processed} products parsed")
        time.sleep(DELAY)

    _flush()
    batch.clear()

    print(f"\nFinished: {processed:,} products parsed.")
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
        description="Scrape ProGoods Medicamentos -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",   type=int, default=None,   help="Stop after N products (test)")
    parser.add_argument("--csv",     action="store_true",      help="Also export a CSV file after scrape")
    parser.add_argument("--output",  type=str, default=None,   help="CSV path (implies --csv)")
    parser.add_argument("--env",     type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import ProgoodsDB, load_env
    load_env(args.env)

    db    = ProgoodsDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = ProgoodsDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
