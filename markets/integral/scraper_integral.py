"""
scraper_integral.py — Scraper for Integralmed (https://integralmed.com.br)

Platform  : Angular SPA backed by a public JSON API (custom, "Wiizi"-style).
API       : /api/api/categorias/                       -> 41 categories
            /api/api/produtos/categoria/{slug}         -> products in a category
Enumeration: list categories, fetch each category's products, dedupe by id
             (~931 products, ~488 with a price).
Fields    : the product object already includes everything we need:
                id / codigo         -> product_id
                nome                -> product_name
                ean                 -> ean / barcode (100% coverage)
                preco               -> price (single selling price)
                estoque (bool)      -> is_available
                fabricante.descricao-> brand
                slug                -> product_url (/produto/{slug})
                imagemUrl           -> image_url
EAN       : first-class field; no enrichment step needed.

Single selling price (`descontoFinanceiro` is a payment-method discount, not a
product promo), so regular_price = preco and promo_price is left null.

Usage:
    python -m markets.integral.scraper_integral              # scrape -> DB
    python -m markets.integral.scraper_integral --limit 100  # test run -> DB
    python -m markets.integral.scraper_integral --csv        # scrape -> DB + CSV
"""

import csv
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL   = "https://integralmed.com.br"
STORE_ID   = "integral"
API        = f"{BASE_URL}/api/api"
DELAY      = 0.10
MAX_TRIES  = 5

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


def _get_json(session: requests.Session, url: str) -> Optional[Any]:
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException:
            time.sleep(min(2 * (attempt + 1), 12))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 15))
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


def _brand(p: Dict) -> str:
    fab = p.get("fabricante")
    if isinstance(fab, dict):
        return str(fab.get("descricao") or fab.get("nome") or "").strip()
    return str(fab or "").strip()


def _image(p: Dict) -> str:
    img = str(p.get("imagemUrl") or "").strip()
    if not img or img == "default-image.jpg":
        return ""
    if img.startswith("http"):
        return img
    return f"{BASE_URL}/{img.lstrip('/')}"


def _standardize(p: Dict) -> Optional[Dict]:
    name = str(p.get("nome") or "").strip()
    pid  = str(p.get("id") or p.get("codigo") or "").strip()
    if not name or not pid:
        return None

    regular = _to_float(p.get("preco"))
    if regular is None or regular <= 0:
        return None

    slug = str(p.get("slug") or "").strip()
    product_url = f"{BASE_URL}/produto/{slug}" if slug else ""

    return {
        "product_id":    pid,
        "store_id":      STORE_ID,
        "product_name":  name,
        "brand":         _brand(p),
        "category_path": "",
        "ean":           str(p.get("ean") or "").strip(),
        "regular_price": regular,
        "promo_price":   None,
        "discount_pct":  None,
        "unit":          "",
        "is_available":  bool(p.get("estoque")),
        "stock":         None,
        "offer_tag":     str(p.get("codigo") or "").strip(),
        "is_discounted": False,
        "prescription":  bool(p.get("medicamentoControlado")),
        "product_url":   product_url,
        "image_url":     _image(p),
        "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def _products_of(data: Any) -> List[Dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("produtos", "itens", "results", "data"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return []


def scrape(db, limit: Optional[int] = None) -> Dict:
    session = _make_session()

    print("Fetching category list ...")
    cats = _get_json(session, f"{API}/categorias/")
    if not cats:
        print("ERROR: could not fetch categories — aborting.")
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}
    cats = cats if isinstance(cats, list) else _products_of(cats)
    print(f"  Categories: {len(cats)}")

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

    for i, c in enumerate(cats, 1):
        slug = str(c.get("slug") or "").strip()
        if not slug:
            continue
        data = _get_json(session, f"{API}/produtos/categoria/{slug}")
        for p in _products_of(data):
            pid = str(p.get("id") or p.get("codigo") or "").strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            offer = _standardize(p)
            if offer:
                batch.append(offer)
                processed += 1
                if limit and processed >= limit:
                    break
        if len(batch) >= 300:
            _flush()
            batch.clear()
        if i % 10 == 0 or i == len(cats):
            print(f"  [{i}/{len(cats)} categories] unique priced so far: {processed}")
        if limit and processed >= limit:
            break
        time.sleep(DELAY)

    _flush()
    batch.clear()

    print(f"\nFinished: {processed:,} priced products parsed  ({len(seen):,} unique seen).")
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
        description="Scrape Integralmed -> PostgreSQL (DB always written; CSV optional)"
    )
    parser.add_argument("--limit",   type=int, default=None,   help="Stop after N products (test)")
    parser.add_argument("--csv",     action="store_true",      help="Also export a CSV file after scrape")
    parser.add_argument("--output",  type=str, default=None,   help="CSV path (implies --csv)")
    parser.add_argument("--env",     type=str, default=".env", help=".env file path")
    args = parser.parse_args()

    from db.db_manager import IntegralDB, load_env
    load_env(args.env)

    db    = IntegralDB()
    stats = scrape(db, limit=args.limit)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if args.csv or args.output:
        output_dir = args.output or "."
        db2 = IntegralDB()
        db2.export(output_dir, tables=["offers"])
        db2.close()
