"""
scraper_permanente.py — Scraper for Farmácia Permanente (https://www.farmaciapermanente.com.br)

Platform  : custom Django storefront (Nissei family — csrfmiddlewaretoken,
            /sitemaps/produtos.xml, /core/produto/pbm/*). Prices/availability are
            NOT in the page HTML (the visible R$ values are template placeholders);
            they come from a batched AJAX price endpoint.
Discovery : /sitemaps/produtos.xml (flat urlset, ~15.8k product URLs).
Per page  : EAN in plain text ("EAN: 7896…"), name in og:title, image in
            og:image, and the product id in `data-produto_id="N"`.
Prices    : POST /pegar/preco with form-array `produtos_ids[]` (batched) + the
            Django CSRF token -> {"precos": {id: {"publico": {...}}}} where
                valor_ini      -> regular price ("de")
                valor_fim      -> selling price ("por")  (promo when < valor_ini)
                is_disponivel  -> is_available
            Products not returned have no price -> skipped.
EAN       : first-class in the page text; no enrichment needed.

Two phases: (1) threaded fetch of every product page for id+ean+name; (2) batched
/pegar/preco calls for price+availability. ~15.8k page fetches.

Usage:
    python -m markets.permanente.scraper_permanente              # scrape -> DB
    python -m markets.permanente.scraper_permanente --limit 300  # test run -> DB
"""

import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.stdout.reconfigure(line_buffering=True)

BASE_URL  = "https://www.farmaciapermanente.com.br"
STORE_ID  = "permanente"
SITEMAP   = f"{BASE_URL}/sitemaps/produtos.xml"
PRECO_API = f"{BASE_URL}/pegar/preco"
WORKERS   = 12
PRICE_BATCH = 80
MAX_TRIES = 4

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_RE_LOC   = re.compile(r'<loc>\s*([^<\s]+)\s*</loc>')
_RE_PID   = re.compile(r'data-produto_id=["\'](\d+)["\']')
_RE_EAN   = re.compile(r'EAN:\s*(\d{8,14})')
_RE_H1    = re.compile(r'<h1[^>]*>(.*?)</h1>', re.S | re.I)
_RE_TAGS  = re.compile(r'<[^>]+>')
_RE_IMG   = re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I)
_RE_CSRF  = re.compile(r"csrfmiddlewaretoken['\"]?\s*:\s*['\"]([^'\"]+)")


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    s.get(BASE_URL + "/", timeout=30)   # prime session cookies (csrftoken)
    return s


def _get(session: requests.Session, url: str, diag: bool = False) -> Optional[str]:
    last = None
    for attempt in range(MAX_TRIES):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException as exc:
            last = f"exc {exc.__class__.__name__}"
            time.sleep(min(2 * (attempt + 1), 10))
            continue
        last = f"HTTP {r.status_code}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(3 * (attempt + 1), 15))
            continue
        if r.status_code != 200:
            if diag:
                print(f"  [get] {url} -> {last}")
            return None
        return r.text
    if diag:
        print(f"  [get] {url} -> gave up (last: {last})")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Discovery
# ──────────────────────────────────────────────────────────────────────────────

def fetch_product_urls(session: requests.Session) -> List[str]:
    xml = _get(session, SITEMAP, diag=True)
    if xml is None:
        print("ERROR: could not fetch sitemaps/produtos.xml.")
        return []
    urls = _RE_LOC.findall(xml)
    print(f"  Product URLs in sitemap: {len(urls):,}")
    return urls


# ──────────────────────────────────────────────────────────────────────────────
# Price API
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _pegar_preco(session: requests.Session, token: str, ids: List[str]) -> Dict[str, Dict]:
    """Batched POST /pegar/preco -> {produto_id: publico-price-dict}."""
    data = [("csrfmiddlewaretoken", token)] + [("produtos_ids[]", i) for i in ids]
    for attempt in range(MAX_TRIES):
        try:
            r = session.post(PRECO_API, data=data,
                             headers={"Referer": BASE_URL + "/",
                                      "X-Requested-With": "XMLHttpRequest",
                                      "X-CSRFToken": token},
                             timeout=30)
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code != 200:
            time.sleep(2 * (attempt + 1))
            continue
        try:
            precos = r.json().get("precos") or {}
        except ValueError:
            return {}
        out = {}
        for pid, tiers in precos.items():
            pub = (tiers or {}).get("publico") or {}
            if pub:
                out[str(pid)] = pub
        return out
    return {}


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape
# ──────────────────────────────────────────────────────────────────────────────

def scrape(db, limit: Optional[int] = None, workers: int = WORKERS) -> Dict:
    session = _make_session()

    print("Fetching product URLs from sitemap ...")
    urls = fetch_product_urls(session)
    if not urls:
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}
    if limit:
        urls = urls[:limit]

    # ── Phase 1: fetch every product page for id + ean + name + image ──────────
    metas: Dict[str, Dict] = {}   # produto_id -> meta
    token_box: List[str] = []
    failed = 0

    def _page_meta(url: str) -> Optional[Tuple[str, Dict]]:
        html = _get(session, url)
        if html is None:
            return None
        m = _RE_PID.search(html)
        if not m:
            return None
        if not token_box:
            tk = _RE_CSRF.search(html)
            if tk:
                token_box.append(tk.group(1))
        h1   = _RE_H1.search(html)
        name = _RE_TAGS.sub("", h1.group(1)).strip() if h1 else ""
        img  = _RE_IMG.search(html)
        ean  = _RE_EAN.search(html)
        return m.group(1), {
            "product_url": url,
            "product_name": name,
            "ean": (ean.group(1) if ean else ""),
            "image_url": (img.group(1).strip() if img else ""),
        }

    print(f"Phase 1: fetching {len(urls):,} product pages with {workers} workers ...")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_page_meta, u): u for u in urls}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res is None:
                failed += 1
            else:
                pid, meta = res
                if meta["product_name"]:
                    metas[pid] = meta
            if i % 2000 == 0:
                print(f"  [{i:>6}/{len(urls)}] pages  (collected={len(metas)}, failed={failed})")

    if not metas or not token_box:
        print("ERROR: no product ids / CSRF token collected.")
        return {"upserted": 0, "history_inserted": 0, "skipped_zero": 0, "total_unique": 0}
    token = token_box[0]
    print(f"Phase 1 done: {len(metas):,} products with ids. Phase 2: prices ...")

    # ── Phase 2: batched /pegar/preco -> price + availability, save ────────────
    total_upserted = total_history = total_skipped = total_saved = 0
    priced = 0
    batch: List[Dict] = []
    ids = list(metas)

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

    for i in range(0, len(ids), PRICE_BATCH):
        chunk = ids[i:i + PRICE_BATCH]
        prices = _pegar_preco(session, token, chunk)
        for pid, pub in prices.items():
            meta = metas.get(pid)
            if not meta:
                continue
            regular = _to_float(pub.get("valor_ini"))
            sell    = _to_float(pub.get("valor_fim"))
            if sell is None or sell <= 0:
                sell = regular
            if regular is None or regular <= 0:
                regular = sell
            if regular is None or regular <= 0:
                continue
            promo = sell if (sell is not None and 0 < sell < regular) else None
            disc  = round((1 - promo / regular) * 100, 1) if promo else None
            batch.append({
                "product_id":    pid,
                "store_id":      STORE_ID,
                "product_name":  meta["product_name"],
                "brand":         "",
                "category_path": "",
                "ean":           meta["ean"],
                "regular_price": regular,
                "promo_price":   promo,
                "discount_pct":  disc,
                "unit":          "",
                "is_available":  bool(pub.get("is_disponivel")),
                "stock":         None,
                "offer_tag":     "",
                "is_discounted": promo is not None,
                "product_url":   meta["product_url"],
                "image_url":     meta["image_url"],
                "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            })
            priced += 1
        if len(batch) >= 300:
            _flush()
            batch.clear()
        if (i // PRICE_BATCH) % 25 == 0:
            print(f"  priced {priced:,}/{len(ids):,}")
        time.sleep(0.05)

    _flush()
    batch.clear()

    print(f"\nFinished: {priced:,} products priced/saved  "
          f"({len(metas) - priced} without a price, {failed} page fetches failed).")
    return {"upserted": total_upserted, "history_inserted": total_history,
            "skipped_zero": total_skipped, "total_unique": total_saved}


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Farmácia Permanente -> PostgreSQL")
    parser.add_argument("--limit",   type=int, default=None,    help="Stop after N products (test)")
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"Parallel workers (default: {WORKERS})")
    parser.add_argument("--env",     type=str, default=".env",  help=".env file path")
    args = parser.parse_args()

    from db.db_manager import PermanenteDB, load_env
    load_env(args.env)

    db    = PermanenteDB()
    stats = scrape(db, limit=args.limit, workers=args.workers)
    db.close()

    print(f"\nDone.")
    print(f"  Upserted: {stats['upserted']:,}  "
          f"history: {stats['history_inserted']:,}  "
          f"skipped (zero): {stats['skipped_zero']:,}")

    if stats["total_unique"] == 0 and stats["upserted"] == 0:
        print("ERROR: scrape produced zero products — treating as failure.")
        sys.exit(1)
