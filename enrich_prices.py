"""
enrich_prices.py — run the heavy product-page price enrichers.

Some stores (Drogasil, Drogaraia) only expose a reference/max price (PMC) in
their fast category listing; the real selling price lives on each product page.
These enrichers fetch every product page and backfill the true selling price
(promo_price = value_to) into the store DB.

This is intentionally decoupled from the fast 8h listing scrape and runs as its
own daily job (in the dedicated enrichment repo) because it is heavy
(~160k page fetches per store).

Usage:
    python -m enrich_prices                      # all registered stores
    python -m enrich_prices drogasil             # one store
    python -m enrich_prices --workers 24         # more threads
    python -m enrich_prices --limit 500          # test run
"""

import argparse
import importlib
import sys
import time
import traceback

# store -> (enricher module, DB class name)
_PRICE_ENRICHERS = {
    "drogasil":  "markets.drogasil.enrich_price_drogasil",
    "drogaraia": "markets.drogaraia.enrich_price_drogaraia",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run product-page price enrichers")
    parser.add_argument("stores", nargs="*", default=None,
                        help=f"Stores to enrich (default: all — {', '.join(_PRICE_ENRICHERS)})")
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--limit",   type=int, default=None)
    parser.add_argument("--min-price", type=float, default=1000.0, dest="min_price",
                        help="Only enrich products with regular_price >= this (default 1000)")
    parser.add_argument("--env",     type=str, default=".env")
    args = parser.parse_args()

    from db.db_manager import STORE_REGISTRY, load_env
    load_env(args.env)

    stores = args.stores or list(_PRICE_ENRICHERS)
    failures = []
    for store in stores:
        if store not in _PRICE_ENRICHERS:
            print(f"[{store}] no price enricher registered — skipping")
            continue
        print(f"\n{'='*60}\n=== PRICE ENRICH: {store} ===\n{'='*60}")
        t0 = time.time()
        db = None
        try:
            mod = importlib.import_module(_PRICE_ENRICHERS[store])
            db = STORE_REGISTRY[store]()
            stats = mod.enrich(db, limit=args.limit, workers=args.workers, min_price=args.min_price)
            print(f"[{store}] done in {(time.time()-t0)/60:.1f} min — "
                  f"promos: {stats.get('with_promo', 0):,}, updated: {stats.get('updated', 0):,}")
        except Exception as exc:
            print(f"[{store}] FAILED: {exc}")
            traceback.print_exc()
            failures.append(store)
        finally:
            if db is not None:
                db.close()

    if failures:
        print(f"\nFAILED stores: {', '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
