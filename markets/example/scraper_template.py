"""
scraper_template.py — Copy this file and rename it for each new store.

Steps to create a scraper for a new store:
1. Copy this file to markets/<storename>/scraper_<storename>.py
2. Rename ExampleStoreScraper to <StoreName>Scraper
3. Fill in BASE_URL, STORE_ID, and implement _fetch_products() + _standardize()
4. Register it in main.py STORE_REGISTRY
5. Add the store to config.py STORE_TIER and STORE_DB_SUFFIX
6. Add DATABASE_URL_<SUFFIX> to .env
7. Add the store to deploy/deploy.py STORE_SCHEDULE and STORE_DB_SUFFIX
"""

import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from db.db_manager import DatabaseManager


class ExampleStoreScraper:
    """
    Scraper for Example Store.

    Replace this docstring with:
    - API endpoint or website URL
    - Authentication method (none / cookie / token)
    - How pagination works (page number / cursor / offset)
    - Where barcodes are (inline in list / on product page / not available)
    """

    BASE_URL = "https://www.example-store.com.br"
    STORE_ID = "example-store"   # unique slug — used in offer IDs and DB

    def __init__(self):
        self.db = DatabaseManager()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9",
        })

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        """Parse price strings like 'R$ 12,99' or '12.99' to float."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace("R$", "").replace("\xa0", "").replace(" ", "")
        if "," in text:
            text = text.replace(".", "").replace(",", ".")
        try:
            return float(text)
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Data fetching — implement these for your store
    # ------------------------------------------------------------------

    def _fetch_products(
        self, zip_code: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch raw product data from the store's API or website.

        Returns a list of raw dicts — one per product.
        This method handles pagination internally.

        Tips:
        - Use self.session.get/post with timeout=25
        - Add time.sleep(0.15) between pages to avoid rate limiting
        - Handle HTTP 429 (rate limit) with exponential backoff
        - Use limit to stop early when testing

        Example for a simple paginated JSON API:
        """
        all_items: List[Dict[str, Any]] = []
        page = 1

        while True:
            try:
                r = self.session.get(
                    f"{self.BASE_URL}/api/products",
                    params={"page": page, "per_page": 48, "zip": zip_code},
                    timeout=25,
                )
                if r.status_code != 200:
                    print(f"  Example p{page}: HTTP {r.status_code}")
                    break

                data = r.json()
                items = data.get("products") or []
                if not items:
                    break

                all_items.extend(items)
                print(f"  page={page} items={len(items)} total={len(all_items)}")

                if limit and len(all_items) >= limit:
                    break
                if page >= (data.get("totalPages") or 1):
                    break

                page += 1
                time.sleep(0.15)

            except Exception as exc:
                print(f"  Example p{page} error: {exc}")
                break

        return all_items[:limit] if limit else all_items

    def _standardize(
        self, item: Dict[str, Any], zip_code: str
    ) -> Optional[Dict[str, Any]]:
        """
        Convert one raw API item to the standard offer dict.

        Required fields: id, product_name, regular_price
        Everything else is optional but fill in as much as you can.

        The offer dict schema must match db/db_manager.py save_offers() expectations.
        """
        name = str(item.get("name") or "").strip()
        if not name:
            return None

        # build_offer_id creates a stable UUID from store+name (or EAN if available)
        offer_id = self.db.build_offer_id(
            "example",          # market_key (short lowercase slug)
            self.STORE_ID,      # store_id
            item.get("ean"),    # ean/barcode (or None)
            None,               # product_id (or None)
            name,               # product_name (used as fallback for ID)
        )
        if not offer_id:
            return None

        regular_price = self._to_float(item.get("price"))
        promo_price = self._to_float(item.get("promo_price"))
        if promo_price and regular_price and promo_price >= regular_price:
            promo_price = None  # ignore invalid promos

        barcode = str(item.get("ean") or item.get("barcode") or "").strip() or None

        return {
            "id":                    offer_id,
            "product_name":          name,
            "brand":                 str(item.get("brand") or "").strip() or None,
            "description":           str(item.get("description") or "").strip() or None,
            "regular_price":         regular_price,
            "promo_price":           promo_price,
            "promo_min_quantity":    None,     # e.g. 2 if "buy 2 get discount"
            "unit":                  None,     # e.g. "kg", "un", "L"
            "gtin":                  barcode,
            "barcode":               barcode,
            "product_url":           str(item.get("url") or "").strip() or None,
            "image_url":             str(item.get("image") or "").strip() or None,
            "stock_balance":         None,
            "stock_general":         None,     # 1=in stock, 0=out, None=unknown
            "sold_quantity":         None,
            "offer_name":            None,     # human-readable promo label
            "offer_tag":             None,     # promo category: "App", "Cartao", etc.
            "app_membership_required": False,
            "promo_end_at":          None,
            "last_updated":          datetime.now().isoformat(),
            "store_id":              self.STORE_ID,
            "zip_code":              zip_code,
        }

    # ------------------------------------------------------------------
    # Public interface — called by main.py
    # ------------------------------------------------------------------

    def fetch_offers(
        self, zip_code: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        print(f"Fetching Example Store offers (zip={zip_code}, limit={limit})...")

        raw_items = self._fetch_products(zip_code, limit=limit)
        offers: List[Dict[str, Any]] = []

        for item in raw_items:
            offer = self._standardize(item, zip_code)
            if offer:
                offers.append(offer)

        print(f"Example Store: {len(offers)} products collected.")
        return offers


if __name__ == "__main__":
    scraper = ExampleStoreScraper()
    offers = scraper.fetch_offers("01310-100", limit=10)
    print(f"\nTotal: {len(offers)} offers")
    for o in offers[:3]:
        print(o)
