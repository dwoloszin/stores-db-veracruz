"""
db_manager.py — Centralized PostgreSQL persistence for all store scrapers.

Each store has its own DATABASE_URL (separate Postgres instance). This module
provides a single StoreDB base class and one small subclass per store.

Two tables per database:
  offers        — current state, always upserted with the latest valid price.
                  Zero / null regular_price rows are skipped.
  price_history — append-only change log; new row only when price changes.
                  Identical consecutive scrapes produce no new rows.

Offer normalization:
  Scrapers that produce a single "price" field (e.g. Drogasil) have it
  automatically mapped to "regular_price" so save() handles both layouts.

Schema migration:
  _ensure_tables() runs ALTER TABLE ADD COLUMN IF NOT EXISTS to upgrade
  existing databases that were created with the older per-store DDL.

Usage (imported):
    from db.db_manager import DrogalesteDB, DrogasilDB
    db = DrogasilDB()
    db.save(offers)
    db.close()

CLI:
    python -m db.db_manager drogaleste drogaleste_20260519.csv
    python -m db.db_manager drogasil   drogasil_20260519.csv
    python -m db.db_manager drogasil   drogasil_20260519.csv --env .env
"""

import csv
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

sys.stdout.reconfigure(line_buffering=True)

# Sentinel "price not available" placeholder some source APIs return
# (e.g. drogariasaopaulo lists reg=9999876.00 with the real price in promo_price).
# Treated as missing so it never counts as a real regular_price.
PRICE_SENTINEL = 9_999_000


def effective_price_sql(reg: str, promo: str) -> str:
    """
    Single source of truth for a row's *effective* price in SQL.

    Uses promo_price whenever it's a real, lower price than regular_price
    (with no dependency on the unreliable is_discounted flag); otherwise
    falls back to regular_price. A regular_price at/above the sentinel is
    treated as missing so the promo wins.
    """
    return (
        f"CASE "
        f"WHEN {promo} IS NOT NULL AND {promo} > 0 "
        f"AND ({reg} IS NULL OR {reg} >= {PRICE_SENTINEL} OR {promo} < {reg}) "
        f"THEN {promo} "
        f"ELSE {reg} "
        f"END"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Unified DDL
# ──────────────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS offers (
    product_id      TEXT PRIMARY KEY,
    store_id        TEXT NOT NULL DEFAULT '',
    product_name    TEXT NOT NULL,
    brand           TEXT,
    category_path   TEXT,
    ean             TEXT,
    regular_price   NUMERIC(10, 2),
    promo_price     NUMERIC(10, 2),
    discount_pct    NUMERIC(5, 1),
    unit            TEXT,
    is_available    BOOLEAN,
    stock           INTEGER,
    offer_tag       TEXT,
    is_discounted   BOOLEAN,
    is_generic      BOOLEAN,
    prescription    BOOLEAN,
    product_url     TEXT,
    image_url       TEXT,
    scraped_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS price_history (
    id              BIGSERIAL PRIMARY KEY,
    product_id      TEXT NOT NULL,
    store_id        TEXT NOT NULL DEFAULT '',
    product_name    TEXT,
    ean             TEXT,
    regular_price   NUMERIC(10, 2),
    promo_price     NUMERIC(10, 2),
    discount_pct    NUMERIC(5, 1),
    is_available    BOOLEAN,
    offer_tag       TEXT,
    is_discounted   BOOLEAN,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ph_product  ON price_history(product_id);
CREATE INDEX IF NOT EXISTS idx_ph_recorded ON price_history(recorded_at DESC);

CREATE OR REPLACE FUNCTION trg_price_history()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP = 'INSERT') OR (
        TG_OP = 'UPDATE' AND (
            NEW.regular_price IS DISTINCT FROM OLD.regular_price OR
            NEW.promo_price   IS DISTINCT FROM OLD.promo_price
        )
    ) THEN
        INSERT INTO price_history (
            product_id, store_id, product_name, ean,
            regular_price, promo_price, discount_pct,
            is_available, offer_tag, is_discounted, recorded_at
        ) VALUES (
            NEW.product_id, NEW.store_id, NEW.product_name, NEW.ean,
            NEW.regular_price, NEW.promo_price, NEW.discount_pct,
            NEW.is_available, NEW.offer_tag, NEW.is_discounted, NOW()
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER price_history_trigger
AFTER INSERT OR UPDATE ON offers
FOR EACH ROW EXECUTE FUNCTION trg_price_history();
"""

# Columns added in the unified schema that older per-store tables won't have.
# ALTER TABLE … ADD COLUMN IF NOT EXISTS is idempotent on PostgreSQL 9.6+.
_OFFERS_MIGRATIONS = [
    ("store_id",      "TEXT NOT NULL DEFAULT ''"),
    ("is_discounted", "BOOLEAN"),
    ("is_generic",    "BOOLEAN"),
    ("prescription",  "BOOLEAN"),
    # Drogaleste originally used regular_price NOT NULL; unified schema uses nullable.
    # The NOT NULL constraint on existing columns is NOT relaxed here — that's fine
    # because Drogaleste always supplies a valid regular_price before upsert.
]

_HISTORY_MIGRATIONS = [
    ("store_id",      "TEXT NOT NULL DEFAULT ''"),
    ("is_discounted", "BOOLEAN"),
]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _to_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return None


def _to_int(v: Any) -> Optional[int]:
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None



def load_env(path: str = ".env") -> None:
    """Minimal .env loader — sets os.environ for KEY=VALUE lines (no overwrite)."""
    try:
        with open(path, encoding="latin-1") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        pass


def load_csv(path: str) -> List[Dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# ──────────────────────────────────────────────────────────────────────────────
# Base class
# ──────────────────────────────────────────────────────────────────────────────

class StoreDB:
    """
    Base class for per-store PostgreSQL persistence.

    Subclass and set STORE_ID + DB_ENV_KEY — that's all that's required.

    class MyStoreDB(StoreDB):
        STORE_ID  = "mystore"
        DB_ENV_KEY = "DATABASE_URL_MYSTORE"
    """

    STORE_ID:   str = ""
    DB_ENV_KEY: str = ""

    def __init__(self, database_url: Optional[str] = None):
        url = database_url or os.environ.get(self.DB_ENV_KEY, "")
        if not url:
            raise RuntimeError(
                f"{self.DB_ENV_KEY} is not set. "
                "Add it to .env or pass database_url= explicitly."
            )
        self._db_url = url
        self._conn = psycopg2.connect(url)
        self._conn.autocommit = False
        self._ensure_tables()

    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(_DDL)
            for col, defn in _OFFERS_MIGRATIONS:
                cur.execute(
                    f"ALTER TABLE offers ADD COLUMN IF NOT EXISTS {col} {defn}"
                )
            for col, defn in _HISTORY_MIGRATIONS:
                cur.execute(
                    f"ALTER TABLE price_history ADD COLUMN IF NOT EXISTS {col} {defn}"
                )
            # Backfill store_id for rows created before the unified schema
            # (ALTER TABLE sets DEFAULT '' for pre-existing rows)
            if self.STORE_ID:
                cur.execute(
                    "UPDATE offers SET store_id = %s "
                    "WHERE store_id IS NULL OR store_id = ''",
                    (self.STORE_ID,),
                )
                cur.execute(
                    "UPDATE price_history SET store_id = %s "
                    "WHERE store_id IS NULL OR store_id = ''",
                    (self.STORE_ID,),
                )
        self._conn.commit()
        print("DB tables ready (offers, price_history).")

    # ------------------------------------------------------------------

    def _reconnect(self) -> None:
        """Re-open the DB connection after an SSL/network drop."""
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = psycopg2.connect(self._db_url)
        self._conn.autocommit = False
        print("  DB reconnected.")

    # ------------------------------------------------------------------

    def load_existing_product_ids(self) -> set:
        """
        Return the set of all product_ids currently in the offers table.
        Used by scrapers to seed seen_ids on restart, enabling resume-from-checkpoint.
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT product_id FROM offers")
                return {row[0] for row in cur.fetchall()}
        except Exception:
            return set()

    # ------------------------------------------------------------------

    def save(self, offers: List[Dict], batch_size: int = 200, verbose: bool = True,
             preserve_promo: bool = False) -> Dict[str, int]:
        """
        Upsert offers and append price_history rows for changed prices.
        Returns {"upserted": N, "history_inserted": N, "skipped_zero": N}.
        Set verbose=False for silent per-category saves (scraper chunk mode).
        Set preserve_promo=True when the scraper does NOT know the promo price
        (e.g. Drogasil/Drogaraia, where a separate daily enrichment pass owns
        promo_price/discount_pct) — the upsert then keeps the existing values
        instead of nulling them out on every listing scrape.
        Automatically reconnects once on SSL/network drop.
        """
        try:
            return self._save_impl(offers, batch_size, verbose, preserve_promo)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            print(f"  DB connection lost ({exc.__class__.__name__}: {exc}) — reconnecting and retrying...")
            self._reconnect()
            return self._save_impl(offers, batch_size, verbose, preserve_promo)

    def _save_impl(self, offers: List[Dict], batch_size: int = 200, verbose: bool = True,
                   preserve_promo: bool = False) -> Dict[str, int]:
        now      = datetime.now(timezone.utc)
        store_id = self.STORE_ID

        # Normalize single-price scrapers: map 'price' -> 'regular_price'
        rows: List[tuple] = []
        skipped_zero = 0
        for o in offers:
            if "price" in o and "regular_price" not in o:
                o = {**o, "regular_price": o["price"]}
            rp = _to_float(o.get("regular_price"))
            if rp is None or rp <= 0:
                skipped_zero += 1
                continue
            rows.append((
                str(o.get("product_id",    "")).strip(),
                store_id,
                str(o.get("product_name",  "")).strip(),
                str(o.get("brand",         "") or "").strip() or None,
                str(o.get("category_path", "") or "").strip() or None,
                str(o.get("ean",           "") or "").strip() or None,
                rp,
                _to_float(o.get("promo_price")),
                _to_float(o.get("discount_pct")),
                str(o.get("unit",          "") or "").strip() or None,
                _to_bool(o.get("is_available")),
                _to_int(o.get("stock")),
                str(o.get("offer_tag",     "") or "").strip() or None,
                _to_bool(o.get("is_discounted")),
                _to_bool(o.get("is_generic")),
                _to_bool(o.get("prescription")),
                str(o.get("product_url",   "") or "").strip() or None,
                str(o.get("image_url",     "") or "").strip() or None,
                o.get("scraped_at") or now.isoformat(),
                now,
            ))

        if verbose:
            print(f"  Offers: {len(rows):,} valid, {skipped_zero:,} skipped (zero/null price)")

        # When preserve_promo is set, the listing scrape must not clobber the
        # promo_price/discount_pct/is_discounted that the daily enrichment owns.
        if preserve_promo:
            promo_set = (
                "promo_price   = COALESCE(EXCLUDED.promo_price, offers.promo_price),\n"
                "                discount_pct  = COALESCE(EXCLUDED.discount_pct, offers.discount_pct),\n"
                "                is_discounted = (COALESCE(EXCLUDED.promo_price, offers.promo_price) IS NOT NULL),"
            )
        else:
            promo_set = (
                "promo_price   = EXCLUDED.promo_price,\n"
                "                discount_pct  = EXCLUDED.discount_pct,\n"
                "                is_discounted = EXCLUDED.is_discounted,"
            )

        # Pure upsert — no prior SELECT needed.
        # price_history rows are written automatically by the DB trigger
        # (trg_price_history) on INSERT and on price-changing UPDATEs.
        upsert_sql = f"""
            INSERT INTO offers (
                product_id, store_id, product_name, brand, category_path, ean,
                regular_price, promo_price, discount_pct, unit,
                is_available, stock, offer_tag, is_discounted, is_generic, prescription,
                product_url, image_url, scraped_at, updated_at
            ) VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                store_id      = EXCLUDED.store_id,
                product_name  = EXCLUDED.product_name,
                brand         = EXCLUDED.brand,
                category_path = EXCLUDED.category_path,
                ean           = COALESCE(NULLIF(EXCLUDED.ean, ''), offers.ean),
                regular_price = EXCLUDED.regular_price,
                {promo_set}
                unit          = EXCLUDED.unit,
                is_available  = EXCLUDED.is_available,
                stock         = EXCLUDED.stock,
                offer_tag     = EXCLUDED.offer_tag,
                is_generic    = EXCLUDED.is_generic,
                prescription  = EXCLUDED.prescription,
                product_url   = EXCLUDED.product_url,
                image_url     = EXCLUDED.image_url,
                scraped_at    = EXCLUDED.scraped_at,
                updated_at    = EXCLUDED.updated_at
        """

        with self._conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                psycopg2.extras.execute_values(cur, upsert_sql, rows[i : i + batch_size])

        self._conn.commit()

        return {
            "upserted":         len(rows),
            "history_inserted": 0,
            "skipped_zero":     skipped_zero,
        }

    def update_eans(self, ean_map: Dict[str, str]) -> int:
        """
        Bulk-update the ean column in offers for the given {product_id: ean} map.
        Returns the number of rows updated. Reconnects once on SSL/network drop.
        """
        try:
            return self._update_eans_impl(ean_map)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            print(f"  DB connection lost ({exc.__class__.__name__}: {exc}) — reconnecting and retrying...")
            self._reconnect()
            return self._update_eans_impl(ean_map)

    def _update_eans_impl(self, ean_map: Dict[str, str]) -> int:
        if not ean_map:
            return 0
        rows = [(ean, pid) for pid, ean in ean_map.items() if ean]
        total = 0
        with self._conn.cursor() as cur:
            # Use execute_values to generate a single UPDATE per 500-row batch.
            # This gives a correct cur.rowcount (unlike execute_batch which joins
            # multiple statements and only reports the last one).
            for i in range(0, len(rows), 500):
                batch = rows[i:i + 500]
                psycopg2.extras.execute_values(
                    cur,
                    "UPDATE offers SET ean = v.ean "
                    "FROM (VALUES %s) AS v(ean, product_id) "
                    "WHERE offers.product_id = v.product_id "
                    "  AND (offers.ean IS NULL OR offers.ean = '')",
                    batch,
                    page_size=500,
                )
                total += cur.rowcount if cur.rowcount >= 0 else 0
        self._conn.commit()
        return total

    def load_missing_eans(self) -> Dict[str, str]:
        """
        Returns {product_id: product_url} for all offers with no EAN yet.
        Used by enrichment scripts to know which products still need a page fetch.
        Reconnects once on SSL/network drop.
        """
        try:
            return self._load_missing_eans_impl()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            print(f"  DB connection lost ({exc.__class__.__name__}: {exc}) — reconnecting and retrying...")
            self._reconnect()
            return self._load_missing_eans_impl()

    def load_all_urls(self, min_price: float = 0.0) -> Dict[str, str]:
        """
        Returns {product_id: product_url} for offers with a product URL and
        regular_price >= min_price. Used by the price-enrichment pass
        (Drogasil/Drogaraia), where the fast listing only exposes the
        reference/max price (PMC) and the real selling price is on each product
        page. min_price limits enrichment to high-value products to keep the
        pass fast (e.g. 1000 → ~2% of the catalogue).
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT product_id, product_url FROM offers "
                    "WHERE product_url IS NOT NULL AND product_url != '' "
                    "  AND regular_price >= %s",
                    (min_price,),
                )
                return {row[0]: row[1] for row in cur.fetchall()}
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self._reconnect()
            return self.load_all_urls(min_price)

    def update_promo_prices(self, price_map: Dict[str, tuple]) -> int:
        """
        Bulk-update regular_price / promo_price / discount_pct for the given
        {product_id: (regular_price, promo_price, discount_pct)} map.
        price_history rows are written automatically by the DB trigger when a
        price actually changes. Returns rows updated.
        """
        rows = [
            (reg, promo, disc, pid)
            for pid, (reg, promo, disc) in price_map.items()
            if reg is not None
        ]
        if not rows:
            return 0
        total = 0
        try:
            with self._conn.cursor() as cur:
                for i in range(0, len(rows), 500):
                    psycopg2.extras.execute_values(
                        cur,
                        "UPDATE offers SET "
                        "  regular_price = v.reg::numeric, "
                        "  promo_price   = v.promo::numeric, "
                        "  discount_pct  = v.disc::numeric, "
                        "  is_discounted = (v.promo IS NOT NULL), "
                        "  updated_at    = NOW() "
                        "FROM (VALUES %s) AS v(reg, promo, disc, product_id) "
                        "WHERE offers.product_id = v.product_id::text",
                        rows[i:i + 500],
                        page_size=500,
                    )
                    total += cur.rowcount if cur.rowcount >= 0 else 0
            self._conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self._reconnect()
            return self.update_promo_prices(price_map)
        return total

    def _load_missing_eans_impl(self) -> Dict[str, str]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT product_id, product_url FROM offers "
                "WHERE (ean IS NULL OR ean = '') AND product_url IS NOT NULL AND product_url != ''"
            )
            return {row[0]: row[1] for row in cur.fetchall()}

    def export(
        self,
        output_dir: str = "exports",
        tables: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Dump DB tables to UTF-8-BOM CSV files (Excel-friendly).

        Returns the list of file paths written.
        Default tables: ["offers", "price_history"].
        Files are named: <store_id>_<table>_<YYYYMMDD_HHMM>.csv
        """
        os.makedirs(output_dir, exist_ok=True)
        if tables is None:
            tables = ["offers", "price_history"]

        ts = datetime.now().strftime("%Y%m%d_%H%M")
        written: List[str] = []

        for table in tables:
            path = os.path.join(output_dir, f"{self.STORE_ID}_{table}_{ts}.csv")
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table} ORDER BY 1")
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)

            print(f"  [{self.STORE_ID}] {table}: {len(rows):,} rows -> {path}")
            written.append(path)

        return written

    def query_combined_rows(self) -> List[Dict]:
        """
        Return rows in the combined export format:
          barcode, price, quantity, store_name, date_recorded, notes,
          product_url, image_url, product_name

        price       = promo_price if set, else regular_price
        quantity    = 1 (minimum buy quantity not captured)
        notes       = "Min:X Max:Y" from price_history (falls back to current price)
        date_recorded = scraped_at formatted as M/D/YYYY HH:MM
        Only rows with a non-empty EAN are included.
        """
        eff_offer = effective_price_sql("o.regular_price", "o.promo_price")
        eff_hist = effective_price_sql("regular_price", "promo_price")
        sql = f"""
            SELECT
                o.ean                                                AS barcode,
                {eff_offer}                                          AS price,
                o.store_id                                           AS store_name,
                o.scraped_at                                         AS date_recorded,
                COALESCE(ph.min_price, {eff_offer})                  AS hist_min,
                COALESCE(ph.max_price, {eff_offer})                  AS hist_max,
                o.product_url,
                o.image_url,
                o.product_name
            FROM offers o
            LEFT JOIN (
                SELECT product_id,
                       MIN({eff_hist}) AS min_price,
                       MAX({eff_hist}) AS max_price
                FROM price_history
                GROUP BY product_id
            ) ph ON o.product_id = ph.product_id
            WHERE o.ean IS NOT NULL AND o.ean != ''
              AND o.regular_price IS NOT NULL AND o.regular_price > 0
            ORDER BY o.ean
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            raw_rows = cur.fetchall()

        def _fmt(v) -> str:
            if v is None:
                return "0"
            return f"{float(v):.2f}".rstrip("0").rstrip(".")

        def _fmt_dt(dt) -> str:
            if dt is None:
                return ""
            if isinstance(dt, str):
                try:
                    from datetime import timezone
                    dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                except ValueError:
                    return dt
            return f"{dt.month}/{dt.day}/{dt.year} {dt.hour:02d}:{dt.minute:02d}"

        result = []
        for raw in raw_rows:
            r = dict(zip(cols, raw))
            result.append({
                "barcode":       r["barcode"],
                "price":         _fmt(r["price"]),
                "quantity":      1,
                "store_name":    r["store_name"],
                "date_recorded": _fmt_dt(r["date_recorded"]),
                "notes":         f"Min:{_fmt(r['hist_min'])} Max:{_fmt(r['hist_max'])}",
                "product_url":   r["product_url"] or "",
                "image_url":     r["image_url"] or "",
                "product_name":  r["product_name"] or "",
            })
        return result

    def prune_history(self, days: int = 180) -> int:
        """
        Delete price_history rows older than `days`, but always keep each
        product's oldest surviving price point intact (the latest row per
        product is never older than the last scrape anyway).
        Returns the number of rows deleted.
        Keeps the DB under NeonDB's free-tier size limit.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM price_history
                WHERE recorded_at < NOW() - make_interval(days => %s)
                  AND id NOT IN (
                      SELECT MIN(id) FROM price_history GROUP BY product_id
                  )
                """,
                (days,),
            )
            deleted = cur.rowcount
        self._conn.commit()
        print(f"  Pruned {deleted:,} price_history rows older than {days} days.")
        return deleted

    def mark_stale_unavailable(self, hours: int = 24) -> int:
        """
        Mark as unavailable any offer still flagged available but whose row was
        NOT refreshed by a scrape in the last `hours`. Scrapers upsert
        updated_at=NOW() for every product they return, so a stale updated_at
        means the product dropped out of the catalogue (very likely no longer
        sold). The 24h default spans several scrape cycles, so a single failed
        or partial run never wrongly flips products — they only go unavailable
        after genuinely disappearing for a day. A product that reappears is
        upserted back to is_available=true automatically.
        Returns the number of rows flipped.
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "UPDATE offers SET is_available = false "
                    "WHERE is_available IS TRUE "
                    "  AND updated_at < NOW() - make_interval(hours => %s)",
                    (hours,),
                )
                flipped = cur.rowcount
            self._conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self._reconnect()
            return self.mark_stale_unavailable(hours)
        print(f"  Marked {flipped:,} stale offers unavailable (not seen in {hours}h).")
        return flipped

    def last_update(self) -> Optional[datetime]:
        """Return the most recent offers.updated_at, or None if the table is
        empty. Used by the local `--only-stale` refresh to decide whether a
        store's Neon data has gone stale (e.g. a store GitHub can't reach)."""
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT max(updated_at) FROM offers")
                row = cur.fetchone()
                return row[0] if row else None
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self._reconnect()
            return self.last_update()

    def close(self) -> None:
        self._conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Store subclasses — add one per new drugstore
# ──────────────────────────────────────────────────────────────────────────────

class DrogalesteDB(StoreDB):
    STORE_ID   = "drogaleste"
    DB_ENV_KEY = "DATABASE_URL_DROGALESTE"


class DrogasilDB(StoreDB):
    STORE_ID   = "drogasil"
    DB_ENV_KEY = "DATABASE_URL_DROGASIL"


class DrogaraiaDB(StoreDB):
    STORE_ID   = "drogaraia"
    DB_ENV_KEY = "DATABASE_URL_DROGARAIA"


class DrogariaSaoPauloDB(StoreDB):
    STORE_ID   = "drogariasaopaulo"
    DB_ENV_KEY = "DATABASE_URL_DROGARIASAOPAULO"


class UltrafarmDB(StoreDB):
    STORE_ID   = "ultrafarma"
    DB_ENV_KEY = "DATABASE_URL_ULTRAFARMA"


class PagueMenosDB(StoreDB):
    STORE_ID   = "paguemenos"
    DB_ENV_KEY = "DATABASE_URL_PAGUEMENOS"


class FarmaisDB(StoreDB):
    STORE_ID   = "farmais"
    DB_ENV_KEY = "DATABASE_URL_FARMAIS"


class PanvelDB(StoreDB):
    STORE_ID   = "panvel"
    DB_ENV_KEY = "DATABASE_URL_PANVEL"


class FarmaciasAppDB(StoreDB):
    STORE_ID   = "farmaciasapp"
    DB_ENV_KEY = "DATABASE_URL_FARMACIASAPP"


class FarmacondeDB(StoreDB):
    STORE_ID   = "farmaconde"
    DB_ENV_KEY = "DATABASE_URL_FARMACONDE"


class EualiriaDB(StoreDB):
    STORE_ID   = "eualiria"
    DB_ENV_KEY = "DATABASE_URL_EUALIRIA"


class AgillemedDB(StoreDB):
    STORE_ID   = "agillemed"
    DB_ENV_KEY = "DATABASE_URL_AGILLEMED"


class NovamedDB(StoreDB):
    STORE_ID   = "novamed"
    DB_ENV_KEY = "DATABASE_URL_NOVAMED"


class PharmedDB(StoreDB):
    STORE_ID   = "pharmed"
    DB_ENV_KEY = "DATABASE_URL_PHARMED"


class JustMedicamentosDB(StoreDB):
    STORE_ID   = "justmedicamentos"
    DB_ENV_KEY = "DATABASE_URL_JUSTMEDICAMENTOS"


class GhfarmaDB(StoreDB):
    STORE_ID   = "ghfarma"
    DB_ENV_KEY = "DATABASE_URL_GHFARMA"


class LevittaDB(StoreDB):
    STORE_ID   = "levitta"
    DB_ENV_KEY = "DATABASE_URL_LEVITTA"


class DinamicaDB(StoreDB):
    STORE_ID   = "dinamica"
    DB_ENV_KEY = "DATABASE_URL_DINAMICA"


class FacilitaDB(StoreDB):
    STORE_ID   = "facilita"
    DB_ENV_KEY = "DATABASE_URL_FACILITA"


class QualidocDB(StoreDB):
    STORE_ID   = "qualidoc"
    DB_ENV_KEY = "DATABASE_URL_QUALIDOC"


class MevofarmaDB(StoreDB):
    STORE_ID   = "mevofarma"
    DB_ENV_KEY = "DATABASE_URL_MEVOFARMA"


class OncoexpressoDB(StoreDB):
    STORE_ID   = "oncoexpresso"
    DB_ENV_KEY = "DATABASE_URL_ONCOEXPRESSO"


class OncoHealthMedicamentosDB(StoreDB):
    STORE_ID   = "oncohealthmedicamentos"
    DB_ENV_KEY = "DATABASE_URL_ONCOHEALTHMEDICAMENTOS"


class RemedDB(StoreDB):
    STORE_ID   = "remed"
    DB_ENV_KEY = "DATABASE_URL_REMED"


class LjOncoexpressDB(StoreDB):
    STORE_ID   = "lj_oncoexpress"
    DB_ENV_KEY = "DATABASE_URL_LJ_ONCOEXPRESS"


class CampeaDB(StoreDB):
    STORE_ID   = "campea"
    DB_ENV_KEY = "DATABASE_URL_CAMPEA"


class PachecoDB(StoreDB):
    STORE_ID   = "pacheco"
    DB_ENV_KEY = "DATABASE_URL_PACHECO"


class MundialDB(StoreDB):
    STORE_ID   = "mundial"
    DB_ENV_KEY = "DATABASE_URL_MUNDIAL"


class FastDB(StoreDB):
    STORE_ID   = "fast"
    DB_ENV_KEY = "DATABASE_URL_FAST"


class ProgoodsDB(StoreDB):
    STORE_ID   = "progoods"
    DB_ENV_KEY = "DATABASE_URL_PROGOODS"


class HeraDB(StoreDB):
    STORE_ID   = "hera"
    DB_ENV_KEY = "DATABASE_URL_HERA"


class AlianzaDB(StoreDB):
    STORE_ID   = "alianza"
    DB_ENV_KEY = "DATABASE_URL_ALIANZA"


class SingularDB(StoreDB):
    STORE_ID   = "singular"
    DB_ENV_KEY = "DATABASE_URL_SINGULAR"


class IntegralDB(StoreDB):
    STORE_ID   = "integral"
    DB_ENV_KEY = "DATABASE_URL_INTEGRAL"


class NisseiDB(StoreDB):
    STORE_ID   = "nissei"
    DB_ENV_KEY = "DATABASE_URL_NISSEI"


class VeraCruzDB(StoreDB):
    STORE_ID   = "veracruz"
    DB_ENV_KEY = "DATABASE_URL_VERACRUZ"


class FarmagertyDB(StoreDB):
    STORE_ID   = "farmagerty"
    DB_ENV_KEY = "DATABASE_URL_FARMAGERTY"


class FarmSaoPauloDB(StoreDB):
    STORE_ID   = "farmsaopaulo"
    DB_ENV_KEY = "DATABASE_URL_FARMSAOPAULO"


class SampharmaDB(StoreDB):
    STORE_ID   = "sampharma"
    DB_ENV_KEY = "DATABASE_URL_SAMPHARMA"


class DrogalDB(StoreDB):
    STORE_ID   = "drogal"
    DB_ENV_KEY = "DATABASE_URL_DROGAL"


class RedeSuperPopularDB(StoreDB):
    STORE_ID   = "redesuperpopular"
    DB_ENV_KEY = "DATABASE_URL_REDESUPERPOPULAR"


class SaoJoaoDB(StoreDB):
    STORE_ID   = "saojoao"
    DB_ENV_KEY = "DATABASE_URL_SAOJOAO"


class VenancioDB(StoreDB):
    STORE_ID   = "venancio"
    DB_ENV_KEY = "DATABASE_URL_VENANCIO"


class IndianaDB(StoreDB):
    STORE_ID   = "indiana"
    DB_ENV_KEY = "DATABASE_URL_INDIANA"


class GloboDB(StoreDB):
    STORE_ID   = "globo"
    DB_ENV_KEY = "DATABASE_URL_GLOBO"


class PermanenteDB(StoreDB):
    STORE_ID   = "permanente"
    DB_ENV_KEY = "DATABASE_URL_PERMANENTE"


class MinasBrasilDB(StoreDB):
    STORE_ID   = "minasbrasil"
    DB_ENV_KEY = "DATABASE_URL_MINASBRASIL"


class AnossaDrogariaDB(StoreDB):
    STORE_ID   = "anossadrogaria"
    DB_ENV_KEY = "DATABASE_URL_ANOSSADROGARIA"


class ModernaDB(StoreDB):
    STORE_ID   = "moderna"
    DB_ENV_KEY = "DATABASE_URL_MODERNA"


class SantaLuciaDB(StoreDB):
    STORE_ID   = "santalucia"
    DB_ENV_KEY = "DATABASE_URL_SANTALUCIA"


class AraujoDB(StoreDB):
    STORE_ID   = "araujo"
    DB_ENV_KEY = "DATABASE_URL_ARAUJO"


class CatarinenseDB(StoreDB):
    STORE_ID   = "catarinense"
    DB_ENV_KEY = "DATABASE_URL_CATARINENSE"


class CallfarmaDB(StoreDB):
    STORE_ID   = "callfarma"
    DB_ENV_KEY = "DATABASE_URL_CALLFARMA"


class DrogasmilDB(StoreDB):
    STORE_ID   = "drogasmil"
    DB_ENV_KEY = "DATABASE_URL_DROGASMIL"


class RosarioDB(StoreDB):
    STORE_ID   = "rosario"
    DB_ENV_KEY = "DATABASE_URL_ROSARIO"


# Registry used by the CLI
STORE_REGISTRY: Dict[str, type] = {
    "drogaleste":       DrogalesteDB,
    "drogasil":         DrogasilDB,
    "drogaraia":        DrogaraiaDB,
    "drogariasaopaulo": DrogariaSaoPauloDB,
    "ultrafarma":       UltrafarmDB,
    "paguemenos":       PagueMenosDB,
    "farmais":          FarmaisDB,
    "panvel":           PanvelDB,
    "farmaciasapp":     FarmaciasAppDB,
    "farmaconde":       FarmacondeDB,
    "eualiria":         EualiriaDB,
    "agillemed":        AgillemedDB,
    "novamed":          NovamedDB,
    "pharmed":          PharmedDB,
    "justmedicamentos": JustMedicamentosDB,
    "ghfarma":          GhfarmaDB,
    "levitta":          LevittaDB,
    "dinamica":         DinamicaDB,
    "facilita":         FacilitaDB,
    "qualidoc":         QualidocDB,
    "mevofarma":        MevofarmaDB,
    "oncoexpresso":     OncoexpressoDB,
    "oncohealthmedicamentos": OncoHealthMedicamentosDB,
    "remed":            RemedDB,
    "lj_oncoexpress":   LjOncoexpressDB,
    "campea":           CampeaDB,
    "pacheco":          PachecoDB,
    "mundial":          MundialDB,
    "fast":             FastDB,
    "progoods":         ProgoodsDB,
    "hera":             HeraDB,
    "alianza":          AlianzaDB,
    "singular":         SingularDB,
    "integral":         IntegralDB,
    "nissei":           NisseiDB,
    "veracruz":         VeraCruzDB,
    "farmagerty":       FarmagertyDB,
    "farmsaopaulo":     FarmSaoPauloDB,
    "sampharma":        SampharmaDB,
    "drogal":           DrogalDB,
    "redesuperpopular": RedeSuperPopularDB,
    "saojoao":          SaoJoaoDB,
    "venancio":         VenancioDB,
    "indiana":          IndianaDB,
    "globo":            GloboDB,
    "permanente":       PermanenteDB,
    # minasbrasil DROPPED 2026-08-21 (10h/59k, no bulk EAN source) — class kept dormant below
    "anossadrogaria":   AnossaDrogariaDB,
    "moderna":          ModernaDB,
    "santalucia":       SantaLuciaDB,
    "araujo":           AraujoDB,
    "catarinense":      CatarinenseDB,
    "callfarma":        CallfarmaDB,
    "drogasmil":        DrogasmilDB,
    "rosario":          RosarioDB,
}


# ──────────────────────────────────────────────────────────────────────────────
# Module-level export helper
# ──────────────────────────────────────────────────────────────────────────────

def export_all(output_dir: str = "exports") -> None:
    """Export offers + price_history from every registered store to CSV files."""
    for store_name, db_cls in STORE_REGISTRY.items():
        print(f"\n[{store_name}] connecting ...")
        try:
            db = db_cls()
            db.export(output_dir)
            db.close()
        except Exception as exc:
            print(f"  ERROR exporting {store_name}: {exc}")


_COMBINED_FIELDS = [
    "barcode", "price", "quantity", "store_name",
    "date_recorded", "notes", "product_url", "image_url", "product_name",
]


def export_all_together(output_dir: str = "exports") -> str:
    """
    Export all stores into a single combined CSV with the columns:
      barcode, price, quantity, store_name, date_recorded, notes,
      product_url, image_url, product_name

    Only rows with a non-empty EAN (barcode) are included.
    Returns the path of the written file.
    """
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    path = os.path.join(output_dir, f"all_stores_combined_{ts}.csv")

    total_rows = 0
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=_COMBINED_FIELDS)
        writer.writeheader()

        for store_name, db_cls in STORE_REGISTRY.items():
            print(f"  [{store_name}] connecting ...")
            try:
                db = db_cls()
                rows = db.query_combined_rows()
                writer.writerows(rows)
                total_rows += len(rows)
                print(f"  [{store_name}] {len(rows):,} rows")
                db.close()
            except Exception as exc:
                print(f"  [{store_name}] ERROR: {exc}")

    print(f"\nCombined export: {total_rows:,} total rows -> {path}")
    return path


# ──────────────────────────────────────────────────────────────────────────────
# CLI
#
#   push   <store> <csv_file>   — upsert a scraper CSV into the store's DB
#   export <store>              — dump offers + price_history to CSV
#   export-all                  — dump every registered store
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="DB manager for pharmacy scrapers.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m db.db_manager push drogaleste drogaleste_20260519.csv\n"
            "  python -m db.db_manager export drogasil\n"
            "  python -m db.db_manager export-all --dir exports/2026-05-19"
        ),
    )
    parser.add_argument("--env", default=".env", help=".env file path (default: .env)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # push
    p_push = sub.add_parser("push", help="Upsert a scraper CSV into the store's DB")
    p_push.add_argument("store", choices=list(STORE_REGISTRY))
    p_push.add_argument("csv_file")

    # export
    p_exp = sub.add_parser("export", help="Dump one store's tables to CSV")
    p_exp.add_argument("store", choices=list(STORE_REGISTRY))
    p_exp.add_argument("--dir", default="exports", dest="output_dir",
                       help="Output directory (default: exports/)")
    p_exp.add_argument("--tables", nargs="+", default=None,
                       metavar="TABLE",
                       help="Tables to export (default: offers price_history)")

    # export-all
    p_all = sub.add_parser("export-all", help="Dump every registered store's tables to CSV")
    p_all.add_argument("--dir", default="exports", dest="output_dir",
                       help="Output directory (default: exports/)")

    # export-all-together
    p_tog = sub.add_parser(
        "export-all-together",
        help="Single combined CSV with barcode/price/store/notes columns from all stores",
    )
    p_tog.add_argument("--dir", default="exports", dest="output_dir",
                       help="Output directory (default: exports/)")

    # prune
    p_prune = sub.add_parser(
        "prune",
        help="Delete old price_history rows (keeps each product's first price point)",
    )
    p_prune.add_argument("store", choices=list(STORE_REGISTRY) + ["all"])
    p_prune.add_argument("--days", type=int, default=180,
                         help="Delete history older than N days (default: 180)")

    # mark-stale
    p_stale = sub.add_parser(
        "mark-stale",
        help="Mark offers not seen in the last N hours as unavailable",
    )
    p_stale.add_argument("store", choices=list(STORE_REGISTRY) + ["all"])
    p_stale.add_argument("--hours", type=int, default=24,
                         help="Flip offers not updated in the last N hours (default: 24)")

    args = parser.parse_args()
    load_env(args.env)

    if args.cmd == "push":
        print(f"Loading {args.csv_file} ...")
        offers = load_csv(args.csv_file)
        print(f"  {len(offers):,} rows loaded")

        db = STORE_REGISTRY[args.store]()
        stats = db.save(offers)
        db.close()

        print(f"\nDone.")
        print(f"  Upserted to offers:     {stats['upserted']:,}")
        print(f"  New price_history rows: {stats['history_inserted']:,}")
        print(f"  Skipped (zero price):   {stats['skipped_zero']:,}")

    elif args.cmd == "export":
        db = STORE_REGISTRY[args.store]()
        paths = db.export(args.output_dir, args.tables)
        db.close()
        print(f"\nExported {len(paths)} file(s) to {args.output_dir}/")

    elif args.cmd == "export-all":
        export_all(args.output_dir)
        print(f"\nAll stores exported to {args.output_dir}/")

    elif args.cmd == "export-all-together":
        export_all_together(args.output_dir)

    elif args.cmd == "prune":
        stores = list(STORE_REGISTRY) if args.store == "all" else [args.store]
        for store in stores:
            print(f"[{store}] pruning history older than {args.days} days ...")
            try:
                db = STORE_REGISTRY[store]()
                db.prune_history(args.days)
                db.close()
            except Exception as exc:
                print(f"[{store}] ERROR: {exc}")

    elif args.cmd == "mark-stale":
        stores = list(STORE_REGISTRY) if args.store == "all" else [args.store]
        for store in stores:
            print(f"[{store}] marking offers not seen in {args.hours}h as unavailable ...")
            try:
                db = STORE_REGISTRY[store]()
                db.mark_stale_unavailable(args.hours)
                db.close()
            except Exception as exc:
                print(f"[{store}] ERROR: {exc}")
