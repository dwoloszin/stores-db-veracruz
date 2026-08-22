"""
main.py — Run all store scrapers in parallel.

Each store runs as an independent subprocess so memory is fully isolated
and output goes to a dedicated log file. The console only shows
start / done / error lines per store, keeping it readable.

Usage:
    python -m main                          # run all stores
    python -m main --limit 100              # test run (100 products per store)
    python -m main --stores drogasil        # run one store
    python -m main --enrich-ean             # scrape + fetch EAN for stores that need it
    python -m main --enrich-only            # fetch EAN only (no re-scrape)
    python -m main --csv                    # also export CSV after each scrape
    python -m main --workers 20             # EAN enrichment threads
    python -m main --env .env.prod          # custom .env path
"""

import argparse
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ──────────────────────────────────────────────────────────────────────────────
# Store registry — base command per store
# ──────────────────────────────────────────────────────────────────────────────

_STORES: Dict[str, List[str]] = {
    "drogaleste":       [sys.executable, "-m", "markets.drogaleste.scraper_drogaleste"],
    "drogasil":         [sys.executable, "-m", "markets.drogasil.scraper_drogasil"],
    "drogaraia":        [sys.executable, "-m", "markets.drogaraia.scraper_drogaraia"],
    "farmaconde":       [sys.executable, "-m", "markets.farmaconde.scraper_farmaconde"],
    "drogariasaopaulo": [sys.executable, "-m", "markets.drogariasaopaulo.scraper_drogariasaopaulo"],
    "ultrafarma":       [sys.executable, "-m", "markets.ultrafarma.scraper_ultrafarma"],
    "paguemenos":       [sys.executable, "-m", "markets.paguemenos.scraper_paguemenos"],
    "farmais":          [sys.executable, "-m", "markets.farmais.scraper_farmais"],
    "panvel":           [sys.executable, "-m", "markets.panvel.scraper_panvel"],
    "farmaciasapp":     [sys.executable, "-m", "markets.farmaciasapp.scraper_farmaciasapp"],
    "eualiria":         [sys.executable, "-m", "markets.eualiria.scraper_eualiria"],
    "agillemed":        [sys.executable, "-m", "markets.agillemed.scraper_agillemed"],
    "novamed":          [sys.executable, "-m", "markets.novamed.scraper_novamed"],
    "pharmed":          [sys.executable, "-m", "markets.pharmed.scraper_pharmed"],
    "justmedicamentos": [sys.executable, "-m", "markets.justmedicamentos.scraper_justmedicamentos"],
    "ghfarma":          [sys.executable, "-m", "markets.ghfarma.scraper_ghfarma"],
    "levitta":          [sys.executable, "-m", "markets.levitta.scraper_levitta"],
    "dinamica":         [sys.executable, "-m", "markets.dinamica.scraper_dinamica"],
    "facilita":         [sys.executable, "-m", "markets.facilita.scraper_facilita"],
    "qualidoc":         [sys.executable, "-m", "markets.qualidoc.scraper_qualidoc"],
    "mevofarma":        [sys.executable, "-m", "markets.mevofarma.scraper_mevofarma"],
    "oncoexpresso":     [sys.executable, "-m", "markets.oncoexpresso.scraper_oncoexpresso"],
    "oncohealthmedicamentos": [sys.executable, "-m", "markets.oncohealthmedicamentos.scraper_oncohealthmedicamentos"],
    "remed":            [sys.executable, "-m", "markets.remed.scraper_remed"],
    "lj_oncoexpress":   [sys.executable, "-m", "markets.lj_oncoexpress.scraper_lj_oncoexpress"],
    "campea":           [sys.executable, "-m", "markets.campea.scraper_campea"],
    "pacheco":          [sys.executable, "-m", "markets.pacheco.scraper_pacheco"],
    "mundial":          [sys.executable, "-m", "markets.mundial.scraper_mundial"],
    "fast":             [sys.executable, "-m", "markets.fast.scraper_fast"],
    "progoods":         [sys.executable, "-m", "markets.progoods.scraper_progoods"],
    "hera":             [sys.executable, "-m", "markets.hera.scraper_hera"],
    "alianza":          [sys.executable, "-m", "markets.alianza.scraper_alianza"],
    "singular":         [sys.executable, "-m", "markets.singular.scraper_singular"],
    "integral":         [sys.executable, "-m", "markets.integral.scraper_integral"],
    "nissei":           [sys.executable, "-m", "markets.nissei.scraper_nissei"],
    "veracruz":         [sys.executable, "-m", "markets.veracruz.scraper_veracruz"],
    "farmagerty":       [sys.executable, "-m", "markets.farmagerty.scraper_farmagerty"],
    "farmsaopaulo":     [sys.executable, "-m", "markets.farmsaopaulo.scraper_farmsaopaulo"],
    "sampharma":        [sys.executable, "-m", "markets.sampharma.scraper_sampharma"],
    "drogal":           [sys.executable, "-m", "markets.drogal.scraper_drogal"],
    "redesuperpopular": [sys.executable, "-m", "markets.redesuperpopular.scraper_redesuperpopular"],
    "saojoao":          [sys.executable, "-m", "markets.saojoao.scraper_saojoao"],
    "venancio":         [sys.executable, "-m", "markets.venancio.scraper_venancio"],
    "indiana":          [sys.executable, "-m", "markets.indiana.scraper_indiana"],
    "globo":            [sys.executable, "-m", "markets.globo.scraper_globo"],
    "permanente":       [sys.executable, "-m", "markets.permanente.scraper_permanente"],
    "anossadrogaria":   [sys.executable, "-m", "markets.anossadrogaria.scraper_anossadrogaria"],
    "moderna":          [sys.executable, "-m", "markets.moderna.scraper_moderna"],
    "santalucia":       [sys.executable, "-m", "markets.santalucia.scraper_santalucia"],
    "araujo":           [sys.executable, "-m", "markets.araujo.scraper_araujo"],
    "catarinense":      [sys.executable, "-m", "markets.catarinense.scraper_catarinense"],
    "callfarma":        [sys.executable, "-m", "markets.callfarma.scraper_callfarma"],
}

# Stores that share a rate-limited host must NOT run at the same time, or the
# shared WAF throttles/blocks the IP (it trips on cumulative load — the biggest
# store then fails mid-run). Stores mapped to the same group run one at a time;
# different groups and ungrouped stores still run fully in parallel. The three
# Convertiez pharmacies all sit behind the same Cloudflare/Convertiez WAF.
_SERIAL_GROUPS: Dict[str, str] = {
    "campea":       "convertiez",
    "veracruz":     "convertiez",
    "farmsaopaulo": "convertiez",
}

# Stores whose EAN must be enriched from product pages after scraping
_EAN_ENRICHERS: Dict[str, List[str]] = {
    "drogasil":  [sys.executable, "-m", "markets.drogasil.enrich_ean_drogasil"],
    "drogaraia": [sys.executable, "-m", "markets.drogaraia.enrich_ean_drogaraia"],
    "farmaconde": [sys.executable, "-m", "markets.farmaconde.enrich_ean_farmaconde"],
    "ultrafarma":[sys.executable, "-m", "markets.ultrafarma.enrich_ean_ultrafarma"],
    "panvel":    [sys.executable, "-m", "markets.panvel.enrich_ean_panvel"],
    "eualiria":  [sys.executable, "-m", "markets.eualiria.enrich_ean_eualiria"],
    "novamed":   [sys.executable, "-m", "markets.novamed.enrich_ean_novamed"],
    "pharmed":   [sys.executable, "-m", "markets.pharmed.enrich_ean_pharmed"],
    "dinamica":  [sys.executable, "-m", "markets.dinamica.enrich_ean_dinamica"],
    "facilita":  [sys.executable, "-m", "markets.facilita.enrich_ean_facilita"],
}


def _build_cmd(store: str, args: argparse.Namespace) -> List[str]:
    cmd = _STORES[store].copy()
    if args.limit:
        cmd += ["--limit", str(args.limit)]
    if args.env != ".env":
        cmd += ["--env", args.env]
    if args.csv:
        cmd += ["--csv"]
    return cmd


def _build_enrich_cmd(store: str, args: argparse.Namespace) -> List[str]:
    cmd = _EAN_ENRICHERS[store].copy()
    if args.env != ".env":
        cmd += ["--env", args.env]
    if args.workers != 12:
        cmd += ["--workers", str(args.workers)]
    return cmd


# ──────────────────────────────────────────────────────────────────────────────
# Per-store runner (executed in a thread)
# ──────────────────────────────────────────────────────────────────────────────

def _run_store(
    store:    str,
    cmd:      List[str],
    log_path: Path,
    results:  Dict[str, bool],
    durations: Dict[str, float],
    starts: Dict[str, float],
    lock: threading.Lock,
    log_enabled: bool = False,
    group_lock: Optional[threading.Lock] = None,
) -> None:
    # Serial-group gate: if this store shares a WAF group with another running
    # store, wait until the group frees. Time spent waiting is NOT counted as run
    # time — t0 is taken only after the lock is held — so durations/ETA stay honest.
    if group_lock is not None and not group_lock.acquire(blocking=False):
        _log(store, "queued (serial group busy - starts when the group frees) ...")
        group_lock.acquire()
    try:
        t0 = time.time()
        with lock:
            starts[store] = t0
        if log_enabled:
            _log(store, f"started  (log -> {log_path.name})")
        else:
            _log(store, "started")

        try:
            if log_enabled:
                with log_path.open("w", encoding="utf-8") as lf:
                    proc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, text=True)
            else:
                proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            elapsed = time.time() - t0
            ok = proc.returncode == 0
            with lock:
                results[store] = ok
                durations[store] = elapsed
                starts.pop(store, None)
            status = "done" if ok else f"FAILED (exit {proc.returncode})"
            _log(store, f"{status}  [{elapsed / 60:.1f} min]")

            if not ok:
                if log_enabled:
                    _tail(log_path, lines=30)
                else:
                    _log(store, "  (re-run with --log to see error details)")

        except Exception as exc:
            elapsed = time.time() - t0
            with lock:
                results[store] = False
                durations[store] = elapsed
                starts.pop(store, None)
            _log(store, f"ERROR: {exc}")
            import traceback
            traceback.print_exc()
    finally:
        if group_lock is not None:
            group_lock.release()


def _log(store: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{store}] {msg}", flush=True)


def _tail(log_path: Path, lines: int = 30) -> None:
    """Print the last N lines of a log file (shown on failure)."""
    try:
        with log_path.open(encoding="utf-8") as f:
            tail = f.readlines()[-lines:]
        print(f"\n--- last {lines} lines of {log_path.name} ---")
        for line in tail:
            print(f"  {line}", end="")
        print("--- end ---\n")
    except Exception:
        pass


def _fmt_minutes(seconds: float) -> str:
    return f"{seconds / 60:.1f} min"


def _phase_monitor(
    *,
    phase_name: str,
    stores: List[str],
    results: Dict[str, bool],
    durations: Dict[str, float],
    starts: Dict[str, float],
    log_paths: Dict[str, Path],
    lock: threading.Lock,
    phase_start: float,
    done_event: threading.Event,
) -> None:
    """Print coarse progress and ETA while a phase is running."""
    while not done_event.wait(timeout=15):
        with lock:
            total = len(stores)
            completed = len(results)
            running = [s for s in stores if s in starts and s not in results]
            completed_durations = [durations[s] for s in stores if s in durations]

        remaining = max(total - completed, 0)
        elapsed = time.time() - phase_start

        eta_msg = "ETA: unknown"
        now = time.time()

        # Best ETA source: parse live progress percentages from store logs.
        running_eta: List[float] = []
        progress_signals = 0
        for store in running:
            pct = _extract_progress_percent(log_paths.get(store))
            if pct is None or pct <= 0:
                continue
            start_ts = starts.get(store)
            if start_ts is None:
                continue
            elapsed_store = max(now - start_ts, 1.0)
            remaining_store = elapsed_store * (100.0 - pct) / pct
            running_eta.append(remaining_store)
            progress_signals += 1

        if running_eta:
            eta_seconds = max(running_eta)  # phase ends when the slowest running store completes
            eta_msg = f"ETA: ~{_fmt_minutes(eta_seconds)} (source=logs {progress_signals}/{len(running)})"
        elif completed > 0 and remaining > 0:
            # Dynamic fallback: updates every monitor tick and reflects slow tail behavior.
            eta_seconds = elapsed * (remaining / completed)
            eta_msg = f"ETA: ~{_fmt_minutes(eta_seconds)} (source=throughput)"
        elif remaining == 0:
            eta_msg = "ETA: complete"
        elif elapsed >= 60 and remaining > 0:
            # Soft fallback after warm-up period to avoid long "unknown" windows.
            eta_msg = f"ETA: >{_fmt_minutes(elapsed * 0.5)} (source=warmup)"

        running_preview = ", ".join(running[:3])
        if len(running) > 3:
            running_preview += ", ..."
        if not running_preview:
            running_preview = "none"

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"[{phase_name}] progress {completed}/{total} done | "
            f"running: {running_preview} | elapsed: {_fmt_minutes(elapsed)} | {eta_msg}",
            flush=True,
        )


def _run_phase(
    *,
    phase_name: str,
    stores: List[str],
    build_cmd,
    args: argparse.Namespace,
    log_dir: Path,
    ts: str,
    suffix: str = "",
) -> Tuple[Dict[str, bool], Dict[str, float], float]:
    """Run one parallel phase (scrape or enrich) and return status + durations + wall."""
    if not stores:
        return {}, {}, 0.0

    log_enabled: bool = getattr(args, "log", False)
    print(f"\n{phase_name.upper()} phase: running {len(stores)} store(s): {', '.join(stores)}")

    results: Dict[str, bool] = {}
    durations: Dict[str, float] = {}
    starts: Dict[str, float] = {}
    log_paths: Dict[str, Path] = {}
    lock = threading.Lock()
    threads: List[threading.Thread] = []

    # One lock per serial group present in this run; stores sharing a group run
    # one at a time (see _SERIAL_GROUPS), everything else stays parallel.
    group_locks: Dict[str, threading.Lock] = {}
    serialized = {s: _SERIAL_GROUPS[s] for s in stores if s in _SERIAL_GROUPS}
    if serialized:
        for grp in sorted(set(serialized.values())):
            members = [s for s in stores if serialized.get(s) == grp]
            if len(members) > 1:
                print(f"  serial group '{grp}': {', '.join(members)} will run one at a time")

    for store in stores:
        cmd = build_cmd(store, args)
        log_name = f"{store}{suffix}_{ts}.log"
        log_path = log_dir / log_name
        log_paths[store] = log_path
        grp = _SERIAL_GROUPS.get(store)
        glock = group_locks.setdefault(grp, threading.Lock()) if grp else None
        t = threading.Thread(
            target=_run_store,
            args=(store, cmd, log_path, results, durations, starts, lock),
            kwargs={"log_enabled": log_enabled, "group_lock": glock},
            name=f"{phase_name}:{store}",
            daemon=False,
        )
        threads.append(t)

    phase_start = time.time()
    done_event = threading.Event()
    monitor = threading.Thread(
        target=_phase_monitor,
        kwargs={
            "phase_name": phase_name,
            "stores": stores,
            "results": results,
            "durations": durations,
            "starts": starts,
            "log_paths": log_paths,
            "lock": lock,
            "phase_start": phase_start,
            "done_event": done_event,
        },
        name=f"monitor:{phase_name}",
        daemon=True,
    )

    for t in threads:
        t.start()
    monitor.start()
    for t in threads:
        t.join()

    done_event.set()
    monitor.join(timeout=1)

    wall = time.time() - phase_start
    _print_phase_summary(phase_name=phase_name, stores=stores, results=results, durations=durations, wall=wall, ts=ts, suffix=suffix)
    return results, durations, wall


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run all pharmacy scrapers in parallel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m main\n"
            "  python -m main --limit 100\n"
            "  python -m main --enrich-only\n"
            "  python -m main --stores drogasil drogariasaopaulo\n"
            "  python -m main --enrich-ean --workers 20"
        ),
    )
    parser.add_argument(
        "--stores", nargs="+", default=None,
        choices=list(_STORES),
        metavar="STORE",
        help=f"Stores to run (default: all). Choices: {', '.join(_STORES)}",
    )
    parser.add_argument("--limit",       type=int,  default=None,  help="Stop after N products per store (test)")
    parser.add_argument("--enrich-ean",  action="store_true",       help="Fetch EAN after scraping (drogasil, ultrafarma, panvel)")
    parser.add_argument("--enrich-only", action="store_true",       help="Skip scrape — run EAN enrichment only")
    parser.add_argument("--workers",     type=int,  default=12,    help="EAN enrichment threads (default: 12)")
    parser.add_argument("--csv",         action="store_true",       help="Also export a CSV file after each scrape")
    parser.add_argument("--env",         type=str,  default=".env", help=".env file path (default: .env)")
    parser.add_argument("--log",         action="store_true",       help="Write per-store log files to logs/ (default: off)")
    parser.add_argument("--only-stale",  action="store_true", dest="only_stale",
                        help="Only run stores whose Neon data is older than --stale-hours "
                             "(local self-heal for stores GitHub can't refresh, e.g. Convertiez)")
    parser.add_argument("--stale-hours", type=int, default=24, dest="stale_hours",
                        help="Staleness threshold in hours for --only-stale (default: 24)")
    args = parser.parse_args()

    stores = args.stores or list(_STORES)
    global_start = time.time()

    # Load env so DB_URL vars are available to subprocesses via inherited environment
    _load_env(args.env)

    # ── stale filter ──────────────────────────────────────────────────────────
    # Keep only stores whose Neon offers.updated_at is missing or older than the
    # threshold. Lets a local `python -m main --only-stale` refresh exactly the
    # stores that fell behind (blocked from GitHub, or a failed cloud run) while
    # skipping the ones the cloud already keeps fresh. `--stores X` still forces X.
    if args.only_stale:
        stores = _filter_stale(stores, args.stale_hours)
        if not stores:
            print(f"All selected stores are fresh (< {args.stale_hours}h). Nothing to do.")
            return

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    # ── enrich-only mode ──────────────────────────────────────────────────────
    if args.enrich_only:
        enrich_stores = [s for s in stores if s in _EAN_ENRICHERS]
        if not enrich_stores:
            print("No stores in the selection have an EAN enricher.")
            print(f"Stores with enrichers: {', '.join(_EAN_ENRICHERS)}")
            sys.exit(0)

        print(f"\nEAN enrichment only for: {', '.join(enrich_stores)}")
        enrich_results, _, _ = _run_phase(
            phase_name="enrich",
            stores=enrich_stores,
            build_cmd=_build_enrich_cmd,
            args=args,
            log_dir=log_dir,
            ts=ts,
            suffix="_enrich",
        )
        _print_global_summary(
            scrape_wall=None,
            enrich_wall=None,
            global_wall=time.time() - global_start,
            did_scrape=False,
            did_enrich=True,
        )

        if any(not enrich_results.get(s) for s in enrich_stores):
            sys.exit(1)
        return

    # ── normal scrape mode ────────────────────────────────────────────────────
    print(f"\nRunning {len(stores)} store(s) in parallel: {', '.join(stores)}")
    if args.limit:
        print(f"Test mode: --limit {args.limit}")
    print()

    scrape_results, _, scrape_wall = _run_phase(
        phase_name="scrape",
        stores=stores,
        build_cmd=_build_cmd,
        args=args,
        log_dir=log_dir,
        ts=ts,
        suffix="",
    )

    enrich_wall: Optional[float] = None
    enrich_results: Dict[str, bool] = {}
    if args.enrich_ean:
        enrich_stores = [s for s in stores if s in _EAN_ENRICHERS and scrape_results.get(s)]
        skipped = [s for s in stores if s in _EAN_ENRICHERS and not scrape_results.get(s)]

        if enrich_stores:
            print(f"\nStarting EAN enrichment after scrape for: {', '.join(enrich_stores)}")
            enrich_results, _, enrich_wall = _run_phase(
                phase_name="enrich",
                stores=enrich_stores,
                build_cmd=_build_enrich_cmd,
                args=args,
                log_dir=log_dir,
                ts=ts,
                suffix="_enrich",
            )
        else:
            print("\nEAN enrichment skipped: no successful scrape stores with enrichers.")

        if skipped:
            print(f"Skipped enrichment (scrape failed): {', '.join(skipped)}")

    _print_global_summary(
        scrape_wall=scrape_wall,
        enrich_wall=enrich_wall,
        global_wall=time.time() - global_start,
        did_scrape=True,
        did_enrich=args.enrich_ean,
    )

    if any(not scrape_results.get(s) for s in stores):
        sys.exit(1)
    if args.enrich_ean and any(not enrich_results.get(s) for s in enrich_results):
        sys.exit(1)


def _print_phase_summary(
    phase_name: str,
    stores:  List[str],
    results: Dict[str, bool],
    durations: Dict[str, float],
    wall:    float,
    ts:      str,
    suffix: str,
) -> None:
    ok     = [s for s in stores if results.get(s)]
    failed = [s for s in stores if not results.get(s)]

    print(f"\n{'='*55}")
    print(f"  {phase_name.upper()} finished in {_fmt_minutes(wall)}")
    print("  Individual store times:")
    for s in stores:
        status = "OK" if results.get(s) else "FAILED"
        d = durations.get(s, 0.0)
        print(f"    - {s:<16} {status:<6} {_fmt_minutes(d)}")
    print(f"  OK:     {', '.join(ok) if ok else 'none'}")
    if failed:
        print(f"  FAILED: {', '.join(failed)}")
        print(f"  Logs:   logs/<store>{suffix}_{ts}.log")
    print(f"{'='*55}\n")


def _print_global_summary(
    *,
    scrape_wall: Optional[float],
    enrich_wall: Optional[float],
    global_wall: float,
    did_scrape: bool,
    did_enrich: bool,
) -> None:
    print(f"\n{'='*55}")
    print("  GLOBAL timing")
    if did_scrape and scrape_wall is not None:
        print(f"  Scrape phase wall time: {_fmt_minutes(scrape_wall)}")
    if did_enrich:
        if enrich_wall is None:
            print("  Enrich phase wall time: n/a")
        else:
            print(f"  Enrich phase wall time: {_fmt_minutes(enrich_wall)}")
    print(f"  Total process wall time (start -> finish): {_fmt_minutes(global_wall)}")
    print(f"{'='*55}\n")


def _extract_progress_percent(log_path: Optional[Path]) -> Optional[float]:
    """
    Best-effort parse of progress percentage from scraper/enricher logs.
    Looks for values like "( 35.0%)" or "100.0%" and returns the latest.
    """
    if not log_path or not log_path.exists():
        return None
    try:
        with log_path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()[-200:]
    except Exception:
        return None

    latest: Optional[float] = None
    for line in lines:
        for m in re.finditer(r"(\d{1,3}(?:\.\d+)?)%", line):
            try:
                val = float(m.group(1))
            except ValueError:
                continue
            if 0.0 < val <= 100.0:
                latest = val
    return latest


def _filter_stale(stores: List[str], hours: int) -> List[str]:
    """Return the subset of `stores` whose Neon offers.updated_at is missing or
    older than `hours`. A store is considered stale (and kept) if its DB has no
    rows, is unreachable, or has no registered env key — i.e. anything that means
    "needs a local run". Fresh stores are skipped. Requires env already loaded."""
    from datetime import datetime, timezone
    from db.db_manager import STORE_REGISTRY

    print(f"\nChecking Neon freshness (threshold {hours}h) for {len(stores)} store(s) ...")
    now = datetime.now(timezone.utc)
    stale: List[str] = []
    for store in stores:
        cls = STORE_REGISTRY.get(store)
        if cls is None or not os.environ.get(cls.DB_ENV_KEY):
            print(f"  {store:<22} no DB key — SKIP (can't run)")
            continue
        db = None
        try:
            db = cls()
            last = db.last_update()
        except Exception as exc:
            print(f"  {store:<22} DB error ({exc.__class__.__name__}) — STALE (will run)")
            stale.append(store)
            continue
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass
        if last is None:
            print(f"  {store:<22} empty — STALE (will run)")
            stale.append(store)
            continue
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age_h = (now - last).total_seconds() / 3600
        if age_h >= hours:
            print(f"  {store:<22} {age_h:5.1f}h old — STALE (will run)")
            stale.append(store)
        else:
            print(f"  {store:<22} {age_h:5.1f}h old — fresh, skip")
    print(f"Stale stores to run: {len(stale)}"
          + (f" ({', '.join(stale)})" if stale else ""))
    return stale


def _load_env(path: str = ".env") -> None:
    """Load .env into os.environ so subprocesses inherit the vars."""
    try:
        with open(path, encoding="latin-1") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip(); v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    main()
