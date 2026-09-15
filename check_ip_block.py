"""
check_ip_block.py — diagnose the Convertiez WAF block (campea / veracruz /
farmsaopaulo) on THIS connection, per store.

The 3 Convertiez stores sit behind a WAF that IP-bans by reputation, PER SITE:
the same IP can pass one store and be blocked on another (mobile/CGNAT IPs are
shared and often pre-flagged). This probe hits each store a few times and
classifies the response so you know WHICH fix applies:

    OK          -> reachable, just run it
    BLOCK1      -> hard IP block ({"message":"block1"}); only a different/clean IP helps
    CHALLENGE   -> JS/Cloudflare challenge page; a headless real browser could pass
    CONN-DROP   -> connection reset (also an IP block, different WAF layer)
    RATE(429)   -> rate limited; slowing down / retry helps

A store that is OK on some tries and BLOCK1 on others = probabilistic -> retry
helps. Consistently BLOCK1 = hard block -> need a clean IP (residential proxy).

Usage:
    python check_ip_block.py                      # test the current connection
    python check_ip_block.py --proxy http://user:pass@host:port   # test THROUGH a proxy
    python check_ip_block.py --tries 5
    # exit 0 if every store is reachable at least once, else 1
"""
import argparse
import sys
import requests

BR = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
SITES = {
    "campea":       "https://www.drogariascampea.com.br/",
    "veracruz":     "https://www.drogariaveracruz.com.br/",
    "farmsaopaulo": "https://www.farmaciassaopaulo.com.br/",   # NB: double-s domain (slug is single-s)
}


def classify(r: requests.Response) -> str:
    body = (r.text or "")[:400].lower()
    if r.status_code == 200 and "block1" not in body:
        # a 200 that is actually a challenge interstitial is not real content
        if "just a moment" in body or "cf-challenge" in body or "challenge-platform" in body:
            return "CHALLENGE"
        return "OK"
    if "block1" in body:
        return "BLOCK1"
    if r.status_code == 429:
        return "RATE(429)"
    if r.status_code in (403, 503) and ("just a moment" in body or "challenge" in body):
        return "CHALLENGE"
    return f"HTTP{r.status_code}"


def probe(url: str, proxies, tries: int):
    outcomes = []
    for _ in range(tries):
        try:
            r = requests.get(url, headers={"User-Agent": BR}, timeout=25,
                             proxies=proxies, allow_redirects=True)
            outcomes.append(classify(r))
        except requests.RequestException as e:
            name = type(e).__name__
            outcomes.append("CONN-DROP" if "Conn" in name or "Timeout" in name else f"ERR:{name}")
    return outcomes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--proxy", help="route through this proxy (http://user:pass@host:port)")
    ap.add_argument("--tries", type=int, default=3, help="requests per store (detect probabilistic blocks)")
    args = ap.parse_args()

    proxies = {"http": args.proxy, "https": args.proxy} if args.proxy else None

    try:
        ip = requests.get("https://api.ipify.org", timeout=10, proxies=proxies).text.strip()
    except requests.RequestException:
        ip = "?"
    print(f"public IP{' (via proxy)' if proxies else ''}: {ip}   tries/store: {args.tries}\n")

    reachable = []
    for name, url in SITES.items():
        outs = probe(url, proxies, args.tries)
        oks = outs.count("OK")
        verdict = "OK" if oks == args.tries else ("PROBABILISTIC" if oks else "BLOCKED")
        if oks:
            reachable.append(name)
        print(f"  {name:14} {verdict:14} {oks}/{args.tries} OK   [{', '.join(outs)}]")

    print()
    if len(reachable) == len(SITES):
        print(f"-> all reachable. Run:  python -m main --stores {' '.join(SITES)}")
        return 0
    if reachable:
        print(f"-> only reachable: {', '.join(reachable)}")
        print(f"   Run just those:  python -m main --stores {' '.join(reachable)}")
        print(f"   For the rest: switch to a cleaner IP (or --proxy a residential proxy) and re-run.")
        return 1
    print("-> all blocked on this IP. Switch connection / use a residential proxy and re-run.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
