#!/usr/bin/env bash
# End-of-day paper-trading report: which KOL and which PATH (public-pending
# vs private-confirmed) was most profitable. Reads the two separate ledgers.
#
#   scripts/eod-report.sh               # last 24h (UTC)
#   scripts/eod-report.sh --day 2026-05-19   # a specific UTC day
#   scripts/eod-report.sh --all         # whole history
set -uo pipefail
exec python3 - "$@" <<'PY'
import csv, sys, time, datetime, os

PUB = "/data/bsc-meme-mev/trader/closed_trades.csv"
PRV = "/data/bsc-meme-mev/trader_private/closed_trades.csv"

args = sys.argv[1:]
mode = ("all" if "--all" in args else
        ("day" if "--day" in args else "24h"))
day = args[args.index("--day")+1] if mode == "day" else None
now = time.time()

def in_window(ts_ns):
    t = ts_ns / 1e9
    if mode == "all":
        return True
    if mode == "24h":
        return (now - t) <= 86400
    d = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")
    return d == day

def load(path, label):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                ts = int(r["ts_unix_ns"])
            except (ValueError, KeyError):
                continue
            if not in_window(ts):
                continue
            r["_path"] = label
            rows.append(r)
    return rows

rows = load(PUB, "public") + load(PRV, "private")
if not rows:
    print(f"no closed trades in window ({mode}{' '+day if day else ''})")
    sys.exit(0)

REAL = lambda r: r.get("close_reason") != "price_unavailable"  # exclude unpriced

def agg(rs):
    rs = [r for r in rs if REAL(r)]
    n = len(rs)
    pnl_bnb = sum(float(r["pnl_bnb"]) for r in rs)
    pnl_usd = sum(float(r["pnl_usd"]) for r in rs)
    wins = sum(1 for r in rs if float(r["pnl_pct"]) > 0)
    wr = (wins / n * 100) if n else 0.0
    return n, pnl_bnb, pnl_usd, wr

_utcnow = datetime.datetime.now(datetime.timezone.utc)
hdr = f"=== EOD REPORT  ({mode}{' '+day if day else ''})  {_utcnow:%Y-%m-%d %H:%M}Z ==="
print(hdr)
unpriced = sum(1 for r in rows if not REAL(r))
print(f"rows={len(rows)}  excluded(price_unavailable)={unpriced}\n")

print(f"{'PATH':9} {'trades':>6} {'net BNB':>12} {'net USD':>10} {'winrate':>8}")
print("-" * 50)
best_path = None
for path in ("public", "private"):
    n, b, u, wr = agg([r for r in rows if r["_path"] == path])
    print(f"{path:9} {n:6d} {b:12.5f} {u:10.2f} {wr:7.0f}%")
    if best_path is None or u > best_path[1]:
        best_path = (path, u)

print(f"\n{'KOL':4} {'PATH':8} {'trades':>6} {'net BNB':>11} {'net USD':>9} {'win':>5}")
print("-" * 52)
per = {}
for r in rows:
    if not REAL(r):
        continue
    k = (r["kol_name"], r["_path"])
    per.setdefault(k, []).append(r)
ranked = []
for (kol, path), rs in per.items():
    n, b, u, wr = agg(rs)
    ranked.append((u, kol, path, n, b, wr))
ranked.sort(reverse=True)
for u, kol, path, n, b, wr in ranked:
    print(f"{kol:4} {path:8} {n:6d} {b:11.5f} {u:9.2f} {wr:4.0f}%")

if ranked:
    top = ranked[0]
    print(f"\n🏆 best KOL×path: {top[1]} via {top[2]}  (${top[0]:.2f}, {top[3]} trades, {top[5]:.0f}% win)")
print(f"🥇 best PATH overall: {best_path[0]}  (${best_path[1]:.2f})")
PY
