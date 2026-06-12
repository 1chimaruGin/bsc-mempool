#!/usr/bin/env bash
# LIVE paper-trading dashboard: public vs private path, per-KOL, open
# positions, today's realised PnL. Auto-refreshes. Pure local files
# (closed_trades.csv + open_positions.json) — zero node/NodeReal load.
#
#   scripts/live-report.sh            # refresh every 15s, "today" (UTC)
#   scripts/live-report.sh 30         # refresh every 30s
#   scripts/live-report.sh 15 --all   # all-time realised instead of today
set -uo pipefail
exec python3 - "$@" <<'PY'
import csv, json, os, sys, time, datetime

ARGS = sys.argv[1:]
INTERVAL = next((int(a) for a in ARGS if a.isdigit()), 15)
ALL = "--all" in ARGS

PATHS = {
    "public":  ("/data/bsc-meme-mev/trader/closed_trades.csv",
                "/data/bsc-meme-mev/trader/open_positions.json"),
    "private": ("/data/bsc-meme-mev/trader_private/closed_trades.csv",
                "/data/bsc-meme-mev/trader_private/open_positions.json"),
}
REAL = lambda r: r.get("close_reason") != "price_unavailable"

def today_utc():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

def load_closed(path):
    out = []
    if not os.path.exists(path):
        return out
    try:
        with open(path) as f:
            for r in csv.DictReader(f):
                out.append(r)
    except Exception:
        pass
    return out

def load_open(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            d = json.load(f)
        return d if isinstance(d, list) else []
    except Exception:
        return []

def f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d

def wei(x):
    # bnb_in is a serialised U256 — hex ("0x..") or decimal string/int.
    s = str(x).strip()
    try:
        v = int(s, 16) if s.lower().startswith("0x") else int(s)
        return v / 1e18
    except (TypeError, ValueError):
        return f(x)

def render():
    day = today_utc()
    lines = []
    now = datetime.datetime.now(datetime.timezone.utc)
    scope = "ALL-TIME" if ALL else f"TODAY {day}"
    lines.append(f"\x1b[1mLIVE PAPER REPORT\x1b[0m  {now:%H:%M:%S}Z  "
                 f"scope={scope}  (refresh {INTERVAL}s, Ctrl-C quit)")
    lines.append("=" * 64)

    path_tot = {}
    per_kol = {}
    open_rows = []
    for pth, (csvp, jsonp) in PATHS.items():
        rs = []
        for r in load_closed(csvp):
            if not REAL(r):
                continue
            if not ALL:
                try:
                    d = datetime.datetime.fromtimestamp(
                        int(r["ts_unix_ns"]) / 1e9, datetime.timezone.utc
                    ).strftime("%Y-%m-%d")
                    if d != day:
                        continue
                except (KeyError, ValueError):
                    continue
            rs.append(r)
        n = len(rs)
        pb = sum(f(r.get("pnl_bnb")) for r in rs)
        pu = sum(f(r.get("pnl_usd")) for r in rs)
        wins = sum(1 for r in rs if f(r.get("pnl_pct")) > 0)
        wr = wins / n * 100 if n else 0.0
        op = load_open(jsonp)
        path_tot[pth] = (n, pb, pu, wr, len(op))
        for r in rs:
            k = r.get("kol_name", "?")
            a = per_kol.setdefault(k, {"public": [0, 0.0], "private": [0, 0.0]})
            a[pth][0] += 1
            a[pth][1] += f(r.get("pnl_usd"))
        for p in op:
            open_rows.append((
                pth, p.get("kol_name", "?"),
                p.get("token_symbol", "?")[:12],
                wei(p.get("bnb_in")),
                int(time.time()) - int(p.get("opened_at_unix_ns", 0)) // 1_000_000_000,
            ))

    lines.append(f"{'PATH':9} {'closed':>6} {'netBNB':>11} {'netUSD':>9} "
                 f"{'win':>5} {'open':>5}")
    lines.append("-" * 52)
    for pth in ("public", "private"):
        n, pb, pu, wr, o = path_tot.get(pth, (0, 0.0, 0.0, 0.0, 0))
        col = "\x1b[32m" if pu >= 0 else "\x1b[31m"
        lines.append(f"{pth:9} {n:6d} {pb:11.5f} "
                      f"{col}{pu:9.2f}\x1b[0m {wr:4.0f}% {o:5d}")

    lines.append(f"\n{'KOL':4} {'pub#':>5} {'pubUSD':>9} "
                 f"{'prv#':>5} {'prvUSD':>9}  best")
    lines.append("-" * 48)
    rank = []
    for k, a in per_kol.items():
        tot = a["public"][1] + a["private"][1]
        rank.append((tot, k, a))
    for tot, k, a in sorted(rank, reverse=True)[:12]:
        best = "private" if a["private"][1] >= a["public"][1] else "public"
        lines.append(f"{k:4} {a['public'][0]:5d} {a['public'][1]:9.2f} "
                      f"{a['private'][0]:5d} {a['private'][1]:9.2f}  {best}")

    if open_rows:
        lines.append(f"\n\x1b[1mOPEN ({len(open_rows)})\x1b[0m  "
                      f"{'path':8}{'KOL':4} {'token':13} {'in BNB':>9} {'age':>6}")
        for pth, k, sym, bnb, age in sorted(open_rows, key=lambda x: -x[4])[:15]:
            am = f"{age//60}m" if age < 7200 else f"{age//3600}h"
            lines.append(f"          {pth:8}{k:4} {sym:13} {bnb:9.4f} {am:>6}")

    if rank:
        rank.sort(reverse=True)
        lines.append(f"\n🏆 {rank[0][1]} leads (${rank[0][0]:.2f} combined)")
    pp = path_tot.get("public", (0,)*5)[2]
    pr = path_tot.get("private", (0,)*5)[2]
    lines.append(f"🥇 path: public ${pp:.2f}  vs  private ${pr:.2f}  → "
                 f"{'private' if pr >= pp else 'public'}")
    return "\n".join(lines)

try:
    while True:
        sys.stdout.write("\x1b[2J\x1b[H")  # clear
        sys.stdout.write(render() + "\n")
        sys.stdout.flush()
        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass
PY
