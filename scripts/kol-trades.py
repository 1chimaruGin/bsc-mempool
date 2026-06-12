#!/usr/bin/env python3
"""Per-KOL trade-by-trade log across both visibility channels.

Usage:
  scripts/kol-trades.py A                  # all of A's trades (public + private)
  scripts/kol-trades.py A --since 6h       # windowed
  scripts/kol-trades.py A --public         # public channel only
  scripts/kol-trades.py A --private        # private channel only
  scripts/kol-trades.py --all              # everyone, sorted by ts
"""
import argparse
import csv
import datetime as dt
import json
import os
import time
from pathlib import Path

ROOT = Path("/data/bsc-meme-mev")
SOURCES = [
    ("PUB", ROOT / "trader" / "closed_trades.csv"),
    ("PRV", ROOT / "trader_private" / "closed_trades.csv"),
]
OPEN = [
    ("PUB", ROOT / "trader" / "open_positions.json"),
    ("PRV", ROOT / "trader_private" / "open_positions.json"),
]


def parse_since(s):
    if not s:
        return 0
    unit = s[-1]
    n = int(s[:-1])
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit)
    if not mult:
        raise SystemExit(f"bad --since: {s!r}; use 10m / 6h / 2d")
    return time.time() - n * mult


def ts(ns):
    return dt.datetime.fromtimestamp(int(ns) / 1e9).strftime("%m-%d %H:%M:%S")


def load_closed(path: Path, kol_filter, since_s):
    if not path.exists():
        return []
    out = []
    for r in csv.DictReader(open(path)):
        if kol_filter and r.get("kol_name") != kol_filter:
            continue
        try:
            t = int(r["ts_unix_ns"]) / 1e9
        except (KeyError, ValueError):
            continue
        if t < since_s:
            continue
        out.append(r)
    return out


def load_open(path: Path):
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        if isinstance(data, dict) and "positions" in data:
            return data["positions"]
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def parse_wei(v):
    """OpenPosition wei fields may be hex strings (alloy U256 serialization)."""
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    s = str(v)
    try:
        return int(s, 16) if s.startswith("0x") else int(s)
    except ValueError:
        return 0


def short(addr):
    return addr[:6] + "…" + addr[-4:] if addr and len(addr) > 12 else (addr or "?")


def fmt_row(channel, r):
    bnb_in = int(r.get("bnb_in_wei", 0) or 0) / 1e18
    bnb_out = int(r.get("bnb_out_wei", 0) or 0) / 1e18
    pnl_usd = float(r.get("pnl_usd") or 0)
    pnl_pct = float(r.get("pnl_pct") or 0) * 100
    held = int(r.get("held_secs") or 0)
    held_str = f"{held}s" if held < 60 else (f"{held//60}m" if held < 3600 else f"{held//3600}h")
    sign = "🟢" if pnl_usd > 0 else ("🔴" if pnl_usd < 0 else "⚪")
    reason = r.get("close_reason", "?")
    mcap_in = float(r.get("our_entry_mcap_usd") or 0)
    mcap_out = float(r.get("our_avg_exit_mcap_usd") or 0)
    mcap_str = ""
    if mcap_in > 0 and mcap_out > 0:
        mcap_str = f"  mcap ${mcap_in/1000:.1f}k→${mcap_out/1000:.1f}k"
    addr = r.get("token_address", "") or ""
    sell_tx = r.get("trigger_sell_tx", "") or ""
    return (
        f"  {ts(r['ts_unix_ns'])}  {channel}  {sign} {r.get('token_symbol','?'):>10s}  "
        f"{bnb_in:.4f}→{bnb_out:.4f} BNB  "
        f"${pnl_usd:+7.2f} ({pnl_pct:+6.1f}%)  "
        f"{held_str:>5s}  {reason:<14s}{mcap_str}\n"
        f"    token: {addr}"
        + (f"  sell_tx: {sell_tx}" if sell_tx else "")
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kol", nargs="?", help="KOL name (e.g. A); omit with --all")
    ap.add_argument("--all", action="store_true", help="all KOLs combined")
    ap.add_argument("--since", help="e.g. 30m, 6h, 2d (default: all time)")
    ap.add_argument("--public", action="store_true", help="public channel only")
    ap.add_argument("--private", action="store_true", help="private channel only")
    ap.add_argument("--open", action="store_true",
                    help="also show currently OPEN positions for this KOL")
    args = ap.parse_args()

    if not args.all and not args.kol:
        ap.error("specify KOL or --all")
    if args.all and args.kol:
        ap.error("--all and KOL are mutually exclusive")

    kol_filter = None if args.all else args.kol
    since_s = parse_since(args.since)

    channels = []
    if not args.private:
        channels.append(("PUB", SOURCES[0][1]))
    if not args.public:
        channels.append(("PRV", SOURCES[1][1]))

    all_rows = []
    for ch, path in channels:
        for r in load_closed(path, kol_filter, since_s):
            all_rows.append((int(r["ts_unix_ns"]), ch, r))
    all_rows.sort()

    label = "all KOLs" if args.all else f"KOL {kol_filter}"
    window = f"last {args.since}" if args.since else "all time"
    print(f"=== {label} trades ({window}) ===")
    print()

    if not all_rows:
        print("  (no closed trades in window)")
    else:
        for _, ch, r in all_rows:
            if args.all:
                print(fmt_row(f"{ch} {r.get('kol_name','?'):<3s}", r))
            else:
                print(fmt_row(ch, r))

    # Summary
    if all_rows:
        n = len(all_rows)
        wins = sum(1 for _, _, r in all_rows if float(r.get("pnl_usd") or 0) > 0)
        pnl = sum(float(r.get("pnl_usd") or 0) for _, _, r in all_rows)
        print()
        print(f"  {n} trades, {wins} wins ({100*wins/n:.0f}%), total ${pnl:+.2f}")

    # Open positions
    if args.open:
        print()
        print(f"=== currently OPEN positions ===")
        for ch, path in OPEN:
            for p in load_open(path):
                if kol_filter and p.get("kol_name") != kol_filter:
                    continue
                bnb_in = parse_wei(p.get("bnb_in", 0)) / 1e18
                opened_ns = int(p.get("opened_at_unix_ns", 0) or 0)
                age_s = int(time.time() - opened_ns / 1e9) if opened_ns else 0
                age_str = f"{age_s}s" if age_s < 60 else (f"{age_s//60}m" if age_s < 3600 else f"{age_s//3600}h")
                print(
                    f"  {ch}  {p.get('kol_name','?'):<3s}  {p.get('token_symbol','?'):>10s}  "
                    f"{bnb_in:.4f} BNB in  age {age_str}\n"
                    f"    token: {p.get('token_address','')}"
                )


if __name__ == "__main__":
    main()
