#!/usr/bin/env python3
"""Per-(KOL, visibility) paper-trading P&L scorecard.

Reads the live state from both paper-trader portfolios:
  - /data/bsc-meme-mev/trader/          → PUBLIC mempool signals
  - /data/bsc-meme-mev/trader_private/  → PRIVATE confirmed signals

Each KOL has a $200 (~0.3 BNB) closed-loop pot per visibility, sized at
10% of current cash per trade. Prints a side-by-side leaderboard so you
can rank KOLs on signal quality independently for each channel.

Usage:
  scripts/kol-paper-report.py                 # snapshot now (stdout)
  scripts/kol-paper-report.py --since 6h      # only count trades within window
  scripts/kol-paper-report.py --telegram      # send compact format to TG
"""
import argparse
import csv
import json
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

ROOT = Path("/data/bsc-meme-mev")
PORTFOLIOS = [
    ("PUBLIC", ROOT / "trader"),
    ("PRIVATE", ROOT / "trader_private"),
]


def bnb_usd():
    try:
        req = urllib.request.Request(
            "https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        return float(json.loads(urllib.request.urlopen(req, timeout=4).read())["price"])
    except Exception:
        return 0.0


def parse_since(s):
    if not s:
        return 0
    unit = s[-1]
    n = int(s[:-1])
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit)
    if not mult:
        raise SystemExit(f"bad --since: {s!r}; use 10m / 6h / 2d")
    return time.time() - n * mult


def load_closed(path: Path, since_unix_s: float):
    """Yield closed trades from a paper-trader CSV, filtered by ts."""
    if not path.exists():
        return
    for r in csv.DictReader(open(path)):
        try:
            ts_s = int(r["ts_unix_ns"]) / 1e9
        except (KeyError, ValueError):
            continue
        if ts_s < since_unix_s:
            continue
        yield r


def load_budgets(path: Path):
    """Return {kol_name: KolBudget} from kol_budgets.json (or {})."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def aggregate(rows, bnb_usd_now):
    """Group closed trades by kol_name."""
    by_kol = defaultdict(lambda: {
        "trades": 0, "wins": 0, "pnl_usd": 0.0, "pnl_bnb": 0.0,
    })
    for r in rows:
        kol = r.get("kol_name", "?")
        try:
            pnl_usd = float(r.get("pnl_usd") or 0.0)
            pnl_bnb = float(r.get("pnl_bnb") or (float(r["pnl_wei"]) / 1e18))
        except (KeyError, ValueError):
            continue
        d = by_kol[kol]
        d["trades"] += 1
        if pnl_usd > 0:
            d["wins"] += 1
        d["pnl_usd"] += pnl_usd
        d["pnl_bnb"] += pnl_bnb
    return by_kol


def load_env(path="/data/bsc-meme-mev/.env"):
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def send_telegram(text_html: str):
    env = load_env()
    tok = env.get("TELEGRAM_BOT_TOKEN", "")
    chat = env.get("TELEGRAM_CHAT_ID", "")
    if not tok or not chat:
        print("ERROR: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing in .env")
        return False
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/x-www-form-urlencoded"})
    try:
        r = json.loads(urllib.request.urlopen(req, timeout=10).read())
        return bool(r.get("ok"))
    except Exception as e:
        print(f"telegram send failed: {e}")
        return False


def format_telegram(scoreboard, bnb_now, window_label):
    """Compact mobile-friendly Telegram digest. HTML mode."""
    # totals per visibility
    totals = {"PUBLIC": {"trades": 0, "pnl": 0.0, "equity": 0.0, "init": 0.0, "skip": 0},
              "PRIVATE": {"trades": 0, "pnl": 0.0, "equity": 0.0, "init": 0.0, "skip": 0}}
    for sides in scoreboard.values():
        for vis, t in totals.items():
            d = sides.get(vis, {})
            t["trades"] += d.get("trades", 0)
            t["pnl"] += d.get("pnl_usd", 0.0)
            t["equity"] += d.get("equity_bnb", 0.0) * bnb_now
            t["init"] += d.get("init_bnb", 0.0) * bnb_now
            t["skip"] += d.get("skipped_budget", 0)

    lines = []
    lines.append(f"<b>📊 KOL paper scorecard</b>")
    lines.append(f"window: {window_label}  •  BNB ${bnb_now:.0f}")
    lines.append("")

    for vis in ("PUBLIC", "PRIVATE"):
        t = totals[vis]
        roi = 100 * (t["equity"] - t["init"]) / t["init"] if t["init"] > 0 else 0.0
        lines.append(f"<b>{vis}</b>  pnl ${t['pnl']:+.2f}  roi {roi:+.1f}%  ({t['trades']} tr)")
        # per-KOL sorted by pnl desc
        rows = []
        for kol, sides in scoreboard.items():
            d = sides.get(vis, {})
            if not d or d.get("trades", 0) == 0:
                continue
            equity_usd = d["equity_bnb"] * bnb_now
            kol_roi = 100 * (equity_usd - d["init_bnb"] * bnb_now) / (d["init_bnb"] * bnb_now) \
                if d["init_bnb"] > 0 else 0.0
            rows.append((d["pnl_usd"], kol, d["trades"], d["wins"], d["pnl_usd"], equity_usd, kol_roi))
        rows.sort(reverse=True)
        for _, kol, tr, wins, pnl, eq, roi in rows:
            sign = "🟢" if pnl > 0 else ("🔴" if pnl < 0 else "⚪")
            lines.append(f"  {sign} <code>{kol}</code> {wins}/{tr}  ${pnl:+.2f}  eq ${eq:.1f}  ({roi:+.1f}%)")
        if not rows:
            lines.append("  <i>no trades yet</i>")
        lines.append("")

    return "\n".join(lines).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="e.g. 30m, 6h, 2d (default: all time)")
    ap.add_argument("--telegram", action="store_true",
                    help="send compact format to Telegram (uses TELEGRAM_* from .env)")
    args = ap.parse_args()

    since_s = parse_since(args.since)
    bnb_now = bnb_usd()
    window_label = f"last {args.since}" if args.since else "all time"

    if not args.telegram:
        print(f"=== Per-KOL paper-trading scorecard ===")
        print(f"  window: {window_label}")
        print(f"  BNB/USD: ${bnb_now:.2f}")
        print()

    # Per visibility: closed-trade aggregate + live budget book
    scoreboard = {}  # kol → {pub:{...}, priv:{...}}
    for label, dir_ in PORTFOLIOS:
        closed_path = dir_ / "closed_trades.csv"
        budget_path = dir_ / "kol_budgets.json"
        rows = list(load_closed(closed_path, since_s))
        agg = aggregate(rows, bnb_now)
        budgets = load_budgets(budget_path)

        # union of KOLs seen in either source
        kols = set(agg.keys()) | set(budgets.keys())
        for kol in kols:
            scoreboard.setdefault(kol, {})
            d = scoreboard[kol].setdefault(label, {})
            a = agg.get(kol, {})
            b = budgets.get(kol, {})
            init_bnb = (b.get("initial_wei", 0) or 0) / 1e18
            cash_bnb = (b.get("cash_wei", 0) or 0) / 1e18
            committed_bnb = (b.get("committed_wei", 0) or 0) / 1e18
            realized_bnb = (b.get("realized_pnl_wei", 0) or 0) / 1e18
            d.update({
                "trades": a.get("trades", 0),
                "wins": a.get("wins", 0),
                "pnl_usd": a.get("pnl_usd", 0.0),
                "pnl_bnb": a.get("pnl_bnb", 0.0),
                "init_bnb": init_bnb,
                "cash_bnb": cash_bnb,
                "committed_bnb": committed_bnb,
                "equity_bnb": cash_bnb + committed_bnb,
                "realized_bnb_live": realized_bnb,  # from live state (may differ if --since used)
                "skipped_budget": b.get("trades_skipped_budget", 0),
            })

    # Telegram path: compact format + send, skip the wide stdout table.
    if args.telegram:
        body = format_telegram(scoreboard, bnb_now, window_label)
        ok = send_telegram(body)
        print("telegram: sent" if ok else "telegram: FAILED")
        return

    # Print table — KOL × {PUBLIC, PRIVATE}
    header = (
        f"  {'KOL':<4} | "
        f"{'PUBLIC':^46} | "
        f"{'PRIVATE':^46}"
    )
    sub = (
        f"  {'':<4} | "
        f"{'trades':>6} {'wins':>4} {'pnl$':>9} {'equity$':>9} {'roi%':>6}"
        f" | "
        f"{'trades':>6} {'wins':>4} {'pnl$':>9} {'equity$':>9} {'roi%':>6}"
    )
    print(header)
    print(sub)
    print("  " + "-" * (len(sub) - 2))

    def fmt_side(d):
        if not d or not d.get("init_bnb"):
            return f"{'-':>6} {'-':>4} {'-':>9} {'-':>9} {'-':>6}"
        equity_usd = d["equity_bnb"] * bnb_now
        init_usd = d["init_bnb"] * bnb_now
        roi_pct = 100 * (equity_usd - init_usd) / init_usd if init_usd > 0 else 0
        return (
            f"{d['trades']:>6} {d['wins']:>4} "
            f"{d['pnl_usd']:>+9.2f} {equity_usd:>9.2f} {roi_pct:>+6.1f}"
        )

    # Sort by combined PnL desc
    def key(kv):
        kol, sides = kv
        pub = sides.get("PUBLIC", {})
        priv = sides.get("PRIVATE", {})
        return -((pub.get("pnl_usd", 0) or 0) + (priv.get("pnl_usd", 0) or 0))

    for kol, sides in sorted(scoreboard.items(), key=key):
        pub = fmt_side(sides.get("PUBLIC"))
        priv = fmt_side(sides.get("PRIVATE"))
        print(f"  {kol:<4} | {pub} | {priv}")

    # Totals
    print("  " + "-" * (len(sub) - 2))
    for label, _ in PORTFOLIOS:
        total_pnl_usd = sum(s.get(label, {}).get("pnl_usd", 0) for s in scoreboard.values())
        total_trades = sum(s.get(label, {}).get("trades", 0) for s in scoreboard.values())
        total_equity = sum(s.get(label, {}).get("equity_bnb", 0) for s in scoreboard.values())
        total_init = sum(s.get(label, {}).get("init_bnb", 0) for s in scoreboard.values())
        total_skip = sum(s.get(label, {}).get("skipped_budget", 0) for s in scoreboard.values())
        invested_usd = total_init * bnb_now
        equity_usd = total_equity * bnb_now
        roi_pct = 100 * (equity_usd - invested_usd) / invested_usd if invested_usd > 0 else 0
        print(
            f"  {label:<7}  trades={total_trades}  pnl=${total_pnl_usd:+.2f}  "
            f"equity=${equity_usd:.2f}/${invested_usd:.2f}  "
            f"roi={roi_pct:+.1f}%  budget_skips={total_skip}"
        )


if __name__ == "__main__":
    main()
