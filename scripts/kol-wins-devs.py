#!/usr/bin/env python3
"""Export winning paper-trader trades for D and I with their token DEVs.

For each winning trade (pnl_usd > 0) by D or I in either visibility:
  - token address + symbol
  - dev address (contract creator, from bscscan free API)
  - KOL entry mcap, our entry mcap
  - KOL exit mcap, our exit mcap
  - bnb in/out, pnl, pct, ts, hashes

Output:
  trader_private/d_i_wins.csv        — per-trade rows
  Also prints a DEV FREQUENCY summary so you can see whether D & I keep
  buying tokens from the same handful of devs (signal candidate).

Usage:
  scripts/kol-wins-devs.py
  scripts/kol-wins-devs.py --kols D,I --min-pnl 0  # default
  scripts/kol-wins-devs.py --kols D --min-pnl 5    # D wins > $5
"""
import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

ROOT = Path("/data/bsc-meme-mev")
SOURCES = [
    ("public", ROOT / "trader" / "closed_trades.csv"),
    ("private", ROOT / "trader_private" / "closed_trades.csv"),
]
OUT_CSV = ROOT / "trader_private" / "d_i_wins.csv"  # central location

UA = "Mozilla/5.0 (X11; Linux x86_64) bsc-meme-mev/0.1"
FOURMEME = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
# Empirically-identified Four.Meme TokenCreate event topic. Verified by
# probing the launchpad's logs and matching against a known token. The
# token address appears in the event DATA (not topics), so we scan for
# the address substring in `data` after filtering by this topic.
FOURMEME_CREATE_TOPIC = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"


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


ENV = load_env()
NODEREAL_RPC = ENV.get("NODEREAL_RPC_URL", "")
LOCAL_RPC = "http://127.0.0.1:8545"


def rpc(url, method, params, timeout=20):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":1}).encode()
    req = urllib.request.Request(url, body, {"Content-Type":"application/json","User-Agent": UA})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        return {"error": str(e)}


def find_dev_for_token(token: str, anchor_block: int, cache: dict) -> str:
    """Find the Four.Meme creator of `token` by scanning launchpad logs in a
    narrow window before `anchor_block` (typically our paper-trade entry block).

    Returns empty string if not found in 10k blocks before anchor — the token
    either wasn't a Four.Meme creation or our anchor is too far past creation.
    """
    tok = token.lower()
    if tok in cache:
        return cache[tok]
    tok_short = tok[2:]
    for span in (200, 1000, 5000, 20000):
        start = max(anchor_block - span, 0)
        end = anchor_block
        params = {
            "address": FOURMEME,
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "topics": [FOURMEME_CREATE_TOPIC],
        }
        r = rpc(NODEREAL_RPC, "eth_getLogs", [params], timeout=30)
        if r.get("error"):
            continue
        logs = r.get("result") or []
        for l in logs:
            data = (l.get("data") or "").lower()
            if tok_short in data:
                tx = rpc(NODEREAL_RPC, "eth_getTransactionByHash",
                          [l["transactionHash"]]).get("result")
                if tx and tx.get("from"):
                    cache[tok] = tx["from"].lower()
                    return cache[tok]
    cache[tok] = ""
    return ""


def resolve_devs(rows):
    """For each unique token, find the Four.Meme creator. Uses each row's
    opened_at_block as the anchor for a narrow log scan."""
    # Index by token → earliest anchor block (smallest open block we observed)
    anchors = {}
    for r in rows:
        tok = r["token_address"].lower()
        try:
            b = int(r.get("opened_at_block") or 0)
        except ValueError:
            continue
        if b > 0 and (tok not in anchors or b < anchors[tok]):
            anchors[tok] = b
    out = {}
    cache = {}
    print(f"  resolving {len(anchors)} unique tokens via NodeReal launchpad-log scan…")
    for i, (tok, anchor) in enumerate(sorted(anchors.items()), 1):
        dev = find_dev_for_token(tok, anchor, cache)
        out[tok] = dev
        if i % 5 == 0:
            print(f"    {i}/{len(anchors)} resolved (last: {tok[:10]}… → {dev[:10] if dev else '?'}…)")
        time.sleep(0.1)
    return out


def fmt_ts(ns):
    return dt.datetime.fromtimestamp(int(ns) / 1e9).strftime("%Y-%m-%d %H:%M:%S")


def fmt_money(x):
    try:
        return f"{float(x):.2f}"
    except (TypeError, ValueError):
        return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kols", default="D,I", help="comma-separated KOL names")
    ap.add_argument("--min-pnl", type=float, default=0.0,
                    help="only include trades with pnl_usd > this")
    ap.add_argument("--out", default=str(OUT_CSV),
                    help="output CSV path")
    args = ap.parse_args()
    kols = set(k.strip() for k in args.kols.split(",") if k.strip())

    # Collect all winning rows across both visibility channels.
    rows = []
    for vis, path in SOURCES:
        if not path.exists():
            continue
        for r in csv.DictReader(open(path)):
            if r.get("kol_name") not in kols:
                continue
            try:
                pnl = float(r.get("pnl_usd") or 0)
            except ValueError:
                continue
            if pnl <= args.min_pnl:
                continue
            r["__visibility"] = vis
            rows.append(r)

    print(f"=== winning trades for {sorted(kols)}, pnl > ${args.min_pnl} ===")
    print(f"  rows: {len(rows)} (public={sum(1 for r in rows if r['__visibility']=='public')},"
          f" private={sum(1 for r in rows if r['__visibility']=='private')})")

    # Unique tokens → dev lookup
    tokens = sorted({r["token_address"].lower() for r in rows})
    print(f"  unique tokens: {len(tokens)}")
    if not NODEREAL_RPC:
        print("  ERROR: NODEREAL_RPC_URL missing from .env — cannot resolve devs",
              file=sys.stderr)
        sys.exit(1)
    devs = resolve_devs(rows)
    n_known = sum(1 for v in devs.values() if v)
    print(f"  dev addresses resolved: {n_known}/{len(tokens)}")
    print()

    # Write per-trade CSV.
    fields = [
        "ts_utc", "kol", "visibility", "token_symbol", "token_address",
        "dev_address",
        "kol_entry_mcap_usd", "our_entry_mcap_usd",
        "kol_exit_mcap_usd", "our_exit_mcap_usd",
        "bnb_in", "bnb_out", "pnl_usd", "pnl_pct",
        "held_secs", "close_reason",
        "buy_kol_implied", "sell_trigger_tx",
    ]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in sorted(rows, key=lambda r: -float(r.get("pnl_usd") or 0)):
            tok = r["token_address"].lower()
            w.writerow({
                "ts_utc": fmt_ts(r["ts_unix_ns"]),
                "kol": r["kol_name"],
                "visibility": r["__visibility"],
                "token_symbol": r.get("token_symbol") or "",
                "token_address": tok,
                "dev_address": devs.get(tok, ""),
                "kol_entry_mcap_usd": fmt_money(r.get("d_mcap_usd")),
                "our_entry_mcap_usd": fmt_money(r.get("our_entry_mcap_usd")),
                "kol_exit_mcap_usd": fmt_money(r.get("kol_exit_mcap_first_usd")),
                "our_exit_mcap_usd": fmt_money(r.get("our_avg_exit_mcap_usd")),
                "bnb_in": f"{int(r['bnb_in_wei'])/1e18:.6f}",
                "bnb_out": f"{int(r['bnb_out_wei'])/1e18:.6f}",
                "pnl_usd": fmt_money(r.get("pnl_usd")),
                "pnl_pct": fmt_money(float(r.get("pnl_pct") or 0) * 100),
                "held_secs": r.get("held_secs", ""),
                "close_reason": r.get("close_reason", ""),
                "buy_kol_implied": "",  # left blank — we don't currently record the KOL buy tx
                "sell_trigger_tx": r.get("trigger_sell_tx", ""),
            })
    print(f"  ✓ wrote {args.out}  ({len(rows)} rows)")
    print()

    # ── DEV FREQUENCY SUMMARY ──────────────────────────────────────────────
    dev_agg = defaultdict(lambda: {"n_tokens": set(), "n_wins": 0, "pnl_usd": 0.0,
                                    "kols": set(), "tokens_seen": []})
    for r in rows:
        tok = r["token_address"].lower()
        dev = devs.get(tok) or "(unknown)"
        d = dev_agg[dev]
        d["n_tokens"].add(tok)
        d["n_wins"] += 1
        try:
            d["pnl_usd"] += float(r.get("pnl_usd") or 0)
        except ValueError:
            pass
        d["kols"].add(r["kol_name"])
        d["tokens_seen"].append((r.get("token_symbol") or "?", r["kol_name"], r["__visibility"]))

    ranked = sorted(
        dev_agg.items(),
        key=lambda kv: (-len(kv[1]["n_tokens"]), -kv[1]["pnl_usd"])
    )
    print("=== DEV FREQUENCY (winning trades only) ===")
    print(f"  {'#':>2}  {'dev':<44}  {'#tok':>4} {'#wins':>5}  {'pnl$':>9}  kols  tokens")
    for i, (dev, agg) in enumerate(ranked, 1):
        kols_s = ",".join(sorted(agg["kols"]))
        toks_label = ", ".join(sorted({sym for sym, _, _ in agg["tokens_seen"]}))[:60]
        print(
            f"  {i:>2}  {dev:<44}  "
            f"{len(agg['n_tokens']):>4} {agg['n_wins']:>5}  "
            f"{agg['pnl_usd']:>+9.2f}  {kols_s:<4}  {toks_label}"
        )

    # Recurring devs only (>= 2 tokens) — the actual signal candidates
    recurring = [(dev, agg) for dev, agg in ranked
                 if dev != "(unknown)" and len(agg["n_tokens"]) >= 2]
    if recurring:
        print()
        print("  ↑ RECURRING devs (≥ 2 winning tokens):", len(recurring))
        for dev, agg in recurring:
            print(f"    {dev}  →  {len(agg['n_tokens'])} tokens, "
                  f"${agg['pnl_usd']:+.2f}, KOLs: {','.join(sorted(agg['kols']))}")
    else:
        print()
        print("  (no dev appears on 2+ winning tokens — each win is a different dev)")


if __name__ == "__main__":
    main()
