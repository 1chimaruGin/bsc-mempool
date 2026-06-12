#!/usr/bin/env python3
"""
Recompute paper-trading PnL using REAL chain-observed swap prices —
no synthesis, no static haircut. For each closed trade in the CSV we
re-derive the entry and exit fill prices by querying eth_getLogs on the
same +1..+5 block window the trader actually landed in, then take the
MEDIAN of real buyers' / sellers' executed prices from their receipts.

  scripts/recompute-report.py             # report TODAY UTC (read-only)
  scripts/recompute-report.py --all       # report all-time (read-only)
  scripts/recompute-report.py --backfill  # OVERWRITE closed_trades.csv
                                          #   with chain-derived values
                                          #   (backs up to .pre-backfill-*)

With --backfill, rows that have no observable +1-block chain swap are
booked FLAT (bnb_out = bnb_in, pnl = 0, close_reason = price_unavailable)
so they don't pollute aggregates with stale synthetic numbers.
"""
import csv, datetime, json, os, sys, time
from collections import defaultdict
import urllib.request
import urllib.error


# ── env / endpoints ────────────────────────────────────────────────────────
def load_env(path="/data/bsc-meme-mev/.env"):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


ENV = load_env()
NODEREAL = ENV.get("NODEREAL_RPC_URL") or os.environ.get("NODEREAL_RPC_URL")
LOCAL = "http://127.0.0.1:8545"
if not NODEREAL:
    print("ERROR: NODEREAL_RPC_URL not set (.env)", file=sys.stderr)
    sys.exit(1)

WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


# ── RPC helpers + rate-cap on archive ──────────────────────────────────────
_last_archive_ns = [0]
_MIN_GAP_NS = 400_000_000  # ≤ ~2.5 req/s on NodeReal free
_archive_calls = [0]
_ARCHIVE_DAILY_CAP = 4000  # one-off recompute; well within NodeReal free CUs


def now_ns():
    return time.time_ns()


def rpc(url, method, params, archive=False, retries=2):
    if archive:
        if _archive_calls[0] >= _ARCHIVE_DAILY_CAP:
            return None
        delta = now_ns() - _last_archive_ns[0]
        if delta < _MIN_GAP_NS:
            time.sleep((_MIN_GAP_NS - delta) / 1e9)
        _last_archive_ns[0] = now_ns()
        _archive_calls[0] += 1
    body = json.dumps(
        {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    ).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read()).get("result")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if attempt < retries:
                time.sleep(0.5)
                continue
            return None
        except Exception:
            return None


def hx(h):
    if h is None:
        return 0
    if isinstance(h, int):
        return h
    if isinstance(h, str):
        if h in ("", "0x"):
            return 0
        try:
            return int(h, 16) if h.startswith("0x") else int(h, 16)
        except ValueError:
            try:
                return int(h)
            except ValueError:
                return 0
    return 0


# ── per-tx price from receipt (BNB-wei per raw token unit) ─────────────────
def price_from_tx(tx_hash, token_lc, is_buy):
    """Returns BNB-wei / raw-token-unit price extracted from a tx receipt.
    Decimals cancel out in the entry/exit RATIO, so we don't need them."""
    r = rpc(LOCAL, "eth_getTransactionReceipt", [tx_hash])
    if not r or r.get("status") != "0x1":
        return None
    logs = r.get("logs") or []
    tok_amt = 0
    wbnb_amt = 0
    for lg in logs:
        topics = lg.get("topics") or []
        if not topics or (topics[0] or "").lower() != TRANSFER_TOPIC:
            continue
        amt = hx(lg.get("data"))
        addr = (lg.get("address") or "").lower()
        if addr == token_lc:
            tok_amt = max(tok_amt, amt)
        elif addr == WBNB:
            wbnb_amt = max(wbnb_amt, amt)
    if tok_amt == 0:
        return None
    if is_buy:
        tx = rpc(LOCAL, "eth_getTransactionByHash", [tx_hash])
        if not tx:
            return None
        v = hx(tx.get("value"))
        bnb_wei = v if v > 0 else wbnb_amt
    else:
        if wbnb_amt > 0:
            bnb_wei = wbnb_amt
        else:
            blk = hx(r.get("blockNumber"))
            gas_used = hx(r.get("gasUsed"))
            gas_price = hx(r.get("effectiveGasPrice"))
            tx = rpc(LOCAL, "eth_getTransactionByHash", [tx_hash])
            if not tx:
                return None
            d_addr = tx.get("from")
            if not d_addr:
                return None
            # Local-only balance lookup. Archive fallback skipped here
            # because per-tx native-sell pricing would otherwise burn ~10
            # archive calls per row. Out-of-window sells just get skipped
            # for that tx (the median across other txs still survives).
            bal_a = rpc(LOCAL, "eth_getBalance", [d_addr, hex(blk)])
            bal_b = rpc(LOCAL, "eth_getBalance", [d_addr, hex(blk - 1)])
            if bal_a is None or bal_b is None:
                return None
            proceeds = (hx(bal_a) - hx(bal_b)) + gas_used * gas_price
            if proceeds <= 0:
                return None
            bnb_wei = proceeds
    if bnb_wei <= 0 or tok_amt <= 0:
        return None
    return bnb_wei / tok_amt  # BNB-wei / raw-token-unit


# ── median real price across a block window ────────────────────────────────
_CACHE = {}


def median_chain_price(token, from_block, to_block, is_buy):
    key = (token.lower(), from_block, to_block, is_buy)
    if key in _CACHE:
        return _CACHE[key]
    filt = [{
        "address": token,
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "topics": [TRANSFER_TOPIC],
    }]
    logs = rpc(NODEREAL, "eth_getLogs", filt, archive=True) or []
    tx_hashes = []
    seen = set()
    for lg in logs:
        h = lg.get("transactionHash")
        if h and h not in seen:
            seen.add(h)
            tx_hashes.append(h)
    prices = []
    for txh in tx_hashes[:12]:
        p = price_from_tx(txh, token.lower(), is_buy)
        if p and p > 0:
            prices.append(p)
    res = None
    if prices:
        prices.sort()
        res = prices[len(prices) // 2]
    _CACHE[key] = res
    return res


# ── per-row recompute ──────────────────────────────────────────────────────
def recompute(row):
    """Returns (pnl_bnb, pnl_usd) using real chain prices, or None on skip."""
    try:
        bnb_in_wei = int(row["bnb_in_wei"])
        opened = int(row["opened_at_block"])
        closed = int(row["closed_at_block"])
        token = row["token_address"]
    except (KeyError, ValueError):
        return None
    if opened == 0 or closed == 0 or not token.startswith("0x"):
        return None  # pre-block-aware rows or malformed
    bnb_in = bnb_in_wei / 1e18
    bnb_usd_close = float(row.get("bnb_usd_close") or 0)

    entry_p = median_chain_price(token, opened + 1, opened + 5, is_buy=True)
    exit_p = median_chain_price(token, closed, closed + 5, is_buy=False)
    if not entry_p or not exit_p or entry_p == 0:
        return None
    bnb_out = bnb_in * (exit_p / entry_p)
    pnl_bnb = bnb_out - bnb_in
    pnl_usd = pnl_bnb * bnb_usd_close
    return (pnl_bnb, pnl_usd)


# ── CSV loading + scope ────────────────────────────────────────────────────
def load_csv(path, label, today_only):
    if not os.path.exists(path):
        return []
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    out = []
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                ts = int(r["ts_unix_ns"])
            except (KeyError, ValueError):
                continue
            if today_only:
                d = datetime.datetime.fromtimestamp(
                    ts / 1e9, datetime.timezone.utc
                ).strftime("%Y-%m-%d")
                if d != today:
                    continue
            r["_path"] = label
            out.append(r)
    return out


def backfill_csv(path):
    """Rewrite path in place: each row's bnb_out_wei, pnl_*, close_reason
    replaced with chain-derived values. Rows that can't be priced are
    booked FLAT (out=in, pnl=0, reason=price_unavailable). The original
    file is copied to {path}.pre-backfill-{ts} first."""
    if not os.path.exists(path):
        print(f"  {path}: missing — skipping", file=sys.stderr)
        return (0, 0)
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = f"{path}.pre-backfill-{ts}"
    with open(path) as f:
        original = f.read()
    with open(backup, "w") as f:
        f.write(original)
    print(f"  {path}\n    backed up → {backup}", file=sys.stderr)

    with open(path) as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    n_priced, n_flat = 0, 0
    for r in rows:
        res = recompute(r)
        try:
            bnb_in_wei = int(r["bnb_in_wei"])
        except (KeyError, ValueError):
            bnb_in_wei = 0
        bnb_usd_close = float(r.get("bnb_usd_close") or 0)
        if res is None:
            r["bnb_out_wei"] = str(bnb_in_wei)
            r["pnl_wei"] = "0"
            r["pnl_bnb"] = f"{0.0:.6f}"
            r["pnl_usd"] = f"{0.0:.2f}"
            r["pnl_pct"] = f"{0.0:.6f}"
            r["close_reason"] = "price_unavailable"
            n_flat += 1
        else:
            pnl_bnb, pnl_usd = res
            bnb_in = bnb_in_wei / 1e18
            new_bnb_out = bnb_in + pnl_bnb
            bnb_out_wei = int(max(new_bnb_out, 0) * 1e18)
            pnl_wei = bnb_out_wei - bnb_in_wei
            pnl_pct = (pnl_bnb / bnb_in) if bnb_in > 0 else 0.0
            r["bnb_out_wei"] = str(bnb_out_wei)
            r["pnl_wei"] = str(pnl_wei)
            r["pnl_bnb"] = f"{pnl_bnb:.6f}"
            r["pnl_usd"] = f"{pnl_usd:.2f}"
            r["pnl_pct"] = f"{pnl_pct:.6f}"
            n_priced += 1

    tmp = f"{path}.tmp-{ts}"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    os.replace(tmp, path)
    print(f"    rewrote {len(rows)} rows: {n_priced} chain-derived, "
          f"{n_flat} unpriced (booked flat)", file=sys.stderr)
    return (n_priced, n_flat)


def main():
    if "--backfill" in sys.argv:
        print("BACKFILL MODE — rewriting closed_trades.csv with "
              "chain-derived values…", file=sys.stderr)
        a = backfill_csv("/data/bsc-meme-mev/trader/closed_trades.csv")
        b = backfill_csv("/data/bsc-meme-mev/trader_private/closed_trades.csv")
        print(f"\ntotal: {a[0]+b[0]} chain-derived, {a[1]+b[1]} flat",
              file=sys.stderr)
        print(f"[archive calls used: {_archive_calls[0]} / {_ARCHIVE_DAILY_CAP}]",
              file=sys.stderr)
        return

    today_only = "--all" not in sys.argv
    pub = load_csv("/data/bsc-meme-mev/trader/closed_trades.csv",
                   "public", today_only)
    prv = load_csv("/data/bsc-meme-mev/trader_private/closed_trades.csv",
                   "private", today_only)
    rows = pub + prv
    if not rows:
        print("no rows in scope")
        return

    print(f"Recomputing {len(rows)} rows against real chain swaps "
          f"(NodeReal archive, this takes a few minutes)…", file=sys.stderr)

    by_path = defaultdict(lambda: {"n": 0, "bnb": 0.0, "usd": 0.0,
                                    "wins": 0, "skip": 0})
    by_kol = defaultdict(lambda: {"pub_n": 0, "pub_usd": 0.0,
                                   "prv_n": 0, "prv_usd": 0.0})
    for i, r in enumerate(rows, 1):
        if i % 20 == 0:
            print(f"  {i}/{len(rows)}  (archive calls: {_archive_calls[0]})",
                  file=sys.stderr)
        res = recompute(r)
        path = r["_path"]
        if res is None:
            by_path[path]["skip"] += 1
            continue
        pnl_bnb, pnl_usd = res
        by_path[path]["n"] += 1
        by_path[path]["bnb"] += pnl_bnb
        by_path[path]["usd"] += pnl_usd
        if pnl_usd > 0:
            by_path[path]["wins"] += 1
        kol = r["kol_name"]
        if path == "public":
            by_kol[kol]["pub_n"] += 1
            by_kol[kol]["pub_usd"] += pnl_usd
        else:
            by_kol[kol]["prv_n"] += 1
            by_kol[kol]["prv_usd"] += pnl_usd

    scope = "ALL-TIME" if not today_only else (
        "TODAY " + datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"))
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%SZ")
    print()
    print(f"\033[1mRECOMPUTED PAPER REPORT\033[0m  {now}  scope={scope}  "
          f"(chain-derived prices, no estimate)")
    print("=" * 72)
    print(f"{'PATH':9} {'closed':>6} {'netBNB':>11} {'netUSD':>9} "
          f"{'win':>5} {'skipped':>8}")
    print("-" * 62)
    for path in ("public", "private"):
        a = by_path[path]
        wr = (a["wins"] / a["n"] * 100) if a["n"] else 0
        col = "\033[32m" if a["usd"] >= 0 else "\033[31m"
        print(f"{path:9} {a['n']:6d} {a['bnb']:11.5f} "
              f"{col}{a['usd']:9.2f}\033[0m {wr:4.0f}% {a['skip']:8d}")

    print()
    print(f"{'KOL':4} {'pub#':>5} {'pubUSD':>9} {'prv#':>5} {'prvUSD':>9}  best")
    print("-" * 48)
    rank = sorted(by_kol.items(),
                  key=lambda kv: -(kv[1]["pub_usd"] + kv[1]["prv_usd"]))
    for kol, a in rank:
        best = "private" if a["prv_usd"] >= a["pub_usd"] else "public"
        print(f"{kol:4} {a['pub_n']:5d} {a['pub_usd']:9.2f} "
              f"{a['prv_n']:5d} {a['prv_usd']:9.2f}  {best}")
    if rank:
        top_k, top = rank[0]
        print(f"\n🏆 {top_k} leads (${top['pub_usd'] + top['prv_usd']:.2f} combined)")
    pp = by_path["public"]["usd"]
    pr = by_path["private"]["usd"]
    print(f"🥇 path: public ${pp:.2f}  vs  private ${pr:.2f}  → "
          f"{'private' if pr >= pp else 'public'}")
    print(f"\n[archive calls used: {_archive_calls[0]} / {_ARCHIVE_DAILY_CAP}]",
          file=sys.stderr)


if __name__ == "__main__":
    main()
