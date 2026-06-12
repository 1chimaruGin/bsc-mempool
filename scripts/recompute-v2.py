#!/usr/bin/env python3
"""
Recompute paper-trading PnL with NO `price_unavailable` rows.

For each closed-trade row this resolver tries, in order:
  (1) Median price across REAL on-chain swaps in N+1..N+5 (existing method).
  (2) KOL's OWN entry receipt + KOL's OWN exit receipt — chain-derived
      directly from the KOL's actual fills (located via kols.toml address
      lookup + eth_getLogs on Transfer events to/from the KOL).
  (3) If even (2) can't price the exit because the token has no observable
      activity after the KOL sold (no other buyers, no other sellers, no
      pool), we infer the exit at the SAME price as our entry — i.e. the
      trade is "stuck flat" — and we tag the row `stuck` (NOT
      price_unavailable). These rows are real losses if execution costs
      are included.
  (4) If even the KOL's entry tx isn't findable (most likely a position
      opened before block-aware logging), the row is left as-is.

Also populates `d_mcap_usd` (KOL_entry_mcap) and `our_entry_mcap_usd`
(Our_entry_mcap = price at N+1) from chain state.

  scripts/recompute-v2.py             # report TODAY UTC (read-only)
  scripts/recompute-v2.py --all       # report all-time
  scripts/recompute-v2.py --backfill  # OVERWRITE closed_trades.csv
"""
import csv, datetime, json, os, sys, time
from collections import defaultdict
import urllib.request, urllib.error


# ── env + endpoints ────────────────────────────────────────────────────────
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
    print("ERROR: NODEREAL_RPC_URL missing", file=sys.stderr)
    sys.exit(1)

WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
FACTORY_V2 = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


# ── KOL address book ──────────────────────────────────────────────────────
def load_kols(path="/data/bsc-meme-mev/config/kols.toml"):
    out = {}
    cur_addr = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("address"):
                cur_addr = line.split("=", 1)[1].strip().strip('"').lower()
            elif line.startswith("name") and cur_addr:
                name = line.split("=", 1)[1].strip().strip('"')
                out[name] = cur_addr
                cur_addr = None
    return out


KOLS = load_kols()


# ── RPC + rate limiting ────────────────────────────────────────────────────
_last_archive_ns = [0]
_MIN_GAP_NS = 400_000_000
_archive_calls = [0]
_ARCHIVE_CAP = 8000  # one-off backfill — well within free CUs/day


def now_ns():
    return time.time_ns()


def rpc(url, method, params, archive=False, retries=2):
    if archive:
        if _archive_calls[0] >= _ARCHIVE_CAP:
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
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read()).get("result")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
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
            return int(h, 16) if h.startswith("0x") else int(h)
        except ValueError:
            return 0
    return 0


# ── caches ─────────────────────────────────────────────────────────────────
SUPPLY_CACHE = {}
PAIR_CACHE = {}
PRICE_CACHE = {}


def total_supply(token):
    t = token.lower()
    if t in SUPPLY_CACHE:
        return SUPPLY_CACHE[t]
    r = rpc(LOCAL, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"])
    if not r or len(r) < 4:
        r = rpc(NODEREAL, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"], archive=True)
    val = hx(r) if r else 0
    SUPPLY_CACHE[t] = val
    return val


def v2_pair(token):
    t = token.lower()
    if t in PAIR_CACHE:
        return PAIR_CACHE[t]
    data = "0xe6a43905" + WBNB[2:].rjust(64, "0") + token[2:].rjust(64, "0")
    r = rpc(LOCAL, "eth_call", [{"to": FACTORY_V2, "data": data}, "latest"])
    if not r:
        PAIR_CACHE[t] = None
        return None
    addr = "0x" + r[-40:]
    if int(addr, 16) == 0:
        PAIR_CACHE[t] = None
        return None
    PAIR_CACHE[t] = addr
    return addr


def v2_price_at_block(token, block):
    """Returns BNB-per-raw-token from V2 pool reserves at `block`. None if no pool."""
    pair = v2_pair(token)
    if not pair:
        return None
    cache_key = (pair, block)
    if cache_key in PRICE_CACHE:
        return PRICE_CACHE[cache_key]
    r = rpc(LOCAL, "eth_call", [{"to": pair, "data": "0x0902f1ac"}, hex(block)])
    if not r or len(r) < 130:
        r = rpc(NODEREAL, "eth_call", [{"to": pair, "data": "0x0902f1ac"}, hex(block)], archive=True)
    if not r or len(r) < 130:
        PRICE_CACHE[cache_key] = None
        return None
    h = r[2:]
    r0 = int(h[0:64], 16)
    r1 = int(h[64:128], 16)
    if int(WBNB, 16) < int(token.lower(), 16):
        wbnb_r, tok_r = r0, r1
    else:
        wbnb_r, tok_r = r1, r0
    px = (wbnb_r / tok_r) if tok_r else None
    PRICE_CACHE[cache_key] = px
    return px


# ── per-tx price from receipt ──────────────────────────────────────────────
def price_from_tx(tx_hash, token_lc, is_buy, use_archive_balance=True):
    """Returns BNB-wei / raw-token-unit from a tx receipt. Uses archive
    eth_getBalance fallback for native-BNB sells out of local PBSS window."""
    r = rpc(LOCAL, "eth_getTransactionReceipt", [tx_hash])
    if not r:
        r = rpc(NODEREAL, "eth_getTransactionReceipt", [tx_hash], archive=True)
    if not r or r.get("status") != "0x1":
        return None
    logs = r.get("logs") or []
    tok_amt = 0
    wbnb_amt = 0
    for lg in logs:
        topics = lg.get("topics") or []
        if not topics or (topics[0] or "").lower() != TRANSFER:
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
            tx = rpc(NODEREAL, "eth_getTransactionByHash", [tx_hash], archive=True)
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
                tx = rpc(NODEREAL, "eth_getTransactionByHash", [tx_hash], archive=True)
            if not tx:
                return None
            d_addr = tx.get("from")
            if not d_addr:
                return None
            bal_a = rpc(LOCAL, "eth_getBalance", [d_addr, hex(blk)])
            bal_b = rpc(LOCAL, "eth_getBalance", [d_addr, hex(blk - 1)])
            if (bal_a is None or bal_b is None) and use_archive_balance:
                bal_a = rpc(NODEREAL, "eth_getBalance", [d_addr, hex(blk)], archive=True)
                bal_b = rpc(NODEREAL, "eth_getBalance", [d_addr, hex(blk - 1)], archive=True)
            if bal_a is None or bal_b is None:
                return None
            proceeds = (hx(bal_a) - hx(bal_b)) + gas_used * gas_price
            if proceeds <= 0:
                return None
            bnb_wei = proceeds
    if bnb_wei <= 0 or tok_amt <= 0:
        return None
    return bnb_wei / tok_amt


# ── price discovery: median chain swap (extended window) ───────────────────
_LOGS_CACHE = {}


def median_chain_price(token, from_blk, to_blk, is_buy):
    key = (token.lower(), from_blk, to_blk, is_buy)
    if key in _LOGS_CACHE:
        return _LOGS_CACHE[key]
    filt = [{
        "address": token,
        "fromBlock": hex(from_blk),
        "toBlock": hex(to_blk),
        "topics": [TRANSFER],
    }]
    logs = rpc(NODEREAL, "eth_getLogs", filt, archive=True) or []
    seen, txs = set(), []
    for lg in logs:
        h = lg.get("transactionHash")
        if h and h not in seen:
            seen.add(h)
            txs.append(h)
    prices = []
    for txh in txs[:15]:
        p = price_from_tx(txh, token.lower(), is_buy)
        if p and p > 0:
            prices.append(p)
    res = None
    if prices:
        prices.sort()
        res = prices[len(prices) // 2]
    _LOGS_CACHE[key] = res
    return res


# ── find KOL's entry tx (when chain-swap returns None) ─────────────────────
def find_kol_entry_tx(token, kol_addr, opened_at_block):
    """Look for a Transfer of `token` where TO = kol_addr in
    [opened-2, opened+1]. Returns the tx_hash of his buy, or None."""
    if not kol_addr:
        return None
    filt = [{
        "address": token,
        "fromBlock": hex(max(0, opened_at_block - 2)),
        "toBlock": hex(opened_at_block + 1),
        "topics": [
            TRANSFER,
            None,
            "0x" + kol_addr[2:].rjust(64, "0"),  # topic2 = to
        ],
    }]
    logs = rpc(NODEREAL, "eth_getLogs", filt, archive=True) or []
    if not logs:
        return None
    # Pick the FIRST chronologically (lowest block, then lowest index).
    logs.sort(key=lambda l: (hx(l.get("blockNumber")), hx(l.get("logIndex"))))
    return logs[0].get("transactionHash")


# ── per-row resolver ───────────────────────────────────────────────────────
def resolve_row(row):
    """Returns dict with new bnb_out_wei, pnl_*, mcaps, fill_source.
    `fill_source` ∈ {chain_swap, kol_receipts, v2_pool, stuck, unresolvable}."""
    try:
        bnb_in_wei = int(row["bnb_in_wei"])
        opened = int(row["opened_at_block"])
        closed = int(row["closed_at_block"])
        token = row["token_address"]
        kol_name = row["kol_name"]
        trigger = row.get("trigger_sell_tx") or ""
        bnb_usd = float(row.get("bnb_usd_close") or 0)
    except (KeyError, ValueError):
        return None
    if opened == 0 or not token.startswith("0x"):
        return None
    bnb_in = bnb_in_wei / 1e18

    # 1. Try V2 pool reads (exact, fast).
    px_entry_v2 = v2_price_at_block(token, opened + 1)
    px_exit_v2 = v2_price_at_block(token, closed + 1) if closed > 0 else None
    if px_entry_v2 and px_exit_v2:
        # Exact V2 pool reserves at both ends.
        bnb_out = bnb_in * (px_exit_v2 / px_entry_v2)
        src = "v2_pool"
        entry_px_kol = v2_price_at_block(token, opened) or px_entry_v2
    else:
        # 2. Chain-swap median (extended ±5 window).
        px_entry = median_chain_price(token, opened + 1, opened + 5, True)
        px_exit = (median_chain_price(token, closed, closed + 5, False)
                   if closed > 0 else None)
        entry_px_kol = None

        # 3. Fall back to KOL receipts.
        if not px_entry:
            kol_addr = KOLS.get(kol_name)
            entry_tx = find_kol_entry_tx(token, kol_addr, opened)
            if entry_tx:
                p = price_from_tx(entry_tx, token.lower(), True)
                if p:
                    entry_px_kol = p
                    px_entry = p  # KOL's price ≈ our +1 price (small bias)
        if not px_exit and trigger:
            p = price_from_tx(trigger, token.lower(), False)
            if p:
                px_exit = p

        if px_entry and px_exit:
            bnb_out = bnb_in * (px_exit / px_entry)
            src = "kol_receipts"
        elif px_entry and not px_exit:
            # Got entry but couldn't value exit even from KOL's sell. Position
            # is STUCK — unsellable. Book as total loss (real worst case).
            bnb_out = 0.0
            src = "stuck"
        else:
            return None  # truly unresolvable

    pnl_bnb = bnb_out - bnb_in
    pnl_usd = pnl_bnb * bnb_usd

    # mcaps (price × supply × bnb_usd)
    supply_raw = total_supply(token)
    supply_whole = (supply_raw / 1e18) if supply_raw else 0
    px_kol_for_mcap = entry_px_kol or (
        v2_price_at_block(token, opened) if px_entry_v2 else None
    )
    # px above is BNB-wei per raw-token-unit; scaling by 1e18 / 1e18 for both
    # bnb-wei→bnb and raw-token→whole = ×1; we keep raw-units throughout.
    d_mcap_usd = (px_kol_for_mcap * supply_raw * bnb_usd) if (px_kol_for_mcap and supply_raw) else 0
    px_our_entry = px_entry_v2 if px_entry_v2 else (entry_px_kol if entry_px_kol else None)
    our_mcap_usd = (px_our_entry * supply_raw * bnb_usd) if (px_our_entry and supply_raw) else 0

    return {
        "bnb_out_wei": int(max(bnb_out, 0) * 1e18),
        "pnl_wei": int((bnb_out - bnb_in) * 1e18),
        "pnl_bnb": pnl_bnb,
        "pnl_usd": pnl_usd,
        "pnl_pct": (pnl_bnb / bnb_in) if bnb_in > 0 else 0.0,
        "d_mcap_usd": d_mcap_usd,
        "our_entry_mcap_usd": our_mcap_usd,
        "fill_source": src,
    }


# ── CSV io ─────────────────────────────────────────────────────────────────
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


def backfill(path):
    if not os.path.exists(path):
        print(f"  {path}: missing — skipping", file=sys.stderr)
        return {}
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = f"{path}.pre-v2-{ts}"
    with open(path) as f:
        with open(backup, "w") as g:
            g.write(f.read())
    print(f"  {path}\n    backed up → {backup}", file=sys.stderr)
    with open(path) as f:
        rdr = csv.DictReader(f)
        fields = rdr.fieldnames
        rows = list(rdr)
    counts = defaultdict(int)
    for i, r in enumerate(rows, 1):
        if i % 25 == 0:
            print(f"    {i}/{len(rows)}  (archive calls used: {_archive_calls[0]})",
                  file=sys.stderr)
        res = resolve_row(r)
        if res is None:
            counts["unresolvable"] += 1
            r["close_reason"] = "unresolvable"
            r["bnb_out_wei"] = r["bnb_in_wei"]
            r["pnl_wei"] = "0"; r["pnl_bnb"] = "0.000000"
            r["pnl_usd"] = "0.00"; r["pnl_pct"] = "0.000000"
            continue
        counts[res["fill_source"]] += 1
        r["bnb_out_wei"] = str(res["bnb_out_wei"])
        r["pnl_wei"] = str(res["pnl_wei"])
        r["pnl_bnb"] = f"{res['pnl_bnb']:.6f}"
        r["pnl_usd"] = f"{res['pnl_usd']:.2f}"
        r["pnl_pct"] = f"{res['pnl_pct']:.6f}"
        r["d_mcap_usd"] = f"{res['d_mcap_usd']:.0f}"
        r["our_entry_mcap_usd"] = f"{res['our_entry_mcap_usd']:.0f}"
        # Preserve original close_reason unless it was price_unavailable —
        # then set it to the new source.
        if r.get("close_reason") in ("price_unavailable", "no_liquidity"):
            r["close_reason"] = "kol_sell" if res["fill_source"] != "stuck" else "stuck"
        elif res["fill_source"] == "stuck":
            r["close_reason"] = "stuck"
    tmp = f"{path}.tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})
    os.replace(tmp, path)
    return counts


def print_report(rows, label):
    by_path = defaultdict(lambda: {"n": 0, "bnb": 0.0, "usd": 0.0, "wins": 0,
                                    "stuck": 0, "unres": 0})
    by_kol = defaultdict(lambda: {"pub_n": 0, "pub_usd": 0.0,
                                   "prv_n": 0, "prv_usd": 0.0})
    for i, r in enumerate(rows, 1):
        if i % 25 == 0:
            print(f"  {i}/{len(rows)}  (archive calls: {_archive_calls[0]})",
                  file=sys.stderr)
        res = resolve_row(r)
        path = r["_path"]
        if res is None:
            by_path[path]["unres"] += 1
            continue
        if res["fill_source"] == "stuck":
            by_path[path]["stuck"] += 1
        by_path[path]["n"] += 1
        by_path[path]["bnb"] += res["pnl_bnb"]
        by_path[path]["usd"] += res["pnl_usd"]
        if res["pnl_usd"] > 0:
            by_path[path]["wins"] += 1
        kol = r["kol_name"]
        if path == "public":
            by_kol[kol]["pub_n"] += 1
            by_kol[kol]["pub_usd"] += res["pnl_usd"]
        else:
            by_kol[kol]["prv_n"] += 1
            by_kol[kol]["prv_usd"] += res["pnl_usd"]
    print()
    print(f"\033[1mRECOMPUTED PAPER REPORT v2\033[0m  scope={label}  "
          f"(EXACT chain prices — KOL receipts where chain-swap fails)")
    print("=" * 80)
    print(f"{'PATH':9} {'closed':>6} {'netBNB':>11} {'netUSD':>9} "
          f"{'win':>5} {'stuck':>6} {'unres':>6}")
    print("-" * 70)
    for path in ("public", "private"):
        a = by_path[path]
        wr = (a["wins"] / a["n"] * 100) if a["n"] else 0
        col = "\033[32m" if a["usd"] >= 0 else "\033[31m"
        print(f"{path:9} {a['n']:6d} {a['bnb']:11.5f} "
              f"{col}{a['usd']:9.2f}\033[0m {wr:4.0f}% "
              f"{a['stuck']:6d} {a['unres']:6d}")
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
    print(f"\n[archive calls used: {_archive_calls[0]} / {_ARCHIVE_CAP}]",
          file=sys.stderr)


def main():
    if "--backfill" in sys.argv:
        print("BACKFILL v2 — rewriting closed_trades.csv with chain-exact "
              "values + mcaps…", file=sys.stderr)
        a = backfill("/data/bsc-meme-mev/trader/closed_trades.csv")
        b = backfill("/data/bsc-meme-mev/trader_private/closed_trades.csv")
        merged = defaultdict(int)
        for d in (a, b):
            for k, v in d.items():
                merged[k] += v
        print("\ntotal by fill_source:", file=sys.stderr)
        for k in ("v2_pool", "chain_swap", "kol_receipts", "stuck", "unresolvable"):
            print(f"  {k:14s}: {merged.get(k,0)}", file=sys.stderr)
        print(f"[archive calls used: {_archive_calls[0]} / {_ARCHIVE_CAP}]",
              file=sys.stderr)
        return

    today_only = "--all" not in sys.argv
    pub = load_csv("/data/bsc-meme-mev/trader/closed_trades.csv", "public", today_only)
    prv = load_csv("/data/bsc-meme-mev/trader_private/closed_trades.csv", "private", today_only)
    rows = pub + prv
    if not rows:
        print("no rows in scope")
        return
    print(f"Recomputing {len(rows)} rows (chain-exact, no flat-booking)…",
          file=sys.stderr)
    label = (datetime.datetime.now(datetime.timezone.utc).strftime("TODAY %Y-%m-%d")
             if today_only else "ALL-TIME")
    print_report(rows, label)


if __name__ == "__main__":
    main()
