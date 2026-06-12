#!/usr/bin/env python3
"""
v2 microstructure scanner — captures actor-level identity + token holders.

For each D BUY in the scan window:
  1. Pull launchpad TradeBuy + TradeSell events (lookahead window)
     - For each event: keep actor address + side
     - Tag actors that match GOAT-KOL addresses (smart-money cohort)
  2. Pull token-contract Transfer events (same window)
     - Per Transfer: from, to, amount
     - Reconstruct per-block holder set + balance map
  3. Compute per-block enriched features:
       buyers, sellers (counts)
       buy_bnb, sell_bnb
       kol_buyers, kol_sellers (smart-money flow)
       new_holders_block, total_holders_cum
       top10_holders_share (concentration)
       early_buyer_holders_remaining (cohort retention)
  4. Cache as JSON for downstream feature engineering.

Output:
  d_microstructure_v2.csv             flat per-token summary
  d_microstructure_v2_paths.json      full per-block enriched cache
"""
import argparse, csv, json, sys, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

NODEREAL_HTTP = "https://bsc-mainnet.nodereal.io/v1/3bed06fc28e04f73a64a54da9c575a47"
D_ADDR        = "0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373"
LAUNCHPAD     = "0x5c952063c7fc8610FFDB798152D69F0B9550762b"
TRADE_BUY_TOPIC  = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
TRADE_SELL_TOPIC = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"
TRANSFER_TOPIC   = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# 15 GOAT KOL wallets — extracted from config/kols.toml on 2026-06-04.
GOAT_KOLS = {
    "0x38e47fece3ea323e864c65410f6458c820eaa897": "O",
    "0x0cfba30f815e6209bb60480236dad04e00e5c7c9": "L",
    "0xc2c6acd377458010713e733e1b21dd6f670d091c": "M",
    "0x76a280376c5332abbbae1786a73c70116906e757": "N",
    "0x7e8fb0392542812476d9f2d0d71c01d1fa0776c5": "G",
    "0xa7d4ffc4eca3c71af150ce302560a9d04a1d2b9f": "H",
    "0x077b9981bc8a2ca417cea41861111da63266988b": "I",
    "0xa05ec35f7d1eba823cff2ed26aeaed419683742f": "K",
    "0x085111103c78e708199e1779789eefe9529d5d3a": "E",
    "0x176e6378b7c9010f0456bee76ce3039d36dc37c8": "F",
    "0x8d5624fa29526c879a1ca7560961e4c5a08089ae": "J",
    "0xfe631cd3c9f7e879f936515265302677805f87b9": "B",
    "0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373": "D",
    "0xbf004bff64725914ee36d03b87d6965b0ced4903": "A",
    "0x7a2363a401b2340c7941dd2eeff0196a5078d2e6": "C",
}

LOOKAHEAD_BLOCKS = 4000
SCAN_BLOCKS      = 6_000_000   # 30 days
CHUNK_SIZE       = 5000
WORKERS          = 16
EARLY_BUYER_N    = 20          # define "early buyer cohort" = first N distinct buyers

# ── RPC ──────────────────────────────────────────────────────────────

def rpc(method, params, retries=4):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":1}).encode()
    last = None
    for attempt in range(retries):
        try:
            req = Request(NODEREAL_HTTP, data=body, headers={"Content-Type":"application/json"})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                if "error" in data:
                    msg = data["error"].get("message","?")
                    if "limit" in msg.lower() or "exceed" in msg.lower():
                        time.sleep(2 ** attempt); continue
                    raise RuntimeError(msg)
                return data.get("result")
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            last = e; time.sleep(2 ** attempt)
    raise RuntimeError(f"rpc failed: {last}")

def get_logs(addr, from_block, to_block, topics):
    return rpc("eth_getLogs", [{
        "address":   addr,
        "fromBlock": hex(from_block),
        "toBlock":   hex(to_block),
        "topics":    topics,
    }]) or []

def block_now():
    return int(rpc("eth_blockNumber", []), 16)

# ── Event decoders ──────────────────────────────────────────────────

def decode_trade(log, is_buy):
    data = log["data"][2:]
    if len(data) < 32 * 6 * 2: return None
    try:
        w = lambda i: data[i*64:(i+1)*64]
        token  = "0x" + w(0)[24:]
        actor  = "0x" + w(1)[24:]
        tokens = int(w(3), 16)
        bnb_n  = int(w(4), 16)
        fee    = int(w(5), 16)
    except ValueError:
        return None
    bnb = bnb_n + fee
    if tokens < 10**15 or bnb < 1000: return None
    price = bnb / tokens
    if not (1e-15 <= price <= 1e-4): return None
    blk = log.get("blockNumber")
    if not blk: return None
    return {
        "side":   "buy" if is_buy else "sell",
        "token":  token.lower(),
        "actor":  actor.lower(),
        "block":  int(blk, 16),
        "log_index": int(log.get("logIndex","0x0"), 16),
        "price":  price,
        "bnb":    bnb,
        "tokens": tokens,
    }

def decode_transfer(log):
    """Decode ERC20 Transfer event. Returns dict or None."""
    topics = log.get("topics", [])
    if len(topics) < 3: return None
    try:
        # topic[1] = from (indexed), topic[2] = to (indexed)
        from_addr = "0x" + topics[1][-40:]
        to_addr   = "0x" + topics[2][-40:]
        amount    = int(log["data"], 16)
    except (ValueError, KeyError):
        return None
    blk = log.get("blockNumber")
    if not blk: return None
    return {
        "from":   from_addr.lower(),
        "to":     to_addr.lower(),
        "amount": amount,
        "block":  int(blk, 16),
        "log_index": int(log.get("logIndex","0x0"), 16),
    }

# ── Scanners ────────────────────────────────────────────────────────

def scan_topic_parallel(addr, topic, from_block, to_block, label, decoder, filter_fn=None):
    """Bulk scan with thread-pool, optional decoder + filter."""
    chunks = [(b, min(b + CHUNK_SIZE - 1, to_block))
              for b in range(from_block, to_block + 1, CHUNK_SIZE)]
    out = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(get_logs, addr, lo, hi, [topic]): (lo, hi) for lo, hi in chunks}
        for fut in as_completed(futs):
            try:
                for log in fut.result():
                    rec = decoder(log)
                    if rec is None: continue
                    if filter_fn and not filter_fn(rec): continue
                    out.append(rec)
            except Exception:
                pass
    out.sort(key=lambda r: (r["block"], r["log_index"]))
    return out

def scan_d_buys(from_block, to_block):
    print(f"[scan_d_buys] window={from_block}..{to_block}", file=sys.stderr)
    return scan_topic_parallel(
        LAUNCHPAD, TRADE_BUY_TOPIC, from_block, to_block, "d_buys",
        lambda log: decode_trade(log, is_buy=True),
        filter_fn=lambda r: r["actor"] == D_ADDR.lower(),
    )

def scan_token_trades(token, from_block, to_block):
    """All launchpad TradeBuy + TradeSell events for one token."""
    chunks = [(b, min(b + CHUNK_SIZE - 1, to_block))
              for b in range(from_block, to_block + 1, CHUNK_SIZE)]
    events = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {}
        for lo, hi in chunks:
            futs[ex.submit(get_logs, LAUNCHPAD, lo, hi, [TRADE_BUY_TOPIC])]  = (lo, hi, True)
            futs[ex.submit(get_logs, LAUNCHPAD, lo, hi, [TRADE_SELL_TOPIC])] = (lo, hi, False)
        for fut in as_completed(futs):
            lo, hi, is_buy = futs[fut]
            try:
                for log in fut.result():
                    rec = decode_trade(log, is_buy=is_buy)
                    if rec and rec["token"] == token.lower():
                        events.append(rec)
            except Exception:
                pass
    events.sort(key=lambda r: (r["block"], r["log_index"]))
    return events

def scan_token_transfers(token, from_block, to_block):
    """All Transfer events on the token contract."""
    return scan_topic_parallel(
        token, TRANSFER_TOPIC, from_block, to_block, "transfers",
        decode_transfer,
    )

# ── Per-token enrichment ────────────────────────────────────────────

def analyze_token(d_buy, lookahead):
    token   = d_buy["token"]
    d_block = d_buy["block"]
    end     = d_block + lookahead

    trades    = scan_token_trades(token, d_block, end)
    transfers = scan_token_transfers(token, d_block, end)
    if not trades and not transfers:
        return None

    # Reconstruct holder balances from Transfer events.
    # Note: launchpad address holds curve reserves; we exclude it from
    # "holders" since its balance reflects pool state, not retail demand.
    balances = defaultdict(int)
    holders_at_block = {}        # block → set of addresses with balance > 0
    new_holder_count_at = {}     # block → count of newly-distinct addresses

    ever_seen_holders = set()
    for ev in transfers:
        balances[ev["from"]] -= ev["amount"]
        balances[ev["to"]]   += ev["amount"]
        # Track who's actually a holder
        first_time_holders = set()
        if balances[ev["to"]] > 0 and ev["to"] not in ever_seen_holders:
            first_time_holders.add(ev["to"])
            ever_seen_holders.add(ev["to"])
        # We aggregate at block boundaries
        b = ev["block"]
        if b not in holders_at_block:
            holders_at_block[b] = set()
            new_holder_count_at[b] = 0
        new_holder_count_at[b] += len(first_time_holders)

    # Build sorted unique block list
    all_blocks = sorted(set(e["block"] for e in trades) | set(holders_at_block.keys()))
    if not all_blocks: return None

    # Snapshot holder set per block (cumulative — only adds, can shrink if balance→0)
    # For memory efficiency we don't keep per-block full holder sets, just:
    #   holder_count_cum, top_holder_share (calculate at sentinel snapshots)

    # Re-walk transfers in order, tracking running balances + holder count + top concentration
    balances2 = defaultdict(int)
    holder_set = set()
    per_block_holders = {}     # block → (holder_count, top10_share, kol_holders_count)

    LP = LAUNCHPAD.lower()
    excluded = {LP, "0x0000000000000000000000000000000000000000"}

    transfers_by_block = defaultdict(list)
    for ev in transfers:
        transfers_by_block[ev["block"]].append(ev)

    # Walk blocks chronologically
    early_buyers = []  # first EARLY_BUYER_N buyer addresses (preserves order)
    early_buyer_set = set()

    for b in sorted(transfers_by_block.keys()):
        for ev in transfers_by_block[b]:
            f, t, a = ev["from"], ev["to"], ev["amount"]
            if f not in excluded:
                balances2[f] -= a
                if balances2[f] <= 0 and f in holder_set:
                    holder_set.discard(f)
            if t not in excluded:
                balances2[t] += a
                if balances2[t] > 0 and t not in holder_set:
                    holder_set.add(t)
                    # An incoming Transfer where from = launchpad means "BUY" → track early-buyer cohort
                    if f == LP and len(early_buyers) < EARLY_BUYER_N and t not in early_buyer_set:
                        early_buyers.append(t)
                        early_buyer_set.add(t)
        # Snapshot at end-of-block
        if holder_set:
            # Top-10 holder share of NON-EXCLUDED balance
            non_excluded_total = sum(balances2[h] for h in holder_set)
            top_holders = sorted(holder_set, key=lambda h: -balances2[h])[:10]
            top10_amount = sum(balances2[h] for h in top_holders)
            top10_share  = top10_amount / non_excluded_total if non_excluded_total > 0 else 0
        else:
            top10_share = 0
        kol_holders = sum(1 for h in holder_set if h in GOAT_KOLS)
        early_remaining = sum(1 for e in early_buyers if e in holder_set)
        per_block_holders[b] = {
            "holder_count":    len(holder_set),
            "top10_share":     top10_share,
            "kol_holders":     kol_holders,
            "early_remaining": early_remaining,
        }

    # Per-block aggregation of trade tape
    per_block_trade = defaultdict(lambda: {
        "buyers": set(), "sellers": set(),
        "kol_buyers": set(), "kol_sellers": set(),
        "buy_bnb": 0, "sell_bnb": 0,
        "buys": 0, "sells": 0,
        "max_price": 0, "last_price": 0,
    })

    for ev in trades:
        b = per_block_trade[ev["block"]]
        a = ev["actor"]
        if ev["side"] == "buy":
            b["buyers"].add(a)
            b["buy_bnb"] += ev["bnb"]
            b["buys"]    += 1
            if a in GOAT_KOLS:
                b["kol_buyers"].add(GOAT_KOLS[a])
        else:
            b["sellers"].add(a)
            b["sell_bnb"] += ev["bnb"]
            b["sells"]    += 1
            if a in GOAT_KOLS:
                b["kol_sellers"].add(GOAT_KOLS[a])
        b["max_price"]  = max(b["max_price"], ev["price"])
        b["last_price"] = ev["price"]

    # Combine: build the final per-block dict
    all_blocks = sorted(set(per_block_trade.keys()) | set(per_block_holders.keys()))
    per_block_out = {}
    last_holder_state = {"holder_count": 0, "top10_share": 0, "kol_holders": 0, "early_remaining": 0}
    last_price = 0
    for b in all_blocks:
        t = per_block_trade.get(b)
        h = per_block_holders.get(b, last_holder_state)
        last_holder_state = h
        if t:
            last_price = t["last_price"]
        per_block_out[b] = {
            "buyers":         len(t["buyers"])  if t else 0,
            "sellers":        len(t["sellers"]) if t else 0,
            "buy_bnb":        t["buy_bnb"]      if t else 0,
            "sell_bnb":       t["sell_bnb"]     if t else 0,
            "kol_buyers":     sorted(t["kol_buyers"])  if t else [],
            "kol_sellers":    sorted(t["kol_sellers"]) if t else [],
            "max_price":      t["max_price"]    if t else last_price,
            "last_price":     t["last_price"]   if t else last_price,
            "holder_count":   h["holder_count"],
            "top10_share":    h["top10_share"],
            "kol_holders":    h["kol_holders"],
            "early_remaining":h["early_remaining"],
        }

    # n2_price = first observed at block ≥ d_block + 2
    n2_price = None
    for b in sorted(per_block_out.keys()):
        if b >= d_block + 2 and per_block_out[b]["last_price"] > 0:
            n2_price = per_block_out[b]["last_price"]
            break
    if n2_price is None:
        n2_price = d_buy["price"]

    return {
        "d_block":  d_block,
        "token":    token,
        "tx":       d_buy.get("tx", ""),
        "d_price":  d_buy["price"],
        "n2_price": n2_price,
        "n_events": len(trades) + len(transfers),
        "n_trades": len(trades),
        "n_transfers": len(transfers),
        "n_blocks_active": len(all_blocks),
        "_per_block": per_block_out,
    }

# ── main ────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blocks", type=int, default=SCAN_BLOCKS)
    ap.add_argument("--lookahead", type=int, default=LOOKAHEAD_BLOCKS)
    ap.add_argument("--out", default="d_microstructure_v2.csv")
    ap.add_argument("--cache", default="d_microstructure_v2_paths.json")
    ap.add_argument("--max", type=int, default=0)
    ap.add_argument("--reuse_d_buys", default=None,
                    help="Path to JSON with cached d_buys list (skip rescan)")
    args = ap.parse_args()

    # Try reusing the d_buys list from the previous run
    d_buys = None
    if args.reuse_d_buys:
        try:
            with open(args.reuse_d_buys) as f:
                prev = json.load(f)
            d_buys = [{"token": p["token"], "block": p["d_block"], "tx": p.get("tx",""),
                       "price": p.get("d_price", 0), "actor": D_ADDR.lower(), "log_index": 0}
                      for p in prev]
            print(f"reused {len(d_buys)} D-buys from {args.reuse_d_buys}", file=sys.stderr)
        except Exception as e:
            print(f"reuse failed: {e}", file=sys.stderr)
            d_buys = None

    if d_buys is None:
        latest = block_now()
        from_block = latest - args.blocks
        print(f"head={latest} window={from_block}..{latest}", file=sys.stderr)
        d_buys = scan_d_buys(from_block, latest)
        print(f"found {len(d_buys)} D BUYs", file=sys.stderr)

    if args.max and len(d_buys) > args.max:
        d_buys = d_buys[-args.max:]

    rows = []
    t0 = time.time()
    for i, d in enumerate(d_buys, 1):
        r = analyze_token(d, args.lookahead)
        if r: rows.append(r)
        if i % 20 == 0 or i == len(d_buys):
            rate = i / (time.time() - t0 + 1e-9)
            eta  = (len(d_buys) - i) / rate if rate else 0
            print(f"  [{i}/{len(d_buys)}] rate={rate:.1f}/s eta={eta:.0f}s", file=sys.stderr)

    try:
        with open(args.cache, "w") as f:
            json.dump(rows, f)
        print(f"cached {len(rows)} → {args.cache}", file=sys.stderr)
    except Exception as e:
        print(f"cache save failed: {e}", file=sys.stderr)

    # Flat CSV summary
    cols = ["d_block","token","tx","d_price","n2_price","n_trades","n_transfers","n_blocks_active"]
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f); w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") if r.get(c) is not None else "" for c in cols])
    print(f"\nwrote {len(rows)} rows → {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()
