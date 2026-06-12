#!/usr/bin/env python3
"""
Per-token microstructure analysis on KOL D's purchases.

For each token D bought in the scan window:
  1. Pull every TradeBuy + TradeSell event for `LOOKAHEAD_BLOCKS` after D's BUY
  2. Aggregate per block: # unique buyers, # unique sellers, buy/sell BNB,
     net flow, largest single trade per side, max-price
  3. Compute single-token signals:
       d_entry_block, d_entry_price, n2_price
       ath_block, ath_price, blocks_to_ath
       first_sell_block        : first block with ANY sell after D entry
       net_flip_block          : first block where net_flow_bnb went negative AFTER ATH
       dump_block (20% from peak)  : first block where price ≤ peak × 0.8
       dump_block (30% from peak)  : first block where price ≤ peak × 0.7
       buyer_count_decline_block: first block where 5-block rolling buyer
                                  count drops 50%+ vs 5-block window before
       max_single_sell_bnb     : largest individual sell in the window
       max_single_sell_block   : when the whale dumped
  4. Aggregate across all tokens:
       distribution of blocks-to-ATH
       distribution of blocks-from-D-to-dump
       lead time of each signal vs the actual dump

Goal: find a leading signal that fires BEFORE the dump completes so we
can exit the position with more of the gain locked in than the trail's
30% give-back.

Run:
  python3 scripts/analyze-kol-microstructure.py [--blocks N] [--out CSV]
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

LOOKAHEAD_BLOCKS = 4000      # ~30 min — full memecoin lifecycle
SCAN_BLOCKS      = 1_350_000 # ~1 week of BSC blocks
CHUNK_SIZE       = 5000
WORKERS          = 16

# ── RPC helpers ──────────────────────────────────────────────────────

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
    raise RuntimeError(f"rpc {method} failed: {last}")

def get_logs(addr, from_block, to_block, topics):
    return rpc("eth_getLogs", [{
        "address":   addr,
        "fromBlock": hex(from_block),
        "toBlock":   hex(to_block),
        "topics":    topics,
    }]) or []

def block_now():
    return int(rpc("eth_blockNumber", []), 16)

# ── event decoders (Trade Buy/Sell share layout) ────────────────────

def decode_trade_event(log, is_buy):
    data = log["data"][2:]
    if len(data) < 32 * 6 * 2: return None
    try:
        w = lambda i: data[i*64:(i+1)*64]
        token  = "0x" + w(0)[24:]
        actor  = "0x" + w(1)[24:]  # buyer or seller
        tokens = int(w(3), 16)
        bnb_n  = int(w(4), 16)
        fee    = int(w(5), 16)
    except ValueError:
        return None
    bnb = bnb_n + fee
    if tokens < 10**15 or bnb < 1000:
        return None
    price = bnb / tokens
    if not (1e-15 <= price <= 1e-4): return None
    blk = log.get("blockNumber")
    if not blk: return None
    return {
        "side":   "buy" if is_buy else "sell",
        "token":  token.lower(),
        "actor":  actor.lower(),
        "block":  int(blk, 16),
        "tx":     log["transactionHash"],
        "log_index": int(log.get("logIndex","0x0"), 16),
        "price":  price,           # BNB-wei / raw-token (same units both sides)
        "bnb":    bnb,             # gross BNB in wei (paid for buy, received for sell)
        "tokens": tokens,          # raw token amount
    }

# ── bulk scanners ───────────────────────────────────────────────────

def scan_topic(topic, from_block, to_block, label=""):
    chunks = [(b, min(b + CHUNK_SIZE - 1, to_block))
              for b in range(from_block, to_block + 1, CHUNK_SIZE)]
    logs = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(get_logs, LAUNCHPAD, lo, hi, [topic]): (lo, hi) for lo, hi in chunks}
        done = 0
        for fut in as_completed(futs):
            try:
                r = fut.result()
                logs.extend(r)
            except Exception as e:
                lo, hi = futs[fut]
                print(f"  [{label} chunk {lo}-{hi}] {e}", file=sys.stderr)
            done += 1
            if done % 20 == 0 or done == len(chunks):
                print(f"  [{label}] {done}/{len(chunks)} chunks, {len(logs)} events", file=sys.stderr)
    return logs

def scan_d_buys(from_block, to_block):
    print(f"[scan_d_buys] {from_block}..{to_block} ({to_block-from_block:,} blocks)", file=sys.stderr)
    raw = scan_topic(TRADE_BUY_TOPIC, from_block, to_block, "d_buys")
    out = []
    for log in raw:
        rec = decode_trade_event(log, is_buy=True)
        if rec and rec["actor"] == D_ADDR.lower():
            out.append(rec)
    out.sort(key=lambda r: (r["block"], r["log_index"]))
    return out

def scan_token_window(token, from_block, to_block):
    """All buys + sells on the given token in the window."""
    chunks = [(b, min(b + CHUNK_SIZE - 1, to_block))
              for b in range(from_block, to_block + 1, CHUNK_SIZE)]
    events = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {}
        for lo, hi in chunks:
            futs[ex.submit(get_logs, LAUNCHPAD, lo, hi, [TRADE_BUY_TOPIC])] = (lo, hi, "buy")
            futs[ex.submit(get_logs, LAUNCHPAD, lo, hi, [TRADE_SELL_TOPIC])] = (lo, hi, "sell")
        for fut in as_completed(futs):
            lo, hi, side = futs[fut]
            try:
                for log in fut.result():
                    rec = decode_trade_event(log, is_buy=(side=="buy"))
                    if rec and rec["token"] == token.lower():
                        events.append(rec)
            except Exception:
                pass
    events.sort(key=lambda r: (r["block"], r["log_index"]))
    return events

# ── per-token microstructure ───────────────────────────────────────

def analyze_token(d_buy, lookahead):
    """
    For one D BUY, fetch the full event tape and compute signals.
    """
    token   = d_buy["token"]
    d_block = d_buy["block"]
    end     = d_block + lookahead
    events  = scan_token_window(token, d_block, end)

    if not events:
        return None

    # Per-block aggregation
    per_block = defaultdict(lambda: {
        "buyers": set(), "sellers": set(),
        "buy_bnb": 0,   "sell_bnb": 0,
        "buys": 0,      "sells": 0,
        "max_price": 0, "min_price": float("inf"),
        "max_buy_bnb": 0, "max_sell_bnb": 0,
        "last_price": 0,
    })

    for ev in events:
        b = per_block[ev["block"]]
        if ev["side"] == "buy":
            b["buyers"].add(ev["actor"])
            b["buy_bnb"]    += ev["bnb"]
            b["buys"]       += 1
            b["max_buy_bnb"] = max(b["max_buy_bnb"], ev["bnb"])
        else:
            b["sellers"].add(ev["actor"])
            b["sell_bnb"]   += ev["bnb"]
            b["sells"]      += 1
            b["max_sell_bnb"] = max(b["max_sell_bnb"], ev["bnb"])
        b["max_price"] = max(b["max_price"], ev["price"])
        b["min_price"] = min(b["min_price"], ev["price"])
        b["last_price"] = ev["price"]

    # Walk blocks in order, deriving rolling state
    blocks_sorted = sorted(per_block.keys())
    if not blocks_sorted:
        return None

    # N+2 entry price: first observed at block ≥ d_block + 2
    n2_price = None
    for b in blocks_sorted:
        if b >= d_block + 2:
            n2_price = per_block[b]["last_price"]
            break
    if n2_price is None:
        n2_price = d_buy["price"]

    # ATH (highest price) within window
    ath_block = max(blocks_sorted, key=lambda b: per_block[b]["max_price"])
    ath_price = per_block[ath_block]["max_price"]

    # First sell block (any sell, after D-entry)
    first_sell_block = None
    for b in blocks_sorted:
        if b >= d_block and per_block[b]["sells"] > 0:
            first_sell_block = b
            break

    # Net-flip after ATH: first block where sell_bnb > buy_bnb
    net_flip_block = None
    for b in blocks_sorted:
        if b > ath_block and per_block[b]["sell_bnb"] > per_block[b]["buy_bnb"]:
            net_flip_block = b
            break

    # Dump triggers: -20% and -30% from peak
    dump_20_block = None
    dump_30_block = None
    for b in blocks_sorted:
        if b <= ath_block: continue
        pr = per_block[b]["last_price"]
        if dump_20_block is None and pr <= ath_price * 0.80:
            dump_20_block = b
        if dump_30_block is None and pr <= ath_price * 0.70:
            dump_30_block = b
        if dump_20_block and dump_30_block:
            break

    # Buyer-count decline (rolling 5-block window): first block where
    # mean(buyer_count) in [b-4..b] < 50% of mean in [b-9..b-5]
    buyer_decline_block = None
    bc = [len(per_block[b]["buyers"]) for b in blocks_sorted]
    for i in range(10, len(blocks_sorted)):
        prev = sum(bc[i-9:i-4]) / 5
        curr = sum(bc[i-4:i+1]) / 5
        if prev >= 2 and curr < prev * 0.5:
            buyer_decline_block = blocks_sorted[i]
            break

    # Biggest single sell + when
    max_sell_bnb = 0
    max_sell_block = None
    for ev in events:
        if ev["side"] == "sell" and ev["bnb"] > max_sell_bnb:
            max_sell_bnb = ev["bnb"]
            max_sell_block = ev["block"]

    return {
        "d_block":  d_block,
        "token":    token,
        "tx":       d_buy["tx"],
        "d_price":  d_buy["price"],
        "n2_price": n2_price,
        "ath_block":     ath_block,
        "ath_price":     ath_price,
        "ath_offset":    ath_block - d_block,
        "ath_mult_vs_n2": ath_price / n2_price if n2_price else None,
        "first_sell_offset":     first_sell_block - d_block if first_sell_block else None,
        "net_flip_offset":       net_flip_block - d_block if net_flip_block else None,
        "dump_20_offset":        dump_20_block  - d_block if dump_20_block  else None,
        "dump_30_offset":        dump_30_block  - d_block if dump_30_block  else None,
        "buyer_decline_offset":  buyer_decline_block - d_block if buyer_decline_block else None,
        "max_sell_bnb_wei":      max_sell_bnb,
        "max_sell_offset":       max_sell_block - d_block if max_sell_block else None,
        "n_events":              len(events),
        "n_blocks_active":       len(blocks_sorted),
        "_per_block":            {b: {
            "buyers":    len(per_block[b]["buyers"]),
            "sellers":   len(per_block[b]["sellers"]),
            "buy_bnb":   per_block[b]["buy_bnb"],
            "sell_bnb":  per_block[b]["sell_bnb"],
            "max_price": per_block[b]["max_price"],
            "last_price": per_block[b]["last_price"],
        } for b in blocks_sorted},
    }

# ── main ────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blocks", type=int, default=SCAN_BLOCKS)
    ap.add_argument("--lookahead", type=int, default=LOOKAHEAD_BLOCKS)
    ap.add_argument("--out", default="d_microstructure.csv")
    ap.add_argument("--cache", default="d_microstructure_paths.json")
    ap.add_argument("--use_cache", action="store_true")
    ap.add_argument("--max", type=int, default=0)
    args = ap.parse_args()

    rows = []
    if args.use_cache:
        try:
            import os
            if os.path.exists(args.cache):
                with open(args.cache) as f:
                    rows = json.load(f)
                print(f"loaded {len(rows)} from cache", file=sys.stderr)
        except Exception as e:
            print(f"cache load failed: {e}", file=sys.stderr)
            rows = []

    if not rows:
        latest = block_now()
        from_block = latest - args.blocks
        print(f"head={latest} window={from_block}..{latest}", file=sys.stderr)
        d_buys = scan_d_buys(from_block, latest)
        print(f"found {len(d_buys)} D BUYs", file=sys.stderr)
        if args.max and len(d_buys) > args.max:
            d_buys = d_buys[-args.max:]

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
            print(f"cached → {args.cache}", file=sys.stderr)
        except Exception as e:
            print(f"cache save failed: {e}", file=sys.stderr)

    # Flat CSV (one row per token, no per-block embedded)
    cols = ["d_block","tx","token","d_price","n2_price",
            "ath_block","ath_price","ath_offset","ath_mult_vs_n2",
            "first_sell_offset","net_flip_offset",
            "dump_20_offset","dump_30_offset","buyer_decline_offset",
            "max_sell_bnb_wei","max_sell_offset",
            "n_events","n_blocks_active"]
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f); w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") if r.get(c) is not None else "" for c in cols])

    print(f"\nwrote {len(rows)} tokens → {args.out}", file=sys.stderr)

    # ── Aggregate insights ─────────────────────────────────────────
    if not rows:
        return

    def pctile(arr, p):
        if not arr: return None
        s = sorted(arr); k = int(round((len(s)-1) * p))
        return s[k]

    def stats(label, arr, fmt="{:.0f}"):
        arr = [a for a in arr if a is not None]
        if not arr:
            print(f"  {label:<30}: no data", file=sys.stderr); return
        print(f"  {label:<30}: n={len(arr)}  med={fmt.format(pctile(arr,0.5))}  P25={fmt.format(pctile(arr,0.25))}  P75={fmt.format(pctile(arr,0.75))}", file=sys.stderr)

    print(f"\n=== Aggregate microstructure ({len(rows)} D tokens) ===", file=sys.stderr)
    stats("ATH offset (blocks)",          [r["ath_offset"] for r in rows])
    stats("ATH multiple vs N+2",          [r["ath_mult_vs_n2"] for r in rows], "{:.2f}x")
    stats("first sell offset",            [r["first_sell_offset"] for r in rows])
    stats("net-flip offset (post-ATH)",   [r["net_flip_offset"] for r in rows])
    stats("dump -20% offset",             [r["dump_20_offset"] for r in rows])
    stats("dump -30% offset",             [r["dump_30_offset"] for r in rows])
    stats("buyer decline offset",         [r["buyer_decline_offset"] for r in rows])
    stats("max-sell BNB (wei → BNB)",     [r["max_sell_bnb_wei"]/1e18 for r in rows], "{:.4f}")
    stats("max-sell offset",              [r["max_sell_offset"] for r in rows])

    # Lead time analysis: how many BLOCKS earlier do leading signals
    # fire vs the -20% dump? (Positive = leading; negative = lagging.)
    print(f"\n=== Lead time vs the -20%-from-peak dump (blocks) ===", file=sys.stderr)
    def lead(signal_key, ref_key="dump_20_offset"):
        return [r[ref_key] - r[signal_key]
                for r in rows
                if r.get(signal_key) is not None and r.get(ref_key) is not None]

    for sig in ("net_flip_offset", "buyer_decline_offset", "max_sell_offset"):
        diffs = lead(sig)
        if not diffs:
            print(f"  {sig:<30}: no overlap", file=sys.stderr); continue
        pos = sum(1 for d in diffs if d > 0)  # signal fired BEFORE dump
        med = pctile(diffs, 0.5)
        print(f"  {sig:<30}: n={len(diffs)}  led_dump={pos}/{len(diffs)} ({100*pos/len(diffs):.0f}%)  median_lead={med:+d} blocks", file=sys.stderr)

    # Hit rate: for trades that DID hit a meaningful peak (≥1.5x),
    # what % of those had each signal fire BEFORE the -20% dump?
    print(f"\n=== Hit rate among winning tokens (ath ≥ 1.5x from N+2) ===", file=sys.stderr)
    winners = [r for r in rows if r.get("ath_mult_vs_n2") and r["ath_mult_vs_n2"] >= 1.5]
    print(f"  winners: {len(winners)}/{len(rows)}", file=sys.stderr)
    for sig in ("net_flip_offset","buyer_decline_offset","max_sell_offset"):
        leads = [r["dump_20_offset"] - r[sig]
                 for r in winners
                 if r.get(sig) is not None and r.get("dump_20_offset") is not None]
        if not leads:
            print(f"  {sig:<30}: no signal observed for winners", file=sys.stderr); continue
        pos = sum(1 for d in leads if d > 0)
        med = pctile(leads, 0.5)
        print(f"  {sig:<30}: n_with_signal={len(leads)}  led_dump={pos}/{len(leads)} ({100*pos/len(leads):.0f}%)  median_lead={med:+d} blocks ({med*0.45:.0f}s)", file=sys.stderr)

if __name__ == "__main__":
    main()
