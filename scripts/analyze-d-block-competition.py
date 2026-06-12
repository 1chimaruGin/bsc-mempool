#!/usr/bin/env python3
"""
Per-block competition analysis around D's BUYs.

For every D BUY in the cached 30-day window:
  1. Fetch the block D landed in (full tx objects)
  2. Identify ALL Four.Meme buy-like txs in the same block:
       to == launchpad         AND value > 0  → direct curve buy
       to == GMGN router       AND value > 0  → router-mediated buy
  3. Extract per tx: gas_price, tx_index, from, value, to
  4. Find D's tx_index, gas, value
  5. Categorize competitors:
       BEFORE D (tx_index < D)   → outpaced D, executed first
       AFTER  D (tx_index > D)   → racing D, executed after
  6. Aggregate:
       What gas % outranked D in his own block?
       What's the min gas to crack the top 5 in a D block?
       Block fullness — how many buy-like txs total per D block?

Same-token competition (Level 1) is captured when the calldata first
word matches D's token (works for direct launchpad calls; GMGN router
calldata is opaque without ABI but we still count by visit).
"""
import argparse, csv, json, sys, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

NODEREAL = "https://bsc-mainnet.nodereal.io/v1/3bed06fc28e04f73a64a54da9c575a47"
D_ADDR    = "0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373"
LAUNCHPAD = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
GMGN      = "0x1de460f363af910f51726def188f9004276bf4bc"
TARGETS   = {LAUNCHPAD, GMGN}

WORKERS = 16

def rpc(method, params, retries=4):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":1}).encode()
    last = None
    for attempt in range(retries):
        try:
            req = Request(NODEREAL, data=body, headers={"Content-Type":"application/json"})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                if "error" in data:
                    msg = data["error"].get("message","?")
                    if "limit" in msg.lower():
                        time.sleep(2 ** attempt); continue
                    raise RuntimeError(msg)
                return data.get("result")
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            last = e; time.sleep(2 ** attempt)
    raise RuntimeError(f"rpc failed: {last}")

def get_block_with_txs(block_n):
    """Returns block with full tx objects."""
    return rpc("eth_getBlockByNumber", [hex(block_n), True])

# ── decode helpers ─────────────────────────────────────────────────

def buy_like(tx):
    """A tx is buy-like if it sends BNB into the launchpad or GMGN router."""
    to = (tx.get("to") or "").lower()
    if to not in TARGETS:
        return False
    val = int(tx.get("value","0x0"), 16)
    return val > 0

def gas_price_wei(tx):
    """Prefer effective gas price (1559) if present, else legacy gasPrice."""
    if "gasPrice" in tx:
        return int(tx["gasPrice"], 16)
    if "maxFeePerGas" in tx:
        return int(tx["maxFeePerGas"], 16)
    return 0

def extract_token(tx):
    """
    For direct launchpad calls, the calldata layout is:
      4-byte selector + 32-byte token + 32-byte amount + …
    First word after selector = token address.
    Returns lowercased '0x…' or None.
    """
    data = (tx.get("input") or "").lower()
    if len(data) < 10 + 64:
        return None
    word = data[10:10+64]
    if word[:24] != "0"*24:
        return None
    return "0x" + word[24:]

# ── per-D-block analysis ───────────────────────────────────────────

def analyze_block(d_block, d_token, d_tx_hash):
    blk = get_block_with_txs(d_block)
    if not blk: return None
    txs = blk.get("transactions") or []
    buys = []
    d_tx = None
    for i, tx in enumerate(txs):
        if not buy_like(tx):
            continue
        rec = {
            "idx":    i,
            "from":   (tx.get("from") or "").lower(),
            "to":     (tx.get("to") or "").lower(),
            "gas":    gas_price_wei(tx),
            "value":  int(tx.get("value","0x0"), 16),
            "token":  extract_token(tx),
            "hash":   (tx.get("hash") or "").lower(),
        }
        buys.append(rec)
        if rec["hash"] == d_tx_hash.lower():
            d_tx = rec

    if d_tx is None:
        return None
    # Sort buy-like txs by tx_index ascending (= execution order in block)
    buys.sort(key=lambda r: r["idx"])
    # Find D's rank among buy-like txs
    rank = next((i for i, r in enumerate(buys) if r["hash"] == d_tx_hash.lower()), -1)
    before = [r for r in buys if r["idx"] < d_tx["idx"]]
    after  = [r for r in buys if r["idx"] > d_tx["idx"]]
    same_token = [r for r in buys if r["token"] == (d_token or "").lower() and r["hash"] != d_tx_hash.lower()]

    return {
        "d_block":    d_block,
        "d_token":    d_token,
        "d_tx":       d_tx_hash,
        "d_gas_gwei": d_tx["gas"] / 1e9,
        "d_idx":      d_tx["idx"],
        "d_rank_in_buys": rank,
        "n_buy_txs":  len(buys),
        "n_before":   len(before),
        "n_after":    len(after),
        "n_same_token_competitors": len(same_token),
        # Gas distribution
        "before_gas_gwei":     [r["gas"]/1e9 for r in before],
        "after_gas_gwei":      [r["gas"]/1e9 for r in after],
        "same_token_gas_gwei": [r["gas"]/1e9 for r in same_token],
    }

# ── main ───────────────────────────────────────────────────────────

def pctile(arr, p):
    if not arr: return None
    s = sorted(arr); k = int(round((len(s)-1) * p))
    return s[k]

def fmt_dist(arr, fmt="{:.1f}", unit=""):
    if not arr: return "n=0"
    return (f"n={len(arr)}  med={fmt.format(pctile(arr,0.5))}{unit}  "
            f"P25={fmt.format(pctile(arr,0.25))}{unit}  P75={fmt.format(pctile(arr,0.75))}{unit}  "
            f"P90={fmt.format(pctile(arr,0.9))}{unit}  max={fmt.format(max(arr))}{unit}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="d_microstructure_v2_paths.json")
    ap.add_argument("--out", default="d_block_competition.csv")
    ap.add_argument("--max", type=int, default=0)
    args = ap.parse_args()

    with open(args.source) as f:
        rows = json.load(f)
    print(f"loaded {len(rows)} D buys", file=sys.stderr)
    if args.max:
        rows = rows[-args.max:]

    results = []
    t0 = time.time()
    # Parallelize block fetches (NodeReal handles concurrent requests fine)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(analyze_block, r["d_block"], r["token"], r.get("tx","0x0")): r for r in rows}
        done = 0
        for fut in as_completed(futs):
            r = futs[fut]
            try:
                res = fut.result()
                if res: results.append(res)
            except Exception as e:
                print(f"  block {r['d_block']}: {e}", file=sys.stderr)
            done += 1
            if done % 25 == 0 or done == len(rows):
                rate = done / (time.time() - t0 + 1e-9)
                eta = (len(rows) - done) / rate if rate > 0 else 0
                print(f"  [{done}/{len(rows)}] rate={rate:.1f}/s eta={eta:.0f}s", file=sys.stderr)

    print(f"\nanalyzed {len(results)} blocks", file=sys.stderr)

    # CSV: one row per D-block
    cols = ["d_block","d_token","d_tx","d_gas_gwei","d_idx","d_rank_in_buys",
            "n_buy_txs","n_before","n_after","n_same_token_competitors",
            "before_max_gas_gwei","after_max_gas_gwei"]
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f); w.writerow(cols)
        for r in results:
            bg = max(r["before_gas_gwei"]) if r["before_gas_gwei"] else 0
            ag = max(r["after_gas_gwei"])  if r["after_gas_gwei"]  else 0
            w.writerow([r["d_block"], r["d_token"], r["d_tx"],
                        f'{r["d_gas_gwei"]:.2f}', r["d_idx"], r["d_rank_in_buys"],
                        r["n_buy_txs"], r["n_before"], r["n_after"], r["n_same_token_competitors"],
                        f'{bg:.2f}', f'{ag:.2f}'])
    print(f"wrote {args.out}", file=sys.stderr)

    # ── Aggregate ─────────────────────────────────────────────────
    print(f"\n=== D's block landscape ({len(results)} blocks) ===", file=sys.stderr)
    print(f"  buy_like txs per D block:    {fmt_dist([r['n_buy_txs'] for r in results])}", file=sys.stderr)
    print(f"  txs that landed BEFORE D:    {fmt_dist([r['n_before'] for r in results])}", file=sys.stderr)
    print(f"  txs that landed AFTER D:     {fmt_dist([r['n_after']  for r in results])}", file=sys.stderr)
    print(f"  same-token competitors:      {fmt_dist([r['n_same_token_competitors'] for r in results])}", file=sys.stderr)
    print(f"  D's tx_index in block:       {fmt_dist([r['d_idx'] for r in results])}", file=sys.stderr)
    print(f"  D's rank among buy txs:      {fmt_dist([r['d_rank_in_buys'] for r in results])}", file=sys.stderr)

    print(f"\n=== D's gas vs others' gas (gwei) ===", file=sys.stderr)
    print(f"  D's own gas:                 {fmt_dist([r['d_gas_gwei'] for r in results], '{:.2f}', '')}", file=sys.stderr)
    # ALL competitors (before+after) gas
    all_comp_gas = []
    for r in results:
        all_comp_gas.extend(r["before_gas_gwei"])
        all_comp_gas.extend(r["after_gas_gwei"])
    print(f"  ALL competitors gas:         {fmt_dist(all_comp_gas, '{:.2f}', '')}", file=sys.stderr)
    before_gas = [g for r in results for g in r["before_gas_gwei"]]
    after_gas  = [g for r in results for g in r["after_gas_gwei"]]
    print(f"  BEFORE-D txs gas:            {fmt_dist(before_gas, '{:.2f}', '')}", file=sys.stderr)
    print(f"  AFTER-D txs gas:             {fmt_dist(after_gas, '{:.2f}', '')}", file=sys.stderr)
    same_tok_gas = [g for r in results for g in r["same_token_gas_gwei"]]
    print(f"  SAME-TOKEN competitor gas:   {fmt_dist(same_tok_gas, '{:.2f}', '')}", file=sys.stderr)

    # ── To beat D's position in the block, what gas do we need? ────
    # = the MIN gas among txs that landed BEFORE D in his block
    print(f"\n=== Gas required to outrank D within his own block ===", file=sys.stderr)
    min_gas_to_beat = []
    for r in results:
        if r["before_gas_gwei"]:
            min_gas_to_beat.append(min(r["before_gas_gwei"]))
    print(f"  blocks where someone beat D: {len(min_gas_to_beat)}/{len(results)}", file=sys.stderr)
    if min_gas_to_beat:
        print(f"  gas to outrank D (the bare-min beater):  {fmt_dist(min_gas_to_beat, '{:.2f}', '')}", file=sys.stderr)
    # The max-gas in block N tells us "the most aggressive bidder"
    max_gas_in_d_block = []
    for r in results:
        all_g = r["before_gas_gwei"] + r["after_gas_gwei"] + [r["d_gas_gwei"]]
        max_gas_in_d_block.append(max(all_g))
    print(f"  Max gas seen in D's block:    {fmt_dist(max_gas_in_d_block, '{:.2f}', '')}", file=sys.stderr)

if __name__ == "__main__":
    main()
