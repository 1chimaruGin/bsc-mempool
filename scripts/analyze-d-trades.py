#!/usr/bin/env python3
"""
Analyse KOL D's BUYs to find the best trail-strategy fit.

For each Four.Meme BUY by D:
  - mcap_d           : mcap at D's BUY block
  - mcap_n2          : mcap at D's BUY block + 2 (= our N+1 entry point)
  - mcap_ath         : peak mcap within `LOOKAHEAD_BLOCKS` of D's BUY
  - ath_offset_blocks: blocks from D-entry → ATH
  - mcap_low         : lowest mcap between D-entry and ATH (drawdown bottom before peak)
  - ath_mult_vs_n2   : ATH / N+1 (the upside we leave on the table if we don't ride)
  - dd_mult_vs_n2    : low / N+1 (how deep we'd have to survive to capture ATH)
  - ath_mult_vs_d    : ATH / D-entry (D's own captured upside, theoretical)

Data source: NodeReal BSC mainnet (HTTPS, batched eth_getLogs).
Why NodeReal: this is a one-off analysis tool, not the live trader. The live
trader is and stays self-hosted-only per project policy.

Run:
  python3 scripts/analyze-d-trades.py [--blocks N] [--out CSV]
"""
import argparse, csv, json, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

NODEREAL_HTTP = "https://bsc-mainnet.nodereal.io/v1/3bed06fc28e04f73a64a54da9c575a47"
D_ADDR        = "0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373"
LAUNCHPAD     = "0x5c952063c7fc8610FFDB798152D69F0B9550762b"
TRADE_BUY_TOPIC = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"

# Defaults — overridable via CLI.
LOOKAHEAD_BLOCKS = 4000      # ~30 min; matches trail max_hold_blocks
SCAN_BLOCKS      = 200_000   # ~25h of history (default; override --blocks)
CHUNK_SIZE       = 5000      # per eth_getLogs call (NodeReal max ~10k)
WORKERS          = 16        # parallel chunks (was 8 — NodeReal handles 16+ fine)

# ── helpers ──────────────────────────────────────────────────────────

def rpc(method, params, rpc_id=1, retries=4):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":rpc_id}).encode()
    last_err = None
    for attempt in range(retries):
        try:
            req = Request(NODEREAL_HTTP, data=body, headers={"Content-Type":"application/json"})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                if "error" in data:
                    msg = data["error"].get("message","?")
                    # Rate-limit / range-too-wide → backoff
                    if "limit" in msg.lower() or "exceed" in msg.lower():
                        time.sleep(2 ** attempt)
                        continue
                    raise RuntimeError(f"RPC error: {msg}")
                return data.get("result")
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"rpc {method} failed after {retries} retries: {last_err}")

def get_logs(addr, from_block, to_block, topics):
    return rpc("eth_getLogs", [{
        "address":   addr,
        "fromBlock": hex(from_block),
        "toBlock":   hex(to_block),
        "topics":    topics,
    }]) or []

def get_block_number():
    return int(rpc("eth_blockNumber", []), 16)

# ── TradeBuy event decoder ───────────────────────────────────────────

def decode_trade_buy(log):
    """
    Layout (verified empirically against tx 0x1c3c…b87e8 log[2]):
      data[0]: token (right-aligned address)
      data[1]: buyer (right-aligned address)
      data[3]: tokens delivered (raw, 1e18 decimals)
      data[4]: BNB paid (net) wei
      data[5]: fee wei
    Returns dict or None on malformed/dust events.
    """
    data = log["data"][2:]  # strip 0x
    if len(data) < 32 * 6 * 2:
        return None
    def word(i): return data[i*64:(i+1)*64]
    try:
        token = "0x" + word(0)[24:]
        buyer = "0x" + word(1)[24:]
        tokens_out = int(word(3), 16)
        bnb_net    = int(word(4), 16)
        fee        = int(word(5), 16)
    except ValueError:
        return None
    bnb_gross = bnb_net + fee
    # Dust / corrupt filters: real BUYs have ≥0.001 whole tokens
    # out (= 1e15 raw) and ≥1000 wei BNB in. Anything below is a
    # decode error or non-trade event sharing topic[0].
    if tokens_out < 10**15 or bnb_gross < 1000:
        return None
    price = bnb_gross / tokens_out   # BNB-wei per raw-token
    # Sanity bound: memecoin prices in this unit are 1e-15 .. 1e-4.
    # Anything outside is corrupt.
    if not (1e-15 <= price <= 1e-4):
        return None
    block = log.get("blockNumber")
    if not block:
        return None
    return {
        "token": token.lower(),
        "buyer": buyer.lower(),
        "block": int(block, 16),
        "tx":    log["transactionHash"],
        "price": price,
        "bnb_in":     bnb_gross / 1e18,
        "tokens_out": tokens_out / 1e18,
    }

# ── bulk scanners ────────────────────────────────────────────────────

def scan_d_buys(from_block, to_block):
    """Return D's BUYs (one row per BUY) within [from_block, to_block]."""
    print(f"[scan_d] from={from_block} to={to_block} blocks={to_block-from_block:,}", file=sys.stderr)
    chunks = []
    b = from_block
    while b <= to_block:
        end = min(b + CHUNK_SIZE - 1, to_block)
        chunks.append((b, end))
        b = end + 1
    out = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(get_logs, LAUNCHPAD, lo, hi, [TRADE_BUY_TOPIC]): (lo, hi) for lo, hi in chunks}
        done = 0
        for fut in as_completed(futs):
            lo, hi = futs[fut]
            try:
                logs = fut.result()
            except Exception as e:
                print(f"  [chunk {lo}-{hi}] error: {e}", file=sys.stderr); continue
            for log in logs:
                rec = decode_trade_buy(log)
                if rec and rec["buyer"] == D_ADDR.lower():
                    out.append(rec)
            done += 1
            if done % 5 == 0:
                print(f"  scanned {done}/{len(chunks)} chunks, D-buys so far: {len(out)}", file=sys.stderr)
    out.sort(key=lambda r: r["block"])
    return out

def scan_token_prices(token, from_block, to_block):
    """All TradeBuy events on the given token in [from, to]. Returns sorted price points."""
    chunks = []
    b = from_block
    while b <= to_block:
        end = min(b + CHUNK_SIZE - 1, to_block)
        chunks.append((b, end))
        b = end + 1
    points = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(get_logs, LAUNCHPAD, lo, hi, [TRADE_BUY_TOPIC]): (lo, hi) for lo, hi in chunks}
        for fut in as_completed(futs):
            try:
                logs = fut.result()
            except Exception as e:
                continue
            for log in logs:
                rec = decode_trade_buy(log)
                if rec and rec["token"] == token.lower():
                    points.append((rec["block"], rec["price"]))
    points.sort()
    return points

# ── per-trade analysis ───────────────────────────────────────────────

def analyze_one(d_buy, lookahead_blocks):
    """For a single D BUY, scan forward and compute the metrics."""
    token   = d_buy["token"]
    d_block = d_buy["block"]
    d_price = d_buy["price"]
    points = scan_token_prices(token, d_block, d_block + lookahead_blocks)
    if not points:
        return {**d_buy, "n2_price": None, "ath_price": None, "ath_block": None,
                "ath_offset": None, "low_price": None,
                "ath_mult_vs_n2": None, "dd_mult_vs_n2": None, "ath_mult_vs_d": None,
                "post_buy_points": 0}
    # N+2 price: first observation at block >= d_block + 2
    n2_price = None
    for blk, p in points:
        if blk >= d_block + 2:
            n2_price = p
            break
    if n2_price is None:
        # No post-block+2 activity — fall back to d_price
        n2_price = d_price

    # ATH and ATH block — points are (block, price)
    ath_block, ath_price = max(points, key=lambda bp: bp[1])
    ath_offset = ath_block - d_block

    # Low between d_block+2 and ath_block (inclusive)
    pre_ath = [p for blk, p in points if d_block + 2 <= blk <= ath_block]
    low_price = min(pre_ath) if pre_ath else n2_price

    # Keep the post-N+2 path so we can run a TRUE step-through simulation
    # against the real price sequence (not just min/max/peak summaries).
    post_n2_path = [(blk, p) for blk, p in points if blk >= d_block + 2]
    return {
        **d_buy,
        "n2_price":         n2_price,
        "ath_price":        ath_price,
        "ath_block":        ath_block,
        "ath_offset":       ath_offset,
        "low_price":        low_price,
        "ath_mult_vs_n2":   ath_price / n2_price if n2_price else None,
        "dd_mult_vs_n2":    low_price / n2_price if n2_price else None,
        "ath_mult_vs_d":    ath_price / d_price,
        "post_buy_points":  len(points),
        "_path":            post_n2_path,   # underscore = exclude from CSV
    }

# ── mcap helpers ─────────────────────────────────────────────────────

STANDARD_SUPPLY_RAW = 10**27  # 1e9 tokens × 1e18 decimals

def mcap_usd(price_wei_per_raw, bnb_usd):
    """price units: wei BNB / raw token. mcap = price × supply_raw → wei BNB → BNB → USD."""
    if price_wei_per_raw is None: return None
    mcap_wei_bnb = price_wei_per_raw * STANDARD_SUPPLY_RAW
    mcap_bnb     = mcap_wei_bnb / 1e18
    return mcap_bnb * bnb_usd

# ── main ─────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blocks", type=int, default=SCAN_BLOCKS,
                    help=f"scan back this many blocks (default {SCAN_BLOCKS})")
    ap.add_argument("--lookahead", type=int, default=LOOKAHEAD_BLOCKS,
                    help=f"per-trade lookahead in blocks (default {LOOKAHEAD_BLOCKS})")
    ap.add_argument("--bnb_usd", type=float, default=630.0,
                    help="BNB/USD oracle value for mcap (default 630)")
    ap.add_argument("--out", default="d_trades_analysis.csv")
    ap.add_argument("--max", type=int, default=0, help="cap N most recent D buys (0=all)")
    ap.add_argument("--use_cache", action="store_true",
                    help="load paths from <out>_paths.json if present (skip re-scan)")
    args = ap.parse_args()

    latest = get_block_number()
    from_block = latest - args.blocks
    print(f"head block: {latest}  scan window: {from_block}..{latest}", file=sys.stderr)

    d_buys = scan_d_buys(from_block, latest)
    print(f"\nfound {len(d_buys)} D BUYs", file=sys.stderr)
    if args.max and len(d_buys) > args.max:
        d_buys = d_buys[-args.max:]
        print(f"capped to most recent {args.max}", file=sys.stderr)

    # Try to load cached paths to skip the slow network re-scan.
    cache_path = args.out.replace(".csv", "_paths.json")
    rows = []
    if args.use_cache:
        try:
            import os
            if os.path.exists(cache_path):
                with open(cache_path) as f:
                    rows = json.load(f)
                print(f"loaded {len(rows)} cached rows from {cache_path}", file=sys.stderr)
        except Exception as e:
            print(f"cache load failed: {e} — re-scanning", file=sys.stderr)
            rows = []

    if not rows:
        t0 = time.time()
        for i, d in enumerate(d_buys, 1):
            rows.append(analyze_one(d, args.lookahead))
            if i % 25 == 0 or i == len(d_buys):
                rate = i / (time.time() - t0 + 1e-9)
                eta  = (len(d_buys) - i) / rate if rate > 0 else 0
                print(f"  [analyze {i}/{len(d_buys)}] rate={rate:.1f}/s  eta={eta:.0f}s", file=sys.stderr)
        # Persist for fast re-sim.
        try:
            with open(cache_path, "w") as f:
                json.dump(rows, f)
            print(f"cached {len(rows)} rows → {cache_path}", file=sys.stderr)
        except Exception as e:
            print(f"cache save failed: {e}", file=sys.stderr)

    cols = [
        "block","tx","token","bnb_in","tokens_out",
        "mcap_d_usd","mcap_n2_usd","mcap_low_usd","mcap_ath_usd",
        "ath_offset_blocks","post_buy_points",
        "ath_mult_vs_d","ath_mult_vs_n2","dd_mult_vs_n2",
    ]
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([
                r["block"], r["tx"], r["token"],
                f'{r["bnb_in"]:.6f}', f'{r["tokens_out"]:.2f}',
                f'{mcap_usd(r["price"], args.bnb_usd):.0f}',
                f'{mcap_usd(r["n2_price"], args.bnb_usd):.0f}' if r["n2_price"] else "",
                f'{mcap_usd(r["low_price"], args.bnb_usd):.0f}' if r["low_price"] else "",
                f'{mcap_usd(r["ath_price"], args.bnb_usd):.0f}' if r["ath_price"] else "",
                r["ath_offset"] if r["ath_offset"] is not None else "",
                r["post_buy_points"],
                f'{r["ath_mult_vs_d"]:.2f}'   if r["ath_mult_vs_d"]   else "",
                f'{r["ath_mult_vs_n2"]:.2f}'  if r["ath_mult_vs_n2"]  else "",
                f'{r["dd_mult_vs_n2"]:.2f}'   if r["dd_mult_vs_n2"]   else "",
            ])

    # Quick summary stats — focus on the n2 (our entry) perspective.
    runs   = [r for r in rows if r.get("ath_mult_vs_n2")]
    if runs:
        ath_mults = sorted(r["ath_mult_vs_n2"] for r in runs)
        dd_mults  = sorted(r["dd_mult_vs_n2"]  for r in runs if r["dd_mult_vs_n2"])
        def pctile(arr, p):
            if not arr: return None
            k = int(round((len(arr)-1) * p))
            return arr[k]
        print(f"\n=== Summary ({len(runs)} D BUYs with post-buy data) ===", file=sys.stderr)
        print(f"ATH multiple vs N+2 (= our entry):", file=sys.stderr)
        print(f"  median: {pctile(ath_mults, 0.50):.2f}x", file=sys.stderr)
        print(f"  P75:    {pctile(ath_mults, 0.75):.2f}x", file=sys.stderr)
        print(f"  P90:    {pctile(ath_mults, 0.90):.2f}x", file=sys.stderr)
        print(f"  max:    {ath_mults[-1]:.2f}x", file=sys.stderr)
        if dd_mults:
            print(f"Drawdown multiple before ATH (low/N+2):", file=sys.stderr)
            print(f"  median: {pctile(dd_mults, 0.50):.2f}x", file=sys.stderr)
            print(f"  P25:    {pctile(dd_mults, 0.25):.2f}x", file=sys.stderr)
            print(f"  P10:    {pctile(dd_mults, 0.10):.2f}x", file=sys.stderr)

        # Survival analysis: for various candidate hard-SL levels, what % of
        # tokens reached ≥2x / ≥3x / ≥5x without first hitting the SL?
        print(f"\n=== Survival vs SL: of {len(runs)} trades, how many reach Nx without first hitting SL? ===", file=sys.stderr)
        print(f"{'SL':>6} | {'≥1.5x':>6} {'≥2x':>6} {'≥3x':>6} {'≥5x':>6} {'≥7x':>6}", file=sys.stderr)
        for sl in [0.50, 0.60, 0.70, 0.80]:  # SL floor as multiple of N+2 (0.70 = -30%)
            row = [f"{int((1-sl)*100):>3}% "]
            for target in [1.5, 2.0, 3.0, 5.0, 7.0]:
                wins = sum(1 for r in runs
                           if r["dd_mult_vs_n2"] and r["dd_mult_vs_n2"] >= sl
                           and r["ath_mult_vs_n2"] >= target)
                row.append(f"{wins:>5}")
            print("  " + " ".join(row), file=sys.stderr)

        # ── Strategy simulator (PATH-BASED, not summary-based) ────────
        # Walks the actual block-by-block price tape per trade, applying
        # the trail state machine as it would have fired LIVE:
        #   - peak ratchets on every observation
        #   - arm flips ON when peak ≥ entry × (1 + arm_pct)
        #   - hard SL exits when price ≤ entry × (1 - hard_sl_pct)
        #   - trail exits when armed AND price ≤ peak × (1 - trail_pct)
        #   - timeout exits at final observed price if no other exit fired
        # Returns realised exit multiple = exit_price / n2_price.
        def replay(path, n2, arm, trail, sl):
            """Walk the real tape; return (exit_mult, reason)."""
            if not path or n2 is None or n2 <= 0:
                return None, None
            peak   = n2
            armed  = False
            hard_floor = n2 * (1 - sl)
            for blk, p in path:
                if p > peak:
                    peak = p
                if not armed and peak >= n2 * (1 + arm):
                    armed = True
                if p <= hard_floor:
                    return p / n2, "hard_sl"
                if armed and p <= peak * (1 - trail):
                    return p / n2, "trail"
            # Timeout — closed at last observed
            return path[-1][1] / n2, "timeout"

        def sim(arm, trail, sl):
            exits = []
            reasons = {"trail":0, "hard_sl":0, "timeout":0}
            for r in runs:
                path = r.get("_path") or []
                mult, why = replay(path, r["n2_price"], arm, trail, sl)
                if mult is None: continue
                exits.append(mult)
                reasons[why] = reasons.get(why, 0) + 1
            n = len(exits)
            if not n: return 0, 0, 0, reasons
            avg   = sum(exits) / n
            wins  = sum(1 for x in exits if x > 1.0)
            big   = sum(1 for x in exits if x > 2.0)
            return avg, wins, big, reasons

        # ── Multi-step TP/SL replay ───────────────────────────────────
        # Predetermined ladder: at each TP level, sell a slice. SL fires
        # for whatever is left. Timeout closes remainder at last observed.
        #
        # Config = (tps, sl) where tps is a list of (tp_pct, sell_fraction)
        # tuples — fractions are of ORIGINAL position, summed ≤ 1.0.
        # Whatever's left at end → trail OR timeout (see hybrid below).
        def replay_multistep(path, n2, tps, sl):
            if not path or n2 is None or n2 <= 0:
                return None, None
            hard_floor = n2 * (1 - sl)
            remaining  = 1.0
            realised   = 0.0
            hit = [False] * len(tps)
            for blk, p in path:
                if p <= hard_floor:
                    realised += remaining * (p / n2)
                    return realised, "hard_sl"
                for i, (tp_pct, frac) in enumerate(tps):
                    if hit[i]: continue
                    if p >= n2 * (1 + tp_pct):
                        take = min(frac, remaining)
                        realised  += take * (p / n2)
                        remaining -= take
                        hit[i] = True
                        if remaining <= 1e-9:
                            return realised, f"tp{i+1}_full"
            # Timeout — close remainder at last price
            last_p = path[-1][1]
            realised += remaining * (last_p / n2)
            return realised, "timeout"

        # ── Hybrid: take partial TPs then trail the rest ───────────────
        def replay_hybrid(path, n2, tps, arm, trail, sl):
            if not path or n2 is None or n2 <= 0:
                return None, None
            hard_floor = n2 * (1 - sl)
            remaining  = 1.0
            realised   = 0.0
            hit = [False] * len(tps)
            peak  = n2
            armed = False
            for blk, p in path:
                if p > peak: peak = p
                if not armed and peak >= n2 * (1 + arm):
                    armed = True
                if p <= hard_floor:
                    realised += remaining * (p / n2)
                    return realised, "hard_sl"
                # First take TPs
                for i, (tp_pct, frac) in enumerate(tps):
                    if hit[i]: continue
                    if p >= n2 * (1 + tp_pct):
                        take = min(frac, remaining)
                        realised  += take * (p / n2)
                        remaining -= take
                        hit[i] = True
                # Then trail the rest
                if armed and remaining > 1e-9 and p <= peak * (1 - trail):
                    realised += remaining * (p / n2)
                    return realised, "trail"
                if remaining <= 1e-9:
                    return realised, "tp_full"
            # Timeout
            last_p = path[-1][1]
            realised += remaining * (last_p / n2)
            return realised, "timeout"

        def sim_multistep(tps, sl):
            exits = []
            reasons = {}
            for r in runs:
                path = r.get("_path") or []
                mult, why = replay_multistep(path, r["n2_price"], tps, sl)
                if mult is None: continue
                exits.append(mult)
                reasons[why] = reasons.get(why, 0) + 1
            n = len(exits)
            if not n: return 0, 0, 0, reasons
            avg, wins = sum(exits)/n, sum(1 for x in exits if x > 1.0)
            big = sum(1 for x in exits if x > 2.0)
            return avg, wins, big, reasons

        def sim_hybrid(tps, arm, trail, sl):
            exits = []; reasons = {}
            for r in runs:
                path = r.get("_path") or []
                mult, why = replay_hybrid(path, r["n2_price"], tps, arm, trail, sl)
                if mult is None: continue
                exits.append(mult)
                reasons[why] = reasons.get(why, 0) + 1
            n = len(exits)
            if not n: return 0, 0, 0, reasons
            avg, wins = sum(exits)/n, sum(1 for x in exits if x > 1.0)
            big = sum(1 for x in exits if x > 2.0)
            return avg, wins, big, reasons

        print(f"\n=== Strategy simulator (PATH-BASED replay of {len(runs)} D trades) ===", file=sys.stderr)
        print(f"  {'arm/trail/SL':<24}  {'avg_exit':>8}  {'wins':>4}  {'≥2x':>4}  {'trail/SL/timeout':>20}", file=sys.stderr)
        for arm, trail, sl in [
            (0.20, 0.10, 0.30),   # OLD KOL config — sold 0xd2687907 at +8%
            (0.10, 0.30, 0.30),   # CURRENT KOL (matches sniper, set 2026-06-04)
            (0.30, 0.20, 0.30),
            (0.30, 0.30, 0.30),
            (0.30, 0.40, 0.30),
            (0.50, 0.20, 0.30),
            (0.50, 0.30, 0.30),
            (0.50, 0.40, 0.30),
            (0.50, 0.50, 0.30),
            (1.00, 0.30, 0.30),
            (1.00, 0.40, 0.30),
            (1.00, 0.50, 0.30),
            (2.00, 0.40, 0.30),   # only engage on 3x+ runners
            (2.00, 0.50, 0.30),
            # Asymmetric SL: tight stop, wide trail
            (0.30, 0.40, 0.20),   # SL -20%
            (0.30, 0.40, 0.15),
        ]:
            avg, wins, big, reasons = sim(arm, trail, sl)
            label = f"arm+{int(arm*100)}/trail-{int(trail*100)}/SL-{int(sl*100)}"
            r_summary = f"{reasons['trail']}/{reasons['hard_sl']}/{reasons['timeout']}"
            print(f"  {label:<24}  {avg:>7.2f}x  {wins:>4}  {big:>4}  {r_summary:>20}", file=sys.stderr)

        # ── Multi-step TP/SL sweep ────────────────────────────────────
        print(f"\n=== Multi-step TP/SL replay ({len(runs)} D trades) ===", file=sys.stderr)
        print(f"  {'TP ladder / SL':<32}  {'avg_exit':>8}  {'wins':>4}  {'≥2x':>4}  {'breakdown':>30}", file=sys.stderr)
        multi_configs = [
            # (tp_ladder, sl_pct, label)
            ([(0.50, 1.00)],                    0.30, "100%@+50 / SL-30"),
            ([(1.00, 1.00)],                    0.30, "100%@+100 / SL-30"),
            ([(2.00, 1.00)],                    0.30, "100%@+200 / SL-30"),
            ([(0.50, 0.50), (1.00, 0.50)],      0.30, "50@+50, 50@+100 / SL-30"),
            ([(0.30, 0.50), (1.00, 0.50)],      0.30, "50@+30, 50@+100 / SL-30"),
            ([(0.50, 0.50), (2.00, 0.50)],      0.30, "50@+50, 50@+200 / SL-30"),
            ([(0.50, 0.33), (1.50, 0.33), (3.00, 0.34)], 0.30, "33@+50/+150/+300 / SL-30"),
            ([(1.00, 0.50), (2.00, 0.50)],      0.30, "50@+100, 50@+200 / SL-30"),
            ([(0.50, 0.33), (1.00, 0.33), (2.00, 0.34)], 0.30, "33@+50/+100/+200 / SL-30"),
            ([(0.30, 0.33), (1.00, 0.33), (3.00, 0.34)], 0.30, "33@+30/+100/+300 / SL-30"),
            # Tighter SL variants
            ([(0.50, 0.50), (1.00, 0.50)],      0.20, "50@+50, 50@+100 / SL-20"),
            ([(0.30, 0.50), (1.00, 0.50)],      0.20, "50@+30, 50@+100 / SL-20"),
        ]
        for tps, sl, label in multi_configs:
            avg, wins, big, reasons = sim_multistep(tps, sl)
            br = " ".join(f"{k}:{v}" for k,v in sorted(reasons.items()))
            print(f"  {label:<32}  {avg:>7.2f}x  {wins:>4}  {big:>4}  {br:>30}", file=sys.stderr)

        # ── Hybrid: partial TP + trail the rest ───────────────────────
        print(f"\n=== Hybrid TP-then-trail ({len(runs)} D trades) ===", file=sys.stderr)
        print(f"  {'TP / arm-trail-SL':<40}  {'avg_exit':>8}  {'wins':>4}  {'≥2x':>4}", file=sys.stderr)
        hybrid_configs = [
            # (tps, arm, trail, sl, label)
            ([(0.50, 0.50)],            0.30, 0.30, 0.30, "50@+50 + arm30/trail-30/SL-30"),
            ([(0.30, 0.50)],            0.30, 0.30, 0.30, "50@+30 + arm30/trail-30/SL-30"),
            ([(0.50, 0.50)],            0.50, 0.30, 0.30, "50@+50 + arm50/trail-30/SL-30"),
            ([(1.00, 0.50)],            0.30, 0.30, 0.30, "50@+100 + arm30/trail-30/SL-30"),
            ([(0.50, 0.33), (1.00, 0.33)], 0.30, 0.30, 0.30, "33@+50, 33@+100 + arm30/trail-30/SL-30"),
            ([(0.50, 0.33), (2.00, 0.33)], 0.30, 0.30, 0.30, "33@+50, 33@+200 + arm30/trail-30/SL-30"),
        ]
        for tps, arm, trail, sl, label in hybrid_configs:
            avg, wins, big, reasons = sim_hybrid(tps, arm, trail, sl)
            print(f"  {label:<40}  {avg:>7.2f}x  {wins:>4}  {big:>4}", file=sys.stderr)

    print(f"\nwrote {len(rows)} rows → {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()
