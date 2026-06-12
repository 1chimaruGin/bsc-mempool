#!/usr/bin/env python3
"""
Per-block trajectory backtest. For each closed D trade, fetch curve state
(Four.Meme Word[8] BNB) at EVERY block from N+1 (entry) to N+1+30. Then
simulate candidate exit rules and compute hypothetical realized PnL.

Variants:
  V0  ACTUAL          — what we did, from chain receipts
  V1  -20% hard_sl    — tighter floor
  V2  -50% hard_sl    — wider floor
  V3  -15% / 1-blk    — exit if curve drops > 15% in a single block
  V4  -25% / 2-blk    — exit if curve drops > 25% over 2 consecutive blocks
  V5  Peak ratchet 20 — exit if dropped 20% from running peak (no arm needed)
  V6  Peak ratchet 50 — exit if dropped 50% from running peak

Each variant's "exit block" is the FIRST block where its rule fires within
the 30-block window. We then assume a 10% SELL-slippage haircut (consistent
with observed +5-15% extra realized loss) to compute hypothetical PnL.
"""
import json, os, re, subprocess, sys, urllib.request

RPC = os.environ.get("NODEREAL_RPC_URL") or sys.exit("set NODEREAL_RPC_URL")
FOURMEME = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
TOKEN_INFOS_SEL = "0xe684626b"
WALLET = "0x530306684a29e23676d30fa80dc6100e80b042ea"
BNB_USD = 586.0
TRACE_WINDOW = 30  # blocks after entry to track

def rpc(m, p):
    req = urllib.request.Request(RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode(),
        headers={"content-type":"application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())["result"]

def to_int(h): return int(h, 16) if isinstance(h, str) else h

def curve_bnb(token, block):
    data = TOKEN_INFOS_SEL + token.lower().lstrip("0x").zfill(64)
    try:
        v = rpc("eth_call", [{"to": FOURMEME, "data": data}, hex(block)])
    except Exception:
        return None
    if not v or v == "0x": return None
    h = v.lstrip("0x")
    if len(h) < 9*64: return None
    return int(h[8*64:9*64], 16)

# Parse journal for trades (same as backtest-entry-filter.py)
print("Parsing journal …", file=sys.stderr)
log = subprocess.run(["journalctl","-u","bsc-runner","--since","2026-06-01","--no-pager"],
    capture_output=True, text=True).stdout.splitlines()

our_buy_tx, our_sell_tx, exit_reason = {}, {}, {}
re_buy = re.compile(r'BROADCAST kol=D token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')
re_sell = re.compile(r'SELL BROADCAST kol=TRAIL_([a-z_]+) token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')
for line in log:
    if 'BROADCAST kol=D' in line:
        m = re_buy.search(line)
        if m: our_buy_tx[m.group(1).lower()] = m.group(2)
    elif 'SELL BROADCAST kol=TRAIL_' in line:
        m = re_sell.search(line)
        if m:
            reason, tok, h = m.group(1), m.group(2).lower(), m.group(3)
            exit_reason[tok] = reason; our_sell_tx[tok] = h

trades = []
for tok, buy_h in our_buy_tx.items():
    if tok in our_sell_tx:
        trades.append({"token": tok, "buy_h": buy_h, "sell_h": our_sell_tx[tok], "reason": exit_reason[tok]})
trades = trades[-30:]
print(f"Backtesting {len(trades)} trades x {TRACE_WINDOW} blocks each", file=sys.stderr)

# For each trade: fetch trajectory + realized PnL
SELL_SLIPPAGE = 0.10  # 10% haircut on hypothetical exits to match observed
results = []
for i, t in enumerate(trades):
    print(f"  [{i+1}/{len(trades)}] {t['token'][:10]}", file=sys.stderr)
    try:
        br = rpc("eth_getTransactionReceipt", [t["buy_h"]])
        bt = rpc("eth_getTransactionByHash", [t["buy_h"]])
        sr = rpc("eth_getTransactionReceipt", [t["sell_h"]])
        if br is None or sr is None or br["status"] != "0x1": continue
        buy_block  = to_int(br["blockNumber"])
        sell_block = to_int(sr["blockNumber"])
        buy_value  = to_int(bt["value"])
        buy_gas    = to_int(br["gasUsed"]) * to_int(br.get("effectiveGasPrice","0x0"))
        sell_gas   = to_int(sr["gasUsed"]) * to_int(sr.get("effectiveGasPrice","0x0"))
        # Realized PnL via wallet diff
        bal_before = int(rpc("eth_getBalance", [WALLET, hex(sell_block-1)]), 16)
        bal_after  = int(rpc("eth_getBalance", [WALLET, hex(sell_block)]), 16)
        actual_sell_proceeds = (bal_after - bal_before) + sell_gas
        actual_realized = actual_sell_proceeds - (buy_value + buy_gas + sell_gas)
        # Fetch trajectory
        entry_curve = curve_bnb(t["token"], buy_block)
        if entry_curve is None or entry_curve == 0: continue
        traj = []
        for off in range(1, TRACE_WINDOW + 1):
            b = buy_block + off
            cb = curve_bnb(t["token"], b)
            if cb is None: traj.append(None)
            else: traj.append(cb / entry_curve - 1.0)  # % change from entry
        results.append({
            **t, "buy_block": buy_block, "sell_block": sell_block,
            "buy_value": buy_value, "buy_gas": buy_gas, "sell_gas": sell_gas,
            "actual_realized": actual_realized, "actual_pct": actual_realized / (buy_value+buy_gas+sell_gas) * 100,
            "entry_curve": entry_curve, "traj": traj,
        })
    except Exception as e:
        print(f"    error: {e}", file=sys.stderr)

# Simulate variants
def simulate(r, exit_rule):
    """Find first block where exit_rule fires; returns (block_offset, exit_pct_change) or (TRACE_WINDOW, last_known)"""
    traj = r["traj"]
    peak = 0.0
    prev = 0.0
    prev2 = 0.0
    for i, pct in enumerate(traj):
        if pct is None: continue
        peak = max(peak, pct)
        decision = exit_rule(pct, peak, prev, prev2)
        if decision:
            return i+1, pct
        prev2 = prev; prev = pct
    # If no rule fires, use last available
    last = next((p for p in reversed(traj) if p is not None), 0.0)
    return TRACE_WINDOW, last

def variant_pnl(r, exit_blk_off, exit_pct):
    """Compute PnL if exit fired at exit_blk_off with curve change exit_pct."""
    # Hypothetical sell proceeds = buy_value × (1 + exit_pct) × (1 - SELL_SLIPPAGE)
    sell_proceeds = r["buy_value"] * (1 + exit_pct) * (1 - SELL_SLIPPAGE)
    return sell_proceeds - (r["buy_value"] + r["buy_gas"] + r["sell_gas"])

# Define rules
rules = {
    "V0_actual":        None,  # special
    "V1_-20%_hard_sl":  lambda p, peak, prev, prev2: p < -0.20,
    "V2_-50%_hard_sl":  lambda p, peak, prev, prev2: p < -0.50,
    "V3_-15%_1block":   lambda p, peak, prev, prev2: (p - prev) < -0.15,
    "V4_-25%_2block":   lambda p, peak, prev, prev2: (p - prev2) < -0.25,
    "V5_peak-20%":      lambda p, peak, prev, prev2: p < peak - 0.20,
    "V6_peak-50%":      lambda p, peak, prev, prev2: p < peak - 0.50,
}

totals = {k: 0.0 for k in rules}
totals["V0_actual"] = sum(r["actual_realized"] for r in results) / 1e18

print(f"\n=== Per-trade comparison ({len(results)} trades) ===\n")
print(f"{'token':<12} {'reason':<12} {'actual':>8} | {' '.join(f'{k:>14}' for k in list(rules.keys())[1:])}")
print("-" * 130)
for r in results:
    row = [f"{r['token'][:10]:<12} {r['reason']:<12} {r['actual_pct']:>+7.1f}% |"]
    for name, rule in rules.items():
        if name == "V0_actual": continue
        blk, pct = simulate(r, rule)
        pnl = variant_pnl(r, blk, pct) / 1e18
        totals[name] += pnl
        # Compare to actual: positive = better
        delta = pnl - r["actual_realized"]/1e18
        marker = "↑" if delta > 0.001 else ("↓" if delta < -0.001 else " ")
        row.append(f"{marker}{pnl*BNB_USD:+5.2f}@b{blk:<3}")
    print(" ".join(row))

print("\n=== Totals over {} trades ===".format(len(results)))
print(f"  V0_actual:        {totals['V0_actual']:+.5f} BNB ≈ ${totals['V0_actual']*BNB_USD:+.2f}")
for k in list(rules.keys())[1:]:
    delta = totals[k] - totals["V0_actual"]
    print(f"  {k:<22} {totals[k]:+.5f} BNB ≈ ${totals[k]*BNB_USD:+.2f}   Δ vs actual: ${delta*BNB_USD:+.2f}")
