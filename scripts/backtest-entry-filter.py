#!/usr/bin/env python3
"""
Backtest entry-side filter ideas against actual D-following trades.

For each closed D trade in the journal we:
1. Compute REALIZED PnL via wallet-balance diff (true fill including slippage + gas)
2. Pull Four.Meme curve state (Word[8] = BNB in curve) at blocks N-2, N-1, N+0, N+1
3. Derive per-block NET BNB FLOW (negative = sells > buys in that block)
4. Subtract D's known BUY value from N+0 flow → "non-D flow" in D's block
5. Apply candidate filters and compute hypothetical PnL

Filters tested:
  PRE  : skip if net_flow(N-2)+net_flow(N-1) < 0       (visible before our entry, ZERO latency cost)
  POST : skip if non_D_flow(N+0) < 0                   (requires waiting for D's block to mine; +1 block gap)
  MCAP : skip if our_entry_curve > 2x D_pre_curve      (swarm-trap; same data as POST)
"""
import json, os, re, subprocess, sys, urllib.request
from collections import defaultdict

RPC = os.environ.get("NODEREAL_RPC_URL") or sys.exit("set NODEREAL_RPC_URL env")
FOURMEME = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
TOKEN_INFOS_SEL = "0xe684626b"
WALLET = "0x530306684a29e23676d30fa80dc6100e80b042ea"
BNB_USD = 586.0

def rpc(m, p):
    req = urllib.request.Request(RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode(),
        headers={"content-type":"application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())["result"]

def to_int(h):
    return int(h, 16) if isinstance(h, str) else h

def curve_bnb(token, block):
    """eth_call _tokenInfos(token) at block; return Word[8] = current BNB in wei."""
    data = TOKEN_INFOS_SEL + token.lower().lstrip("0x").zfill(64)
    try:
        v = rpc("eth_call", [{"to": FOURMEME, "data": data}, hex(block)])
    except Exception:
        return None
    if not v or v == "0x":
        return None
    h = v.lstrip("0x")
    # 16 words × 64 hex chars each
    if len(h) < 9*64:
        return None
    return int(h[8*64:9*64], 16)

# --- Parse journal for D trades ------------------------------------------------
print("Parsing journal …", file=sys.stderr)
log = subprocess.run(
    ["journalctl", "-u", "bsc-runner", "--since", "2026-06-01", "--no-pager"],
    capture_output=True, text=True
).stdout.splitlines()

# Per-token state during parse
d_buy_meta = {}      # token → (gas_gwei, value_bnb_wei)
our_buy_tx = {}      # token → our BUY tx_hash
our_sell_tx = {}     # token → our SELL tx_hash
exit_reason = {}     # token → reason

re_d_obs   = re.compile(r'kol_name=D\b.*side="BUY".*token=(0x[0-9a-f]+).*gas_price_gwei=([\d.]+).*value_bnb=([\d.]+)')
re_our_buy = re.compile(r'BROADCAST kol=D token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')
re_our_sell = re.compile(r'SELL BROADCAST kol=TRAIL_([a-z_]+) token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')

for line in log:
    if 'side="BUY"' in line and 'kol_name=D' in line and 'method_label' in line:
        m = re_d_obs.search(line)
        if m:
            tok = m.group(1).lower()
            d_buy_meta[tok] = (float(m.group(2)), float(m.group(3)))
    elif 'BROADCAST kol=D' in line:
        m = re_our_buy.search(line)
        if m:
            our_buy_tx[m.group(1).lower()] = m.group(2)
    elif 'SELL BROADCAST kol=TRAIL_' in line:
        m = re_our_sell.search(line)
        if m:
            reason, tok, h = m.group(1), m.group(2).lower(), m.group(3)
            exit_reason[tok] = reason
            our_sell_tx[tok] = h

# Build trade list: closed trades (both BUY and SELL known)
trades = []
for tok, buy_h in our_buy_tx.items():
    if tok not in our_sell_tx: continue
    trades.append({
        "token": tok,
        "buy_h": buy_h,
        "sell_h": our_sell_tx[tok],
        "reason": exit_reason[tok],
        "d_gas": d_buy_meta.get(tok, (None, None))[0],
        "d_bnb": d_buy_meta.get(tok, (None, None))[1],
    })

# Limit to most recent 30 for speed
trades = trades[-30:]
print(f"Backtesting {len(trades)} closed D trades", file=sys.stderr)

# --- Compute realized PnL + curve state ---------------------------------------
results = []
for i, t in enumerate(trades):
    print(f"  [{i+1}/{len(trades)}] {t['token'][:10]} …", file=sys.stderr, flush=True)
    try:
        br = rpc("eth_getTransactionReceipt", [t["buy_h"]])
        bt = rpc("eth_getTransactionByHash", [t["buy_h"]])
        sr = rpc("eth_getTransactionReceipt", [t["sell_h"]])
        if br is None or sr is None:
            continue
        if br["status"] != "0x1":
            continue  # buy reverted; ignore
        buy_block  = to_int(br["blockNumber"])
        sell_block = to_int(sr["blockNumber"])
        buy_value  = to_int(bt["value"])
        buy_gas    = to_int(br["gasUsed"]) * to_int(br.get("effectiveGasPrice","0x0"))
        sell_gas   = to_int(sr["gasUsed"]) * to_int(sr.get("effectiveGasPrice","0x0"))

        # Realized PnL via wallet balance diff in sell block (assume our SELL is the only tx of ours in that block)
        bal_before = int(rpc("eth_getBalance", [WALLET, hex(sell_block - 1)]), 16)
        bal_after  = int(rpc("eth_getBalance", [WALLET, hex(sell_block)]), 16)
        sell_proceeds = (bal_after - bal_before) + sell_gas

        total_cost = buy_value + buy_gas + sell_gas
        realized_wei = sell_proceeds - total_cost
        realized_pct = 100 * realized_wei / total_cost if total_cost > 0 else 0

        # Curve state at relevant blocks (D's block = N+0, we land in N+1 = buy_block)
        # So pre-D = buy_block - 2, D's block = buy_block - 1, our block = buy_block
        n_minus2 = curve_bnb(t["token"], buy_block - 3)  # before pre-D
        n_minus1 = curve_bnb(t["token"], buy_block - 2)  # D's block - 1 (pre-D state)
        n_zero   = curve_bnb(t["token"], buy_block - 1)  # D's block (D's BUY + swarm)
        n_one    = curve_bnb(t["token"], buy_block)      # our block (we landed here)

        results.append({
            **t,
            "buy_block": buy_block,
            "realized_wei": realized_wei,
            "realized_pct": realized_pct,
            "n_minus2": n_minus2,
            "n_minus1": n_minus1,
            "n_zero": n_zero,
            "n_one": n_one,
        })
    except Exception as e:
        print(f"    error: {e}", file=sys.stderr)

# --- Apply filters and report -------------------------------------------------
print(f"\n=== Backtest results: {len(results)} trades ===\n")

# Per-trade table
print(f"{'token':<12} {'reason':<13} {'D_gas':>6} {'realized':>10}  flows: pre(N-1)  D(N+0)  ours(N+1)  | filter A | filter B | filter C")
print("-" * 140)

def f_pre_negative(r):
    """Filter A: pre-D 2-block net flow < 0 (visible before entry)"""
    if r["n_minus2"] is None or r["n_minus1"] is None or r["n_zero"] is None:
        return False, "no_data"
    pre_flow = (r["n_zero"] - r["n_minus2"]) - (r["n_zero"] - r["n_minus1"])  # = n_minus1 - n_minus2
    pre_flow = r["n_minus1"] - r["n_minus2"]  # flow IN block (N-1)
    return pre_flow < 0, pre_flow

def f_post_non_d_negative(r):
    """Filter B: in D's block, non-D net flow < 0 (D's contribution subtracted)"""
    if r["n_minus1"] is None or r["n_zero"] is None or r["d_bnb"] is None:
        return False, "no_data"
    n0_flow_wei = r["n_zero"] - r["n_minus1"]
    d_contrib_wei = int(r["d_bnb"] * 1e18)
    non_d_flow = n0_flow_wei - d_contrib_wei
    return non_d_flow < 0, non_d_flow

def f_mcap_2x(r):
    """Filter C: our entry curve > 2x pre-D curve (swarm doubled the curve)"""
    if r["n_minus1"] is None or r["n_one"] is None or r["n_minus1"] == 0:
        return False, "no_data"
    inflate = r["n_one"] / r["n_minus1"]
    return inflate > 2.0, inflate

total_actual = 0
total_a = 0; skipped_a = 0
total_b = 0; skipped_b = 0
total_c = 0; skipped_c = 0
for r in results:
    actual_bnb = r["realized_wei"] / 1e18
    total_actual += actual_bnb
    a_skip, a_val = f_pre_negative(r)
    b_skip, b_val = f_post_non_d_negative(r)
    c_skip, c_val = f_mcap_2x(r)
    if not a_skip: total_a += actual_bnb
    else: skipped_a += 1
    if not b_skip: total_b += actual_bnb
    else: skipped_b += 1
    if not c_skip: total_c += actual_bnb
    else: skipped_c += 1

    pre_str  = f"{(r['n_minus1']-r['n_minus2'])/1e18:+.3f}" if r["n_minus1"] and r["n_minus2"] else "  n/a"
    d_str    = f"{(r['n_zero']-r['n_minus1'])/1e18:+.3f}" if r["n_zero"] and r["n_minus1"] else "  n/a"
    o_str    = f"{(r['n_one']-r['n_zero'])/1e18:+.3f}" if r["n_one"] and r["n_zero"] else "  n/a"
    a_tag = "SKIP" if a_skip else "    "
    b_tag = "SKIP" if b_skip else "    "
    c_tag = "SKIP" if c_skip else "    "
    gas_s = f"{r['d_gas']:>5.1f}" if r['d_gas'] is not None else "  n/a"
    print(f"{r['token'][:10]:<12} {r['reason']:<13} {gas_s:>6} {r['realized_pct']:>+8.1f}%   {pre_str:>9}  {d_str:>9}  {o_str:>9}  |  {a_tag}    |  {b_tag}    |  {c_tag}")

print("-" * 140)
print(f"\n=== Summary (over {len(results)} trades) ===")
print(f"  ACTUAL realized PnL:         {total_actual:+.5f} BNB ≈ ${total_actual*BNB_USD:+.2f}")
print(f"  Filter A (pre-D negative):   {total_a:+.5f} BNB ≈ ${total_a*BNB_USD:+.2f}  (skipped {skipped_a})  Δ = ${(total_a-total_actual)*BNB_USD:+.2f}")
print(f"  Filter B (D-block non-D <0): {total_b:+.5f} BNB ≈ ${total_b*BNB_USD:+.2f}  (skipped {skipped_b})  Δ = ${(total_b-total_actual)*BNB_USD:+.2f}")
print(f"  Filter C (mcap >2x at entry):{total_c:+.5f} BNB ≈ ${total_c*BNB_USD:+.2f}  (skipped {skipped_c})  Δ = ${(total_c-total_actual)*BNB_USD:+.2f}")
