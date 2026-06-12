#!/usr/bin/env python3
"""
Voted stop-loss variants. Instead of firing hard_sl at a fixed -30% from
entry, require MULTIPLE bearish conditions to agree before exiting (like
signal_vote on the upside).

Features at each block:
  F1  price_drop   :  price <= entry * (1 - drop_pct)
  F2  vel_3        :  3-block velocity < vel_thresh (e.g. -0.05)
  F3  no_recovery  :  no +5%-block in last `lookback` blocks
  F4  net_outflow  :  curve BNB strictly decreasing in last 3 blocks

Variants:
  V0  ACTUAL    : what we did (live trail logic)
  V1  -30% fixed: classic hard_sl at -30% (no condition)
  V2  voted-2of4: exit if 2 of {F1@-25%, F2, F3, F4}
  V3  voted-3of4: exit if 3 of {F1@-25%, F2, F3, F4}
  V4  staged    : at -20% need 3of4, at -40% need 2of4, at -60% exit unconditionally
  V5  veto-on-rec: classic -30% but VETO if F3 fails (i.e. recent recovery exists)

Each variant retains the EXISTING peak-trail (+10% arm, -30% trail) and
signal_vote/signal_dump exits; we only change the hard-SL clause.
"""
import json, os, re, subprocess, sys, urllib.request

RPC = os.environ.get("NODEREAL_RPC_URL") or sys.exit("set NODEREAL_RPC_URL")
FOURMEME = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
TOKEN_INFOS_SEL = "0xe684626b"
WALLET = "0x530306684a29e23676d30fa80dc6100e80b042ea"
BNB_USD = 586.0
TRACE_WINDOW = 40
SELL_SLIPPAGE = 0.10  # 10% haircut on hypothetical exits

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

# Fetch trajectory + realized PnL for each
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
        bal_before = int(rpc("eth_getBalance", [WALLET, hex(sell_block-1)]), 16)
        bal_after  = int(rpc("eth_getBalance", [WALLET, hex(sell_block)]), 16)
        actual_sell_proceeds = (bal_after - bal_before) + sell_gas
        actual_realized = actual_sell_proceeds - (buy_value + buy_gas + sell_gas)
        entry_curve = curve_bnb(t["token"], buy_block)
        if entry_curve is None or entry_curve == 0: continue
        traj = []  # list of curve BNB per block from N+1 onward
        for off in range(0, TRACE_WINDOW + 1):
            b = buy_block + off
            cb = curve_bnb(t["token"], b)
            traj.append(cb)
        results.append({
            **t, "buy_block": buy_block, "sell_block": sell_block,
            "buy_value": buy_value, "buy_gas": buy_gas, "sell_gas": sell_gas,
            "actual_realized": actual_realized,
            "actual_pct": actual_realized / (buy_value+buy_gas+sell_gas) * 100,
            "entry_curve": entry_curve, "traj": traj,
        })
    except Exception as e:
        print(f"    error: {e}", file=sys.stderr)

# --- Variant simulators --------------------------------------------------------

def features_at_block(r, i):
    """Compute feature flags for trade r at block-offset i (0 = entry)."""
    traj = r["traj"]
    entry = r["entry_curve"]
    cur = traj[i] if i < len(traj) and traj[i] is not None else None
    if cur is None: return None
    pct_drop = (cur - entry) / entry  # negative on loss
    # vel_3: 3-block velocity
    if i >= 3 and traj[i-3] is not None:
        vel3 = (cur - traj[i-3]) / traj[i-3]
    else:
        vel3 = 0
    # no_recovery: no +5% single-block jump in last 5 blocks
    recovered = False
    for j in range(max(0, i-5), i):
        if j+1 < len(traj) and traj[j] is not None and traj[j+1] is not None:
            if traj[j] > 0 and (traj[j+1] - traj[j]) / traj[j] > 0.05:
                recovered = True; break
    no_recovery = not recovered
    # net_outflow: curve strictly decreasing last 3 blocks
    if i >= 3 and traj[i-2] is not None and traj[i-1] is not None and cur is not None:
        net_outflow = cur < traj[i-1] < traj[i-2]
    else:
        net_outflow = False
    return {
        "pct_drop": pct_drop,
        "vel3": vel3,
        "no_recovery": no_recovery,
        "net_outflow": net_outflow,
    }

def variant_simulate(r, decide_fn):
    """Walk blocks; first block where decide_fn says EXIT, return (block, pct_drop)."""
    for i in range(1, len(r["traj"])):
        f = features_at_block(r, i)
        if f is None: continue
        if decide_fn(f, i):
            return i, f["pct_drop"]
    # No exit fired; use last available
    last = next((c for c in reversed(r["traj"]) if c is not None), r["entry_curve"])
    return len(r["traj"])-1, (last - r["entry_curve"]) / r["entry_curve"]

def hypothetical_pnl(r, exit_pct):
    sell_proceeds = r["buy_value"] * (1 + exit_pct) * (1 - SELL_SLIPPAGE)
    return sell_proceeds - (r["buy_value"] + r["buy_gas"] + r["sell_gas"])

# Variant decision functions: return True to exit at this block
def v1_classic(f, i):  return f["pct_drop"] <= -0.30
def v2_voted2of4(f, i):
    votes = sum([f["pct_drop"] <= -0.25, f["vel3"] < -0.05, f["no_recovery"], f["net_outflow"]])
    return votes >= 2
def v3_voted3of4(f, i):
    votes = sum([f["pct_drop"] <= -0.25, f["vel3"] < -0.05, f["no_recovery"], f["net_outflow"]])
    return votes >= 3
def v4_staged(f, i):
    if f["pct_drop"] <= -0.60: return True
    if f["pct_drop"] <= -0.40:
        votes = sum([f["vel3"] < -0.05, f["no_recovery"], f["net_outflow"]])
        return votes >= 2
    if f["pct_drop"] <= -0.20:
        votes = sum([f["vel3"] < -0.05, f["no_recovery"], f["net_outflow"]])
        return votes >= 3
    return False
def v5_veto(f, i):
    return f["pct_drop"] <= -0.30 and f["no_recovery"]

variants = {
    "V1_classic_-30%":  v1_classic,
    "V2_voted_2of4":    v2_voted2of4,
    "V3_voted_3of4":    v3_voted3of4,
    "V4_staged_voting": v4_staged,
    "V5_veto-on-rec":   v5_veto,
}

# Compute totals
totals = {k: 0.0 for k in variants}
totals["V0_actual"] = sum(r["actual_realized"] for r in results) / 1e18

print(f"\n=== Per-trade comparison ({len(results)} trades, $10 size) ===\n")
hdr = f"{'token':<12} {'reason':<13} {'actual':>9}"
for k in variants: hdr += f" {k:>18}"
print(hdr); print("-" * len(hdr))
for r in results:
    row = f"{r['token'][:10]:<12} {r['reason']:<13} ${r['actual_realized']/1e18*BNB_USD:>+7.2f}"
    for name, fn in variants.items():
        blk, pct = variant_simulate(r, fn)
        pnl = hypothetical_pnl(r, pct) / 1e18 * BNB_USD
        totals[name] += pnl / BNB_USD  # store BNB
        delta = pnl - r["actual_realized"]/1e18*BNB_USD
        marker = "↑" if delta > 0.5 else ("↓" if delta < -0.5 else " ")
        row += f"  {marker}${pnl:>+5.2f}@b{blk:<2}"
    print(row)

print(f"\n=== Totals ({len(results)} trades) ===")
print(f"  V0_actual:            {totals['V0_actual']:+.5f} BNB ≈ ${totals['V0_actual']*BNB_USD:+.2f}  (live trail logic)")
for k in variants:
    delta_usd = (totals[k] - totals['V0_actual']) * BNB_USD
    marker = "↑↑" if delta_usd > 5 else ("↑" if delta_usd > 0 else ("↓" if delta_usd < 0 else " "))
    print(f"  {k:<22} {totals[k]:+.5f} BNB ≈ ${totals[k]*BNB_USD:+.2f}   Δ {delta_usd:>+6.2f}  {marker}")
