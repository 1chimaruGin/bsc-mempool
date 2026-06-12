#!/usr/bin/env python3
"""
Parse the runner journal for D's BUY transactions and answer:
  - How visible is D in the public mempool? (% public vs private)
  - For public BUYs: how much lead time (ms) before mining?
  - What gas price does D use? Distribution.
  - How much slot_remaining_ms is there when D's tx confirms?
  - Realistic same-block-as-D feasibility given our internal latency budget.

Input: /tmp/d_journal.log (lines grepped via `grep "kol_name=D"`)
"""
import re, sys, statistics, json
from collections import defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/d_journal.log"

# Patterns
RE_OBSERVED = re.compile(
    r'INFO kol: KOL tx observed kol_name=D[^\n]*?'
    r'tx_hash=(?P<tx>0x[0-9a-f]+)[^\n]*?'
    r'method_label=Some\("(?P<method>[^"]*)"\)[^\n]*?'
    r'side="(?P<side>[^"]*)"[^\n]*?'
    r'token=(?P<token>0x[0-9a-f]+|-)[^\n]*?'
    r'value_bnb=(?P<value_bnb>[\d.e+-]+)[^\n]*?'
    r'gas_price_gwei=(?P<gas_gwei>[\d.e+-]+)[^\n]*?'
    r'gas_limit=(?P<gas_limit>\d+)'
)
RE_CONFIRMED = re.compile(
    r'INFO kol: KOL tx CONFIRMED kol_name=D[^\n]*?'
    r'side="(?P<side>[^"]*)"[^\n]*?'
    r'token=(?P<token>0x[0-9a-f]+|\?)[^\n]*?'
    r'tx_hash=(?P<tx>0x[0-9a-f]+)[^\n]*?'
    r'visibility="(?P<visibility>[^"]*)"[^\n]*?'
    r'seen_block=(?P<seen>"\?"|\d+)[^\n]*?'
    r'mined_block=(?P<mined>\d+)[^\n]*?'
    r'block_delta="(?P<delta>[^"]*)"[^\n]*?'
    r'ms_into_block=(?P<ms_into>\d+)[^\n]*?'
    r'slot_remaining_ms=(?P<slot_rem>\d+)[^\n]*?'
    r'lead_ms="(?P<lead>[^"]*)"[^\n]*?'
    r'detect_ms="(?P<detect>[^"]*)"'
)

observed   = {}
confirmed  = {}

with open(PATH) as f:
    for line in f:
        m = RE_OBSERVED.search(line)
        if m:
            observed[m["tx"]] = {
                "side": m["side"],
                "method": m["method"],
                "value_bnb": float(m["value_bnb"]),
                "gas_gwei": float(m["gas_gwei"]),
                "gas_limit": int(m["gas_limit"]),
                "token": m["token"],
            }
            continue
        m = RE_CONFIRMED.search(line)
        if m:
            try:
                seen = int(m["seen"]) if m["seen"] != '"?"' else None
            except ValueError:
                seen = None
            try:
                lead = int(m["lead"]) if m["lead"] != "?" else None
            except ValueError:
                lead = None
            try:
                detect = int(m["detect"]) if m["detect"] != "?" else None
            except ValueError:
                detect = None
            confirmed[m["tx"]] = {
                "side": m["side"],
                "visibility": m["visibility"],
                "seen_block": seen,
                "mined_block": int(m["mined"]),
                "block_delta": m["delta"],
                "ms_into_block": int(m["ms_into"]),
                "slot_remaining_ms": int(m["slot_rem"]),
                "lead_ms": lead,
                "detect_ms": detect,
            }

print(f"D tx observed (any side): {len(observed)}")
print(f"D tx CONFIRMED (any side): {len(confirmed)}")

# Filter D BUYs only — use the GMGN/launchpad path
buys_obs = {tx: r for tx, r in observed.items()
            if r["side"] == "BUY" and "BNB transfer" not in r["method"]}
print(f"D BUYs observed (filtered to GMGN/launchpad route): {len(buys_obs)}")

buys_conf = {tx: r for tx, r in confirmed.items() if r["side"] == "BUY"}
print(f"D BUYs CONFIRMED: {len(buys_conf)}")

# Visibility breakdown of all confirmed BUYs
vis = defaultdict(int)
for r in buys_conf.values():
    vis[r["visibility"]] += 1
print(f"\nVisibility breakdown:")
for v, c in sorted(vis.items()):
    pct = 100 * c / max(1, len(buys_conf))
    print(f"  {v:>8}: {c:>4}  ({pct:5.1f}%)")

# Match observed ↔ confirmed by tx_hash
both = [(buys_obs[tx], buys_conf[tx]) for tx in buys_obs if tx in buys_conf]
print(f"\nObserved-AND-Confirmed BUYs (matched): {len(both)}")

def pctile(arr, p):
    if not arr: return None
    s = sorted(arr); k = int(round((len(s)-1) * p))
    return s[k]

def fmt_dist(arr, fmt="{:.0f}", unit=""):
    if not arr:
        print(f"  no data"); return
    print(f"  n={len(arr)}  med={fmt.format(pctile(arr,0.5))}{unit}"
          f"  P25={fmt.format(pctile(arr,0.25))}{unit}"
          f"  P75={fmt.format(pctile(arr,0.75))}{unit}"
          f"  P90={fmt.format(pctile(arr,0.9))}{unit}"
          f"  P99={fmt.format(pctile(arr,0.99))}{unit}")

# ── Gas price distribution (D's BUYs only) ───────────────────────────
gas_all = [r["gas_gwei"] for r in buys_obs.values()]
print(f"\n=== D BUY gas_price_gwei distribution ===")
fmt_dist(gas_all, "{:.2f}", " gwei")

# ── Value distribution ───────────────────────────────────────────────
val = [r["value_bnb"] for r in buys_obs.values() if r["value_bnb"] > 0.0001]
print(f"\n=== D BUY value_bnb (filtering dust) ===")
fmt_dist(val, "{:.3f}", " BNB")

# ── PUBLIC BUYs: lead_ms (mempool visibility before mining) ───────────
public_lead = [c["lead_ms"] for o, c in both
               if c["visibility"] == "public" and c["lead_ms"] is not None]
print(f"\n=== PUBLIC D BUY: lead_ms (mempool→mined) ===")
fmt_dist(public_lead, "{:.0f}", " ms")

# ── PUBLIC BUYs: slot_remaining_ms (when in the block did it confirm?) ─
public_slot = [c["slot_remaining_ms"] for o, c in both
               if c["visibility"] == "public"]
print(f"\n=== PUBLIC D BUY: slot_remaining_ms (head-room left in the block) ===")
fmt_dist(public_slot, "{:.0f}", " ms")

# ── PUBLIC BUYs: ms_into_block ───────────────────────────────────────
public_ms_in = [c["ms_into_block"] for o, c in both
                if c["visibility"] == "public"]
print(f"\n=== PUBLIC D BUY: ms_into_block (how late into the block we saw it confirmed) ===")
fmt_dist(public_ms_in, "{:.0f}", " ms")

# ── block_delta breakdown ───────────────────────────────────────────
delta = defaultdict(int)
for c in buys_conf.values():
    delta[c["block_delta"]] += 1
print(f"\n=== Block delta (seen_block → mined_block) ===")
for d, c in sorted(delta.items(), key=lambda kv: (kv[0]=="?", kv[0])):
    pct = 100*c/len(buys_conf)
    print(f"  delta={d:>3}: {c:>4}  ({pct:5.1f}%)")

# ── Same-block feasibility analysis ─────────────────────────────────
# For us to land in the same block as D:
#   Our submit must reach BlockRazor BEFORE the block that includes D closes.
#   slot_remaining_ms at confirm time = headroom we'd had to act on the
#   tx we saw in the prior block.
#
# Latency budget = detect_ms + decide_ms + sign_ms + submit_RTT
#   detect_ms (observed in journal): see below
#   decide_ms (Rust strategy gate): ~5ms
#   sign_ms: ~2ms (alloy local signer)
#   submit_RTT (BlockRazor): ~30-80ms typical
# Total internal budget: ~40-90ms
#
# If lead_ms - 90 > 0, we COULD land in the same block as D (provided
# the validator includes our tx — gas-price is the gate).
print(f"\n=== Same-block feasibility ===")
LATENCY_BUDGET_MS = 90  # detect+decide+sign+submit
feasible = []
for o, c in both:
    if c["visibility"] != "public": continue
    if c["lead_ms"] is None: continue
    # If we'd seen D's pending tx with lead_ms headroom, after spending
    # LATENCY_BUDGET_MS we'd have (lead_ms - 90) ms to also land in the
    # same target block. Anything > 0 is feasible.
    headroom = c["lead_ms"] - LATENCY_BUDGET_MS
    feasible.append((c, headroom))

n_pub = sum(1 for o, c in both if c["visibility"] == "public" and c["lead_ms"] is not None)
n_feas = sum(1 for c, h in feasible if h > 0)
print(f"  Public BUYs with measurable lead: {n_pub}")
print(f"  Same-block-feasible (lead > {LATENCY_BUDGET_MS}ms latency): {n_feas} ({100*n_feas/max(1,n_pub):.1f}%)")
if feasible:
    head = sorted([h for c, h in feasible], reverse=True)
    print(f"  Headroom distribution (lead - latency):")
    fmt_dist(head, "{:+.0f}", " ms")
