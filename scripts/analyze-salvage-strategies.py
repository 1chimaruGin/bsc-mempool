#!/usr/bin/env python3
"""
"Salvage mode" exit-strategy search.

Today's pain: 4 of 4 closed losses peaked +13% to +27% — BELOW our +30%
arm threshold — and rode to -30% hard SL. We never banked any partial
gain. The two winners armed and trailed normally.

This script tests strategies that LOCK SMALL GAINS EARLIER, then compares
against the current trail (arm+30/trail-30/SL-30) on the 30-day cache.

Strategies tested:
  - Tiered TP ladders (sell N% at +K%, multiple levels)
  - Lower arm + trail combos
  - Hybrid: small TP + trail rest

Output:
  - Population avg_exit_mult, win rate, ≥2x count
  - Distribution of exit reasons
  - "Salvage delta": # of losses turned into break-evens or wins
"""
import json, sys
from collections import defaultdict

CACHE = "d_microstructure_v2_paths.json"
N2_OFFSET = 2

with open(CACHE) as f:
    tokens = json.load(f)

# Build (n2_price, path[(block, price)]) per token
def build_paths():
    out = []
    for tok in tokens:
        pb = tok.get("_per_block") or {}
        if not pb: continue
        blocks = sorted(int(b) for b in pb.keys())
        d_block = tok["d_block"]
        n2_block = next((b for b in blocks if b >= d_block + N2_OFFSET), None)
        if n2_block is None: continue
        n2_price = pb[str(n2_block)]["last_price"]
        if n2_price <= 0: continue
        path = [(b, pb[str(b)]["last_price"]) for b in blocks if b >= n2_block]
        out.append({"n2_price": n2_price, "path": path, "d_block": d_block, "token": tok["token"]})
    return out

paths = build_paths()
print(f"loaded {len(paths)} tokens with paths", file=sys.stderr)

def replay_trail(path, n2, arm, trail, sl):
    """Standard trail (current LIVE)."""
    peak = n2
    armed = False
    sl_floor = n2 * (1 - sl)
    for blk, p in path:
        peak = max(peak, p)
        if p <= sl_floor:           return p/n2, "hard_sl"
        if not armed and peak >= n2*(1+arm): armed = True
        if armed and p <= peak*(1-trail):    return p/n2, "trail"
    return path[-1][1]/n2 if path else 1.0, "timeout"

def replay_breakeven(path, n2, arm, trail, sl, be_at):
    """
    Trail + RATCHETING SL: once price hits `be_at` (e.g. +15%), move SL
    up to BREAK-EVEN. Then once armed, follow trail. The break-even
    floor PROTECTS against the "rode a small pump down to -30%" loss
    pattern, without compromising the trail's catch of big runners.
    """
    peak = n2
    armed = False
    sl_floor = n2 * (1 - sl)
    be_floor = n2  # break-even = entry price
    be_active = False
    for blk, p in path:
        peak = max(peak, p)
        if not be_active and peak >= n2*(1+be_at):
            be_active = True
            sl_floor = max(sl_floor, be_floor)  # ratchet SL up to break-even
        if p <= sl_floor:           return p/n2, "be_stop" if be_active else "hard_sl"
        if not armed and peak >= n2*(1+arm): armed = True
        if armed and p <= peak*(1-trail):    return p/n2, "trail"
    return path[-1][1]/n2 if path else 1.0, "timeout"

def replay_breakeven_plus(path, n2, arm, trail, sl, be_at, lock_pct):
    """
    Like break-even, but after hitting `be_at` ratchet SL to +lock_pct
    (e.g. +5%). Locks a tiny gain instead of just break-even.
    """
    peak = n2
    armed = False
    sl_floor = n2 * (1 - sl)
    locked = False
    for blk, p in path:
        peak = max(peak, p)
        if not locked and peak >= n2*(1+be_at):
            locked = True
            sl_floor = max(sl_floor, n2*(1+lock_pct))
        if p <= sl_floor:           return p/n2, "locked" if locked else "hard_sl"
        if not armed and peak >= n2*(1+arm): armed = True
        if armed and p <= peak*(1-trail):    return p/n2, "trail"
    return path[-1][1]/n2 if path else 1.0, "timeout"

def replay_tp_ladder(path, n2, tps, sl, residual="trail", trail_pct=0.30, arm_pct=0.30):
    """
    Multi-tier TP. `tps` = list of (tp_pct_from_entry, fraction_to_sell).
    `residual` = what to do with the unsold portion:
      "trail"  : after armed, trail at -trail_pct from peak; else timeout
      "hold"   : never close until SL or timeout
    """
    sl_floor = n2 * (1 - sl)
    remaining = 1.0
    realized  = 0.0
    hit       = [False]*len(tps)
    peak = n2
    armed = False
    for blk, p in path:
        peak = max(peak, p)
        if p <= sl_floor:
            realized += remaining * (p/n2)
            return realized, "hard_sl"
        if not armed and peak >= n2*(1+arm_pct): armed = True
        for i, (tp_pct, frac) in enumerate(tps):
            if hit[i]: continue
            if p >= n2*(1+tp_pct):
                take = min(frac, remaining)
                realized += take*(p/n2)
                remaining -= take
                hit[i] = True
                if remaining <= 1e-9:
                    return realized, f"tp_full_{i+1}"
        if residual == "trail" and armed and remaining > 1e-9 and p <= peak*(1-trail_pct):
            realized += remaining * (p/n2)
            return realized, "trail"
    last_p = path[-1][1] if path else n2
    realized += remaining * (last_p/n2)
    return realized, "timeout"

def evaluate(strategy_fn, label):
    exits  = []
    reasons = defaultdict(int)
    for tk in paths:
        mult, why = strategy_fn(tk["path"], tk["n2_price"])
        exits.append(mult)
        reasons[why] += 1
    n = len(exits)
    avg  = sum(exits)/n
    wins = sum(1 for x in exits if x > 1.0)
    ge12 = sum(1 for x in exits if x >= 1.2)
    ge15 = sum(1 for x in exits if x >= 1.5)
    ge2  = sum(1 for x in exits if x >= 2.0)
    losses_le_70 = sum(1 for x in exits if x <= 0.70)  # actually hit -30%
    return {"label": label, "avg": avg, "wins": wins, "ge12": ge12,
            "ge15": ge15, "ge2": ge2, "losses_at_sl": losses_le_70,
            "reasons": dict(reasons), "n": n}

# ── Configure strategies ─────────────────────────────────────────
strategies = []

# Current LIVE baseline
strategies.append(("LIVE: arm+30/trail-30/SL-30",
                   lambda p, n: replay_trail(p, n, 0.30, 0.30, 0.30)))

# Looser arm trail
for arm in [0.15, 0.20]:
    strategies.append((f"trail-only arm+{int(arm*100)}/trail-30/SL-30",
                       lambda p, n, a=arm: replay_trail(p, n, a, 0.30, 0.30)))

# Single TP, sell all
for tp in [0.10, 0.15, 0.20]:
    strategies.append((f"single-TP +{int(tp*100)}% sell ALL / SL-30",
                       lambda p, n, t=tp: replay_tp_ladder(p, n, [(t, 1.0)], 0.30)))

# 2-tier TP + trail residual
for tp1 in [0.10, 0.15, 0.20]:
    for f1 in [0.33, 0.50]:
        strategies.append((f"TP@+{int(tp1*100)}%×{int(f1*100)}% + trail-30 (arm{int(tp1*100)})",
                           lambda p, n, t=tp1, f=f1: replay_tp_ladder(
                               p, n, [(t, f)], 0.30, "trail", 0.30, t)))

# Break-even SL ratchet strategies (preserve runners, kill losers)
for be_at in [0.10, 0.15, 0.20]:
    strategies.append((f"BE-stop @ +{int(be_at*100)}% / arm+30/trail-30/SL-30",
                       lambda p, n, b=be_at: replay_breakeven(p, n, 0.30, 0.30, 0.30, b)))
# Same but with lower arm so trail kicks in earlier
for be_at in [0.10, 0.15]:
    strategies.append((f"BE-stop @ +{int(be_at*100)}% / arm+15/trail-30/SL-30",
                       lambda p, n, b=be_at: replay_breakeven(p, n, 0.15, 0.30, 0.30, b)))
# Lock +5% gain after hitting +15% / +20% (instead of just break-even)
for be_at, lock in [(0.15, 0.05), (0.20, 0.10), (0.25, 0.10)]:
    strategies.append((f"Lock +{int(lock*100)}% @ +{int(be_at*100)}% / arm+15/trail-30/SL-30",
                       lambda p, n, b=be_at, L=lock: replay_breakeven_plus(p, n, 0.15, 0.30, 0.30, b, L)))

# 3-tier salvage ladder + trail residual
for ladder in [
    [(0.10, 0.30), (0.20, 0.30), (0.40, 0.20)],  # leaves 20% to trail
    [(0.15, 0.30), (0.30, 0.30), (0.60, 0.20)],
    [(0.10, 0.40), (0.30, 0.30), (0.80, 0.10)],
    [(0.15, 0.50), (0.50, 0.30)],                # 20% left to trail/timeout
]:
    label = "ladder " + " / ".join(f"{int(f*100)}%@+{int(t*100)}%" for t, f in ladder) + " + trail"
    strategies.append((label, lambda p, n, L=ladder: replay_tp_ladder(p, n, L, 0.30, "trail", 0.30, 0.10)))

# ── Run all ──────────────────────────────────────────────────────
results = []
for label, fn in strategies:
    results.append(evaluate(fn, label))

# Sort by avg_exit desc
results.sort(key=lambda r: -r["avg"])

baseline = next(r for r in results if r["label"].startswith("LIVE"))
print(f"\nBaseline (current LIVE): avg={baseline['avg']:.3f}x  wins={baseline['wins']}/{baseline['n']}  ≥1.2x={baseline['ge12']}  ≥1.5x={baseline['ge15']}  ≥2x={baseline['ge2']}  losses_at_SL={baseline['losses_at_sl']}", file=sys.stderr)
print(f"\nSorted by avg_exit (best on top):\n", file=sys.stderr)
print(f"  {'label':<55} {'avg':>6} {'Δbase':>6} {'wins':>5} {'≥1.2x':>5} {'≥2x':>4} {'SL_hits':>7}", file=sys.stderr)
for r in results:
    delta = r["avg"] - baseline["avg"]
    print(f"  {r['label']:<55} {r['avg']:>5.3f}x {delta:+6.3f} {r['wins']:>5} {r['ge12']:>5} {r['ge2']:>4} {r['losses_at_sl']:>7}", file=sys.stderr)

# Today's 4 losers — emulate them as toy paths to check if a strategy salvages
print(f"\n\n=== Replay TODAY's 4 closed losers under top strategies ===", file=sys.stderr)
today_losers = [
    ("bc299aa2 (peak +23%)", 1.0, [(1.0,), (1.05,), (1.10,), (1.18,), (1.23,), (1.10,), (0.90,), (0.75,), (0.64,)]),
    ("f3217fac (peak +13%)", 1.0, [(1.0,), (1.05,), (1.10,), (1.13,), (1.10,), (0.90,), (0.70,), (0.62,)]),
    ("f8cb0a06 (peak +27%, timeout)", 1.0, [(1.0,), (1.10,), (1.20,), (1.27,), (1.20,), (1.15,), (1.10,), (1.05,), (1.00,), (0.95,), (0.93,)]),
    ("35be71c9 (peak +19%)", 1.0, [(1.0,), (1.10,), (1.15,), (1.19,), (1.05,), (0.85,), (0.70,), (0.58,)]),
]

top_strategies_for_today = [r["label"] for r in results[:6]]
top_strategies_for_today.append(baseline["label"])

# Build the strategy lookup
strat_map = dict(strategies)

for token_name, n2, ticks in today_losers:
    print(f"\n  {token_name}", file=sys.stderr)
    # ticks were tuples for csv compat; flatten
    path = [(i, t[0]*n2) for i, t in enumerate(ticks, 1)]
    for label in top_strategies_for_today:
        fn = strat_map[label]
        mult, why = fn(path, n2)
        marker = " ←WIN" if mult > 1.0 else (" ←BE" if mult >= 0.95 else "")
        print(f"    {label:<55} → {mult:.2f}x ({why}){marker}", file=sys.stderr)
