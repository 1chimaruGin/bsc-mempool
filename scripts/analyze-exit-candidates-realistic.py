#!/usr/bin/env python3
"""
Re-run the 8-candidate exit strategy backtest WITH realistic exit
slippage haircut. The cached n2_price already includes entry slippage
(+42% over D's entry) but exit prices are observed-at-decision, not
real-fill — today's data shows real fills are typically 2-16pp WORSE
than observed.

Models tested:
  haircut_0pct:   no slippage (baseline backtest result)
  haircut_3pct:   exit × 0.97 (mild slippage)
  haircut_5pct:   exit × 0.95 (typical)
  haircut_7pct:   exit × 0.93 (pessimistic)
  haircut_10pct:  exit × 0.90 (worst observed today)

Hard SL is NOT haircut — when it fires, the price IS at the SL floor and
will fill near that level (it's a forced exit, not curve-pulled).
"""
import json, sys
from collections import defaultdict

CACHE = "d_microstructure_v2_paths.json"
N2_OFFSET = 2
ARM_PCT  = 0.30
SL_PCT   = 0.30
BE_AT    = 0.15
BE_LOCK  = 0.05

with open(CACHE) as f:
    tokens = json.load(f)

def build_path(tok):
    pb = tok.get("_per_block") or {}
    if not pb: return None
    blocks = sorted(int(b) for b in pb.keys())
    d_block = tok["d_block"]
    n2_block = next((b for b in blocks if b >= d_block + N2_OFFSET), None)
    if n2_block is None: return None
    n2_price = pb[str(n2_block)]["last_price"]
    if n2_price <= 0: return None
    path = []
    for b in blocks:
        if b < n2_block: continue
        d = pb[str(b)]
        path.append({
            "block": b, "price": d["last_price"],
            "buyers": d["buyers"], "sellers": d["sellers"],
            "buy_bnb": d["buy_bnb"], "sell_bnb": d["sell_bnb"],
        })
    return {"n2_block": n2_block, "n2_price": n2_price, "path": path} if path else None

paths = [p for p in (build_path(t) for t in tokens) if p]
print(f"loaded {len(paths)} tokens", file=sys.stderr)

def sum_field(path, idx, field, n):
    return sum(path[j][field] for j in range(max(0, idx-n+1), idx+1))

# Each replay returns (ratio_observed, reason, is_hard_sl)
def apply_haircut(ratio, is_hard_sl, haircut):
    """Hard SL exits at the SL floor (curve gates them) — no haircut.
    Other exits decay by haircut to model fill slippage."""
    if is_hard_sl or haircut == 0:
        return ratio
    return ratio * (1 - haircut)

# ── strategies (same as before but tag hard_sl reasons) ─────────

def replay_live(pd):
    n2 = pd["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    for fb in pd["path"]:
        p = fb["price"]
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        eff = max(sl_floor, lock) if ratcheted else sl_floor
        if p <= eff:
            why = "be_locked" if (ratcheted and lock > sl_floor) else "hard_sl"
            return p/n2, why, why == "hard_sl"
        if armed and p <= peak*(1 - 0.30):
            return p/n2, "trail", False
    return pd["path"][-1]["price"]/n2, "timeout", False

def replay_voting(pd, k=3):
    n2 = pd["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    path = pd["path"]
    for i, fb in enumerate(path):
        p = fb["price"]
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        eff = max(sl_floor, lock) if ratcheted else sl_floor
        if p <= eff:
            why = "be_locked" if (ratcheted and lock > sl_floor) else "hard_sl"
            return p/n2, why, why == "hard_sl"
        if not armed: continue
        recent = path[max(0, i-9):i+1]
        local_max = max(r["price"] for r in recent)
        dist = (local_max - p) / local_max if local_max > 0 else 0
        if i >= 10:
            past = path[i-10]["price"]
            vel_10 = (p - past) / past / 10 if past > 0 else 0
        else: vel_10 = 0
        buy_3  = sum_field(path, i, "buyers", 3)
        buy_10 = sum_field(path, i, "buyers", 10)
        bv3 = buy_3 / 3 if i >= 2 else 0
        bv10 = buy_10 / 10 if i >= 9 else 0
        collapse = (bv3 / bv10) if bv10 > 0 else 1.0
        buy_bnb_3  = sum_field(path, i, "buy_bnb", 3)
        sell_bnb_3 = sum_field(path, i, "sell_bnb", 3)
        net_3 = buy_bnb_3 - sell_bnb_3
        votes = (int(dist > 0.30) + int(vel_10 < -0.01)
                 + int(collapse < 0.5) + int(net_3 < -1e18))
        if votes >= k:
            return p/n2, f"vote_{votes}", False
    return path[-1]["price"]/n2, "timeout", False

def replay_local_max(pd, lookback=10, width=0.30):
    n2 = pd["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    path = pd["path"]
    for i, fb in enumerate(path):
        p = fb["price"]
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        eff = max(sl_floor, lock) if ratcheted else sl_floor
        if p <= eff:
            why = "be_locked" if (ratcheted and lock > sl_floor) else "hard_sl"
            return p/n2, why, why == "hard_sl"
        if armed:
            recent = path[max(0, i-lookback+1):i+1]
            local_peak = max(r["price"] for r in recent)
            if p <= local_peak * (1 - width):
                return p/n2, "trail_local", False
    return pd["path"][-1]["price"]/n2, "timeout", False

# ── Sweep ─────────────────────────────────────────────────────────

def evaluate(fn, label, haircut):
    exits = []
    for pd in paths:
        try:
            ratio, why, is_sl = fn(pd)
        except Exception:
            continue
        realized = apply_haircut(ratio, is_sl, haircut)
        exits.append(realized)
    n = len(exits)
    if not n: return None
    avg = sum(exits)/n
    wins = sum(1 for x in exits if x > 1.0)
    ge15 = sum(1 for x in exits if x >= 1.5)
    ge2  = sum(1 for x in exits if x >= 2.0)
    return {"label": label, "haircut": haircut, "avg": avg, "wins": wins, "ge15": ge15, "ge2": ge2, "n": n}

strategies = [
    ("CURRENT LIVE",                              replay_live),
    ("3-of-4 voting",                             lambda pd: replay_voting(pd, k=3)),
    ("Local-max trail (10/30)",                   lambda pd: replay_local_max(pd, 10, 0.30)),
]

haircuts = [0.00, 0.03, 0.05, 0.07, 0.10]

print(f"\n=== Realistic exit-slippage sensitivity ===\n", file=sys.stderr)
print(f"  {'strategy':<28}  {'haircut':>8}  {'avg':>6}  {'wins':>5}  {'≥2x':>4}  {'daily net @ $20/trade':>23}", file=sys.stderr)
print(f"  " + "-"*100, file=sys.stderr)
for lbl, fn in strategies:
    for hc in haircuts:
        r = evaluate(fn, lbl, hc)
        if r is None: continue
        gross = 19 * 20 * (r["avg"] - 1.0)
        net = gross - 19 * 1.30
        print(f"  {lbl:<28}  {int(hc*100):>5}%   {r['avg']:>5.3f}x  {r['wins']:>5}  {r['ge2']:>4}  ${net:>+8.2f}/d", file=sys.stderr)
    print(f"  " + "-"*100, file=sys.stderr)

print(f"\n=== Headline at REALISTIC -5% exit slippage ===", file=sys.stderr)
print(f"  {'strategy':<28}  {'avg':>6}  {'Δ baseline':>10}  {'daily net':>10}", file=sys.stderr)
base = None
for lbl, fn in strategies:
    r = evaluate(fn, lbl, 0.05)
    if r is None: continue
    if base is None: base = r["avg"]
    gross = 19 * 20 * (r["avg"] - 1.0)
    net = gross - 19 * 1.30
    mark = " ← BEAT" if r["avg"] > base + 0.005 else ""
    print(f"  {lbl:<28}  {r['avg']:>5.3f}x  {r['avg']-base:+8.3f}  ${net:>+8.2f}/d{mark}", file=sys.stderr)
