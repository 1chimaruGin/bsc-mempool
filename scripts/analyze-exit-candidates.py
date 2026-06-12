#!/usr/bin/env python3
"""
Five new exit-strategy candidates head-to-head against current LIVE on
the 30-day cached tape. Goal: find a rule that beats 1.124x avg.

Candidates:
  1. PROFIT-SCALED TRAIL
       Trail tightens as peak grows past thresholds.
       Hypothesis: small wins get a wider trail; runners get a tight trail
       that locks the rally.
  2. TIME-DECAY TRAIL
       Trail starts at -40% (wide for parabolic phase), tightens
       linearly to -10% over `max_hold_blocks`.
       Hypothesis: pre-peak chaos needs wide trail; post-peak drift
       wants tighter exit before timeout.
  3. ROUTE-AWARE TRAIL
       Different trail % for V2 vs Four.Meme route.
       Hypothesis: V2 deeper liquidity = tighter trail OK; Four.Meme
       curve = wider trail needed.
  4. K-OF-N VOTING (3 of 4)
       Exit when ≥3 of these fire same block (after armed):
         dist_from_local_max > 0.30
         vel_10 < -0.01
         buy_velocity_collapse < 0.5
         net_flow_3blk < -1e18
  5. CUMULATIVE-DD EXIT
       Track cumulative draw-down from running peak; exit when
       cumulative DD exceeds a threshold that grows with peak.

Baseline = current LIVE = arm+30/trail-30/SL-30 + ratchet @+15%→+5%.
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
        # carry the per-block features we need for some strategies
        path.append({
            "block": b,
            "price": d["last_price"],
            "buyers": d["buyers"],
            "sellers": d["sellers"],
            "buy_bnb": d["buy_bnb"],
            "sell_bnb": d["sell_bnb"],
        })
    if not path: return None
    return {"n2_block": n2_block, "n2_price": n2_price, "path": path}

paths = [p for p in (build_path(t) for t in tokens) if p]
print(f"loaded {len(paths)} tokens", file=sys.stderr)

# ── Helpers ───────────────────────────────────────────────────────

def sum_field(path, idx, field, n):
    return sum(path[j][field] for j in range(max(0, idx-n+1), idx+1))

# ── Baseline: current LIVE ────────────────────────────────────────

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
            return p/n2, ("be_locked" if (ratcheted and lock > sl_floor) else "hard_sl")
        if armed and p <= peak*(1 - 0.30):
            return p/n2, "trail"
    return pd["path"][-1]["price"]/n2, "timeout"

# ── Candidate 1: PROFIT-SCALED TRAIL ──────────────────────────────

def trail_pct_for_peak(peak_ratio):
    """Tighter trail as peak grows."""
    if peak_ratio < 1.30:  return 0.30
    if peak_ratio < 2.00:  return 0.25
    if peak_ratio < 5.00:  return 0.20
    if peak_ratio < 10.00: return 0.15
    return 0.10

def replay_profit_scaled(pd):
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
            return p/n2, ("be_locked" if (ratcheted and lock > sl_floor) else "hard_sl")
        if armed:
            trail_w = trail_pct_for_peak(peak/n2)
            if p <= peak * (1 - trail_w):
                return p/n2, f"trail_{int(trail_w*100)}"
    return pd["path"][-1]["price"]/n2, "timeout"

# ── Candidate 2: TIME-DECAY TRAIL ─────────────────────────────────

def replay_time_decay(pd, t_start=0.40, t_end=0.10, max_hold=4000):
    """Trail starts wide, tightens linearly with elapsed blocks."""
    n2 = pd["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    open_block = pd["n2_block"]
    for fb in pd["path"]:
        p = fb["price"]
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        eff = max(sl_floor, lock) if ratcheted else sl_floor
        if p <= eff:
            return p/n2, ("be_locked" if (ratcheted and lock > sl_floor) else "hard_sl")
        if armed:
            elapsed = (fb["block"] - open_block)
            frac = min(1.0, elapsed / max_hold)
            trail_w = t_start - (t_start - t_end) * frac
            if p <= peak * (1 - trail_w):
                return p/n2, f"trail_{int(trail_w*100)}"
    return pd["path"][-1]["price"]/n2, "timeout"

# ── Candidate 3: K-OF-N VOTING ───────────────────────────────────

def replay_voting(pd, k=3):
    """Exit when ≥k of these fire same block AFTER armed:
       dist_from_local_max > 0.30,
       vel_10 < -0.01,
       buy_velocity_collapse < 0.5,
       net_flow_3blk < -1e18 (= -1 BNB).
    """
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
            return p/n2, ("be_locked" if (ratcheted and lock > sl_floor) else "hard_sl")
        if not armed: continue

        # Compute features
        recent = path[max(0, i-9):i+1]
        local_max = max(r["price"] for r in recent)
        dist = (local_max - p) / local_max if local_max > 0 else 0
        # vel_10
        if i >= 10:
            past = path[i-10]["price"]
            vel_10 = (p - past) / past / 10 if past > 0 else 0
        else:
            vel_10 = 0
        # buy_velocity_collapse
        buy_3  = sum_field(path, i, "buyers", 3)
        buy_10 = sum_field(path, i, "buyers", 10)
        bv3 = buy_3 / 3 if i >= 2 else 0
        bv10 = buy_10 / 10 if i >= 9 else 0
        collapse = (bv3 / bv10) if bv10 > 0 else 1.0
        # net_flow_3blk
        buy_bnb_3  = sum_field(path, i, "buy_bnb", 3)
        sell_bnb_3 = sum_field(path, i, "sell_bnb", 3)
        net_3 = buy_bnb_3 - sell_bnb_3

        votes = 0
        if dist > 0.30: votes += 1
        if vel_10 < -0.01: votes += 1
        if collapse < 0.5: votes += 1
        if net_3 < -1e18: votes += 1
        if votes >= k:
            return p/n2, f"vote_{votes}"
    return path[-1]["price"]/n2, "timeout"

# ── Candidate 4: TRAIL-FROM-LOCAL-MAX ────────────────────────────

def replay_local_max_trail(pd, lookback=10, width=0.30):
    """Replace 'global peak' with 'local max over last N blocks' for
    the trail floor. Hypothesis: avoids being anchored to an old high
    that won't be revisited (memecoin double-tops are rare)."""
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
            return p/n2, ("be_locked" if (ratcheted and lock > sl_floor) else "hard_sl")
        if armed:
            recent = path[max(0, i-lookback+1):i+1]
            local_peak = max(r["price"] for r in recent)
            if p <= local_peak * (1 - width):
                return p/n2, "trail_local"
    return pd["path"][-1]["price"]/n2, "timeout"

# ── Candidate 5: CASCADE EXIT ────────────────────────────────────

def replay_cascade(pd):
    """Two-stage exit:
       - First trigger (any of: dist>0.30 same block as vel_10<-0.01,
         OR be_locked, OR trail-30) puts position in 'warning' state.
       - If next 3 blocks see net_flow < 0 → confirm exit.
       - If price recovers above peak × 0.90 → cancel warning.
       Hypothesis: filter out single-block flush + recovery patterns."""
    n2 = pd["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    path = pd["path"]
    warning = False; warning_block = 0; warning_count = 0
    for i, fb in enumerate(path):
        p = fb["price"]
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        eff = max(sl_floor, lock) if ratcheted else sl_floor
        # Hard SL always
        if p <= eff:
            return p/n2, ("be_locked" if (ratcheted and lock > sl_floor) else "hard_sl")
        if not armed: continue

        # Check trigger
        recent = path[max(0, i-9):i+1]
        local_max = max(r["price"] for r in recent)
        dist = (local_max - p) / local_max if local_max > 0 else 0
        if i >= 10:
            past = path[i-10]["price"]
            vel_10 = (p - past) / past / 10 if past > 0 else 0
        else:
            vel_10 = 0
        primary_trigger = (dist > 0.30 and vel_10 < -0.01) or (p <= peak*(1 - 0.30))

        if primary_trigger and not warning:
            warning = True
            warning_block = i
            warning_count = 0

        if warning:
            # Recovery cancellation
            if p >= peak * 0.90:
                warning = False
                continue
            # Confirm via net-flow
            buy_3  = sum_field(path, i, "buy_bnb", 3)
            sell_3 = sum_field(path, i, "sell_bnb", 3)
            if (buy_3 - sell_3) < 0:
                warning_count += 1
            if warning_count >= 3:
                return p/n2, "cascade"

    return path[-1]["price"]/n2, "timeout"

# ── Run all ──────────────────────────────────────────────────────

def evaluate(fn, label):
    exits = []
    reasons = defaultdict(int)
    for pd in paths:
        try:
            m, why = fn(pd)
        except Exception as e:
            continue
        exits.append(m)
        # Coarsen trail_NN labels
        key = "trail" if why.startswith("trail") else ("vote" if why.startswith("vote") else why)
        reasons[key] += 1
    n = len(exits)
    if not n: return None
    avg = sum(exits)/n
    wins = sum(1 for x in exits if x > 1.0)
    ge12 = sum(1 for x in exits if x >= 1.2)
    ge15 = sum(1 for x in exits if x >= 1.5)
    ge2  = sum(1 for x in exits if x >= 2.0)
    ge5  = sum(1 for x in exits if x >= 5.0)
    sl   = sum(1 for x in exits if x <= 0.70)
    return {"label": label, "avg": avg, "wins": wins, "ge12": ge12,
            "ge15": ge15, "ge2": ge2, "ge5": ge5, "sl": sl, "n": n,
            "reasons": dict(reasons)}

strategies = [
    ("CURRENT LIVE (baseline)",                  replay_live),
    ("Profit-scaled trail (30→25→20→15→10)",     replay_profit_scaled),
    ("Time-decay trail (40 → 10 over hold)",     replay_time_decay),
    ("3-of-4 voting",                            lambda pd: replay_voting(pd, k=3)),
    ("2-of-4 voting (aggressive)",               lambda pd: replay_voting(pd, k=2)),
    ("Local-max trail (window=10, width=30)",    lambda pd: replay_local_max_trail(pd, 10, 0.30)),
    ("Local-max trail (window=20, width=25)",    lambda pd: replay_local_max_trail(pd, 20, 0.25)),
    ("Cascade (warning → 3-blk confirm)",        replay_cascade),
]

results = [r for r in (evaluate(fn, lbl) for lbl, fn in strategies) if r]
baseline = next(r for r in results if r["label"].startswith("CURRENT"))
b_avg = baseline["avg"]

# Sort by avg desc
results.sort(key=lambda r: -r["avg"])

print(f"\n=== Exit-strategy candidates ({baseline['n']} D tokens, 30-day cache) ===\n", file=sys.stderr)
print(f"  {'strategy':<46} {'avg':>6} {'Δbase':>6} {'wins':>5} {'≥1.5x':>5} {'≥2x':>4} {'≥5x':>4} {'SL':>4}", file=sys.stderr)
for r in results:
    mark = " ← BEAT" if r["avg"] > b_avg + 0.005 else (" ← worse" if r["avg"] < b_avg - 0.005 else "")
    print(f"  {r['label']:<46} {r['avg']:>5.3f}x {r['avg']-b_avg:+6.3f} {r['wins']:>5} {r['ge15']:>5} {r['ge2']:>4} {r['ge5']:>4} {r['sl']:>4}{mark}", file=sys.stderr)

# Net daily PnL at $20 trade, $1.30 gas
print(f"\n=== Net daily PnL @ $20/trade, $1.30 gas, 19 trades/day ===", file=sys.stderr)
print(f"  {'strategy':<46} {'avg':>6} {'daily net':>10}", file=sys.stderr)
for r in results:
    gross = 19 * 20 * (r["avg"] - 1.0)
    net   = gross - 19 * 1.30
    mark = " ← BEAT" if r["avg"] > b_avg + 0.005 else ""
    print(f"  {r['label']:<46} {r['avg']:>5.3f}x  ${net:>+8.2f}/d{mark}", file=sys.stderr)
