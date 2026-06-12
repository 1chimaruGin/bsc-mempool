#!/usr/bin/env python3
"""
Combined-signal exit sweep on top of the per-block feature library.

After the single-feature sweep identified the top signals, this script
tests:
  1. ANY-of (OR logic): exit when ANY signal fires
  2. ALL-of (AND logic): exit when ALL fire same block
  3. Layered: signal fires → enter a "watching" state → confirm next block

Baseline = arm+30/trail-30/SL-30 = 1.128x avg
"""
import json, sys
from collections import defaultdict

CACHE = "d_microstructure_30day_paths.json"
N2_OFFSET = 2
ARM_PCT = 0.30
SL_PCT  = 0.30

# Top features from the single-feature sweep
TOP_FEATURES = [
    ("dist_from_local_max", ">", 0.30),  # best single: 1.162x
    ("vel_10",              "<", -0.01), # 1.161x
    ("buy_velocity_collapse","<", 0.50), # 1.157x
    ("net_flow_3blk",       "<", -1e18), # 1.154x
    ("vel_3",               "<", -0.05), # 1.151x
    ("accel",               "<", -0.05), # 1.151x
]

with open(CACHE) as f:
    tokens = json.load(f)

# ── Replicate the feature computation from analyze-d-exit-features.py ──
def compute_features(tok):
    pb = tok.get("_per_block") or {}
    if not pb: return None
    blocks = sorted(int(b) for b in pb.keys())
    d_block = tok["d_block"]
    n2_block = next((b for b in blocks if b >= d_block + N2_OFFSET), None)
    if n2_block is None: return None
    n2_price = pb[str(n2_block)]["last_price"]
    if n2_price <= 0: return None

    post = [b for b in blocks if b >= n2_block]
    if not post: return None
    feats = []
    for i, b in enumerate(post):
        d = pb[str(b)]
        price = d["last_price"]
        # Velocity over n blocks
        def vel_over(n):
            if i < n: return 0.0
            past_p = pb[str(post[i-n])]["last_price"]
            if past_p <= 0: return 0.0
            return (price - past_p) / past_p
        vel_3  = vel_over(3) / 3 if i >= 3 else 0.0
        vel_10 = vel_over(10) / 10 if i >= 10 else 0.0
        accel  = vel_3 - vel_10

        recent = post[max(0, i-9):i+1]
        local_max = max(pb[str(rb)]["max_price"] for rb in recent) if recent else price
        dist_from_local_max = (local_max - price) / local_max if local_max > 0 else 0

        buy_count_3   = sum(pb[str(post[j])]["buyers"]  for j in range(max(0, i-2), i+1))
        buy_count_10  = sum(pb[str(post[j])]["buyers"]  for j in range(max(0, i-9), i+1))
        buy_bnb_3     = sum(pb[str(post[j])]["buy_bnb"]  for j in range(max(0, i-2), i+1))
        sell_bnb_3    = sum(pb[str(post[j])]["sell_bnb"] for j in range(max(0, i-2), i+1))

        buy_vel_3  = buy_count_3 / 3 if i >= 2 else 0
        buy_vel_10 = buy_count_10 / 10 if i >= 9 else 0
        buy_velocity_collapse = (buy_vel_3 / buy_vel_10) if buy_vel_10 > 0 else 1.0
        net_flow_3blk = buy_bnb_3 - sell_bnb_3

        feats.append({
            "block":   b,
            "price":   price,
            "unrealized_mult": price / n2_price,
            "vel_3":   vel_3,
            "vel_10":  vel_10,
            "accel":   accel,
            "dist_from_local_max":  dist_from_local_max,
            "buy_velocity_collapse": buy_velocity_collapse,
            "net_flow_3blk":         net_flow_3blk,
        })
    return {"n2_block": n2_block, "n2_price": n2_price, "features": feats}

print("computing features…", file=sys.stderr)
all_feats = [f for f in (compute_features(t) for t in tokens) if f]
print(f"got {len(all_feats)} tokens", file=sys.stderr)

# ── Simulators ──────────────────────────────────────────────────────

def signal_fires(fb, feat, op, th):
    v = fb.get(feat)
    if v is None: return False
    return (op == ">" and v > th) or (op == "<" and v < th)

def sim_any_of(features_list):
    """Exit when ANY listed signal fires (after armed)."""
    exits = []; reasons = defaultdict(int)
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - SL_PCT
        armed = False
        chosen, why = None, None
        for fb in fts:
            u = fb["unrealized_mult"]
            if u <= sl_floor: chosen, why = u, "hard_sl"; break
            if not armed and u >= 1 + ARM_PCT: armed = True
            if armed:
                for feat, op, th in features_list:
                    if signal_fires(fb, feat, op, th):
                        chosen, why = u, feat; break
                if chosen is not None: break
        if chosen is None:
            chosen, why = fts[-1]["unrealized_mult"] if fts else 1.0, "timeout"
        exits.append(chosen)
        reasons[why] += 1
    avg = sum(exits)/len(exits)
    wins = sum(1 for x in exits if x > 1.0)
    ge2  = sum(1 for x in exits if x >= 2.0)
    return avg, wins, ge2, reasons

def sim_all_of(features_list):
    """Exit when ALL listed signals fire on same block (after armed)."""
    exits = []; reasons = defaultdict(int)
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - SL_PCT
        armed = False
        chosen, why = None, None
        for fb in fts:
            u = fb["unrealized_mult"]
            if u <= sl_floor: chosen, why = u, "hard_sl"; break
            if not armed and u >= 1 + ARM_PCT: armed = True
            if armed and all(signal_fires(fb, f, o, t) for f, o, t in features_list):
                chosen, why = u, "all_signals"; break
        if chosen is None:
            chosen, why = fts[-1]["unrealized_mult"] if fts else 1.0, "timeout"
        exits.append(chosen)
        reasons[why] += 1
    avg = sum(exits)/len(exits)
    wins = sum(1 for x in exits if x > 1.0)
    ge2  = sum(1 for x in exits if x >= 2.0)
    return avg, wins, ge2, reasons

def sim_n_of_m(features_list, n):
    """Exit when ≥n of the m listed signals fire on same block (after armed)."""
    exits = []; reasons = defaultdict(int)
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - SL_PCT
        armed = False
        chosen, why = None, None
        for fb in fts:
            u = fb["unrealized_mult"]
            if u <= sl_floor: chosen, why = u, "hard_sl"; break
            if not armed and u >= 1 + ARM_PCT: armed = True
            if armed:
                hits = sum(1 for f, o, t in features_list if signal_fires(fb, f, o, t))
                if hits >= n:
                    chosen, why = u, f"{n}_of_{len(features_list)}"; break
        if chosen is None:
            chosen, why = fts[-1]["unrealized_mult"] if fts else 1.0, "timeout"
        exits.append(chosen)
        reasons[why] += 1
    avg = sum(exits)/len(exits)
    wins = sum(1 for x in exits if x > 1.0)
    ge2  = sum(1 for x in exits if x >= 2.0)
    return avg, wins, ge2, reasons

# ── Test combinations ─────────────────────────────────────────────
print(f"\n=== Baseline (current LIVE trail) ===", file=sys.stderr)
print(f"  arm+30/trail-30/SL-30: 1.128x, 239 wins, 44 ≥2x", file=sys.stderr)

print(f"\n=== ANY-of-N (OR logic; aggressive — fires on weakest signal) ===", file=sys.stderr)
print(f"  {'combo':<70} {'avg':>7} {'wins':>5} {'≥2x':>5}", file=sys.stderr)
for combo in [
    [TOP_FEATURES[0]],
    [TOP_FEATURES[0], TOP_FEATURES[1]],
    [TOP_FEATURES[0], TOP_FEATURES[1], TOP_FEATURES[2]],
    [TOP_FEATURES[0], TOP_FEATURES[1], TOP_FEATURES[2], TOP_FEATURES[3]],
    TOP_FEATURES,
]:
    avg, wins, ge2, _ = sim_any_of(combo)
    label = " + ".join(f"{f[0]} {f[1]} {f[2]:g}" for f in combo)[:65]
    print(f"  {label:<70} {avg:>6.3f}x {wins:>5} {ge2:>5}", file=sys.stderr)

print(f"\n=== ALL-of-N (AND logic; conservative — needs all to fire same block) ===", file=sys.stderr)
for combo in [
    TOP_FEATURES[:2],
    TOP_FEATURES[:3],
    TOP_FEATURES[:4],
    TOP_FEATURES,
]:
    avg, wins, ge2, _ = sim_all_of(combo)
    label = " + ".join(f"{f[0]} {f[1]} {f[2]:g}" for f in combo)[:65]
    print(f"  {label:<70} {avg:>6.3f}x {wins:>5} {ge2:>5}", file=sys.stderr)

print(f"\n=== N-of-M (majority voting) ===", file=sys.stderr)
for n in [2, 3, 4]:
    avg, wins, ge2, _ = sim_n_of_m(TOP_FEATURES, n)
    print(f"  ≥{n} of {len(TOP_FEATURES)} top signals     {avg:>6.3f}x {wins:>5} {ge2:>5}", file=sys.stderr)

print(f"\n=== Hybrid: ANY-leading-signal OR trail-30% fallback ===", file=sys.stderr)
# This combines: arm+30, then if any signal fires → exit; else trail-30%
def sim_hybrid_trail(features_list, trail_pct):
    exits = []
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - SL_PCT
        armed = False
        peak = 1.0
        chosen = None
        for fb in fts:
            u = fb["unrealized_mult"]
            peak = max(peak, u)
            if u <= sl_floor: chosen = u; break
            if not armed and peak >= 1 + ARM_PCT: armed = True
            if armed:
                # Leading-signal pre-exit
                if any(signal_fires(fb, f, o, t) for f, o, t in features_list):
                    chosen = u; break
                # Fallback trail
                if u <= peak * (1 - trail_pct):
                    chosen = u; break
        if chosen is None:
            chosen = fts[-1]["unrealized_mult"] if fts else 1.0
        exits.append(chosen)
    avg = sum(exits)/len(exits)
    wins = sum(1 for x in exits if x > 1.0)
    ge2  = sum(1 for x in exits if x >= 2.0)
    return avg, wins, ge2

for trail in [0.30, 0.40, 0.50]:
    avg, wins, ge2 = sim_hybrid_trail(TOP_FEATURES[:3], trail)
    print(f"  ANY of top 3 + trail-{int(trail*100)}%   {avg:>6.3f}x {wins:>5} {ge2:>5}", file=sys.stderr)

for trail in [0.30, 0.40, 0.50]:
    avg, wins, ge2 = sim_hybrid_trail(TOP_FEATURES, trail)
    print(f"  ANY of top 6 + trail-{int(trail*100)}%   {avg:>6.3f}x {wins:>5} {ge2:>5}", file=sys.stderr)
