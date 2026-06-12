#!/usr/bin/env python3
"""
v2 exit-feature sweep: adds smart-money, holder dynamics, top-holder
concentration, and early-buyer-cohort exit signals on top of v1's
price + flow features.

Baseline = current LIVE trail (arm+30/trail-30/SL-30) = 1.128x
"""
import json, sys
from collections import defaultdict

CACHE = "d_microstructure_v2_paths.json"
N2_OFFSET = 2
ARM_PCT   = 0.30
SL_PCT    = 0.30

with open(CACHE) as f:
    tokens = json.load(f)
print(f"loaded {len(tokens)} tokens", file=sys.stderr)

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
    peak_unrealized = 1.0
    peak_holder_count = 0
    peak_kol_holders  = 0
    peak_top10_share  = 0
    # We'll also need rolling history for window aggregates

    for i, b in enumerate(post):
        d = pb[str(b)]
        price = d["last_price"]
        # ── Position-relative
        unrealized = price / n2_price
        peak_unrealized = max(peak_unrealized, unrealized)
        drawdown_from_peak = (peak_unrealized - unrealized) / peak_unrealized if peak_unrealized > 0 else 0
        blocks_held = b - n2_block

        # ── Price velocity / accel / dist from local max
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

        # ── Flow windows
        def sum_field(field, n):
            return sum(pb[str(post[j])][field] for j in range(max(0, i-n+1), i+1))
        buy_count_3 = sum_field("buyers", 3)
        buy_count_10 = sum_field("buyers", 10)
        sell_count_3 = sum_field("sellers", 3)
        buy_bnb_3 = sum_field("buy_bnb", 3)
        sell_bnb_3 = sum_field("sell_bnb", 3)
        net_flow_3blk = buy_bnb_3 - sell_bnb_3

        buy_velocity_3  = buy_count_3 / 3 if i >= 2 else 0
        buy_velocity_10 = buy_count_10 / 10 if i >= 9 else 0
        buy_velocity_collapse = (buy_velocity_3 / buy_velocity_10) if buy_velocity_10 > 0 else 1.0
        buy_sell_ratio_count = buy_count_3 / max(1, sell_count_3)

        # ── SMART MONEY: KOL flow
        kol_buyers_blk  = len(d["kol_buyers"])
        kol_sellers_blk = len(d["kol_sellers"])
        # Over 3-block window
        kol_buyers_3  = sum(len(pb[str(post[j])]["kol_buyers"])  for j in range(max(0, i-2), i+1))
        kol_sellers_3 = sum(len(pb[str(post[j])]["kol_sellers"]) for j in range(max(0, i-2), i+1))
        kol_buyers_10  = sum(len(pb[str(post[j])]["kol_buyers"])  for j in range(max(0, i-9), i+1))
        kol_sellers_10 = sum(len(pb[str(post[j])]["kol_sellers"]) for j in range(max(0, i-9), i+1))
        kol_net_flow_3blk = kol_buyers_3 - kol_sellers_3  # positive = SM accumulating
        kol_net_flow_10blk = kol_buyers_10 - kol_sellers_10

        # ── HOLDER DYNAMICS
        holder_count = d["holder_count"]
        peak_holder_count = max(peak_holder_count, holder_count)
        # Holder-growth: change in holder count over 3 blocks
        if i >= 3:
            holder_growth_3blk = holder_count - pb[str(post[i-3])]["holder_count"]
        else:
            holder_growth_3blk = holder_count
        if i >= 10:
            holder_growth_10blk = holder_count - pb[str(post[i-10])]["holder_count"]
        else:
            holder_growth_10blk = holder_count
        # Decline from peak (concerning if holders leaving)
        holder_decline_from_peak = (peak_holder_count - holder_count) / peak_holder_count if peak_holder_count > 0 else 0

        # ── SMART-MONEY HOLDERS
        kol_holders_count = d["kol_holders"]
        peak_kol_holders = max(peak_kol_holders, kol_holders_count)
        kol_holders_decline = (peak_kol_holders - kol_holders_count) / max(1, peak_kol_holders)
        if i >= 3:
            kol_holders_delta_3blk = kol_holders_count - pb[str(post[i-3])]["kol_holders"]
        else:
            kol_holders_delta_3blk = 0

        # ── TOP-HOLDER CONCENTRATION
        top10_share = d["top10_share"]
        peak_top10_share = max(peak_top10_share, top10_share)
        if i >= 3:
            top10_delta_3blk = top10_share - pb[str(post[i-3])]["top10_share"]
        else:
            top10_delta_3blk = 0

        # ── EARLY-BUYER COHORT RETENTION
        early_remaining = d["early_remaining"]
        # % of early cohort that has EXITED — higher = more exit pressure
        EARLY_N = 20
        early_exit_rate = 1 - (early_remaining / EARLY_N) if EARLY_N > 0 else 0

        feats.append({
            "block": b,
            "blocks_held": blocks_held,
            "unrealized_mult": unrealized,
            # v1 features
            "drawdown_from_peak":  drawdown_from_peak,
            "vel_3":               vel_3,
            "vel_10":              vel_10,
            "accel":               accel,
            "dist_from_local_max": dist_from_local_max,
            "buy_velocity_collapse": buy_velocity_collapse,
            "buy_sell_ratio_count":  buy_sell_ratio_count,
            "net_flow_3blk":         net_flow_3blk,
            # v2 smart-money
            "kol_buyers_blk":       kol_buyers_blk,
            "kol_sellers_blk":      kol_sellers_blk,
            "kol_net_flow_3blk":    kol_net_flow_3blk,
            "kol_net_flow_10blk":   kol_net_flow_10blk,
            "kol_sellers_3":        kol_sellers_3,
            # v2 holders
            "holder_count":         holder_count,
            "holder_growth_3blk":   holder_growth_3blk,
            "holder_growth_10blk":  holder_growth_10blk,
            "holder_decline_from_peak": holder_decline_from_peak,
            # v2 smart-money holders
            "kol_holders_count":    kol_holders_count,
            "kol_holders_decline":  kol_holders_decline,
            "kol_holders_delta_3blk": kol_holders_delta_3blk,
            # v2 concentration
            "top10_share":          top10_share,
            "top10_delta_3blk":     top10_delta_3blk,
            # v2 cohort
            "early_remaining":      early_remaining,
            "early_exit_rate":      early_exit_rate,
        })
    return {"n2_block": n2_block, "n2_price": n2_price, "features": feats}

print("computing features…", file=sys.stderr)
all_feats = [f for f in (compute_features(t) for t in tokens) if f]
print(f"got {len(all_feats)} tokens", file=sys.stderr)

def signal_fires(fb, feat, op, th):
    v = fb.get(feat)
    if v is None: return False
    return (op == ">" and v > th) or (op == "<" and v < th)

def sim_single(feature, op, threshold):
    exits = []
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - SL_PCT
        armed = False
        chosen = None
        for fb in fts:
            u = fb["unrealized_mult"]
            if u <= sl_floor: chosen = u; break
            if not armed and u >= 1 + ARM_PCT: armed = True
            if armed and signal_fires(fb, feature, op, threshold):
                chosen = u; break
        if chosen is None:
            chosen = fts[-1]["unrealized_mult"] if fts else 1.0
        exits.append(chosen)
    n = len(exits)
    if not n: return 0, 0, 0
    return sum(exits)/n, sum(1 for x in exits if x > 1.0), sum(1 for x in exits if x >= 2.0)

# Baseline (trail)
def sim_trail(arm, trail, sl):
    exits = []
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - sl
        peak = 1.0
        armed = False
        chosen = None
        for fb in fts:
            u = fb["unrealized_mult"]
            peak = max(peak, u)
            if u <= sl_floor: chosen = u; break
            if not armed and peak >= 1 + arm: armed = True
            if armed and u <= peak * (1 - trail):
                chosen = u; break
        if chosen is None:
            chosen = fts[-1]["unrealized_mult"] if fts else 1.0
        exits.append(chosen)
    n = len(exits)
    return sum(exits)/n, sum(1 for x in exits if x > 1.0), sum(1 for x in exits if x >= 2.0)

bavg, bwins, bge2 = sim_trail(0.30, 0.30, 0.30)
print(f"\n=== Baseline (current LIVE): arm+30/trail-30/SL-30 ===", file=sys.stderr)
print(f"  avg={bavg:.3f}x  wins={bwins}/{len(all_feats)}  ≥2x={bge2}", file=sys.stderr)

# ── v2 sweeps ────────────────────────────────────────────────────
V2_SWEEPS = [
    # SMART-MONEY exits
    ("kol_sellers_blk",       ">", [0, 1, 2]),
    ("kol_sellers_3",         ">", [0, 1, 2, 3]),
    ("kol_net_flow_3blk",     "<", [0, -1, -2]),
    ("kol_net_flow_10blk",    "<", [0, -1, -2, -3]),
    # HOLDER dynamics
    ("holder_growth_3blk",    "<", [0, -2, -5, -10]),
    ("holder_growth_10blk",   "<", [0, -5, -10, -20]),
    ("holder_decline_from_peak", ">", [0.05, 0.10, 0.20, 0.30]),
    # SMART-MONEY HOLDERS
    ("kol_holders_decline",   ">", [0.2, 0.4, 0.5, 1.0]),
    ("kol_holders_delta_3blk","<", [0, -1, -2]),
    # CONCENTRATION
    ("top10_share",           ">", [0.5, 0.6, 0.7, 0.8, 0.9]),
    ("top10_delta_3blk",      ">", [0.05, 0.10, 0.20]),
    # COHORT EXITS
    ("early_exit_rate",       ">", [0.20, 0.30, 0.50, 0.70]),
]

print(f"\n=== v2 exit-feature sweep ===", file=sys.stderr)
print(f"  {'feature':<28} {'op':>3} {'thr':>10} {'avg':>7} {'wins':>5} {'≥2x':>5}", file=sys.stderr)
v2_results = []
for feat, op, ths in V2_SWEEPS:
    for th in ths:
        avg, wins, ge2 = sim_single(feat, op, th)
        v2_results.append((feat, op, th, avg, wins, ge2))
        marker = " ←" if avg > bavg else ""
        print(f"  {feat:<28} {op:>3} {th:>10.4g} {avg:>7.3f} {wins:>5} {ge2:>5}{marker}", file=sys.stderr)

# Top 15 by avg
print(f"\n=== TOP 15 v2 features ===", file=sys.stderr)
top = sorted(v2_results, key=lambda r: -r[3])[:15]
for feat, op, th, avg, wins, ge2 in top:
    print(f"  {feat:<28} {op} {th:>10.4g}   avg={avg:.3f}x ({avg-bavg:+.3f})  wins={wins}  ≥2x={ge2}", file=sys.stderr)

# Combination tests: previous v1 winner + v2 candidates
V1_WINNER = [("dist_from_local_max", ">", 0.30), ("vel_10", "<", -0.01)]

def sim_all_of(features_list):
    exits = []
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - SL_PCT
        armed = False
        chosen = None
        for fb in fts:
            u = fb["unrealized_mult"]
            if u <= sl_floor: chosen = u; break
            if not armed and u >= 1 + ARM_PCT: armed = True
            if armed and all(signal_fires(fb, f, o, t) for f, o, t in features_list):
                chosen = u; break
        if chosen is None:
            chosen = fts[-1]["unrealized_mult"] if fts else 1.0
        exits.append(chosen)
    n = len(exits)
    return sum(exits)/n, sum(1 for x in exits if x > 1.0), sum(1 for x in exits if x >= 2.0)

def sim_any_of(features_list):
    exits = []
    for tk in all_feats:
        fts = tk["features"]
        sl_floor = 1 - SL_PCT
        armed = False
        chosen = None
        for fb in fts:
            u = fb["unrealized_mult"]
            if u <= sl_floor: chosen = u; break
            if not armed and u >= 1 + ARM_PCT: armed = True
            if armed and any(signal_fires(fb, f, o, t) for f, o, t in features_list):
                chosen = u; break
        if chosen is None:
            chosen = fts[-1]["unrealized_mult"] if fts else 1.0
        exits.append(chosen)
    n = len(exits)
    return sum(exits)/n, sum(1 for x in exits if x > 1.0), sum(1 for x in exits if x >= 2.0)

# Best v1 alone
avg, w, g = sim_all_of(V1_WINNER)
print(f"\n=== Best v1 combo (recap) ===", file=sys.stderr)
print(f"  ALL: dist_from_local_max>0.30 AND vel_10<-0.01    avg={avg:.3f}x wins={w} ≥2x={g}", file=sys.stderr)

# Add the top v2 features to the AND
print(f"\n=== v1+v2 hybrid ALL-of combinations ===", file=sys.stderr)
top_v2 = sorted([r for r in v2_results if r[3] > bavg], key=lambda r: -r[3])[:6]
for feat, op, th, _, _, _ in top_v2:
    combo = V1_WINNER + [(feat, op, th)]
    avg, w, g = sim_all_of(combo)
    print(f"  + {feat} {op} {th:>6.4g}    avg={avg:.3f}x wins={w} ≥2x={g}", file=sys.stderr)

print(f"\n=== v1+v2 hybrid ANY-of combinations ===", file=sys.stderr)
for feat, op, th, _, _, _ in top_v2:
    combo = V1_WINNER + [(feat, op, th)]
    avg, w, g = sim_any_of(combo)
    print(f"  ANY of v1 AND {feat} {op} {th:>6.4g}   avg={avg:.3f}x wins={w} ≥2x={g}", file=sys.stderr)
