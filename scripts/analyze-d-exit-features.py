#!/usr/bin/env python3
"""
Exit-feature library + exhaustive exit-rule sweep on D's 30-day tape.

Loads `d_microstructure_30day_paths.json` (584 tokens, per-block buyer/
seller/BNB aggregates) and computes for each token a per-block feature
vector covering:

  POSITION-RELATIVE:
    unrealized_mult     : last_price / n2_price
    blocks_held         : current_block - n2_block
    mfe                 : max unrealized so far
    drawdown_from_peak  : (mfe - unrealized) / mfe   ← MFE capture

  PRICE / CURVE DYNAMICS:
    price_vel_1         : (price_t - price_{t-1}) / price_{t-1}
    price_vel_3, _10    : window-mean price returns
    price_accel         : vel_3 - vel_10
    dist_from_local_max : (max_recent_10 - last) / max_recent_10
    cumulative_net_bnb  : Σ(buy_bnb − sell_bnb) up to t  (curve growth proxy)

  FLOW (the core leading-signal family):
    buy_count, sell_count
    buy_bnb, sell_bnb
    net_flow_bnb
    buy_sell_ratio_count, _bnb
    buy_velocity_3, _10  : avg buys per block over window
    buy_velocity_collapse: buy_vel_3 / buy_vel_10  (<1 = momentum dying)
    sell_pressure_3blk   : sell_bnb_3blk / max(1, buy_bnb_3blk)

Then runs an exhaustive sweep: for each (feature, direction, threshold),
simulate "exit when feature crosses threshold (after armed at +30%)" and
report avg_exit_mult vs the current trail.

Output:
  d_features_per_block.csv   : flat per-block features (for further analysis)
  stdout                     : top features × thresholds ranked by avg_exit
"""
import json, sys, statistics
from collections import defaultdict

CACHE = "d_microstructure_30day_paths.json"
N2_OFFSET = 2  # we enter at D-block + 2

# ── Load cache ──────────────────────────────────────────────────────
with open(CACHE) as f:
    tokens = json.load(f)
print(f"loaded {len(tokens)} tokens", file=sys.stderr)

# ── Per-block feature computation ───────────────────────────────────

def compute_features(tok):
    """
    Given a token dict from the cache, return:
      blocks_sorted : list of block numbers (sorted ascending) ≥ d_block
      features      : list of dicts (one per block) with all features below
      n2_price      : the entry price we'd have paid (block ≥ d_block+2 first observed)
    """
    pb = tok.get("_per_block") or {}
    if not pb: return None
    blocks = sorted(int(b) for b in pb.keys())
    d_block = tok["d_block"]

    # Establish N+2 entry
    n2_block = None
    n2_price = None
    for b in blocks:
        if b >= d_block + N2_OFFSET:
            n2_block = b
            n2_price = pb[str(b)]["last_price"]
            break
    if n2_price is None or n2_price <= 0:
        return None

    # Subset to blocks ≥ n2_block (our holding window)
    post = [b for b in blocks if b >= n2_block]
    if not post: return None

    feats = []
    cum_buy_bnb  = 0
    cum_sell_bnb = 0
    cum_net_bnb  = 0
    mfe          = 1.0
    peak_price   = n2_price

    # Rolling window history (3 and 10 blocks)
    window_history = []   # list of (block, dict_with_block_data)

    for i, b in enumerate(post):
        d = pb[str(b)]
        price       = d["last_price"]
        buy_bnb_b   = d["buy_bnb"]
        sell_bnb_b  = d["sell_bnb"]
        buyers      = d["buyers"]
        sellers     = d["sellers"]

        cum_buy_bnb  += buy_bnb_b
        cum_sell_bnb += sell_bnb_b
        cum_net_bnb   = cum_buy_bnb - cum_sell_bnb
        unrealized   = price / n2_price
        mfe          = max(mfe, unrealized)
        drawdown     = (mfe - unrealized) / mfe if mfe > 0 else 0
        blocks_held  = b - n2_block
        peak_price   = max(peak_price, price)

        # Price velocity windows
        def vel_over(n):
            if i < n: return 0.0
            past_p = pb[str(post[i-n])]["last_price"]
            if past_p <= 0: return 0.0
            return (price - past_p) / past_p

        vel_1  = vel_over(1)
        vel_3  = vel_over(3) / 3 if i >= 3 else 0.0
        vel_10 = vel_over(10) / 10 if i >= 10 else 0.0
        accel  = vel_3 - vel_10

        # Distance from local-max (last 10 blocks)
        recent = post[max(0, i-9):i+1]
        local_max = max(pb[str(rb)]["max_price"] for rb in recent) if recent else price
        dist_from_local_max = (local_max - price) / local_max if local_max > 0 else 0

        # Flow features over 3 and 10 blocks
        def sum_over(field, n):
            return sum(pb[str(post[j])][field] for j in range(max(0, i-n+1), i+1))

        buys_3   = sum_over("buyers" if False else "buyers", 3)  # buyers is count
        # NB: "buyers" / "sellers" in cache are already INTEGER counts (we stored len(set))
        buy_count_3   = sum(pb[str(post[j])]["buyers"]  for j in range(max(0, i-2), i+1))
        sell_count_3  = sum(pb[str(post[j])]["sellers"] for j in range(max(0, i-2), i+1))
        buy_count_10  = sum(pb[str(post[j])]["buyers"]  for j in range(max(0, i-9), i+1))
        sell_count_10 = sum(pb[str(post[j])]["sellers"] for j in range(max(0, i-9), i+1))
        buy_bnb_3     = sum(pb[str(post[j])]["buy_bnb"]  for j in range(max(0, i-2), i+1))
        sell_bnb_3    = sum(pb[str(post[j])]["sell_bnb"] for j in range(max(0, i-2), i+1))

        buy_velocity_3  = buy_count_3 / 3 if i >= 2 else 0
        buy_velocity_10 = buy_count_10 / 10 if i >= 9 else 0
        buy_velocity_collapse = (buy_velocity_3 / buy_velocity_10) if buy_velocity_10 > 0 else 1.0

        buy_sell_ratio_count = buy_count_3 / max(1, sell_count_3)
        buy_sell_ratio_bnb   = buy_bnb_3   / max(1, sell_bnb_3)
        sell_pressure_3      = sell_bnb_3 / max(1, buy_bnb_3)
        net_flow_3blk        = buy_bnb_3 - sell_bnb_3

        feats.append({
            "block":           b,
            "blocks_held":     blocks_held,
            "price":           price,
            "unrealized_mult": unrealized,
            "mfe":             mfe,
            "drawdown_from_peak": drawdown,
            "vel_1":           vel_1,
            "vel_3":           vel_3,
            "vel_10":          vel_10,
            "accel":           accel,
            "dist_from_local_max": dist_from_local_max,
            "cum_buy_bnb":     cum_buy_bnb,
            "cum_sell_bnb":    cum_sell_bnb,
            "cum_net_bnb":     cum_net_bnb,
            "buy_count":       d["buyers"],
            "sell_count":      d["sellers"],
            "buy_bnb_blk":     buy_bnb_b,
            "sell_bnb_blk":    sell_bnb_b,
            "buy_count_3":     buy_count_3,
            "sell_count_3":    sell_count_3,
            "buy_velocity_3":  buy_velocity_3,
            "buy_velocity_10": buy_velocity_10,
            "buy_velocity_collapse": buy_velocity_collapse,
            "buy_sell_ratio_count":  buy_sell_ratio_count,
            "buy_sell_ratio_bnb":    buy_sell_ratio_bnb,
            "sell_pressure_3":       sell_pressure_3,
            "net_flow_3blk":         net_flow_3blk,
        })

    return {"n2_block": n2_block, "n2_price": n2_price, "features": feats}

# ── Exhaustive exit-rule sweep ──────────────────────────────────────
#
# For each (feature_name, comparison, threshold) tuple, simulate:
#   - We arm at +30% from N+2 (matches current live trail arm_pct)
#   - HARD SL at -30%
#   - EXIT when feature crosses threshold (only after armed)
#   - Otherwise hold to timeout (last observed price)
#
# Returns avg_exit_mult, wins, ≥2x for the population of 584 tokens.

ARM_PCT = 0.30
SL_PCT  = 0.30

def simulate_exit(token_feats, feature, op, threshold):
    """Returns (exit_mult, reason). None if no entry."""
    if not token_feats: return None, None
    fts = token_feats["features"]
    sl_floor = 1 - SL_PCT
    armed = False
    for fb in fts:
        u = fb["unrealized_mult"]
        if u <= sl_floor:
            return u, "hard_sl"
        if not armed and u >= 1 + ARM_PCT:
            armed = True
        if armed:
            v = fb.get(feature)
            if v is None: continue
            if op == ">" and v > threshold:
                return u, "signal"
            if op == "<" and v < threshold:
                return u, "signal"
    # Timeout: exit at last observed
    last = fts[-1]["unrealized_mult"] if fts else 1.0
    return last, "timeout"

def sweep_feature(all_token_feats, feature, op, thresholds):
    """Sweep over thresholds, return list of (threshold, avg, wins, ge2)."""
    rows = []
    for th in thresholds:
        exits = []
        for tk in all_token_feats:
            m, _why = simulate_exit(tk, feature, op, th)
            if m is not None: exits.append(m)
        if not exits: continue
        avg = sum(exits) / len(exits)
        wins = sum(1 for x in exits if x > 1.0)
        ge2  = sum(1 for x in exits if x >= 2.0)
        rows.append((th, avg, wins, ge2, len(exits)))
    return rows

# Compute features for every token (skipping any that fail)
print("computing features…", file=sys.stderr)
all_feats = []
for tk in tokens:
    f = compute_features(tk)
    if f: all_feats.append(f)
print(f"got features for {len(all_feats)} tokens", file=sys.stderr)

# Baseline = current live trail config
def simulate_trail(arm, trail, sl):
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
            if not armed and peak >= 1 + arm: armed = True
            if u <= sl_floor:
                chosen = u; break
            if armed and u <= peak * (1 - trail):
                chosen = u; break
        if chosen is None:
            chosen = fts[-1]["unrealized_mult"] if fts else 1.0
        exits.append(chosen)
    avg = sum(exits) / len(exits)
    wins = sum(1 for x in exits if x > 1.0)
    ge2  = sum(1 for x in exits if x >= 2.0)
    return avg, wins, ge2

bavg, bwins, bge2 = simulate_trail(0.30, 0.30, 0.30)
print(f"\n=== Baseline: arm+30/trail-30/SL-30 (current LIVE) ===", file=sys.stderr)
print(f"  avg={bavg:.3f}x  wins={bwins}/{len(all_feats)}  ≥2x={bge2}", file=sys.stderr)

# Feature sweeps
SWEEPS = [
    # (feature_name, op, [thresholds])
    ("drawdown_from_peak", ">", [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]),
    ("vel_1",              "<", [-0.05, -0.10, -0.15, -0.20, -0.30, -0.50]),
    ("vel_3",              "<", [-0.01, -0.02, -0.05, -0.10, -0.15]),
    ("vel_10",             "<", [-0.005, -0.01, -0.02, -0.05]),
    ("accel",              "<", [-0.01, -0.02, -0.05, -0.10]),
    ("dist_from_local_max", ">", [0.05, 0.10, 0.15, 0.20, 0.30, 0.40]),
    ("buy_velocity_collapse", "<", [0.20, 0.30, 0.50, 0.70, 0.80, 0.90]),
    ("buy_sell_ratio_count", "<", [0.20, 0.30, 0.50, 0.70, 1.00]),
    ("buy_sell_ratio_bnb",  "<", [0.20, 0.30, 0.50, 0.70, 1.00]),
    ("sell_pressure_3",    ">", [1.0, 1.5, 2.0, 3.0, 5.0]),
    ("net_flow_3blk",      "<", [-1e16, -5e16, -1e17, -5e17, -1e18, -3e18]),  # negative wei
    ("buy_velocity_3",     "<", [0.5, 1.0, 2.0, 3.0]),
]

print(f"\n=== Exit-rule sweep (584 tokens) — looking for avg > baseline {bavg:.3f}x ===", file=sys.stderr)
print(f"  {'feature':<25} {'op':>3} {'thr':>10} {'avg':>7} {'wins':>6} {'≥2x':>5}", file=sys.stderr)

all_results = []
for feat, op, ths in SWEEPS:
    for th in ths:
        results = sweep_feature(all_feats, feat, op, [th])
        if not results: continue
        _, avg, wins, ge2, n = results[0]
        all_results.append((feat, op, th, avg, wins, ge2))
        marker = " ←" if avg > bavg else ""
        print(f"  {feat:<25} {op:>3} {th:>10.4g} {avg:>7.3f} {wins:>5}/{n} {ge2:>5}{marker}", file=sys.stderr)

# Top 10 by avg
print(f"\n=== TOP 10 features ranked by avg_exit ===", file=sys.stderr)
top = sorted(all_results, key=lambda r: -r[3])[:10]
for feat, op, th, avg, wins, ge2 in top:
    delta = avg - bavg
    print(f"  {feat:<25} {op} {th:>10.4g}   avg={avg:.3f}x ({delta:+.3f})  wins={wins}  ≥2x={ge2}", file=sys.stderr)
