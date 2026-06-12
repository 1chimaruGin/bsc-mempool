#!/usr/bin/env python3
"""
Backtest the "dynamic exit playbook":
  - Layer 2: ATR/Chandelier trail with regime-switching multiplier
  - Layer 3: Partial-exit ladder (30% @+50, 30% @+100, 20% @+200, 20% trail)
  - Combined: ladder + ATR trail on the residual

Also compares:
  - PUBLIC entry  = price at D's block + 2  (= our current mempool-N+1 entry)
  - PRIVATE entry = price at D's block + 3  (=one extra block of slippage drift)
    Approximates the private-confirmed flow where we can only react to
    kol_confirm (the block AFTER D mines).

Layer 1 (mempool signals) is NOT backtestable from historical TradeBuy
data — flagged as live-only in the feasibility report.

Run:
  python3 scripts/analyze-dynamic-exit.py
"""
import json, sys, math
from collections import defaultdict

CACHE = "d_microstructure_v2_paths.json"
ARM_PCT = 0.30
SL_PCT  = 0.30
BE_AT   = 0.15
BE_LOCK = 0.05

with open(CACHE) as f:
    tokens = json.load(f)
print(f"loaded {len(tokens)} tokens", file=sys.stderr)

# ── Build paths ────────────────────────────────────────────────────

def build_path(tok, entry_offset):
    """Build (n_price, [(block, max_price, min_price, last_price)]) from
    D's block + entry_offset onward. min_price defaults to last_price
    when not recorded — for a block with only one trade, the bar is a
    point."""
    pb = tok.get("_per_block") or {}
    if not pb: return None
    blocks = sorted(int(b) for b in pb.keys())
    d_block = tok["d_block"]
    n_block = next((b for b in blocks if b >= d_block + entry_offset), None)
    if n_block is None: return None
    n_price = pb[str(n_block)]["last_price"]
    if n_price <= 0: return None
    bars = []
    for b in blocks:
        if b < n_block: continue
        d = pb[str(b)]
        last = d["last_price"]
        high = max(d.get("max_price", last), last)
        # No explicit min recorded; approximate with last (conservative — single-tick bar)
        low  = last
        if low <= 0: continue
        bars.append((b, high, low, last))
    if not bars: return None
    return {"n_price": n_price, "bars": bars, "token": tok["token"], "d_block": d_block}

pub_paths = [p for p in (build_path(t, 2) for t in tokens) if p]
prv_paths = [p for p in (build_path(t, 3) for t in tokens) if p]
print(f"public entries (D+2): {len(pub_paths)}", file=sys.stderr)
print(f"private entries (D+3): {len(prv_paths)}", file=sys.stderr)

# ── Exit strategies ────────────────────────────────────────────────

def replay_current_live(path, n2):
    """arm+30/trail-30/SL-30 + ratchet @+15/lock+5%."""
    sl_floor = n2 * (1 - SL_PCT)
    lock_floor = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    for blk, hi, lo, last in path["bars"]:
        p = last
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        effective = max(sl_floor, lock_floor) if ratcheted else sl_floor
        if p <= effective:
            return p/n2, ("be_locked" if ratcheted and lock_floor > sl_floor else "hard_sl")
        if armed and p <= peak*(1 - 0.30):
            return p/n2, "trail"
    return path["bars"][-1][3]/n2, "timeout"

def replay_atr_chandelier(path, n2, atr_n=10, smooth_k=3):
    """
    Chandelier stop = peak - ATR(atr_n) × regime_multiplier
    Regime determined by short/long velocity:
      - PARABOLIC: vel_3 > 0.05 AND vel_10 > 0.02  → 5× ATR (wide)
      - DECAY:     vel_3 < 0 AND vel_10 > 0        → 2× ATR
      - DIST:      vel_3 < 0 AND vel_10 < 0        → 1.5× ATR (tight)
      - NEUTRAL                                    → 3× ATR
    Trigger: median(last k last-prices) <= chandelier_floor (smoothed)
    HARD SL still applies at entry × (1 - SL_PCT).
    """
    sl_floor = n2 * (1 - SL_PCT)
    peak = n2
    bars = path["bars"]
    last_prices = []
    tr_window  = []   # true-range window for ATR
    closes     = []   # closes for velocity
    for i, (blk, hi, lo, last) in enumerate(bars):
        if last > peak: peak = last
        last_prices.append(last)
        # True range (use prev close if available)
        prev_close = closes[-1] if closes else last
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        tr_window.append(tr)
        closes.append(last)
        if last <= sl_floor: return last/n2, "hard_sl"
        if i < atr_n: continue  # need ATR window first
        atr = sum(tr_window[-atr_n:]) / atr_n
        # Velocity
        if len(closes) >= 4:
            vel_3 = (closes[-1] - closes[-4]) / closes[-4] / 3
        else:
            vel_3 = 0
        if len(closes) >= 11:
            vel_10 = (closes[-1] - closes[-11]) / closes[-11] / 10
        else:
            vel_10 = 0
        # Regime
        if vel_3 > 0.05 and vel_10 > 0.02:   mult = 5.0
        elif vel_3 < 0 and vel_10 > 0:        mult = 2.0
        elif vel_3 < 0 and vel_10 < 0:        mult = 1.5
        else:                                  mult = 3.0
        chand_floor = peak - atr * mult
        # Smoothed last (median of last k)
        recent = last_prices[-smooth_k:] if len(last_prices) >= smooth_k else last_prices
        smoothed = sorted(recent)[len(recent)//2]
        if smoothed <= chand_floor:
            return last/n2, f"chand_x{mult:.1f}"
    return bars[-1][3]/n2, "timeout"

def replay_partial_ladder(path, n2, ladder=None, trail_pct=0.30):
    """
    Partial-exit ladder.
    Default: 30% @+50%, 30% @+100%, 20% @+200%, 20% trailing.
    HARD SL at -30% always.
    """
    if ladder is None:
        ladder = [(0.50, 0.30), (1.00, 0.30), (2.00, 0.20)]
        # 20% remains, trail at -trail_pct from peak
    sl_floor = n2 * (1 - SL_PCT)
    remaining = 1.0
    realised  = 0.0
    hit = [False] * len(ladder)
    peak = n2
    for blk, hi, lo, last in path["bars"]:
        p = last
        if p > peak: peak = p
        if p <= sl_floor:
            realised += remaining * (p/n2)
            return realised, "hard_sl"
        for i, (tp, frac) in enumerate(ladder):
            if hit[i]: continue
            if p >= n2*(1+tp):
                take = min(frac, remaining)
                realised  += take * (p/n2)
                remaining -= take
                hit[i] = True
        if remaining > 0.001 and p <= peak * (1 - trail_pct):
            realised += remaining * (p/n2)
            return realised, "trail"
        if remaining <= 0.001:
            return realised, "tp_full"
    realised += remaining * (path["bars"][-1][3]/n2)
    return realised, "timeout"

def replay_hybrid_ladder_atr(path, n2):
    """
    Partial ladder front-end + ATR/Chandelier trail on the residual.
    Locks early cost-basis recovery, then runs the residual on the
    volatility-scaled trail.
    """
    sl_floor = n2 * (1 - SL_PCT)
    ladder = [(0.50, 0.30), (1.00, 0.30), (2.00, 0.20)]
    remaining = 1.0
    realised  = 0.0
    hit = [False] * len(ladder)
    peak = n2
    bars = path["bars"]
    closes = []
    tr_win = []
    for i, (blk, hi, lo, last) in enumerate(bars):
        if last > peak: peak = last
        prev_close = closes[-1] if closes else last
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        tr_win.append(tr); closes.append(last)
        if last <= sl_floor:
            realised += remaining * (last/n2)
            return realised, "hard_sl"
        for j, (tp, frac) in enumerate(ladder):
            if hit[j]: continue
            if last >= n2*(1+tp):
                take = min(frac, remaining)
                realised  += take * (last/n2)
                remaining -= take
                hit[j] = True
        if remaining <= 0.001: return realised, "tp_full"
        # ATR trail on residual
        if i >= 10:
            atr = sum(tr_win[-10:]) / 10
            vel_3  = (closes[-1] - closes[-4])  / closes[-4]  / 3 if len(closes) >= 4 else 0
            vel_10 = (closes[-1] - closes[-11]) / closes[-11] / 10 if len(closes) >= 11 else 0
            if vel_3 > 0.05 and vel_10 > 0.02:   mult = 5.0
            elif vel_3 < 0 and vel_10 > 0:        mult = 2.0
            elif vel_3 < 0 and vel_10 < 0:        mult = 1.5
            else:                                  mult = 3.0
            chand_floor = peak - atr * mult
            if last <= chand_floor:
                realised += remaining * (last/n2)
                return realised, f"chand_x{mult:.1f}"
    realised += remaining * (bars[-1][3]/n2)
    return realised, "timeout"

# ── Evaluator ──────────────────────────────────────────────────────

def evaluate(paths, fn, label):
    exits = []
    reasons = defaultdict(int)
    for p in paths:
        m, why = fn(p, p["n_price"])
        exits.append(m)
        # Coarsen chand reasons
        if isinstance(why, str) and why.startswith("chand_"):
            reasons["chand"] += 1
        else:
            reasons[why] += 1
    n = len(exits)
    avg  = sum(exits) / n
    wins = sum(1 for x in exits if x > 1.0)
    ge12 = sum(1 for x in exits if x >= 1.2)
    ge15 = sum(1 for x in exits if x >= 1.5)
    ge2  = sum(1 for x in exits if x >= 2.0)
    ge5  = sum(1 for x in exits if x >= 5.0)
    sl   = sum(1 for x in exits if x <= 0.70)
    return {"label": label, "avg": avg, "wins": wins, "ge12": ge12,
            "ge15": ge15, "ge2": ge2, "ge5": ge5, "sl": sl,
            "reasons": dict(reasons), "n": n}

strategies = [
    ("CURRENT LIVE (ratchet+trail)", replay_current_live),
    ("ATR/Chandelier (regime-mult)", replay_atr_chandelier),
    ("Ladder 30/30/20/20 + trail-30", replay_partial_ladder),
    ("Hybrid: ladder + ATR-chand residual", replay_hybrid_ladder_atr),
]

def report(paths, name):
    print(f"\n=== {name} ({len(paths)} tokens) ===", file=sys.stderr)
    print(f"  {'strategy':<42} {'avg':>6} {'wins':>5} {'≥1.5x':>5} {'≥2x':>4} {'≥5x':>4} {'SL':>5}", file=sys.stderr)
    rows = []
    for label, fn in strategies:
        r = evaluate(paths, fn, label)
        rows.append(r)
        print(f"  {label:<42} {r['avg']:>5.3f}x {r['wins']:>5} {r['ge15']:>5} {r['ge2']:>4} {r['ge5']:>4} {r['sl']:>5}", file=sys.stderr)
    return rows

pub = report(pub_paths, "PUBLIC entry path (D+2)")
prv = report(prv_paths, "PRIVATE entry path (D+3 — extra block of slippage)")

# Side-by-side delta
print(f"\n=== PUBLIC vs PRIVATE Δ avg_exit ===", file=sys.stderr)
print(f"  {'strategy':<42} {'pub':>6} {'priv':>6} {'Δ':>6}", file=sys.stderr)
for p, q in zip(pub, prv):
    d = q["avg"] - p["avg"]
    print(f"  {p['label']:<42} {p['avg']:>5.3f}x {q['avg']:>5.3f}x {d:+6.3f}", file=sys.stderr)

# Net PnL math at $20/trade, gas $1.30 round-trip
print(f"\n=== Net daily expected PnL @ $20/trade, $1.30 gas, 19 trades/day ===", file=sys.stderr)
print(f"  {'strategy':<42} {'pub avg':>7} {'pub net':>8} {'priv avg':>8} {'priv net':>8}", file=sys.stderr)
for p, q in zip(pub, prv):
    pub_gross  = 19 * 20 * (p["avg"] - 1.0)
    pub_net    = pub_gross - 19 * 1.30
    priv_gross = 19 * 20 * (q["avg"] - 1.0)
    priv_net   = priv_gross - 19 * 1.30
    print(f"  {p['label']:<42} {p['avg']:>6.3f}x ${pub_net:>+7.2f}/d {q['avg']:>7.3f}x ${priv_net:>+7.2f}/d", file=sys.stderr)
