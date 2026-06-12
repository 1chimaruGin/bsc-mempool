#!/usr/bin/env python3
"""
Backtest a PROGRESSIVE TRAIL — width scales with peak magnitude.

Hypothesis: today's two notable trades (0x947af604 peaked +73%, exited
at -1%; 0x547f945a peaked +42%, exited at +4%) both leaked because the
fixed -30% trail is too wide for sub-2x peaks. A tighter trail at higher
peaks locks more gain WITHOUT compromising small-pump protection
(handled by the break-even ratchet).

Tested progressive ladders (peak_threshold → trail_pct):
  (15%, 0.30) → arm + ratchet protects small pumps
  (30%, 0.25)
  (50%, 0.20)
  (100%, 0.15)
  (200%, 0.10)
"""
import json, sys
from collections import defaultdict

CACHE   = "d_microstructure_v2_paths.json"
N2_OFFSET = 2
ARM_PCT = 0.30
SL_PCT  = 0.30
BE_AT   = 0.15
BE_LOCK = 0.05

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
    path = [(b, pb[str(b)]["last_price"]) for b in blocks if b >= n2_block]
    return {"n2": n2_price, "path": path}

paths = [p for p in (build_path(t) for t in tokens) if p]
print(f"loaded {len(paths)} paths", file=sys.stderr)

def trail_pct_for_peak(peak_ratio, ladder):
    """Given peak / entry, return the trail % from the ladder.
       Ladder = list of (peak_threshold, trail_pct), ascending peak."""
    width = ladder[0][1]  # default = first entry
    for thr, w in ladder:
        if peak_ratio >= 1.0 + thr:
            width = w
    return width

def replay_progressive(path, n2, ladder, arm=ARM_PCT, sl=SL_PCT, be_at=BE_AT, be_lock=BE_LOCK):
    sl_floor   = n2 * (1 - sl)
    lock_floor = n2 * (1 + be_lock)
    armed = False; ratcheted = False
    peak = n2
    for blk, p in path["path"]:
        if p > peak: peak = p
        if not armed and peak >= n2*(1+arm): armed = True
        if not ratcheted and peak >= n2*(1+be_at): ratcheted = True
        effective_sl = max(sl_floor, lock_floor) if ratcheted else sl_floor
        if p <= effective_sl:
            why = "be_locked" if (ratcheted and lock_floor > sl_floor) else "hard_sl"
            return p/n2, why
        if armed:
            trail_w = trail_pct_for_peak(peak/n2, ladder)
            if p <= peak * (1 - trail_w):
                return p/n2, f"trail_{int(trail_w*100)}"
    return path["path"][-1][1]/n2, "timeout"

def replay_current_live(path, n2):
    """arm+30/trail-30/SL-30 + ratchet @+15/lock+5%."""
    return replay_progressive(path, n2, [(0.0, 0.30)])

def evaluate(fn, label, *args):
    exits = []; reasons = defaultdict(int)
    for p in paths:
        m, why = fn(p, p["n2"], *args)
        exits.append(m)
        # Coarsen
        key = why if not why.startswith("trail_") else "trail"
        reasons[key] += 1
    n = len(exits)
    avg = sum(exits)/n
    wins  = sum(1 for x in exits if x > 1.0)
    ge12  = sum(1 for x in exits if x >= 1.2)
    ge15  = sum(1 for x in exits if x >= 1.5)
    ge2   = sum(1 for x in exits if x >= 2.0)
    ge5   = sum(1 for x in exits if x >= 5.0)
    sl    = sum(1 for x in exits if x <= 0.70)
    return {"label": label, "avg": avg, "wins": wins, "ge12": ge12,
            "ge15": ge15, "ge2": ge2, "ge5": ge5, "sl": sl,
            "reasons": dict(reasons), "n": n}

# ── Ladder candidates ───────────────────────────────────────────────
LADDERS = [
    ("CURRENT: flat trail-30",
        [(0.0, 0.30)]),
    ("progressive 30/25/20/15",
        [(0.15, 0.30), (0.30, 0.25), (0.50, 0.20), (1.00, 0.15)]),
    ("progressive 30/20/15/10",
        [(0.15, 0.30), (0.30, 0.20), (0.50, 0.15), (1.00, 0.10)]),
    ("progressive 25/15/10",
        [(0.30, 0.25), (0.50, 0.15), (1.00, 0.10)]),
    ("aggressive 20/15/10/8",
        [(0.30, 0.20), (0.50, 0.15), (1.00, 0.10), (2.00, 0.08)]),
    ("tighter mid 30/20/15",
        [(0.15, 0.30), (0.30, 0.20), (0.50, 0.15)]),
    ("VERY-aggressive 15/10/8",
        [(0.30, 0.15), (0.50, 0.10), (1.00, 0.08)]),
    ("playbook-spec 30/20/15 + wider when parabolic",
        [(0.15, 0.30), (0.50, 0.20), (1.00, 0.15)]),
]

# Run each
results = []
for label, ladder in LADDERS:
    r = evaluate(replay_progressive, label, ladder)
    results.append(r)

# Sort by avg descending
results.sort(key=lambda r: -r["avg"])

baseline = next(r for r in results if r["label"].startswith("CURRENT"))
print(f"\nBaseline (CURRENT flat trail-30): avg={baseline['avg']:.3f}x  wins={baseline['wins']}/{baseline['n']}  ≥1.5x={baseline['ge15']}  ≥2x={baseline['ge2']}  ≥5x={baseline['ge5']}  SL_hits={baseline['sl']}", file=sys.stderr)
print(f"\nSorted by avg_exit:\n", file=sys.stderr)
print(f"  {'ladder':<52} {'avg':>6} {'Δbase':>6} {'wins':>5} {'≥1.5x':>5} {'≥2x':>4} {'≥5x':>4} {'SL':>4}", file=sys.stderr)
for r in results:
    delta = r["avg"] - baseline["avg"]
    mark  = " ← BEAT" if delta > 0 else ""
    print(f"  {r['label']:<52} {r['avg']:>5.3f}x {delta:+6.3f} {r['wins']:>5} {r['ge15']:>5} {r['ge2']:>4} {r['ge5']:>4} {r['sl']:>4}{mark}", file=sys.stderr)

# Specific replay: today's two tokens
print(f"\n=== Replay TODAY's two notable tokens ===", file=sys.stderr)
today = [
    # (name, entry, [tick prices])
    ("0x947af604 (peak +73%, exit -1.4% LIVE)",
     1.0, [1.05, 1.15, 1.30, 1.50, 1.73, 1.50, 1.30, 1.10, 1.00, 0.986]),
    ("0x547f945a (peak +42%, exit +3.9% LIVE)",
     1.0, [1.05, 1.10, 1.20, 1.30, 1.42, 1.30, 1.20, 1.10, 1.05, 1.039]),
]

best_3 = [r["label"] for r in results[:3]]
best_3.append(baseline["label"])

# Build ladder lookup
ladder_map = dict(LADDERS)

for name, n2, ticks in today:
    print(f"\n  {name}", file=sys.stderr)
    path = {"n2": n2, "path": [(i, t*n2) for i, t in enumerate(ticks, 1)]}
    for label in best_3:
        m, why = replay_progressive(path, n2, ladder_map[label])
        marker = " ✓" if m > 1.0 else ""
        print(f"    {label:<52} → {m:.2f}x ({why}){marker}", file=sys.stderr)
