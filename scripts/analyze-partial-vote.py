#!/usr/bin/env python3
"""
Test: single-sell-on-vote vs partial-on-vote with moonbag.

Hypothesis: when signal_vote fires, the dump is just starting. Selling
N% locks the bulk, keeping (1-N)% as a moonbag captures runners that
recover. At $20 trade size the extra gas is acceptable.

Method: walk EVERY block in each cached path (no sampling). For each
exit-reason candidate, apply the chosen rule. When signal_vote fires:
  - Variant single: sell 100% at current price → done
  - Variant partial: sell X% at current price, retain (1-X)% as moonbag.
    Moonbag exits on its own rules:
       - moonbag_sl  = X% drop from current price → hard exit
       - moonbag_trail = trail-Y% on the moonbag's own running peak
       - moonbag_max_hold blocks
    Whatever ends first wins.

Final realized = X * (price_at_vote / entry) + (1-X) * (moonbag_exit / entry)
Subtract gas: 1 extra broadcast for partials (~$0.30 at $20 size).

Also report the ATH that occurred AFTER the trail's first exit decision,
so we can see "what we left on the table".

Args:
  --hairshut 0.05  # apply realistic 5% exit slippage (default 0 = none)
"""
import json, sys, argparse
from collections import defaultdict

CACHE = "d_microstructure_v2_paths.json"
N2_OFFSET = 2
ARM_PCT   = 0.30
SL_PCT    = 0.30
BE_AT     = 0.15
BE_LOCK   = 0.05

ap = argparse.ArgumentParser()
ap.add_argument("--haircut", type=float, default=0.05,
                help="Exit slippage haircut applied to non-hard-SL exits (default 0.05)")
ap.add_argument("--bnb_usd", type=float, default=585)
ap.add_argument("--gas_per_sell_usd", type=float, default=0.30)
ap.add_argument("--trade_size_usd", type=float, default=20.0)
args = ap.parse_args()

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

def compute_votes(path, i, peak_ratio):
    """Compute number of voting signals firing at block i (after armed)."""
    p = path[i]["price"]
    # F1: dist_from_local_max
    recent = path[max(0, i-9):i+1]
    local_max = max(r["price"] for r in recent)
    dist = (local_max - p) / local_max if local_max > 0 else 0
    # F2: vel_10
    if i >= 10:
        past = path[i-10]["price"]
        vel_10 = (p - past) / past / 10 if past > 0 else 0
    else: vel_10 = 0
    # F3: buy_velocity_collapse
    buy_3 = sum_field(path, i, "buyers", 3)
    buy_10 = sum_field(path, i, "buyers", 10)
    bv3 = buy_3 / 3 if i >= 2 else 0
    bv10 = buy_10 / 10 if i >= 9 else 0
    collapse = (bv3 / bv10) if bv10 > 0 else 1.0
    # F4: net_flow_3blk
    buy_bnb_3  = sum_field(path, i, "buy_bnb", 3)
    sell_bnb_3 = sum_field(path, i, "sell_bnb", 3)
    net_3 = buy_bnb_3 - sell_bnb_3

    return (int(dist > 0.30) + int(vel_10 < -0.01)
            + int(collapse < 0.5) + int(net_3 < -1e18))

def replay(pd, retain_on_vote=0.0, mb_sl=0.50, mb_trail=0.50, mb_max_hold=4000, haircut=0.05):
    """
    retain_on_vote: fraction (0..1) to keep as moonbag when signal_vote fires
      0.0 = single-sell baseline (current behavior)
    mb_sl: hard SL distance for moonbag (from price at vote)
    mb_trail: trail % for moonbag from its running peak
    """
    n2 = pd["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock_floor = n2 * (1 + BE_LOCK)
    path = pd["path"]
    armed = False; peak = n2; ratcheted = False

    # Track ATH AFTER any exit fires for "missed runner" analysis
    main_exit_blk = None
    main_exit_ratio = None
    main_exit_reason = None
    moonbag_outcome = None

    for i, fb in enumerate(path):
        p = fb["price"]
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        eff = max(sl_floor, lock_floor) if ratcheted else sl_floor

        if p <= eff:
            why = "be_locked" if (ratcheted and lock_floor > sl_floor) else "hard_sl"
            main_exit_blk = i; main_exit_ratio = p/n2; main_exit_reason = why
            break
        if not armed: continue

        # 3-of-4 voting check
        votes = compute_votes(path, i, peak/n2)
        if votes >= 3:
            main_exit_blk = i; main_exit_ratio = p/n2; main_exit_reason = "signal_vote"
            break
        # Fallback to trail-30
        if p <= peak * (1 - 0.30):
            main_exit_blk = i; main_exit_ratio = p/n2; main_exit_reason = "trail"
            break

    # If we never exited, treat the last observation as timeout
    if main_exit_blk is None:
        main_exit_blk = len(path) - 1
        main_exit_ratio = path[-1]["price"] / n2
        main_exit_reason = "timeout"

    # Find post-exit ATH (the runner we missed)
    post_path = path[main_exit_blk + 1:]
    if post_path:
        post_ath = max(r["price"] for r in post_path) / n2
    else:
        post_ath = main_exit_ratio

    # Apply slippage to non-hard_sl/be_locked exits
    is_terminal_force = main_exit_reason in ("hard_sl", "be_locked")
    main_realized = main_exit_ratio if is_terminal_force else main_exit_ratio * (1 - haircut)

    # If retain_on_vote = 0 or exit wasn't via vote, single-sell behavior
    if retain_on_vote <= 0 or main_exit_reason != "signal_vote":
        total_realized = main_realized
        return {
            "realized": total_realized, "reason": main_exit_reason,
            "main_at": main_exit_ratio, "moonbag_at": None,
            "post_ath": post_ath,
            "sell_count": 1,
        }

    # PARTIAL-ON-VOTE: main fraction sells at vote price, moonbag rides
    main_fraction = 1 - retain_on_vote
    moonbag_seed_price = path[main_exit_blk]["price"]
    moonbag_sl_floor = moonbag_seed_price * (1 - mb_sl)
    moonbag_peak = moonbag_seed_price
    moonbag_exit_ratio = moonbag_seed_price / n2
    moonbag_reason = "mb_end_of_data"
    mb_start_block = main_exit_blk

    for i, fb in enumerate(path[main_exit_blk + 1:], start=main_exit_blk + 1):
        p = fb["price"]
        if p > moonbag_peak: moonbag_peak = p
        # Hard moonbag SL
        if p <= moonbag_sl_floor:
            moonbag_exit_ratio = p / n2
            moonbag_reason = "mb_sl"
            break
        # Moonbag trail
        if p <= moonbag_peak * (1 - mb_trail):
            moonbag_exit_ratio = p / n2
            moonbag_reason = "mb_trail"
            break
        # Max hold
        if i - mb_start_block >= mb_max_hold:
            moonbag_exit_ratio = p / n2
            moonbag_reason = "mb_timeout"
            break

    # Apply slippage to moonbag too (it's not a hard SL when forced)
    is_mb_hardsl = (moonbag_reason == "mb_sl")
    mb_realized = moonbag_exit_ratio if is_mb_hardsl else moonbag_exit_ratio * (1 - haircut)

    # Combine: main_fraction sells at vote price, retain_fraction at moonbag exit
    total_realized = main_fraction * main_realized + retain_on_vote * mb_realized

    return {
        "realized": total_realized, "reason": f"vote+{moonbag_reason}",
        "main_at": main_exit_ratio, "moonbag_at": moonbag_exit_ratio,
        "post_ath": post_ath,
        "sell_count": 2,
    }

# ── Evaluator ─────────────────────────────────────────────────────

def evaluate(retain, mb_sl, mb_trail, label):
    results = []
    for pd in paths:
        try:
            r = replay(pd, retain_on_vote=retain, mb_sl=mb_sl,
                       mb_trail=mb_trail, haircut=args.haircut)
            results.append(r)
        except Exception as e:
            continue
    n = len(results)
    if not n: return None
    realized = [r["realized"] for r in results]
    avg = sum(realized)/n
    wins = sum(1 for x in realized if x > 1.0)
    ge15 = sum(1 for x in realized if x >= 1.5)
    ge2  = sum(1 for x in realized if x >= 2.0)
    ge5  = sum(1 for x in realized if x >= 5.0)
    # Average sell count → gas estimate
    avg_sells = sum(r["sell_count"] for r in results) / n
    # Net daily PnL @ args.trade_size_usd, 19 trades/day
    gross = 19 * args.trade_size_usd * (avg - 1.0)
    # Gas: 1 buy + avg_sells sells × 0.30
    gas   = 19 * (args.gas_per_sell_usd + avg_sells * args.gas_per_sell_usd)
    net   = gross - gas

    # Average post-ATH (= what we left on the table) among VOTE exits
    vote_exits = [r for r in results if "vote" in r["reason"]]
    avg_post_ath_vote = (sum(r["post_ath"] for r in vote_exits)/len(vote_exits)) if vote_exits else 0
    return {"label": label, "avg": avg, "wins": wins, "ge15": ge15, "ge2": ge2, "ge5": ge5,
            "sells": avg_sells, "gas": gas, "net": net,
            "vote_n": len(vote_exits), "post_ath_vote": avg_post_ath_vote, "n": n}

# Run candidates
candidates = [
    (0.00, 0.50, 0.50, "single-sell (baseline)"),
    (0.50, 0.50, 0.50, "retain 50% / mb-SL 50% / mb-trail 50%"),
    (0.50, 0.70, 0.50, "retain 50% / mb-SL 70% / mb-trail 50%"),
    (0.30, 0.50, 0.50, "retain 30% / mb-SL 50% / mb-trail 50%"),
    (0.20, 0.50, 0.50, "retain 20% / mb-SL 50% / mb-trail 50%"),
    (0.50, 0.50, 0.70, "retain 50% / mb-SL 50% / mb-trail 70%"),
    (0.30, 0.70, 0.70, "retain 30% / mb-SL 70% / mb-trail 70%"),
    (0.50, 0.90, 0.70, "retain 50% / mb-SL 90% / mb-trail 70%  (free-ride)"),
    (0.20, 0.90, 0.70, "retain 20% / mb-SL 90% / mb-trail 70%  (small free-ride)"),
]

print(f"\n=== Partial-on-vote backtest ({len(paths)} tokens, haircut={int(args.haircut*100)}%) ===\n",
      file=sys.stderr)
print(f"  {'config':<54} {'avg':>6} {'wins':>5} {'≥2x':>4} {'≥5x':>4} {'avg sells':>10} {'daily net':>10}", file=sys.stderr)
print(f"  " + "-"*110, file=sys.stderr)

results = []
for retain, mbsl, mbt, lbl in candidates:
    r = evaluate(retain, mbsl, mbt, lbl)
    if r: results.append(r)

baseline = next(r for r in results if r["label"].startswith("single"))
for r in results:
    delta = r["avg"] - baseline["avg"]
    mark = " ← BEAT" if r["net"] > baseline["net"] + 0.50 else ""
    print(f"  {r['label']:<54} {r['avg']:>5.3f}x {r['wins']:>5} {r['ge2']:>4} {r['ge5']:>4} {r['sells']:>9.2f} ${r['net']:>+7.2f}/d{mark}",
          file=sys.stderr)

# Show the missed-runner analysis on vote exits
print(f"\n=== Missed-runner analysis (only vote exits, single-sell baseline) ===", file=sys.stderr)
b = baseline
print(f"  Vote exits: {b['vote_n']} of {b['n']}", file=sys.stderr)
print(f"  Avg post-exit ATH (the price the runner went on to hit AFTER we sold): {b['post_ath_vote']:.2f}x of our entry", file=sys.stderr)
print(f"  This is the moonbag's upside ceiling per vote exit.\n", file=sys.stderr)
