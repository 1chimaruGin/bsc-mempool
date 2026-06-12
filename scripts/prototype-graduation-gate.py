#!/usr/bin/env python3
"""
Prototype the "graduation-imminent hold gate":
  At every block during a held position, query _tokenInfos(token) on the
  4meme launchpad to read the curve's current BNB reserve.
  Compute progress = current / 18 (the universal threshold).

Rule (when progress >= GRAD_HOLD_THRESHOLD):
  Suppress ALL exit reasons EXCEPT hard_sl (entry × 0.70) and timeout.
  Specifically: be_locked, trail-30, signal_dump are SUPPRESSED.

Hypothesis: graduation = explosive V2 rally; holding through gives runners.

Risk: false-positive holds (curve fills to 40%, then sellers reverse it,
we never graduate, we eat hard SL instead of be_locked +5%).

Sample test: 0x947af604 (known $13.5k entry → graduated → $162k peak).

Output: side-by-side current-LIVE vs gated outcomes.
"""
import json, sys, argparse, time
from urllib.request import Request, urlopen

NODEREAL = "https://bsc-mainnet.nodereal.io/v1/3bed06fc28e04f73a64a54da9c575a47"
LAUNCHPAD = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
SEL_TOKEN_INFOS = "0xe684626b"
GRAD_THRESHOLD_BNB = 18.0
GRAD_HOLD_THRESHOLD = 0.40   # if progress ≥ 40%, hold for graduation
GRAD_HOLD_MAX_BLOCKS = 1000  # but no more than this many blocks
ARM_PCT = 0.30
TRAIL_PCT = 0.30
SL_PCT = 0.30
BE_AT = 0.15
BE_LOCK = 0.05

def rpc(method, params):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":1}).encode()
    req = Request(NODEREAL, data=body, headers={"Content-Type":"application/json"})
    with urlopen(req, timeout=30) as r:
        return json.loads(r.read()).get("result")

def curve_state(token, block):
    """Returns dict with threshold_bnb, current_bnb, progress."""
    arg = token[2:].rjust(64, "0").lower()
    r = rpc("eth_call", [{"to": LAUNCHPAD, "data": SEL_TOKEN_INFOS + arg}, hex(block)])
    if not r or r == "0x": return None
    h = r[2:]
    if len(h) < 64*9: return None
    threshold = int(h[5*64:6*64], 16) / 1e18
    current   = int(h[8*64:9*64], 16) / 1e18
    return {
        "threshold": threshold,
        "current":   current,
        "progress":  current / threshold if threshold > 0 else 0,
    }

# ── Cached path replay (uses v2_paths data we already have) ────────

def load_path(token, cache="d_microstructure_v2_paths.json"):
    with open(cache) as f:
        rows = json.load(f)
    token = token.lower()
    for r in rows:
        if r["token"].lower() == token:
            pb = r.get("_per_block") or {}
            d_block = r["d_block"]
            blocks = sorted(int(b) for b in pb.keys())
            n2_block = next((b for b in blocks if b >= d_block + 2), None)
            if n2_block is None: return None
            n2_price = pb[str(n2_block)]["last_price"]
            path = [(b, pb[str(b)]["last_price"]) for b in blocks if b >= n2_block]
            return {"d_block": d_block, "n2_block": n2_block, "n2_price": n2_price,
                    "path": path, "token": token}
    return None

# ── Replays ───────────────────────────────────────────────────────

def replay_live(path_data):
    """Current LIVE rules: arm+30/trail-30/SL-30 + ratchet @+15/lock+5%."""
    n2 = path_data["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock_floor = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    for blk, p in path_data["path"]:
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        effective = max(sl_floor, lock_floor) if ratcheted else sl_floor
        if p <= effective:
            why = "be_locked" if (ratcheted and lock_floor > sl_floor) else "hard_sl"
            return blk, p/n2, why
        if armed and p <= peak*(1 - TRAIL_PCT):
            return blk, p/n2, "trail"
    blk, p = path_data["path"][-1]
    return blk, p/n2, "timeout"

def replay_with_gate(path_data, gate_thr=GRAD_HOLD_THRESHOLD,
                     max_grad_hold=GRAD_HOLD_MAX_BLOCKS, verbose=False):
    """Same as LIVE but: when progress ≥ gate_thr, SUPPRESS be_locked/trail.
    Only hard_sl and timeout fire. Progress queried PER BLOCK from on-chain."""
    n2 = path_data["n2_price"]
    sl_floor = n2 * (1 - SL_PCT)
    lock_floor = n2 * (1 + BE_LOCK)
    armed = False; peak = n2; ratcheted = False
    gated_blocks = 0
    for blk, p in path_data["path"]:
        if p > peak: peak = p
        if not armed and peak >= n2*(1+ARM_PCT): armed = True
        if not ratcheted and peak >= n2*(1+BE_AT): ratcheted = True
        effective = max(sl_floor, lock_floor) if ratcheted else sl_floor

        # Curve progress check
        cs = curve_state(path_data["token"], blk)
        progress = cs["progress"] if cs else 0
        # Gate active if progress ≥ thr AND we've not been gating too long
        gate_active = (progress >= gate_thr) and (gated_blocks < max_grad_hold)
        if gate_active: gated_blocks += 1

        if verbose:
            print(f"  blk={blk}  p={p:.3e}  ratio={p/n2:.3f}  prog={progress:.1%}  gated={gate_active}")

        # HARD SL always fires (we don't suppress that)
        if p <= sl_floor:
            return blk, p/n2, "hard_sl"
        if gate_active:
            # Once graduated (progress = 100%), we'd switch to V2 tracking,
            # but the cached path may not have post-grad data. Treat full
            # progress as "won the runner" and exit at current as proxy.
            if progress >= 0.999:
                # graduation hit while we held → keep holding (no exit yet)
                # cached path may end before V2 prices arrive
                continue
            # Suppress be_locked, trail, signal — keep holding
            continue

        # Normal exit rules
        if ratcheted and p <= effective and lock_floor > sl_floor:
            return blk, p/n2, "be_locked"
        if armed and p <= peak*(1 - TRAIL_PCT):
            return blk, p/n2, "trail"
    blk, p = path_data["path"][-1]
    return blk, p/n2, "timeout"

# ── Main ──────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tokens", nargs="+", default=[
        "0x947af604e08b4278de287cc3df8be84b57f04444",
        # Today's graduated/large-mcap tokens
    ])
    ap.add_argument("--gate_thr", type=float, default=GRAD_HOLD_THRESHOLD)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    print(f"\n=== Graduation Gate Prototype (threshold {args.gate_thr:.0%}) ===\n")
    print(f"{'token':<46}  {'curr live':<26}  {'gated':<26}  {'Δ':<10}")
    print("-" * 120)
    for token in args.tokens:
        pd = load_path(token)
        if not pd:
            print(f"  {token}: no cached path")
            continue
        # Current LIVE
        bL, ratioL, whyL = replay_live(pd)
        # With gate
        bG, ratioG, whyG = replay_with_gate(pd, args.gate_thr, verbose=args.verbose)
        delta = ratioG - ratioL
        marker = " ← BIG WIN" if delta > 0.50 else (" ← worse" if delta < -0.10 else "")
        live_str  = f"{ratioL:.3f}x ({whyL}, blk+{bL-pd['n2_block']})"
        gate_str  = f"{ratioG:.3f}x ({whyG}, blk+{bG-pd['n2_block']})"
        print(f"  {token}  {live_str:<26}  {gate_str:<26}  {delta:+.3f}{marker}")

if __name__ == "__main__":
    main()
