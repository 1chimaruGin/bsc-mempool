#!/usr/bin/env bash
# Layer 1: aggregate the latency fields we ALREADY log per KOL tx.
# Reads `journalctl -u bsc-runner` over a given window, strips ANSI codes,
# extracts detect_ms / lead_ms / ms_into_block / visibility, then prints
# distribution summaries and "front-run feasibility" thresholds.
#
#   scripts/latency-summary.sh                  # last 24h
#   scripts/latency-summary.sh "6 hours ago"    # any "since" string
set -uo pipefail

SINCE="${1:-24 hours ago}"

exec python3 - "$SINCE" <<'PY'
import re, subprocess, sys, statistics, collections

since = sys.argv[1]
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
KV = re.compile(r'(\w+)=(?:"([^"]*)"|(\S+))')


def strip(s):
    return ANSI.sub("", s)


def parse_kv(line):
    out = {}
    for m in KV.finditer(strip(line)):
        out[m.group(1)] = m.group(2) if m.group(2) is not None else m.group(3)
    return out


def num(s, default=None):
    if s is None:
        return default
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


print(f"reading journal since '{since}' …", file=sys.stderr)
p = subprocess.Popen(
    ["journalctl", "-u", "bsc-runner", "--no-pager", "--since", since, "-o", "cat"],
    stdout=subprocess.PIPE, text=True,
)

rows = []
for line in p.stdout:
    if "KOL tx CONFIRMED" not in line:
        continue
    kv = parse_kv(line)
    vis = (kv.get("visibility") or "").strip('"').strip()
    if vis not in ("public", "private"):
        continue
    rows.append({
        "vis":     vis,
        "side":    (kv.get("side") or "").strip('"').strip(),
        "detect":  num(kv.get("detect_ms", "").strip('"')),
        "lead":    num(kv.get("lead_ms", "").strip('"')),
        "ms_into": num(kv.get("ms_into_block", "").strip('"')),
        "slot":    num(kv.get("slot_remaining_ms", "").strip('"')),
        "delta":   num(kv.get("block_delta", "").strip('"')),
    })

if not rows:
    print("no KOL confirms in this window", file=sys.stderr)
    sys.exit(0)


def stats(name, vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return f"  {name:18} (no data)"
    vals.sort()
    n = len(vals)
    def pct(p):
        i = int(p * (n - 1))
        return vals[i]
    return (f"  {name:18} n={n:>5}  min={vals[0]:>6.0f}  "
            f"p25={pct(.25):>6.0f}  med={pct(.5):>6.0f}  "
            f"p75={pct(.75):>6.0f}  p95={pct(.95):>6.0f}  max={vals[-1]:>6.0f}")


def histogram(name, vals, edges):
    vals = [v for v in vals if v is not None]
    if not vals:
        return
    buckets = [0] * (len(edges) + 1)
    for v in vals:
        placed = False
        for i, e in enumerate(edges):
            if v <= e:
                buckets[i] += 1
                placed = True
                break
        if not placed:
            buckets[-1] += 1
    print(f"\n  {name}  (n={len(vals)}):")
    labels = ([f"≤{edges[0]:.0f}"]
              + [f"{edges[i-1]:.0f}…{edges[i]:.0f}" for i in range(1, len(edges))]
              + [f">{edges[-1]:.0f}"])
    for lab, count in zip(labels, buckets):
        pct = count / len(vals) * 100
        bar = "█" * int(pct / 2)
        print(f"    {lab:>12}  {count:>5}  {pct:>5.1f}%  {bar}")


print()
print("=" * 78)
print(f"LATENCY SUMMARY — {len(rows)} KOL confirms since {since!r}")
print("=" * 78)

# By visibility
for vis in ("public", "private"):
    sub = [r for r in rows if r["vis"] == vis]
    print(f"\n── visibility={vis}  ({len(sub)} txs, "
          f"{len(sub)*100//max(1,len(rows))}% of total) ──")
    print(stats("detect_ms",       [r["detect"] for r in sub]))
    print(stats("lead_ms",         [r["lead"] for r in sub]))
    print(stats("ms_into_block",   [r["ms_into"] for r in sub]))
    print(stats("slot_remaining",  [r["slot"] for r in sub]))
    print(stats("block_delta",     [r["delta"] for r in sub]))

# Front-run feasibility on the PUBLIC subset
pub = [r for r in rows if r["vis"] == "public" and r["lead"] is not None]
if pub:
    print()
    print("─" * 60)
    print("FRONT-RUN FEASIBILITY (PUBLIC ONLY — private is impossible)")
    print("─" * 60)
    print("\nlead_ms = time we had AFTER detection before block sealed.")
    print("To front-run we need: lead_ms > (RTT_to_validator + ~5ms processing)")
    histogram("lead_ms distribution", [r["lead"] for r in pub],
              [10, 25, 50, 75, 100, 150, 200, 300])
    over_50  = sum(1 for r in pub if r["lead"] > 50)
    over_100 = sum(1 for r in pub if r["lead"] > 100)
    over_200 = sum(1 for r in pub if r["lead"] > 200)
    n = len(pub)
    print(f"\n  → lead > 50ms (SG-feasible if RTT~30ms):  "
          f"{over_50}/{n} ({over_50*100//n}%)")
    print(f"  → lead > 100ms (marginal from DE):        "
          f"{over_100}/{n} ({over_100*100//n}%)")
    print(f"  → lead > 200ms (DE-feasible):             "
          f"{over_200}/{n} ({over_200*100//n}%)")

# Public vs private split overall
total = len(rows)
pub_n = sum(1 for r in rows if r["vis"] == "public")
prv_n = sum(1 for r in rows if r["vis"] == "private")
print()
print("─" * 60)
print(f"VISIBILITY MIX   public {pub_n}/{total} ({pub_n*100//total}%)   "
      f"private {prv_n}/{total} ({prv_n*100//total}%)")
print(f"                 private = NEVER front-runnable (we never see them)")
print()
PY
