#!/usr/bin/env python3
"""
Layer 3: per-tx front-run feasibility analysis.

For every PUBLIC-visibility KOL tx in the journal window, compute:
  budget = lead_ms  -  (network_RTT_to_validator + ~5ms processing)
If budget > 0 with margin, that tx was THEORETICALLY front-runnable.

Reports two scenarios:
  (a) current location (Hetzner DE) — uses --de-rtt or measured default
  (b) hypothetical SG location       — uses --sg-rtt or 20ms default

This is the migration-decision number: "X% of public KOL txs would have
been front-runnable from SG that aren't from here."

  scripts/frontrun-feasibility.py                  # last 24h, default RTTs
  scripts/frontrun-feasibility.py --de-rtt 180     # override DE-side RTT
  scripts/frontrun-feasibility.py --sg-rtt 15      # override SG-side RTT
  scripts/frontrun-feasibility.py --since "6h ago" # custom window
"""
import argparse, re, socket, subprocess, sys, time


ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
KV = re.compile(r'(\w+)=(?:"([^"]*)"|(\S+))')


def tcp_rtt_ms(host, port=443, timeout=2.0):
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror:
        return None
    t0 = time.perf_counter_ns()
    try:
        s = socket.create_connection((ip, port), timeout=timeout)
        s.close()
    except OSError:
        return None
    return (time.perf_counter_ns() - t0) / 1e6


def measure_rtt():
    """Actively measure median TCP RTT to representative BSC endpoints
    from THIS server. Filters out CDN-fronted hosts (sub-10ms = anycast
    edge, not a real backend)."""
    hosts = [
        ("bsc-dataseed1.binance.org",  "Binance #1"),
        ("bsc-dataseed2.binance.org",  "Binance #2"),
        ("bsc-dataseed3.binance.org",  "Binance #3"),
        ("bsc-dataseed4.binance.org",  "Binance #4"),
        ("bsc-mainnet.nodereal.io",    "NodeReal"),
    ]
    print("measuring TCP RTT to BSC endpoints (10 samples each)…", file=sys.stderr)
    medians = []
    for host, lab in hosts:
        samples = [tcp_rtt_ms(host) for _ in range(10)]
        samples = sorted(s for s in samples if s is not None)
        if not samples:
            print(f"  {lab:14} {host:32}  (unreachable)", file=sys.stderr)
            continue
        med = samples[len(samples) // 2]
        print(f"  {lab:14} {host:32}  med={med:5.1f}ms", file=sys.stderr)
        # Skip likely-CDN-edge hosts: any <10ms is almost certainly anycast,
        # not a real validator backend. We want the realistic ceiling.
        if med >= 10:
            medians.append(med)
    if not medians:
        return None
    medians.sort()
    measured = medians[len(medians) // 2]
    print(f"\n  → measured median RTT (CDN-filtered): {measured:.1f}ms", file=sys.stderr)
    return measured


def strip(s):
    return ANSI.sub("", s)


def parse_kv(line):
    out = {}
    for m in KV.finditer(strip(line)):
        out[m.group(1)] = m.group(2) if m.group(2) is not None else m.group(3)
    return out


def num(s):
    if s is None:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


PROCESSING_MS = 5     # decode + sign + serialize
MARGIN_MS = 10        # safety buffer for jitter

ap = argparse.ArgumentParser()
ap.add_argument("--since", default="24 hours ago",
                help='journalctl --since (default: "24 hours ago")')
ap.add_argument("--de-rtt", type=float, default=None,
                help="override measured RTT to validators (ms)")
ap.add_argument("--sg-rtt", type=float, default=20.0,
                help="hypothetical RTT from SG location (ms)")
ap.add_argument("--no-measure", action="store_true",
                help="skip the RTT probe; use --de-rtt or fall back to 180ms")
args = ap.parse_args()

# Measure RTT now unless overridden
if args.de_rtt is None and not args.no_measure:
    measured = measure_rtt()
    if measured:
        args.de_rtt = measured
    else:
        print("  (probe failed, falling back to 180ms estimate)", file=sys.stderr)
        args.de_rtt = 180.0
elif args.de_rtt is None:
    args.de_rtt = 180.0

print(f"reading journal since {args.since!r} …", file=sys.stderr)
p = subprocess.Popen(
    ["journalctl", "-u", "bsc-runner", "--no-pager",
     "--since", args.since, "-o", "cat"],
    stdout=subprocess.PIPE, text=True,
)

pub_txs = []
prv_count = 0
for line in p.stdout:
    if "KOL tx CONFIRMED" not in line:
        continue
    kv = parse_kv(line)
    vis = (kv.get("visibility") or "").strip('"').strip()
    if vis == "private":
        prv_count += 1
        continue
    if vis != "public":
        continue
    lead = num((kv.get("lead_ms") or "").strip('"'))
    if lead is None:
        continue
    pub_txs.append({
        "kol": kv.get("kol_name"),
        "side": (kv.get("side") or "").strip('"'),
        "lead": lead,
        "ms_into_block": num((kv.get("ms_into_block") or "").strip('"')),
    })

if not pub_txs:
    print("no public-visibility KOL txs in window", file=sys.stderr)
    sys.exit(0)


def feasible(lead_ms, rtt_ms):
    """True if we could have front-run given this lead and one-way RTT.
    Need to send + validator-accept-into-current-block before slot end.
    Conservative: full RTT (not half) because the network path is not
    symmetric and we want the tx confirmed."""
    return lead_ms >= (rtt_ms + PROCESSING_MS + MARGIN_MS)


def scenario(rtt_ms, label):
    threshold = rtt_ms + PROCESSING_MS + MARGIN_MS
    feas = sum(1 for t in pub_txs if feasible(t["lead"], rtt_ms))
    pct = feas * 100 / len(pub_txs)
    print(f"  {label:24}  RTT={rtt_ms:>5.0f}ms  "
          f"threshold lead≥{threshold:>4.0f}ms  →  "
          f"{feas}/{len(pub_txs)} feasible ({pct:>5.1f}%)")
    return feas, pct


total = len(pub_txs) + prv_count
print()
print("=" * 80)
print(f"FRONT-RUN FEASIBILITY ANALYSIS")
print("=" * 80)
print(f"  Window         : {args.since!r} → now")
print(f"  Total KOL txs  : {total}")
print(f"  Public  (visible in mempool) : {len(pub_txs)} "
      f"({len(pub_txs)*100//total}% — only these are addressable)")
print(f"  Private (direct-to-validator): {prv_count} "
      f"({prv_count*100//total}% — NEVER front-runnable, by definition)")
print()
print("  Front-run requires:  lead_ms ≥ RTT_to_validator + "
      f"{PROCESSING_MS}ms processing + {MARGIN_MS}ms safety")
print()
print("─" * 80)
print("SCENARIO COMPARISON (over the public-visibility subset)")
print("─" * 80)
fde, pde = scenario(args.de_rtt, "Current (Hetzner DE)")
fsg, psg = scenario(args.sg_rtt, "Hypothetical (SG)")

# Also test a few alternate RTTs
for rtt, lab in [(80, "Other (e.g. Frankfurt+)"), (50, "Other (e.g. Tokyo)"),
                  (10, "Other (best case)")]:
    scenario(rtt, lab)

print()
print("─" * 80)
print("MIGRATION DECISION SIGNAL")
print("─" * 80)
delta_txs = fsg - fde
delta_pct = psg - pde
print(f"  SG migration would unlock {delta_txs:+d} additional front-runnable "
      f"txs ({delta_pct:+.1f}% of public).")
if total > 0:
    pct_of_all = delta_txs * 100 / total
    print(f"  That's {pct_of_all:.1f}% of ALL KOL trades (incl. private).")
print()

if delta_pct < 5:
    print("  → Migration NOT obviously worth it (SG unlocks < 5% extra opportunities).")
    print("    Other constraints (private-mempool dominance, MEV competition) bigger.")
elif delta_pct < 20:
    print("  → Migration MARGINAL — depends on capital + how many trades you take.")
    print("    Compute expected $ from those extra trades vs migration cost.")
else:
    print("  → Migration LIKELY WORTH IT — large addressable opportunity unlocks.")
    print("    Validate with $5 SG VPS ping test before committing.")
print()
print("Customize assumptions:")
print(f"  scripts/frontrun-feasibility.py --de-rtt {args.de_rtt:.0f} "
      f"--sg-rtt {args.sg_rtt:.0f}")
print("  (run scripts/probe-validators.py first to find your true --de-rtt)")
print()
