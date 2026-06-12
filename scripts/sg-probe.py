#!/usr/bin/env python3
"""
Single-file SG-vs-DE latency probe for the BSC migration decision.

Copy this file to any candidate SG VPS (Contabo, OVH, Vultr, etc), run it,
read the verdict. No dependencies beyond Python 3 stdlib.

  scp scripts/sg-probe.py user@sg-vps:~
  ssh user@sg-vps
  python3 sg-probe.py

The script measures TCP-handshake RTT (port 443) to 5 BSC RPC endpoints,
filters out anycast/CDN edges (median <10ms ⇒ likely not real backend),
and prints a verdict comparing against the DE-baseline number burned in
below. Update DE_BASELINE_MS if you re-measure from your DE server.
"""
import socket, statistics, sys, time

# Hetzner DE measurement (2026-05-22, from probe-validators.py).
# If you re-measure DE, update this number.
DE_BASELINE_MS = 24.3

SAMPLES = 30
ENDPOINTS = [
    ("bsc-dataseed1.binance.org",  "Binance dataseed #1"),
    ("bsc-dataseed2.binance.org",  "Binance dataseed #2"),
    ("bsc-dataseed3.binance.org",  "Binance dataseed #3"),
    ("bsc-dataseed4.binance.org",  "Binance dataseed #4"),
    ("bsc-mainnet.nodereal.io",    "NodeReal mainnet"),
]


def tcp_rtt_ms(host, port=443, timeout=2.0):
    """One TCP handshake, returns ms (or None on failure)."""
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


def probe(host, label, n=SAMPLES):
    samples = []
    fails = 0
    for _ in range(n):
        r = tcp_rtt_ms(host)
        if r is None:
            fails += 1
        else:
            samples.append(r)
    return (host, label, samples, fails)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else SAMPLES
    print(f"probing {len(ENDPOINTS)} BSC endpoints × {n} TCP samples each "
          f"(~{len(ENDPOINTS) * n // 4}s)…")
    print()

    results = []
    medians = []
    for host, label in ENDPOINTS:
        _, _, samples, fails = probe(host, label, n)
        if not samples:
            print(f"  {label:25} {host:32}  UNREACHABLE ({fails} fails)")
            continue
        samples.sort()
        sz = len(samples)
        med = samples[sz // 2]
        p95 = samples[int(sz * 0.95)]
        print(f"  {label:25} {host:32}  "
              f"min={samples[0]:6.1f}  med={med:6.1f}  "
              f"p95={p95:6.1f}  ms  (fails {fails}/{n})")
        # Skip likely CDN edges (sub-10ms = anycast, not backend)
        if med >= 10:
            medians.append(med)
        results.append((label, med))

    print()
    if not medians:
        print("  All BSC endpoints answered sub-10ms — likely CDN-fronted from")
        print("  here. Cannot tell real validator distance from this location.")
        sys.exit(1)

    sg_med = sorted(medians)[len(medians) // 2]

    print("=" * 72)
    print("  COMPARISON")
    print("=" * 72)
    print(f"  THIS server (presumably SG)   median RTT: {sg_med:6.1f} ms")
    print(f"  DE baseline (Hetzner Germany) median RTT: {DE_BASELINE_MS:6.1f} ms")
    delta = DE_BASELINE_MS - sg_med
    print(f"  Improvement                              : {delta:+6.1f} ms")
    print()
    print("=" * 72)
    print("  VERDICT")
    print("=" * 72)
    if delta >= 15:
        print(f"  ✓ MIGRATION WORTH IT — {delta:.0f}ms RTT saved per round-trip.")
        print(f"    On BSC's 450ms slot, this is {delta/450*100:.1f}% of one block,")
        print(f"    enough to flip many more KOL public-mempool txs into the")
        print(f"    front-run-feasible bucket. Plug {sg_med:.0f} into:")
        print(f"      scripts/frontrun-feasibility.py --de-rtt {sg_med:.0f}")
    elif delta >= 5:
        print(f"  ~ MARGINAL — {delta:.0f}ms saved.")
        print(f"    Some extra opportunities unlock but not transformative.")
        print(f"    Plug {sg_med:.0f} into scripts/frontrun-feasibility.py")
        print(f"    to see exact % of trades that flip feasible.")
    else:
        print(f"  ✗ NOT WORTH IT — {delta:.0f}ms saved.")
        print(f"    Likely your DE server already routes through an APAC")
        print(f"    peering point efficiently, OR this SG endpoint is also")
        print(f"    CDN-edge fronted (try probing different endpoints).")
        print(f"    Don't migrate.")
    print()
    print(f"  Re-run on the DE server side first to update DE_BASELINE_MS:")
    print(f"    python3 scripts/probe-validators.py 30")
    print()


if __name__ == "__main__":
    main()
