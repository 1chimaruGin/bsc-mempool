#!/usr/bin/env python3
"""
Layer 2: TCP RTT probe to public BSC RPC endpoints / MEV gateways. Acts
as a proxy for "how far we are from where validators live" because BSC
validators don't publish their direct IPs — but their geographic clusters
match what these RPC providers serve.

  scripts/probe-validators.py            # 30 samples per target
  scripts/probe-validators.py 60         # 60 samples
"""
import socket, statistics, sys, time
from urllib.parse import urlparse

SAMPLES = int(sys.argv[1]) if len(sys.argv) > 1 else 30
PORT = 443

# Public BSC RPC endpoints, grouped by likely geography. tcping :443.
TARGETS = [
    # ── Binance official dataseed nodes (mixed: SG, HK, JP) ──
    ("bsc-dataseed1.binance.org",      "Binance dataseed #1"),
    ("bsc-dataseed2.binance.org",      "Binance dataseed #2"),
    ("bsc-dataseed3.binance.org",      "Binance dataseed #3"),
    ("bsc-dataseed4.binance.org",      "Binance dataseed #4"),
    ("bsc-dataseed1.defibit.io",       "Defibit dataseed #1"),
    ("bsc-dataseed1.ninicoin.io",      "Ninicoin dataseed #1"),
    # ── Major public RPC providers ──
    ("bsc.publicnode.com",             "PublicNode (multi-region)"),
    ("bsc-rpc.publicnode.com",         "PublicNode RPC"),
    ("rpc.ankr.com",                   "Ankr (global)"),
    ("bsc-mainnet.nodereal.io",        "NodeReal (multi-region)"),
    ("bsc-pokt.nodies.app",            "Pocket Network"),
    ("bsc-mainnet.public.blastapi.io", "Blast API"),
    # ── BSC MEV / private gateways ──
    ("bsc.bloxroute.com",              "bloXroute BSC"),
    ("api.bnb48.club",                 "BNB48 Club"),
    # ── Geographic reference (test if we want to compare) ──
    ("google.com",                     "Google (anycast — baseline)"),
    ("1.1.1.1",                        "Cloudflare 1.1.1.1 (anycast)"),
]


def tcp_rtt_ms(host, port=443, timeout=2.0):
    """One-shot TCP handshake duration in ms. Returns None on failure."""
    try:
        addr = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)[0][-1]
    except (socket.gaierror, IndexError):
        return None
    t0 = time.perf_counter_ns()
    try:
        s = socket.socket(socket.AF_INET if ":" not in addr[0] else socket.AF_INET6,
                          socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect(addr)
        s.close()
    except (socket.timeout, OSError):
        return None
    return (time.perf_counter_ns() - t0) / 1e6


def resolve(host):
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return "(unresolved)"


def probe(host, label):
    ip = resolve(host)
    samples = []
    fails = 0
    for _ in range(SAMPLES):
        r = tcp_rtt_ms(host)
        if r is None:
            fails += 1
        else:
            samples.append(r)
    if not samples:
        return (host, label, ip, None, None, None, None, SAMPLES)
    samples.sort()
    n = len(samples)
    return (host, label, ip,
            samples[0],
            samples[n // 2],
            samples[int(n * 0.95)],
            samples[-1],
            fails)


print(f"probing {len(TARGETS)} endpoints × {SAMPLES} samples each "
      f"(TCP :{PORT}) — takes ~{len(TARGETS) * SAMPLES // 3}s …", file=sys.stderr)
print()

results = []
for host, label in TARGETS:
    r = probe(host, label)
    results.append(r)
    minv = r[3]
    med  = r[4]
    p95  = r[5]
    maxv = r[6]
    fails = r[7]
    if minv is None:
        line = f"  {label[:34]:34}  {host[:30]:30}  FAILED ({fails}/{SAMPLES} timeouts)"
    else:
        line = (f"  {label[:34]:34}  {host[:30]:30}  "
                f"min={minv:>6.1f}  med={med:>6.1f}  "
                f"p95={p95:>6.1f}  max={maxv:>6.1f}  ms  "
                f"({fails} fails)")
    print(line)

# Quick summary against thresholds
print()
print("─" * 90)
print("REFERENCE THRESHOLDS")
print("─" * 90)
print("  < 30ms     :  same-region (e.g. SG node → APAC validator)")
print("  30-80ms    :  cross-region but adjacent (e.g. SG → JP)")
print("  80-180ms   :  cross-ocean (typical DE → APAC)")
print("  > 180ms    :  long-haul; bad for any race-condition strategy")
print()

valid = [r for r in results if r[4] is not None and "anycast" not in r[1].lower()]
if valid:
    meds = sorted([r[4] for r in valid])
    overall_med = meds[len(meds) // 2]
    print(f"  → overall median RTT to BSC endpoints: {overall_med:.1f}ms")
    if overall_med < 80:
        verdict = "EXCELLENT — already near validators"
    elif overall_med < 180:
        verdict = "OK — front-run feasible for slow KOLs, marginal for fast"
    else:
        verdict = "POOR — geographic move likely required for front-runs"
    print(f"  → verdict: {verdict}")
print()
