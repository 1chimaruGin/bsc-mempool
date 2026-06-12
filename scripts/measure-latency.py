#!/usr/bin/env python3
"""
Standalone latency-measurement script for the DE-vs-SG migration decision.

Same script runs on both sides. Output is a parseable table so you can
diff the two runs side-by-side. No external dependencies (Python 3 stdlib).

  Usage:
    python3 measure-latency.py [N_SAMPLES] [--label DE|SG|...]

  On DE box:    python3 measure-latency.py 30 --label DE  > de.txt
  On SG box:    python3 measure-latency.py 30 --label SG  > sg.txt
  Compare:      diff de.txt sg.txt    (or eyeball them)

Endpoints covered:
  • Binance BSC dataseeds  (the official BSC public RPC pool)
  • NodeReal               (commercial BSC RPC; we already use them for archive)
  • PublicNode + Ankr      (popular but usually CDN-fronted — included as ref)
  • BlockRazor placeholder (paste your private endpoint into BLOCKRAZOR list)
  • 48 Club placeholder    (paste their successor-to-Puissant endpoint)

Also probes a few pure-IP anchors (Cloudflare/Google/AWS public anycast and
direct-IP geographic anchors) so you can sanity-check the routing.

Each target gets N_SAMPLES TCP handshakes + an ICMP ping (if available).
"""
import argparse
import http.client
import json
import os
import socket
import ssl
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse


def load_env(path="/data/bsc-meme-mev/.env"):
    """Best-effort .env reader. Returns {} if file missing."""
    out = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return out


_ENV = load_env()
_BLOCKRAZOR_AUTH = _ENV.get("BLOCKRAZOR_AUTH_KEY", "")
_NODEREAL_URL = _ENV.get("NODEREAL_RPC_URL", "")


def nodereal_target():
    """Build a TARGETS-row tuple for the authenticated NodeReal endpoint
    out of the URL in .env. Returns None if not configured."""
    if not _NODEREAL_URL:
        return None
    u = urlparse(_NODEREAL_URL)
    return (u.hostname, u.port or 443,
            "NodeReal (auth)", "bsc_rpc",
            u.path or "/")


# ── targets ────────────────────────────────────────────────────────────────
# (host, port, label, category, json_rpc_path|None)
#
# json_rpc_path = the HTTP path on which an eth_blockNumber call works
# (None = TCP-only target, skip RPC test). Set to "/" for default JSON-RPC.
TARGETS = [
    # Binance official BSC dataseeds — JSON-RPC at /
    ("bsc-dataseed1.binance.org",      443, "Binance dataseed #1",  "bsc_rpc", "/"),
    ("bsc-dataseed2.binance.org",      443, "Binance dataseed #2",  "bsc_rpc", "/"),
    ("bsc-dataseed3.binance.org",      443, "Binance dataseed #3",  "bsc_rpc", "/"),
    ("bsc-dataseed4.binance.org",      443, "Binance dataseed #4",  "bsc_rpc", "/"),
    ("bsc-dataseed1.defibit.io",       443, "Defibit dataseed #1",  "bsc_rpc", "/"),
    ("bsc-dataseed1.ninicoin.io",      443, "Ninicoin dataseed #1", "bsc_rpc", "/"),
    # Commercial BSC RPC providers
    ("bsc-mainnet.nodereal.io",        443, "NodeReal (no-key)",    "bsc_rpc", "/"),
    ("bsc.publicnode.com",             443, "PublicNode",           "bsc_rpc", "/"),
    ("rpc.ankr.com",                   443, "Ankr",                 "bsc_rpc", "/bsc"),
    ("bsc-mainnet.public.blastapi.io", 443, "Blast API",            "bsc_rpc", "/"),
    # BlockRazor BSC builder/relay (auth key sent via Authorization header
    # by the json_rpc_rtt_ms helper; works for reads without auth too)
    ("bsc.blockrazor.xyz",             443, "BlockRazor BSC",       "mev",     "/"),
    # NodeReal authenticated endpoint is appended below by `nodereal_target()`
    # (it pulls the per-account URL out of .env so we don't hard-code keys).
    # ── 48 Club / Puissant successor — paste here when known ──
    # ("xxx.48.club",                 443, "48 Club",      "mev", "/"),
    # Geographic anchors (TCP only — they don't speak JSON-RPC)
    ("1.1.1.1",                        443, "Cloudflare 1.1.1.1",          "anchor", None),
    ("8.8.8.8",                        443, "Google 8.8.8.8",              "anchor", None),
    ("139.180.128.6",                  443, "Vultr Singapore (anchor IP)", "anchor", None),
    ("172.104.32.79",                  443, "Linode Singapore (anchor IP)","anchor", None),
    ("18.140.0.1",                     443, "AWS Singapore (anchor IP)",   "anchor", None),
    ("52.74.0.1",                      443, "AWS Singapore #2 (anchor IP)","anchor", None),
]


# ── helpers ────────────────────────────────────────────────────────────────
def resolve(host):
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return "(unresolved)"


def tcp_rtt_ms(host, port, timeout=2.0):
    try:
        ip = socket.gethostbyname(host) if not _is_ip(host) else host
    except socket.gaierror:
        return None
    t0 = time.perf_counter_ns()
    try:
        s = socket.create_connection((ip, port), timeout=timeout)
        s.close()
    except OSError:
        return None
    return (time.perf_counter_ns() - t0) / 1e6


def _is_ip(s):
    parts = s.split(".")
    return len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts)


def _auth_header_for(host):
    """Return optional auth header dict per host. Adds the BlockRazor key
    when probing their domain so we measure the AUTHENTICATED submission
    path (matters for tx submission though not for reads)."""
    if "blockrazor" in host and _BLOCKRAZOR_AUTH:
        return {"Authorization": _BLOCKRAZOR_AUTH}
    return {}


def json_rpc_rtt_ms(host, port, path, n=10, timeout=4.0):
    """Open ONE HTTPS connection, call eth_blockNumber n times on it, return
    per-call RTT list (ms). First sample is COLD (incl. TCP+TLS handshake);
    rest are WARM (HTTP keep-alive — what production submit will see).

    Reads N samples back-to-back on the same socket — closest proxy we have
    for the latency of eth_sendRawTransaction without actually broadcasting
    a tx (which would cost gas).

    Returns (cold_ms, warm_samples_ms_list) or (None, []) on failure.
    """
    ctx = ssl.create_default_context()
    # Some BSC RPC frontends sniff TLS SNI strictly; default context is fine.
    body = json.dumps({"jsonrpc": "2.0", "method": "eth_blockNumber",
                       "params": [], "id": 1}).encode()
    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "Connection":     "keep-alive",
        "User-Agent":     "measure-latency/1",
        **_auth_header_for(host),
    }
    cold_ms = None
    warm = []
    try:
        t0 = time.perf_counter_ns()
        conn = http.client.HTTPSConnection(host, port, timeout=timeout, context=ctx)
        conn.request("POST", path, body=body, headers=headers)
        resp = conn.getresponse()
        _ = resp.read()
        cold_ms = (time.perf_counter_ns() - t0) / 1e6
        if resp.status != 200:
            conn.close()
            # Non-2xx on first request — bail with the cold number; the
            # endpoint either doesn't support JSON-RPC or rate-limited us.
            return (cold_ms, [])
    except Exception:
        return (None, [])

    for _ in range(n - 1):
        try:
            t0 = time.perf_counter_ns()
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            _ = resp.read()
            dt = (time.perf_counter_ns() - t0) / 1e6
            if resp.status == 200:
                warm.append(dt)
        except Exception:
            break
    try:
        conn.close()
    except Exception:
        pass
    return (cold_ms, warm)


def icmp_ping_ms(host, count=5, timeout=2):
    """Best-effort ICMP ping. Returns median ms or None if not available
    (e.g. system blocks raw sockets). Uses /usr/bin/ping."""
    try:
        out = subprocess.run(
            ["ping", "-c", str(count), "-W", str(timeout), host],
            capture_output=True, text=True, timeout=count * (timeout + 1),
        )
        if out.returncode != 0:
            return None
        # last line typically: rtt min/avg/max/mdev = 24.001/24.123/24.456/0.123 ms
        for line in out.stdout.splitlines()[::-1]:
            if "min/avg/max" in line and "=" in line:
                stats = line.split("=", 1)[1].strip().split(" ")[0]
                return float(stats.split("/")[1])  # avg
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError, IndexError):
        return None
    return None


def stats_of(vals):
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    return {
        "n":    n,
        "min":  s[0],
        "med":  s[n // 2],
        "p95":  s[int(n * 0.95)],
        "max":  s[-1],
    }


# ── main ───────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("samples", nargs="?", type=int, default=30,
                    help="TCP samples per endpoint (default 30)")
    ap.add_argument("--label", default="UNLABELED",
                    help="location label for the report header (DE / SG / ...)")
    ap.add_argument("--no-icmp", action="store_true",
                    help="skip ICMP ping (TCP only)")
    args = ap.parse_args()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"# LATENCY MEASUREMENT  label={args.label}  utc={now}  samples={args.samples}")
    print(f"# host={socket.gethostname()}  python={sys.version.split()[0]}")
    print()
    header = (f"  {'CATEGORY':10}  {'LABEL':24}  {'HOST':32}  "
              f"{'TCP_med':>7}  {'RPC_cold':>8}  {'RPC_warm':>8}  "
              f"{'RPC_p95':>7}  {'ICMP':>6}  {'RESOLVED':>15}")
    print(header)
    print("  " + "-" * (len(header) - 2))
    print("  " + "(TCP_med = network RTT · RPC_cold = first eth_blockNumber incl. TLS · "
          "RPC_warm = keep-alive RPC ≈ what tx submission will see)")
    print("  " + "-" * (len(header) - 2))

    # Append NodeReal authenticated target if .env has it
    targets = list(TARGETS)
    nr = nodereal_target()
    if nr is not None:
        # Insert next to the no-key NodeReal row for easy comparison
        for i, t in enumerate(targets):
            if t[2] == "NodeReal (no-key)":
                targets.insert(i + 1, nr)
                break
        else:
            targets.append(nr)

    by_category = {}
    for host, port, label, cat, rpc_path in targets:
        ip = resolve(host)
        # TCP handshake measurement
        tcp_samples = []
        for _ in range(args.samples):
            r = tcp_rtt_ms(host, port)
            if r is not None:
                tcp_samples.append(r)
        t = stats_of(tcp_samples)
        ping = icmp_ping_ms(host) if not args.no_icmp else None

        # JSON-RPC measurement (eth_blockNumber)
        rpc_cold, rpc_warm = (None, [])
        if rpc_path is not None:
            rpc_cold, rpc_warm = json_rpc_rtt_ms(host, port, rpc_path, n=args.samples)

        warm_stats = stats_of(rpc_warm)
        warm_med = f"{warm_stats['med']:8.1f}" if warm_stats else f"{'—':>8}"
        warm_p95 = f"{warm_stats['p95']:7.1f}" if warm_stats else f"{'—':>7}"
        cold_s = f"{rpc_cold:8.1f}" if rpc_cold is not None else f"{'—':>8}"
        ping_s = f"{ping:6.1f}" if ping is not None else f"{'—':>6}"

        if t is None:
            print(f"  {cat:10}  {label[:24]:24}  {host[:32]:32}  "
                  f"{'FAILED':>7}  {cold_s}  {warm_med}  {warm_p95}  "
                  f"{ping_s}  {ip:>15}")
            continue
        print(f"  {cat:10}  {label[:24]:24}  {host[:32]:32}  "
              f"{t['med']:7.1f}  {cold_s}  {warm_med}  {warm_p95}  "
              f"{ping_s}  {ip:>15}")
        by_category.setdefault(cat, []).append({
            "tcp_med": t["med"],
            "rpc_warm_med": warm_stats["med"] if warm_stats else None,
        })

    print()
    print("  ── per-category median across all reachable endpoints ──")
    for cat, entries in by_category.items():
        tcp_meds = sorted(e["tcp_med"] for e in entries if e["tcp_med"] is not None)
        rpc_meds = sorted(e["rpc_warm_med"] for e in entries
                          if e["rpc_warm_med"] is not None)
        tcp_med = tcp_meds[len(tcp_meds) // 2] if tcp_meds else None
        rpc_med = rpc_meds[len(rpc_meds) // 2] if rpc_meds else None
        edge_count = sum(1 for m in tcp_meds if m < 10)
        note = (f"  ({edge_count}/{len(tcp_meds)} sub-10ms ⇒ CDN-edge)"
                if edge_count else "")
        tcp_s = f"{tcp_med:6.1f}ms" if tcp_med is not None else "  ─ ms"
        rpc_s = f"{rpc_med:6.1f}ms" if rpc_med is not None else "  ─ ms"
        print(f"    {cat:10}  TCP_med={tcp_s}  RPC_warm_med={rpc_s}  "
              f"n={len(entries)}{note}")
    print()
    print("# end of report — copy this file off the host for comparison.")
    print("# RPC_warm is the latency budget your tx submission will spend "
          "per send.")


if __name__ == "__main__":
    main()
