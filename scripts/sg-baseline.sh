#!/usr/bin/env bash
# Layer 4: estimate how much SG migration would buy us, by:
#   (a) measuring CURRENT RTT to APAC-anchored endpoints from this server, AND
#   (b) printing reference numbers for what a SG-located server should see.
#
# True SG measurement requires a SG VPS (see end of script for the $5 sanity
# check you can run before committing to migration).
#
#   scripts/sg-baseline.sh
set -uo pipefail

echo "=================================================================="
echo "  SG MIGRATION BASELINE  (run from current location)"
echo "=================================================================="

# Endpoints anchored in or near SG / APAC
declare -a APAC_HOSTS=(
    "bsc-dataseed1.binance.org"       # Binance SG/HK
    "bsc-dataseed2.binance.org"       # Binance SG/HK
    "bsc-dataseed3.binance.org"       # Binance HK/SG
    "bsc-dataseed4.binance.org"       # Binance JP
    "bsc.publicnode.com"              # Multi-region; SG presence
    "google.com"                      # anycast — best-case for you
    "sg.archive.org"                  # actual Singapore data center
    "asia-southeast1-c.googleapis.com" # GCP Singapore zone
    "speedtest.singtel.com"           # SingTel SG
)

echo
echo "── ping (ICMP) from THIS server to APAC anchors ──"
echo "   (sudo not required — using fping / ping fallback)"
echo

PING_TOOL=$(command -v fping || true)
if [ -z "$PING_TOOL" ]; then
    PING_TOOL=$(command -v ping)
fi

for host in "${APAC_HOSTS[@]}"; do
    if command -v fping >/dev/null 2>&1; then
        out=$(fping -c5 -q "$host" 2>&1 | tail -1)
        printf "  %-40s %s\n" "$host" "$out"
    else
        # standard ping
        result=$(ping -c5 -W2 "$host" 2>/dev/null | tail -1 | sed 's/^/  /' || echo "  (unreachable)")
        printf "  %-40s %s\n" "$host" "$result"
    fi
done

echo
echo "── TCP handshake (port 443) to BSC endpoints, 10 samples each ──"
python3 << 'PY'
import socket, time
TARGETS = [
    ("bsc-dataseed1.binance.org",   "Binance #1"),
    ("bsc-dataseed3.binance.org",   "Binance #3"),
    ("bsc.publicnode.com",          "PublicNode"),
    ("bsc.bloxroute.com",           "bloXroute"),
    ("api.bnb48.club",              "BNB48"),
]
def rtt(host, port=443):
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror:
        return None
    samples = []
    for _ in range(10):
        try:
            t0 = time.perf_counter_ns()
            s = socket.create_connection((ip, port), timeout=2.0)
            s.close()
            samples.append((time.perf_counter_ns() - t0) / 1e6)
        except Exception:
            pass
    return samples

for host, lab in TARGETS:
    s = rtt(host)
    if not s:
        print(f"  {lab:14} {host:32}  (unreachable)")
        continue
    s.sort()
    med = s[len(s)//2]
    print(f"  {lab:14} {host:32}  min={s[0]:5.1f}  med={med:5.1f}  max={s[-1]:5.1f}  ms")
PY

echo
echo "=================================================================="
echo "  REFERENCE NUMBERS  (typical SG-based server should see)"
echo "=================================================================="
cat <<'EOF'
  SG → Binance dataseed (SG/HK):     5-20 ms
  SG → bloXroute BSC (NJ + SG):      10-30 ms
  SG → BNB48 (HK):                   5-15 ms
  SG → Tokyo validators:             40-70 ms

  Your CURRENT location (Hetzner DE) typically sees 150-220 ms.
  Expected improvement: 130-200 ms RTT reduction.

  Translated to BSC block time (450ms): a 200ms RTT cut = recovering
  44% of one slot for the front-run race.
EOF

echo
echo "=================================================================="
echo "  CHEAP VALIDATION BEFORE MIGRATING"
echo "=================================================================="
cat <<'EOF'
  Spin up a $5/mo Vultr SG VPS (or similar) for 1 hour, run:

    # From the SG VPS:
    curl -sLO https://raw.githubusercontent.com/.../probe-validators.py
    python3 probe-validators.py 30

  Compare the median RTTs side-by-side with this script's output.
  If SG median is 5-30ms vs your current 150-200ms, migration is justified.

  Providers with SG presence (cheapest first):
    Vultr:        $5-12/mo, true SG dc
    DigitalOcean: $5-12/mo, SGP1 region
    Hetzner:      NO SG datacenter — would need to switch provider
    OVH:          $6-15/mo, SGP region
    Linode:       $5-12/mo, SG dc

  Storage: a fully-pruned bsc-geth needs ~1.5 TB. Most $5/mo VPS only
  have ~80GB SSD — you'll need a dedicated server or block-storage add-on
  (~$50-150/mo total). Budget for that before committing.
EOF
echo
