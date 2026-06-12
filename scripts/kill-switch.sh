#!/usr/bin/env bash
# Emergency stop for the live trader. Stops the runner immediately and
# flips the phase flags to shadow=true so a restart can't accidentally
# go live again without manual intervention.
#
# Usage:
#   scripts/kill-switch.sh                 # stop + flip to shadow
#   scripts/kill-switch.sh --hard          # stop + DON'T flip flags (just halt)
#   scripts/kill-switch.sh --status        # show current live-trader state
set -uo pipefail

LIMITS="/data/bsc-meme-mev/config/limits.toml"

case "${1:-}" in
  --status)
    echo "=== bsc-runner ==="
    systemctl is-active bsc-runner || true
    echo
    echo "=== current phase flags ==="
    grep -E "^(shadow|tiny|full)" "$LIMITS" 2>/dev/null || echo "(no limits.toml)"
    echo
    echo "=== open positions ==="
    if [ -f /data/bsc-meme-mev/trader_live/open_positions.json ]; then
        python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(f'  {len(d)} open')" \
          /data/bsc-meme-mev/trader_live/open_positions.json 2>/dev/null || echo "  (parse error)"
    else
        echo "  (no live-trader ledger yet)"
    fi
    exit 0
    ;;
  --hard)
    flip=0
    ;;
  "")
    flip=1
    ;;
  *)
    echo "unknown flag: $1" >&2
    echo "usage: $0 [--status|--hard]" >&2
    exit 2
    ;;
esac

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)]  KILL-SWITCH activated"

# 1. Stop the runner
echo "  stopping bsc-runner…"
systemctl stop bsc-runner
sleep 1
systemctl is-active bsc-runner | grep -q inactive && echo "    OK: bsc-runner stopped"

# 2. Flip phase flags back to shadow=true (unless --hard)
if [ $flip -eq 1 ] && [ -f "$LIMITS" ]; then
    echo "  flipping phase flags to shadow=true (no live broadcast on restart)…"
    sed -i -E \
        -e 's/^shadow[[:space:]]*=.*/shadow      = true/' \
        -e 's/^tiny[[:space:]]*=.*/tiny        = false/' \
        -e 's/^full[[:space:]]*=.*/full        = false/' \
        "$LIMITS"
    echo "    new state:"
    grep -E "^(shadow|tiny|full)" "$LIMITS" | sed 's/^/      /'
fi

# 3. Audit-trail line
mkdir -p /data/bsc-meme-mev/trader_live
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)  KILL-SWITCH  flip=$flip" \
    >> /data/bsc-meme-mev/trader_live/kill-switch.log

echo
echo "  next steps to resume:"
echo "    1. inspect what just happened: journalctl -u bsc-runner -n 200 --no-pager"
echo "    2. fix the cause"
echo "    3. (optional) flip a phase flag in $LIMITS"
echo "    4. systemctl start bsc-runner"
