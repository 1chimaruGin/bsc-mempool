#!/usr/bin/env bash
# Polls bsc-geth until snap-sync catches up to chain tip, then auto-runs
# install-runner.sh + verify-all.sh and drops a completion marker for the
# next interactive session to pick up.
#
# Usage:  nohup bash scripts/sync-complete-watch.sh >> /data/bsc-meme-mev/sync-complete.log 2>&1 &
set -uo pipefail

LOG=/data/bsc-meme-mev/sync-complete.log
MARKER=/data/bsc-meme-mev/.sync-complete
RPC=http://127.0.0.1:8545
POLL_SEC=120

exec >> "${LOG}" 2>&1

echo "═══ sync-complete-watch start: $(date -u +%Y-%m-%dT%H:%M:%SZ) ═══"

probe() {
    curl -s --max-time 5 -X POST "${RPC}" \
        -H 'Content-Type: application/json' \
        --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}'
}

while true; do
    RESULT=$(probe | python3 -c '
import json,sys
try:
    r=json.load(sys.stdin).get("result")
    if r is False:
        print("DONE")
    elif r is None:
        print("ERR null")
    else:
        cur=int(r["currentBlock"],16); hi=int(r["highestBlock"],16)
        pct = (cur/hi*100) if hi else 0
        print(f"SYNC {cur}/{hi} {pct:.3f}%")
except Exception as e:
    print(f"ERR {e}")
' 2>/dev/null || echo "ERR curl")

    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ${RESULT}"

    if [[ "${RESULT}" == "DONE" ]]; then
        # Double-check: eth_blockNumber should match chain head
        HEAD_HEX=$(curl -s --max-time 5 -X POST "${RPC}" \
            -H 'Content-Type: application/json' \
            --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
            | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result","0x0"))')
        HEAD=$((HEAD_HEX))
        echo "  caught up. head block = ${HEAD}"

        # Re-confirm twice with 30s gaps to avoid false-positive during brief stalls
        sleep 30
        R2=$(probe | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result"))')
        if [[ "${R2}" != "False" ]]; then
            echo "  flapped back to syncing (${R2}); continuing poll"
            sleep "${POLL_SEC}"
            continue
        fi

        echo "--- sync confirmed at tip. running install-runner.sh ---"
        if bash /data/bsc-meme-mev/scripts/install-runner.sh; then
            echo "  install-runner.sh OK"
        else
            echo "  install-runner.sh FAILED rc=$?"
        fi

        echo "--- running verify-all.sh ---"
        if bash /data/bsc-meme-mev/scripts/verify-all.sh; then
            echo "  verify-all.sh OK"
        else
            echo "  verify-all.sh rc=$? (non-zero is informational, see output)"
        fi

        date -u +%Y-%m-%dT%H:%M:%SZ > "${MARKER}"
        echo "═══ sync-complete-watch DONE: $(date -u +%Y-%m-%dT%H:%M:%SZ) ═══"
        exit 0
    fi

    sleep "${POLL_SEC}"
done
