#!/usr/bin/env bash
# bsc-geth sync dashboard. Designed for `watch -n 5 ./scripts/sync-status.sh`
# but also works standalone. Tracks rate across invocations via a tiny state
# file (/tmp/bsc-sync-state) so ETA gets sharper after a couple of refreshes.
set -uo pipefail

RPC=http://127.0.0.1:8545
DATA=/data/bsc-meme-mev/bsc-geth/data
STATE=/tmp/bsc-sync-state
TARGET_GB=1500    # rough pruned-PBSS final size; used for ETA

# ─── ANSI helpers (degrade to plain when not a TTY) ──────────────────────────
if [[ -t 1 ]] || [[ "${WATCH_FORCE_COLOR:-0}" == "1" ]]; then
    BOLD=$'\e[1m'; DIM=$'\e[2m'; RST=$'\e[0m'
    GREEN=$'\e[32m'; YELL=$'\e[33m'; CYAN=$'\e[36m'; RED=$'\e[31m'
else
    BOLD=''; DIM=''; RST=''; GREEN=''; YELL=''; CYAN=''; RED=''
fi

rpc() {
    curl -s --max-time 3 -X POST "${RPC}" \
        -H 'Content-Type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"$1\",\"params\":$2,\"id\":1}"
}

# ─── service state ───────────────────────────────────────────────────────────
SVC=$(systemctl is-active bsc-geth.service 2>/dev/null || echo "unknown")
PID=$(systemctl show -p MainPID --value bsc-geth.service 2>/dev/null || echo 0)
if [[ "${PID}" -gt 0 ]] && kill -0 "${PID}" 2>/dev/null; then
    ETIME=$(ps -p "${PID}" -o etime= 2>/dev/null | tr -d ' ')
    RSS_KB=$(ps -p "${PID}" -o rss= 2>/dev/null | tr -d ' ')
    RSS_GB=$(python3 -c "print(f'{${RSS_KB:-0}/1024/1024:.2f}')")
else
    ETIME="-"; RSS_GB="-"
fi

# ─── RPC snapshot ────────────────────────────────────────────────────────────
SYNCING=$(rpc eth_syncing '[]' 2>/dev/null)
PEER_HEX=$(rpc net_peerCount '[]' 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result","0x0"))' 2>/dev/null)
PEERS=$((PEER_HEX))

# Parse syncing payload (snap-sync uses an object; "false" means caught up).
parse_syncing() {
    python3 - <<EOF
import json,sys
try:
    r = json.loads('''${SYNCING}''').get("result")
except Exception:
    print("err"); sys.exit(0)
if r is False:
    print("done")
    sys.exit(0)
if r is None:
    print("nil")
    sys.exit(0)
cb = int(r.get("currentBlock","0x0"),16)
hb = int(r.get("highestBlock","0x0"),16)
sa = int(r.get("syncedAccounts","0x0"),16)
ss = int(r.get("syncedStorage","0x0"),16)
sb = int(r.get("syncedBytecodes","0x0"),16)
sab = int(r.get("syncedAccountBytes","0x0"),16)
ssb = int(r.get("syncedStorageBytes","0x0"),16)
sbb = int(r.get("syncedBytecodeBytes","0x0"),16)
hbb = int(r.get("healingBytecode","0x0"),16)
htn = int(r.get("healingTrienodes","0x0"),16)
print(f"ok|{cb}|{hb}|{sa}|{ss}|{sb}|{sab}|{ssb}|{sbb}|{hbb}|{htn}")
EOF
}
PARSED=$(parse_syncing)
STATE_TAG=$(echo "${PARSED}" | cut -d'|' -f1)

# ─── chaindata size + rate tracking ──────────────────────────────────────────
CHAIN_BYTES=$(du -sb "${DATA}/geth/chaindata" 2>/dev/null | awk '{print $1}')
CHAIN_BYTES=${CHAIN_BYTES:-0}
NOW=$(date +%s)

RATE_MBPS="-"
ETA="-"
if [[ -f "${STATE}" ]]; then
    read -r PREV_T PREV_BYTES < "${STATE}" 2>/dev/null || { PREV_T=$NOW; PREV_BYTES=$CHAIN_BYTES; }
    DT=$((NOW - PREV_T))
    DB=$((CHAIN_BYTES - PREV_BYTES))
    if (( DT >= 5 && DB > 0 )); then
        RATE_MBPS=$(python3 -c "print(f'{${DB}/${DT}/1048576:.1f}')")
        REMAINING=$((TARGET_GB * 1073741824 - CHAIN_BYTES))
        if (( REMAINING > 0 )); then
            ETA_SECS=$(python3 -c "print(int(${REMAINING}/(${DB}/${DT})))")
            HRS=$((ETA_SECS / 3600)); MIN=$(((ETA_SECS % 3600) / 60))
            ETA="${HRS}h ${MIN}m"
        fi
    fi
fi
echo "${NOW} ${CHAIN_BYTES}" > "${STATE}"

CHAIN_GB=$(python3 -c "print(f'{${CHAIN_BYTES}/1073741824:.1f}')")

# ─── render ──────────────────────────────────────────────────────────────────
clear 2>/dev/null || true
echo "${BOLD}bsc-geth sync dashboard${RST}    $(date +'%Y-%m-%d %H:%M:%S %Z')"
echo "════════════════════════════════════════════════════════════════════"
case "${SVC}" in
    active)  STATE_COLOR=$GREEN ;;
    failed)  STATE_COLOR=$RED ;;
    *)       STATE_COLOR=$YELL ;;
esac
printf "  service   ${STATE_COLOR}%s${RST}  (pid %s · uptime %s · rss %s GB)\n" \
    "${SVC}" "${PID}" "${ETIME}" "${RSS_GB}"

case "${PEERS}" in
    0|1|2|3|4|5) PEER_COLOR=$RED ;;
    *) PEER_COLOR=$GREEN ;;
esac
printf "  peers     ${PEER_COLOR}%s${RST}  /80\n" "${PEERS}"
echo

if [[ "${STATE_TAG}" == "done" ]]; then
    HEAD=$(rpc eth_blockNumber '[]' | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))' 2>/dev/null)
    echo "  ${GREEN}${BOLD}✅ FULLY SYNCED${RST}    current head: block ${HEAD}"
    echo
elif [[ "${STATE_TAG}" == "ok" ]]; then
    IFS='|' read -r _ CB HB SA SS SB SAB SSB SBB HBB HTN <<< "${PARSED}"
    PCT=$(python3 -c "print(f'{${CB}/${HB}*100:.2f}' if ${HB} else '0.00')")
    echo "  ${BOLD}headers${RST}     ${CYAN}${CB}${RST} / ${HB}  ${DIM}(${PCT}%)${RST}"
    SAB_G=$(python3 -c "print(f'{${SAB}/1e9:5.2f}')")
    SSB_G=$(python3 -c "print(f'{${SSB}/1e9:5.2f}')")
    SBB_M=$(python3 -c "print(f'{${SBB}/1e6:5.1f}')")
    printf "  ${BOLD}state${RST}       accts=%s (%s GB)  storage=%s (%s GB)  bytecode=%s (%s MB)\n" \
        "${SA}" "${SAB_G}" "${SS}" "${SSB_G}" "${SB}" "${SBB_M}"
    if (( HBB > 0 || HTN > 0 )); then
        printf "  ${BOLD}healing${RST}     bytecode=%s  trienodes=%s   ${DIM}(near-tip)${RST}\n" "${HBB}" "${HTN}"
    fi
    echo
else
    echo "  ${YELL}${STATE_TAG}${RST} — RPC didn't return a sync object (service may not be ready)"
    echo
fi

# ─── disk + ETA ──────────────────────────────────────────────────────────────
DISK_PCT=$(df -B1 /data | awk 'NR==2{print int($3*100/$2)}')
DISK_FREE=$(df -h /data | awk 'NR==2{print $4}')
echo "  ${BOLD}chaindata${RST}   ${CHAIN_GB} GB  ${DIM}(target ~${TARGET_GB} GB pruned)${RST}"
printf "  ${BOLD}rate${RST}        %s MB/s  ${BOLD}eta${RST}  %s\n" "${RATE_MBPS}" "${ETA}"
printf "  ${BOLD}disk /data${RST}  ${DISK_FREE} free  ${DIM}(%s%% used)${RST}\n" "${DISK_PCT}"
echo
echo "${DIM}refresh:  watch -n 5 ${0}${RST}"
