#!/usr/bin/env bash
# Comprehensive sanity check — runs the user's "check one more time" pass.
#
# Verifies:
#   - All workspace tests still pass
#   - Release binary builds cleanly
#   - bsc-geth.service is active and synced
#   - mempool-runner.service is active (if installed)
#   - All required RPC endpoints respond
#   - Disk has headroom
#   - Recent log lines show no errors
#   - All commits pushed to GitHub
set -uo pipefail

GREEN=$'\e[32m'
RED=$'\e[31m'
YELL=$'\e[33m'
BOLD=$'\e[1m'
RST=$'\e[0m'

pass() { echo "  ${GREEN}✓${RST} $*"; }
fail() { echo "  ${RED}✗${RST} $*"; FAILS=$((FAILS+1)); }
warn() { echo "  ${YELL}⚠${RST} $*"; WARNS=$((WARNS+1)); }
hdr()  { echo; echo "${BOLD}── $* ──${RST}"; }

FAILS=0
WARNS=0

cd /data/bsc-meme-mev

hdr "1. Workspace tests"
TEST_OUT=$(/root/.cargo/bin/cargo test --workspace 2>&1 | tail -30)
TOTAL_PASS=$(echo "$TEST_OUT" | grep -cE "^test result: ok\." || true)
TOTAL_FAIL=$(echo "$TEST_OUT" | grep -cE "^test result: FAILED" || true)
if (( TOTAL_FAIL > 0 )); then
    fail "$TOTAL_FAIL test groups FAILED"
    echo "$TEST_OUT" | grep -E "FAILED|test result"
else
    pass "all $TOTAL_PASS test groups passed"
fi

hdr "2. Release build"
if /root/.cargo/bin/cargo build --release -p bsc-runner 2>&1 | tail -2 | grep -q "Finished"; then
    pass "bsc-runner release build clean"
    BIN_SIZE=$(stat -c '%s' /data/bsc-meme-mev/target/release/bsc-runner 2>/dev/null)
    if [[ -n "${BIN_SIZE}" ]]; then
        pass "binary at $(du -h /data/bsc-meme-mev/target/release/bsc-runner | cut -f1)"
    fi
else
    fail "release build failed"
fi

hdr "3. bsc-geth service"
if systemctl is-active --quiet bsc-geth.service; then
    pass "bsc-geth.service: active"
    UPTIME=$(systemctl show -p ActiveEnterTimestamp --value bsc-geth.service | head -1)
    pass "uptime since: ${UPTIME}"
else
    fail "bsc-geth.service: $(systemctl is-active bsc-geth.service 2>/dev/null || echo inactive)"
fi

hdr "4. RPC endpoints"
RPC=http://127.0.0.1:8545
HEAD_HEX=$(curl -s --max-time 5 -X POST "${RPC}" \
    -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result","0x0"))' 2>/dev/null)
if [[ -n "${HEAD_HEX}" && "${HEAD_HEX}" != "0x0" ]]; then
    HEAD=$((HEAD_HEX))
    pass "eth_blockNumber: ${HEAD}"
    # Sync status check
    SYNCING=$(curl -s --max-time 5 -X POST "${RPC}" \
        -H 'Content-Type: application/json' \
        --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
        | python3 -c 'import json,sys; r=json.load(sys.stdin)["result"]; print("done" if r is False else f"{int(r[\"currentBlock\"],16)}/{int(r[\"highestBlock\"],16)}")' 2>/dev/null)
    if [[ "${SYNCING}" == "done" ]]; then
        pass "eth_syncing: caught up to chain tip"
    else
        warn "eth_syncing: ${SYNCING}"
    fi
    # Peer count
    PEER_HEX=$(curl -s --max-time 5 -X POST "${RPC}" \
        -H 'Content-Type: application/json' \
        --data '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}' \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result","0x0"))' 2>/dev/null)
    PEERS=$((PEER_HEX))
    if (( PEERS >= 8 )); then
        pass "peers: ${PEERS}"
    else
        warn "peers: ${PEERS} (low)"
    fi
else
    fail "eth_blockNumber RPC unreachable or returned 0"
fi

hdr "5. IPC socket"
IPC=/data/bsc-meme-mev/bsc-geth/geth.ipc
if [[ -S "${IPC}" ]]; then
    pass "IPC socket present: ${IPC}"
else
    fail "IPC socket missing: ${IPC}"
fi

hdr "6. disk"
DF=$(df -h /data | tail -1)
FREE_GB=$(df --output=avail -BG /data | tail -1 | tr -d ' G')
USED_PCT=$(df --output=pcent /data | tail -1 | tr -d ' %')
echo "  ${DF}"
if (( USED_PCT < 80 )); then
    pass "disk usage ${USED_PCT}% (${FREE_GB} GB free)"
elif (( USED_PCT < 90 )); then
    warn "disk usage ${USED_PCT}% — consider pruning"
else
    fail "disk usage ${USED_PCT}% — critical"
fi

hdr "7. mempool-runner service (optional)"
if systemctl list-unit-files | grep -q '^mempool-runner.service'; then
    if systemctl is-active --quiet mempool-runner.service; then
        pass "mempool-runner.service: active"
    else
        warn "mempool-runner.service: installed but not running"
    fi
else
    warn "mempool-runner.service: not installed (run scripts/install-runner.sh)"
fi

hdr "8. recent errors in journal (last hour)"
ERR_COUNT=$(journalctl --since "1 hour ago" --no-pager 2>/dev/null \
    | grep -cE '\sERROR\s|panicked' || true)
if (( ERR_COUNT == 0 )); then
    pass "no ERROR-level lines in journal"
elif (( ERR_COUNT < 5 )); then
    warn "${ERR_COUNT} ERROR lines in last hour (might be benign restarts)"
else
    fail "${ERR_COUNT} ERROR lines in last hour"
fi

hdr "9. git state"
LOCAL=$(git rev-parse HEAD 2>/dev/null)
REMOTE=$(git ls-remote origin main 2>/dev/null | cut -f1)
if [[ "${LOCAL}" == "${REMOTE}" ]]; then
    pass "git in sync with origin/main (${LOCAL:0:7})"
else
    warn "local HEAD ${LOCAL:0:7} differs from origin/main ${REMOTE:0:7}"
fi

DIRTY=$(git status --porcelain 2>/dev/null | wc -l)
if (( DIRTY == 0 )); then
    pass "working tree clean"
else
    warn "${DIRTY} dirty files"
fi

hdr "summary"
if (( FAILS == 0 && WARNS == 0 )); then
    echo "  ${GREEN}${BOLD}ALL GREEN.${RST} system ready."
elif (( FAILS == 0 )); then
    echo "  ${YELL}${WARNS} warnings${RST}, no failures."
else
    echo "  ${RED}${FAILS} failures${RST}, ${WARNS} warnings."
fi
exit $FAILS
