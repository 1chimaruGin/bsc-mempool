#!/usr/bin/env bash
# Restore BSC mainnet from official pruned PBSS snapshot using the bnb-chain
# fetch-snapshot.sh helper (auto-detects --strip-components, includes the
# blocks-pruneancient marker that tells bsc-geth ancient segments are pruned).
#
# Sequence:
#   1. Confirm data dir empty (no `bsc-geth init` first — snapshot is authoritative)
#   2. fetch-snapshot.sh -d -e -c -p --auto-delete
#   3. systemctl start bsc-geth.service
#   4. Probe sync state
#
# Usage: bash scripts/snapshot-restore.sh
set -uo pipefail

# NOTE: caller redirects stdout/stderr to log; do not double-tee from here.

# Base CSV name; fetch-snapshot.sh -p appends "-pruneancient" itself.
CSV_NAME=mainnet-geth-pbss-20260408
DOWNLOAD_DIR=/data/bsc-meme-mev/snapshot-cache
EXTRACT_DIR=/data/bsc-meme-mev/bsc-geth/data
FETCH=${DOWNLOAD_DIR}/fetch-snapshot.sh

echo "═══ snapshot-restore start: $(date -u +%Y-%m-%dT%H:%M:%SZ) ═══"

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

# ─── 1. sanity ─────────────────────────────────────────────────────────────
if [[ ! -x "${FETCH}" ]]; then
    echo "ERROR: ${FETCH} missing or not executable" >&2
    exit 2
fi
if [[ ! -f "${DOWNLOAD_DIR}/${CSV_NAME}-pruneancient.csv" ]]; then
    echo "ERROR: ${DOWNLOAD_DIR}/${CSV_NAME}-pruneancient.csv missing" >&2
    exit 2
fi
if [[ -d "${EXTRACT_DIR}/geth/chaindata" ]] && [[ -n "$(ls -A "${EXTRACT_DIR}/geth/chaindata" 2>/dev/null)" ]]; then
    echo "ERROR: ${EXTRACT_DIR}/geth/chaindata non-empty; wipe first" >&2
    exit 3
fi

systemctl is-active --quiet bsc-geth.service && {
    echo "ERROR: bsc-geth.service is active; stop it before restore" >&2
    exit 4
}

echo "--- disk before ---"
df -h /data | tail -1

# ─── 2. download + checksum + extract + auto-delete archive ────────────────
# -d download, -e extract, -c checksum, -p prune-ancient (uses pruned CSV),
# --auto-delete removes each archive immediately after successful extract.
echo "--- fetch-snapshot.sh (download + extract + checksum, ~hours) ---"
cd "${DOWNLOAD_DIR}"
bash "${FETCH}" -d -e -c -p --auto-delete \
    -D "${DOWNLOAD_DIR}" \
    -E "${EXTRACT_DIR}" \
    "${CSV_NAME}"
FETCH_RC=$?
if (( FETCH_RC != 0 )); then
    echo "ERROR: fetch-snapshot.sh rc=${FETCH_RC}" >&2
    exit 5
fi

echo "--- disk after extract ---"
df -h /data | tail -1
echo "--- chaindata layout ---"
ls "${EXTRACT_DIR}/geth/" 2>/dev/null
du -sh "${EXTRACT_DIR}/geth"/* 2>/dev/null | head

# ─── 3. start bsc-geth ────────────────────────────────────────────────────
echo "--- start bsc-geth.service ---"
systemctl daemon-reload
systemctl start bsc-geth.service
sleep 8
systemctl is-active bsc-geth.service
systemctl --no-pager -l status bsc-geth.service | head -15

# ─── 4. sync probe ────────────────────────────────────────────────────────
echo "--- waiting 30 s for RPC, then probing ---"
sleep 30
echo "--- eth_blockNumber ---"
curl -s -X POST http://127.0.0.1:8545 -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["result"]; print(int(r,16))'
echo "--- eth_syncing ---"
curl -s -X POST http://127.0.0.1:8545 -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
    | python3 -m json.tool | head -25

echo "═══ snapshot-restore DONE: $(date -u +%Y-%m-%dT%H:%M:%SZ) ═══"

# Auto-chain: catch-up sync → install-runner → verify-all
echo "--- launching sync-complete-watch in background ---"
setsid bash /data/bsc-meme-mev/scripts/sync-complete-watch.sh </dev/null >> /data/bsc-meme-mev/sync-complete.log 2>&1 < /dev/null &
disown
echo "sync-complete-watch pid=$!"
