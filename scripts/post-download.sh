#!/usr/bin/env bash
# Runs after aria2c finishes downloading the bnb-chain pruned PBSS snapshot.
# Extracts to the bsc-geth data dir, deletes the archive to free disk, starts
# the node, and prints sync state.
#
# Usage: bash scripts/post-download.sh
set -uo pipefail

ARCHIVE=/data/bsc-meme-mev/snapshot-cache/mainnet-geth-pbss-base-90778787.tar.lz4
DATA_DIR=/data/bsc-meme-mev/bsc-geth/data
LOG=/data/bsc-meme-mev/post-download.log
exec > >(tee -a "${LOG}") 2>&1

echo "═══ post-download.sh start: $(date -u +%Y-%m-%dT%H:%M:%SZ) ═══"

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

# ─── 1. sanity-check archive ────────────────────────────────────────────────
if [[ ! -f "${ARCHIVE}" ]]; then
    echo "ERROR: archive missing at ${ARCHIVE}" >&2
    exit 2
fi
echo "--- archive size on disk ---"
ls -la "${ARCHIVE}"
echo "--- expected size: 1620711258580 bytes (~1.5 TB) ---"
ACT_SIZE=$(stat -c '%s' "${ARCHIVE}")
EXP_SIZE=1620711258580
if (( ACT_SIZE != EXP_SIZE )); then
    echo "WARN: actual size ${ACT_SIZE} != expected ${EXP_SIZE}; will still try"
fi

# ─── 2. ensure data dir is empty (no leftover from earlier attempts) ───────
mkdir -p "${DATA_DIR}"
if [[ -d "${DATA_DIR}/geth/chaindata" ]] && [[ -n "$(ls -A "${DATA_DIR}/geth/chaindata" 2>/dev/null)" ]]; then
    echo "ERROR: ${DATA_DIR}/geth/chaindata is non-empty; refusing to overwrite" >&2
    echo "  rm -rf ${DATA_DIR}/geth && re-run" >&2
    exit 3
fi

# ─── 3. stream-extract ──────────────────────────────────────────────────────
echo "--- stream-extract archive → ${DATA_DIR} ---"
echo "  using --strip-components=2 (strips server/data-seed/ prefix)"
echo "  disk free BEFORE extract:"
df -h /data | tail -1
lz4 -dc "${ARCHIVE}" | tar -xf - --strip-components=2 -C "${DATA_DIR}"
EXTRACT_RC=$?
if (( EXTRACT_RC != 0 )); then
    echo "ERROR: extraction failed rc=${EXTRACT_RC}" >&2
    exit 4
fi
echo "  disk free AFTER extract:"
df -h /data | tail -1
echo "  extracted contents:"
du -sh "${DATA_DIR}/geth"/* 2>/dev/null | head

# ─── 4. delete archive to free 1.5 TB ───────────────────────────────────────
echo "--- delete archive to free disk ---"
rm -f "${ARCHIVE}" "${ARCHIVE}.aria2"
df -h /data | tail -1

# ─── 5. start bsc-geth ──────────────────────────────────────────────────────
echo "--- start bsc-geth ---"
systemctl daemon-reload
systemctl start bsc-geth.service
sleep 5
systemctl is-active bsc-geth.service
echo
echo "--- bsc-geth status ---"
systemctl --no-pager -l status bsc-geth.service | head -15

# ─── 6. sync state ──────────────────────────────────────────────────────────
echo "--- waiting 30 s for RPC to be ready, then probing sync state ---"
sleep 30
echo --- head block ---
curl -s -X POST http://127.0.0.1:8545 -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["result"]; print(int(r,16))' 2>/dev/null
echo --- syncing ---
curl -s -X POST http://127.0.0.1:8545 -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
    | python3 -m json.tool 2>/dev/null | head -20
echo
echo "═══ DONE: $(date -u +%Y-%m-%dT%H:%M:%SZ) ═══"
echo
echo "next: monitor sync to tip via"
echo "  watch -n 5 /data/bsc-meme-mev/scripts/sync-status.sh"
