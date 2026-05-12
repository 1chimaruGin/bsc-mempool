#!/usr/bin/env bash
# Stream-restore the bnb-chain pruned PBSS snapshot directly into the
# bsc-geth data dir, with a disk-space watchdog that aborts if /data
# free space drops below the safety floor.
#
# Why streaming: the snapshot is ~1.5 TB compressed; download-then-extract
# would peak at ~3 TB which doesn't fit on our 1.7 TB RAID1 volume.
# `curl | lz4 -d | tar -xf` keeps peak = just the extracted size.
#
# Usage:  bash scripts/restore-snapshot.sh
#         (run as root; non-interactive; logs to bsc-snapshot-restore.log)
#
# Exit codes: 0 OK, 1 args/env error, 2 disk watchdog aborted, 3 stream failed.
set -uo pipefail

SNAP_URL="https://pub-c0627345c16f47ab858c9469133073a8.r2.dev/mainnet-geth-pbss-base-90778787.tar.lz4"
DATA_DIR=/data/bsc-meme-mev/bsc-geth/data
LOG=/data/bsc-meme-mev/bsc-snapshot-restore.log
STATE=/tmp/bsc-restore-state          # pid file + ongoing flag
SAFETY_FREE_GB=50                     # abort if /data free drops below this

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

for tool in curl lz4 tar du df; do
    command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

mkdir -p "${DATA_DIR}"

# Sanity check: the data dir's `geth/` subdir should NOT already be populated.
if [[ -d "${DATA_DIR}/geth/chaindata" ]] && [[ -n "$(ls -A "${DATA_DIR}/geth/chaindata" 2>/dev/null)" ]]; then
    echo "ERROR: ${DATA_DIR}/geth/chaindata is non-empty — refusing to overwrite." >&2
    echo "  rm -rf ${DATA_DIR}/geth   # if you really want to wipe and re-restore" >&2
    exit 1
fi

echo "▶ snapshot restore starting" | tee "${LOG}"
echo "  snapshot:  ${SNAP_URL}" | tee -a "${LOG}"
echo "  target:    ${DATA_DIR}/geth/" | tee -a "${LOG}"
echo "  safety:    abort if /data free < ${SAFETY_FREE_GB} GB" | tee -a "${LOG}"
echo "  started:   $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "${LOG}"
df -h /data | tail -1 | tee -a "${LOG}"
echo "" | tee -a "${LOG}"

# Start the streaming pipeline. We use a FIFO + named curl PID so the
# watchdog can SIGINT it cleanly if we have to abort. set -e is NOT on
# because we want to react to failures, not bubble them up via traps.

# Run the pipeline in a subshell so we can capture its overall PID. Inside
# the subshell: curl streams the lz4 to stdout, lz4 decompresses, tar
# extracts. The strip-components=2 strips the `server/data-seed/` prefix
# inside the tar so files land as `geth/...` directly under DATA_DIR.
(
    curl -fSL --retry 5 --retry-delay 30 --retry-connrefused "${SNAP_URL}" \
      | lz4 -d - \
      | tar -xf - --strip-components=2 -C "${DATA_DIR}"
) >> "${LOG}" 2>&1 &
PIPELINE_PID=$!
echo "  pipeline pid: ${PIPELINE_PID}" | tee -a "${LOG}"
echo "$$ ${PIPELINE_PID}" > "${STATE}"

# Disk watchdog. Polls every 30 s. If /data free < SAFETY_FREE_GB or the
# pipeline went away, exits the loop. If unsafe, SIGINT the pipeline group.
WATCHDOG_REASON=""
while kill -0 "${PIPELINE_PID}" 2>/dev/null; do
    sleep 30
    FREE_GB=$(df --output=avail -BG /data | tail -1 | tr -d ' G')
    CHAIN_GB=$(du -sh "${DATA_DIR}/geth" 2>/dev/null | cut -f1)
    NOW=$(date -u +%H:%M:%SZ)
    echo "${NOW}  /data free=${FREE_GB} GB  chaindata=${CHAIN_GB}" >> "${LOG}"
    if (( FREE_GB < SAFETY_FREE_GB )); then
        WATCHDOG_REASON="free=${FREE_GB} GB < ${SAFETY_FREE_GB} GB"
        echo "${NOW}  WATCHDOG ABORT  ${WATCHDOG_REASON}" | tee -a "${LOG}"
        # Kill the whole process group, not just curl.
        pkill -INT -P ${PIPELINE_PID} 2>/dev/null
        kill -INT ${PIPELINE_PID} 2>/dev/null
        sleep 5
        pkill -KILL -P ${PIPELINE_PID} 2>/dev/null
        kill -KILL ${PIPELINE_PID} 2>/dev/null
        rm -f "${STATE}"
        exit 2
    fi
done

# Pipeline finished one way or the other. Reap exit code.
wait "${PIPELINE_PID}"
PIPELINE_RC=$?
rm -f "${STATE}"

echo "" | tee -a "${LOG}"
echo "▶ pipeline exited rc=${PIPELINE_RC} at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "${LOG}"
df -h /data | tail -1 | tee -a "${LOG}"
du -sh "${DATA_DIR}/geth"/* 2>/dev/null | tee -a "${LOG}"

if (( PIPELINE_RC != 0 )); then
    echo "ERROR: pipeline non-zero exit; data dir may be incomplete." | tee -a "${LOG}"
    exit 3
fi

echo "▶ restore complete." | tee -a "${LOG}"
echo "  next:  systemctl start bsc-geth.service" | tee -a "${LOG}"
exit 0
