#!/usr/bin/env bash
# Wait for the running snapshot-restore.sh (PID arg) to finish, then auto-launch
# sync-complete-watch.sh.  Needed because the running snapshot-restore.sh was
# spawned BEFORE we appended the auto-chain logic; this wrapper provides the
# bridge without restarting the download.
set -uo pipefail

WATCHER=/data/bsc-meme-mev/scripts/sync-complete-watch.sh
LOG=/data/bsc-meme-mev/post-restore-chain.log
PID=${1:-}

exec >> "${LOG}" 2>&1
echo "═══ post-restore-chain start: $(date -u +%Y-%m-%dT%H:%M:%SZ)  watch_pid=${PID} ═══"

if [[ -z "${PID}" ]]; then
    echo "ERROR: pid arg required" >&2
    exit 1
fi

# tail until that PID exits
while kill -0 "${PID}" 2>/dev/null; do
    sleep 30
done
echo "snapshot-restore.sh pid=${PID} exited at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Sanity: snapshot-restore should have left bsc-geth.service active
sleep 5
if systemctl is-active --quiet bsc-geth.service; then
    echo "bsc-geth.service active — launching sync-complete-watch"
else
    echo "ERROR: bsc-geth.service inactive after snapshot-restore; aborting chain" >&2
    systemctl --no-pager status bsc-geth.service | head -25
    exit 2
fi

setsid bash "${WATCHER}" </dev/null >> /data/bsc-meme-mev/sync-complete.log 2>&1 < /dev/null &
disown
echo "sync-complete-watch launched"
echo "═══ post-restore-chain DONE ═══"
