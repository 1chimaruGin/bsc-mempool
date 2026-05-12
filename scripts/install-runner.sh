#!/usr/bin/env bash
# Install the bsc-runner systemd unit. Builds release binary, copies the
# unit, enables it. Use AFTER bsc-geth has finished snap-sync.
#
# Usage:  sudo bash scripts/install-runner.sh
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

REPO=/data/bsc-meme-mev
BIN_DST=${REPO}/target/release/bsc-runner
SVC_SRC=${REPO}/scripts/systemd/bsc-runner.service
SVC_DST=/etc/systemd/system/bsc-runner.service

echo "[1/4] release build"
cd "${REPO}"
/root/.cargo/bin/cargo build --release -p bsc-runner 2>&1 | tail -3
if [[ ! -x "${BIN_DST}" ]]; then
    echo "ERROR: ${BIN_DST} missing after build" >&2
    exit 2
fi
echo "  binary: $(du -h "${BIN_DST}" | cut -f1) at ${BIN_DST}"

echo "[2/4] install systemd unit → ${SVC_DST}"
cp "${SVC_SRC}" "${SVC_DST}"
systemctl daemon-reload

echo "[3/4] enable + start"
systemctl enable bsc-runner.service 2>&1 | tail -1
systemctl start bsc-runner.service
sleep 3
systemctl --no-pager -l status bsc-runner.service | head -15

echo "[4/4] done. follow with:"
echo "    journalctl -u bsc-runner.service -f"
