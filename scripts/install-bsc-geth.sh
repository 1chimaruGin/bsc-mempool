#!/usr/bin/env bash
# Install bsc-geth (official Binance fork of go-ethereum) for BSC mainnet.
# Pruned-mode sync: ~600 GB. Initial sync ~3-7 days on a good box.
#
# Usage:  sudo bash scripts/install-bsc-geth.sh
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

BIN_DIR=/usr/local/bin
DATA_DIR=/data/bsc-meme-mev/bsc-geth
CONFIG_DIR=/data/bsc-meme-mev/bsc-geth/conf
GENESIS_URL=https://github.com/bnb-chain/bsc/releases/latest/download/mainnet.zip
GETH_BIN_URL=$(curl -s https://api.github.com/repos/bnb-chain/bsc/releases/latest \
    | grep -oE 'https://[^"]+geth_linux' | head -1)

if [[ -z "${GETH_BIN_URL}" ]]; then
    echo "could not resolve latest geth_linux release URL — check https://github.com/bnb-chain/bsc/releases" >&2
    exit 2
fi

mkdir -p "${DATA_DIR}" "${CONFIG_DIR}"

echo "[1/4] downloading bsc-geth binary → ${BIN_DIR}/bsc-geth"
curl -fSL -o "${BIN_DIR}/bsc-geth" "${GETH_BIN_URL}"
chmod +x "${BIN_DIR}/bsc-geth"

echo "[2/4] downloading mainnet genesis + config bundle"
TMPZIP=$(mktemp --suffix=.zip)
curl -fSL -o "${TMPZIP}" "${GENESIS_URL}"
unzip -o "${TMPZIP}" -d "${CONFIG_DIR}"
rm -f "${TMPZIP}"

echo "[3/4] initialising BSC mainnet data dir at ${DATA_DIR}/data"
"${BIN_DIR}/bsc-geth" --datadir "${DATA_DIR}/data" init "${CONFIG_DIR}/genesis.json"

echo "[4/4] done. To start sync:"
echo "    cp scripts/bsc-geth.service /etc/systemd/system/"
echo "    systemctl daemon-reload"
echo "    systemctl enable --now bsc-geth.service"
echo
echo "follow sync progress:"
echo "    journalctl -u bsc-geth -f"
echo
echo "Expected sync time: 3-7 days. Use the ETH-side time on other work meanwhile."
