#!/usr/bin/env bash
# Install bsc-geth v1.7.3 (Apr 2026 release, post-Fermi) for BSC mainnet.
#
# Strategy: snap-sync from peers. Slower than snapshot-restore (~24-48h
# vs ~6-12h) but safer on a single 1.7 TB RAID1 volume — snap-sync's
# peak disk = final synced size (~1.5 TB pruned), no intermediate
# decompression balloon.
#
# Usage:  bash scripts/install-bsc-geth.sh
#         (run as root; this script does not call sudo internally.)
#
# Steps performed:
#   1. Download bsc-geth v1.7.3 binary → /usr/local/bin/bsc-geth
#   2. Download + extract mainnet config bundle → conf/
#   3. Initialize chaindata from genesis.json
#   4. Install systemd unit
#   5. Enable + start the service
#
# After this script returns, follow sync progress with:
#   journalctl -u bsc-geth.service -f
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

BIN=/usr/local/bin/bsc-geth
DATA_DIR=/data/bsc-meme-mev/bsc-geth/data
CONF_DIR=/data/bsc-meme-mev/bsc-geth/conf

# Pinned version — DO NOT auto-upgrade silently; client upgrades on BSC need
# hardfork awareness. Bump this number after reading release notes.
GETH_VERSION="v1.7.3"
GETH_BIN_URL="https://github.com/bnb-chain/bsc/releases/download/${GETH_VERSION}/geth_linux"
GENESIS_URL="https://github.com/bnb-chain/bsc/releases/download/${GETH_VERSION}/mainnet.zip"

mkdir -p "${DATA_DIR}" "${CONF_DIR}"

# ─── Phase 1: binary ──────────────────────────────────────────────────────────
if [[ -x "${BIN}" ]] && "${BIN}" version 2>/dev/null | grep -q "${GETH_VERSION#v}"; then
    echo "[1/5] bsc-geth ${GETH_VERSION} already installed at ${BIN}; skipping download"
else
    echo "[1/5] downloading bsc-geth ${GETH_VERSION} (~117 MB)..."
    curl -fSL --retry 3 --retry-delay 5 -o "${BIN}.new" "${GETH_BIN_URL}"
    chmod +x "${BIN}.new"
    # Quick sanity-check the new binary before replacing in-place.
    "${BIN}.new" version >/dev/null 2>&1 \
        || { echo "downloaded binary fails 'version' check" >&2; exit 2; }
    mv -f "${BIN}.new" "${BIN}"
    echo "    installed:"
    "${BIN}" version 2>/dev/null | head -4 | sed 's/^/      /'
fi

# ─── Phase 2: genesis bundle ──────────────────────────────────────────────────
# Bundle extracts into a `mainnet/` subdir — we then flatten it up to CONF_DIR
# for stable paths regardless of zip layout changes between releases.
if [[ -f "${CONF_DIR}/genesis.json" && -f "${CONF_DIR}/config.toml" ]]; then
    echo "[2/5] mainnet conf bundle already present at ${CONF_DIR}; skipping"
else
    echo "[2/5] downloading mainnet conf bundle..."
    TMPZIP=$(mktemp --suffix=.zip)
    trap 'rm -f "${TMPZIP}"' EXIT
    curl -fSL --retry 3 --retry-delay 5 -o "${TMPZIP}" "${GENESIS_URL}"
    TMPDIR=$(mktemp -d)
    unzip -o "${TMPZIP}" -d "${TMPDIR}" >/dev/null
    # Flatten: copy genesis.json + config.toml into CONF_DIR regardless of
    # whether the zip used a nested "mainnet/" prefix.
    find "${TMPDIR}" -name 'genesis.json' -exec cp {} "${CONF_DIR}/" \;
    find "${TMPDIR}" -name 'config.toml' -exec cp {} "${CONF_DIR}/" \;
    rm -rf "${TMPDIR}" "${TMPZIP}"
    trap - EXIT
    ls -la "${CONF_DIR}/" | head -5 | sed 's/^/      /'
fi

# ─── Phase 3: init chaindata (no-op if already initialised) ───────────────────
if [[ -d "${DATA_DIR}/geth/chaindata" ]] && [[ -n "$(ls -A "${DATA_DIR}/geth/chaindata" 2>/dev/null)" ]]; then
    echo "[3/5] chaindata at ${DATA_DIR}/geth/chaindata already populated; skipping init"
else
    echo "[3/5] initialising chaindata from genesis.json..."
    "${BIN}" --datadir "${DATA_DIR}" init "${CONF_DIR}/genesis.json" 2>&1 | tail -5 | sed 's/^/      /'
fi

# ─── Phase 4: systemd unit ────────────────────────────────────────────────────
SVC_SRC=/data/bsc-meme-mev/scripts/systemd/bsc-geth.service
SVC_DST=/etc/systemd/system/bsc-geth.service
echo "[4/5] installing systemd unit → ${SVC_DST}"
if [[ ! -f "${SVC_SRC}" ]]; then
    echo "    missing source unit at ${SVC_SRC}" >&2
    exit 3
fi
cp "${SVC_SRC}" "${SVC_DST}"
systemctl daemon-reload

# ─── Phase 5: enable + start ──────────────────────────────────────────────────
echo "[5/5] enabling + starting bsc-geth.service"
systemctl enable bsc-geth.service 2>&1 | sed 's/^/      /'
systemctl start bsc-geth.service
sleep 3
systemctl --no-pager -l status bsc-geth.service | head -20

cat <<EOF

── done ──────────────────────────────────────────────────────────────
follow sync progress:
    journalctl -u bsc-geth.service -f
check head block:
    curl -s -X POST http://127.0.0.1:8545 \\
        -H 'Content-Type: application/json' \\
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \\
        | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))'

initial snap sync runtime estimate on this box: ~24-48h (peer-dependent).
when head matches https://bscscan.com/, you're done.
EOF
