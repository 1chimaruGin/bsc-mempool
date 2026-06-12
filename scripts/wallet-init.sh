#!/usr/bin/env bash
# Generate a fresh BSC EOA for live trading. Prints the address ONCE,
# saves the private key to /data/bsc-meme-mev/.trader_wallet (mode 600),
# and exits.
#
# Safety:
#   - Aborts if the keyfile already exists (no accidental overwrite).
#   - Uses Python's `secrets` module (CSPRNG) — never user-input entropy.
#   - Private key is never echoed to stdout, only written to the file.
#   - File permissions enforced (chmod 600, owner-only read).
#
# Usage:
#   scripts/wallet-init.sh                  # generate
#   scripts/wallet-init.sh --show-address   # show existing wallet's address only
set -euo pipefail

KEYFILE="/data/bsc-meme-mev/.trader_wallet"

show_only=0
if [[ "${1:-}" == "--show-address" ]]; then
    show_only=1
fi

if [[ $show_only -eq 0 && -e "$KEYFILE" ]]; then
    echo "ERROR: $KEYFILE already exists. Refusing to overwrite." >&2
    echo "       To generate a new wallet, first move the existing one:" >&2
    echo "       mv $KEYFILE $KEYFILE.bak.\$(date -u +%Y%m%dT%H%M%SZ)" >&2
    exit 1
fi

# Check for eth_keys (preferred) or install fallback
if ! python3 -c "import eth_keys" 2>/dev/null; then
    echo "[wallet-init] installing eth_keys (one-time)…"
    pip3 install --quiet --break-system-packages eth_keys 2>/dev/null \
        || pip3 install --quiet eth_keys
fi

if [[ $show_only -eq 1 ]]; then
    if [[ ! -e "$KEYFILE" ]]; then
        echo "ERROR: no wallet at $KEYFILE" >&2
        exit 1
    fi
    python3 - <<PY
from eth_keys import keys
with open("$KEYFILE") as f:
    pk_hex = f.read().strip()
pk = keys.PrivateKey(bytes.fromhex(pk_hex.removeprefix("0x")))
print(f"Address: {pk.public_key.to_checksum_address()}")
PY
    exit 0
fi

# Generate
python3 - <<PY
import os, secrets, stat
from eth_keys import keys

# 32 bytes of cryptographic randomness
pk_bytes = secrets.token_bytes(32)

# Sanity: re-derive the address twice and confirm consistency
pk = keys.PrivateKey(pk_bytes)
addr1 = pk.public_key.to_checksum_address()
pk2 = keys.PrivateKey(pk_bytes)
addr2 = pk2.public_key.to_checksum_address()
assert addr1 == addr2, "key/address derivation inconsistent — aborting"

# Write key file with 600 perms BEFORE writing content (atomic-ish)
fd = os.open("$KEYFILE", os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o600)
try:
    os.write(fd, pk_bytes.hex().encode())
finally:
    os.close(fd)
os.chmod("$KEYFILE", 0o600)

print()
print("=" * 60)
print("  TRADER WALLET CREATED")
print("=" * 60)
print(f"  Address       : {addr1}")
print(f"  Keyfile       : $KEYFILE  (mode 600)")
print(f"  Key length    : {len(pk_bytes)} bytes")
print()
print("  NEXT: fund this address with ~\$100 of BNB from your own wallet.")
print("  Verify the address by re-running:")
print("    scripts/wallet-init.sh --show-address")
print()
print("  The private key is on disk only. NEVER paste it anywhere.")
print("  Back it up offline (USB, paper) if you want recovery insurance.")
print("=" * 60)
PY
