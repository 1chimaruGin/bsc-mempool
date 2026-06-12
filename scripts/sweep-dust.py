#!/usr/bin/env python3
"""Sweep all current token positions: for each token in our wallet with
non-zero balance, try to sell at any price via PancakeSwap V2 or Four.Meme.
Anything neither route accepts is reported as ABANDONED — those positions
are stuck (token has no curve and no V2 pair).

USE WITH CAUTION: this issues REAL on-chain swaps from your wallet. Each
sell costs ~1 gwei × ~120k gas ≈ $0.08, regardless of whether it succeeds.

Usage:
  scripts/sweep-dust.py --dry-run   # ONLY analyze; submit nothing
  scripts/sweep-dust.py             # actually sell everything possible
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.request

# ── env ────────────────────────────────────────────────────────────────────
def load_env(p="/data/bsc-meme-mev/.env"):
    out = {}
    if not os.path.exists(p):
        return out
    for line in open(p):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


ENV = load_env()
LOCAL = "http://127.0.0.1:8545"
BLOCKRAZOR_URL = ENV.get("BLOCKRAZOR_RPC_URL", "https://bsc.blockrazor.xyz")
BLOCKRAZOR_AUTH = ENV.get("BLOCKRAZOR_AUTH_KEY", "")
WALLET_ADDR = ENV.get("WALLET_ADDRESS", "").lower()
WALLET_KEY = ENV.get("WALLET_PRIVATE_KEY", "").removeprefix("0x")

WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
PCS_ROUTER = "0x10ed43c718714eb63d5aa57b78b54704e256024e"
FOURMEME = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
CHAIN_ID = 56
UA = "Mozilla/5.0 (X11; Linux x86_64) bsc-meme-mev/0.1"
LEDGER = "/data/bsc-meme-mev/trader_live/live_log.csv"


def rpc(url, method, params, headers=None):
    body = json.dumps(
        {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    ).encode()
    h = {"Content-Type": "application/json", "User-Agent": UA, **(headers or {})}
    req = urllib.request.Request(url, body, h)
    try:
        return json.loads(urllib.request.urlopen(req, timeout=8).read())
    except Exception as e:
        return {"error": str(e)}


def hex_to_int(h):
    if not h or h == "0x":
        return 0
    return int(h, 16) if isinstance(h, str) else int(h)


# ── on-chain reads ─────────────────────────────────────────────────────────
def get_pair(token):
    sel = "0xe6a43905"
    data = sel + WBNB[2:].rjust(64, "0") + token[2:].rjust(64, "0")
    r = rpc(LOCAL, "eth_call", [{"to": FACTORY, "data": data}, "latest"])
    if "result" not in r:
        return None
    addr = "0x" + r["result"][-40:]
    return addr if int(addr, 16) != 0 else None


def balance_of(token, who):
    sel = "0x70a08231"
    data = sel + who[2:].rjust(64, "0")
    r = rpc(LOCAL, "eth_call", [{"to": token, "data": data}, "latest"])
    return hex_to_int(r.get("result"))


def allowance(token, owner, spender):
    sel = "0xdd62ed3e"
    data = sel + owner[2:].rjust(64, "0") + spender[2:].rjust(64, "0")
    r = rpc(LOCAL, "eth_call", [{"to": token, "data": data}, "latest"])
    return hex_to_int(r.get("result"))


def fourmeme_would_sell(token, amount):
    """Dry-run sellToken via eth_call. True = Four.Meme accepts the sell.

    The probe runs without our approval set, so Four.Meme's internal
    transferFrom reverts with `ERC20: insufficient allowance` — that
    revert is proof the token IS on the curve (we got past Four.Meme's
    dispatch into the ERC20 transfer), so we treat it as YES. The real
    sell path approves first. Tokens NOT on Four.Meme revert with
    `Invalid token` (or similar) — those stay NO.
    """
    sel = "0xf464e7db"
    data = sel + token[2:].rjust(64, "0") + format(amount, "064x")
    r = rpc(LOCAL, "eth_call", [{
        "from": WALLET_ADDR, "to": FOURMEME, "data": data,
    }, "latest"])
    if "result" in r and "error" not in r:
        return True
    msg = (r.get("error") or {}).get("message", "")
    return "insufficient allowance" in msg


# ── collect held tokens from ledger ────────────────────────────────────────
def held_tokens_from_ledger():
    if not os.path.exists(LEDGER):
        return set()
    tokens = set()
    for row in csv.DictReader(open(LEDGER)):
        if row.get("broadcast") != "true":
            continue
        if row.get("limit_skip_reason"):
            continue
        t = row.get("token_address", "")
        if t.startswith("0x") and len(t) == 42:
            tokens.add(t.lower())
    return tokens


# ── tx construction + sending (requires eth_account) ────────────────────────
def ensure_deps():
    try:
        from eth_account import Account
        from eth_account.signers.local import LocalAccount  # noqa
    except ImportError:
        print("installing eth_account…", file=sys.stderr)
        import subprocess
        subprocess.run(
            ["pip3", "install", "-q", "--break-system-packages", "eth-account"],
            check=False,
        )


def _send(signed_raw_hex):
    """Send raw tx via BlockRazor (writeable). Returns tx_hash or raises."""
    r = rpc(
        BLOCKRAZOR_URL,
        "eth_sendRawTransaction",
        [signed_raw_hex],
        headers={"Authorization": BLOCKRAZOR_AUTH} if BLOCKRAZOR_AUTH else None,
    )
    if "error" in r:
        raise RuntimeError(f"BlockRazor: {r['error']}")
    return r["result"]


def _next_nonce():
    r = rpc(LOCAL, "eth_getTransactionCount", [WALLET_ADDR, "pending"])
    return hex_to_int(r["result"])


def _gas_wei():
    """Floor 2 gwei — BlockRazor rejects sub-1-gwei replays with a
    "must be >1.1× higher" error when their internal mempool has a
    stale tx at the same nonce. 2 gwei gives headroom."""
    r = rpc(LOCAL, "eth_gasPrice", [])
    return max(hex_to_int(r["result"]), 2_000_000_000)


def build_and_send(to, data_hex, value_wei, gas_limit, dry_run):
    """Build a signed EIP-1559 tx and submit via BlockRazor."""
    from eth_account import Account
    from eth_utils import to_checksum_address
    nonce = _next_nonce()
    gas = _gas_wei()
    tx = {
        "to": to_checksum_address(to),
        "value": value_wei,
        "data": data_hex,
        "gas": gas_limit,
        "maxFeePerGas": gas,
        "maxPriorityFeePerGas": gas,
        "nonce": nonce,
        "chainId": CHAIN_ID,
        "type": 2,
    }
    if dry_run:
        print(f"    [dry-run] would submit nonce={nonce} gas={gas//1_000_000_000} gwei")
        return None
    acct = Account.from_key(WALLET_KEY)
    signed = acct.sign_transaction(tx)
    raw_hex = "0x" + signed.raw_transaction.hex()
    tx_hash = _send(raw_hex)
    print(f"    SENT: {tx_hash}")
    return tx_hash


# ── sweep ──────────────────────────────────────────────────────────────────
def sweep(dry_run):
    if not WALLET_ADDR or not WALLET_KEY:
        print("ERROR: WALLET_ADDRESS / WALLET_PRIVATE_KEY missing in .env",
              file=sys.stderr)
        sys.exit(1)
    ensure_deps()
    tokens = held_tokens_from_ledger()
    print(f"\n=== DUST SWEEP {'(dry-run)' if dry_run else '(LIVE)'} ===")
    print(f"  wallet  : {WALLET_ADDR}")
    print(f"  tokens  : {len(tokens)} from ledger\n")

    actions = {"sell_v2": 0, "sell_fm": 0, "abandoned": 0, "empty": 0}
    deadline = int(time.time()) + 60

    for tok in sorted(tokens):
        bal = balance_of(tok, WALLET_ADDR)
        if bal == 0:
            print(f"  {tok}  empty (already sold or never held)")
            actions["empty"] += 1
            continue
        pair = get_pair(tok)
        if pair:
            print(f"  {tok}  bal={bal:.2e}  → V2 sell (pair {pair[:14]}…)")
            actions["sell_v2"] += 1
            # check approval, approve if needed, then sell
            allow = allowance(tok, WALLET_ADDR, PCS_ROUTER)
            if allow < bal:
                approve_data = (
                    "0x095ea7b3"
                    + PCS_ROUTER[2:].rjust(64, "0")
                    + "f" * 64
                )
                build_and_send(tok, approve_data, 0, 60_000, dry_run)
                if not dry_run:
                    time.sleep(1.5)
            # build swapExactTokensForETHSupportingFeeOnTransferTokens(amountIn, amountOutMin, path, to, deadline)
            sel = "0x791ac947"
            sell_data = (
                sel
                + format(bal, "064x")
                + format(0, "064x")
                + format(0xa0, "064x")  # path offset
                + WALLET_ADDR[2:].rjust(64, "0")
                + format(deadline, "064x")
                + format(2, "064x")  # path length
                + tok[2:].rjust(64, "0")
                + WBNB[2:].rjust(64, "0")
            )
            build_and_send(PCS_ROUTER, sell_data, 0, 300_000, dry_run)
        elif fourmeme_would_sell(tok, bal):
            print(f"  {tok}  bal={bal:.2e}  → Four.Meme sell")
            actions["sell_fm"] += 1
            allow = allowance(tok, WALLET_ADDR, FOURMEME)
            if allow < bal:
                approve_data = (
                    "0x095ea7b3"
                    + FOURMEME[2:].rjust(64, "0")
                    + "f" * 64
                )
                build_and_send(tok, approve_data, 0, 60_000, dry_run)
                if not dry_run:
                    time.sleep(1.5)
            sell_data = "0xf464e7db" + tok[2:].rjust(64, "0") + format(bal, "064x")
            build_and_send(FOURMEME, sell_data, 0, 250_000, dry_run)
            if not dry_run:
                time.sleep(3.0)  # let both txs mine before next token
        else:
            print(f"  {tok}  bal={bal:.2e}  ✗ ABANDONED (no V2, Four.Meme rejects)")
            actions["abandoned"] += 1

    print()
    print("=== summary ===")
    for k, v in actions.items():
        print(f"  {k:12} : {v}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="analyze only; submit nothing")
    args = ap.parse_args()
    sweep(args.dry_run)
