#!/usr/bin/env python3
"""Per-trade PnL for the live trader by reading on-chain Transfer/Withdraw
events. Reconciles wallet BNB in (buys) vs BNB out (sells) per token.

Method:
  For each BUY row in live_log.csv:
    - bnb_in_wei is the BNB we sent
    - parse the buy tx receipt for tokens received (Transfer to wallet)
    - check current balance: 0 = closed, >0 = open
    - if closed: find all sell txs from our wallet for this token
                 sum the BNB returned (WBNB Withdraw events or direct BNB)
    - PnL = bnb_out - bnb_in - gas_used × gas_price
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.request

WALLET = "0x530306684A29E23676d30fA80dC6100e80b042ea"
WBNB   = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"

def load_env(p="/data/bsc-meme-mev/.env"):
    out = {}
    if not os.path.exists(p): return out
    for line in open(p):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out

ENV = load_env()
RPC = ENV.get("NODEREAL_RPC_URL") or "http://127.0.0.1:8545"

def rpc(method, params):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":1}).encode()
    req = urllib.request.Request(RPC, body, {"Content-Type":"application/json","User-Agent":"Mozilla/5.0"})
    return json.loads(urllib.request.urlopen(req, timeout=15).read())

def balance_of(token, who):
    sel = "0x70a08231"
    data = sel + who[2:].lower().rjust(64, "0")
    r = rpc("eth_call", [{"to": token, "data": data}, "latest"])
    h = r.get("result") or "0x0"
    return int(h, 16) if h and h != "0x" else 0

def get_receipt(tx_hash):
    r = rpc("eth_getTransactionReceipt", [tx_hash])
    return r.get("result")

def get_tx(tx_hash):
    r = rpc("eth_getTransactionByHash", [tx_hash])
    return r.get("result")

def parse_transfer_to_wallet(receipt, wallet):
    """Sum Transfer events where `to` == wallet, return total tokens received."""
    if not receipt: return 0
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    w = wallet[2:].lower().rjust(64, "0")
    total = 0
    for log in receipt.get("logs", []):
        topics = log.get("topics", [])
        if len(topics) >= 3 and topics[0] == TRANSFER_TOPIC and topics[2][2:].lower() == w:
            data = log.get("data", "0x")
            total += int(data, 16) if data and data != "0x" else 0
    return total

def parse_bnb_returned(receipt, wallet):
    """Find total BNB returned to wallet. For sells via Four.Meme/V2:
       - V2 sells: WBNB Transfer FROM router/pair, then router calls withdraw on WBNB, then sends BNB
                   often easier: parse Withdrawal event from WBNB
       - Four.Meme: sends BNB internally; visible as internal call value or final balance delta
       Simplest cross-route approach: parse WBNB Withdrawal events (router unwraps for us)
                                       PLUS Transfer events of WBNB to our wallet (if we got wrapped)
                                       PLUS direct BNB sent (tx value to us — but receipts don't show this for internal calls)
       For Four.Meme specifically, the curve sends BNB via internal call. Not visible in event logs.
       Workaround: use balance-delta around the tx (read balance at block-1 and block).
    """
    if not receipt: return 0
    WITHDRAWAL_TOPIC = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
    TRANSFER_TOPIC   = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    w = wallet[2:].lower().rjust(64, "0")
    total = 0
    for log in receipt.get("logs", []):
        topics = log.get("topics", [])
        # WBNB Withdrawal(src indexed, wad) — log address must be WBNB
        if log.get("address", "").lower() == WBNB and len(topics) >= 2 and topics[0] == WITHDRAWAL_TOPIC:
            if topics[1][2:].lower() == w:
                d = log.get("data", "0x")
                total += int(d, 16) if d and d != "0x" else 0
        # WBNB Transfer to our wallet (we received wrapped)
        elif log.get("address", "").lower() == WBNB and len(topics) >= 3 and topics[0] == TRANSFER_TOPIC and topics[2][2:].lower() == w:
            d = log.get("data", "0x")
            total += int(d, 16) if d and d != "0x" else 0
    return total

def bnb_balance_at(block):
    r = rpc("eth_getBalance", [WALLET, hex(block)])
    return int(r.get("result", "0x0"), 16)

def find_our_sell_txs(token, from_block):
    """Find Transfer FROM our wallet for this token, starting at the buy
    block (not last-N-blocks — old buys would be missed)."""
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    w_padded = "0x" + WALLET[2:].lower().rjust(64, "0")
    params = {
        "address": token,
        "fromBlock": hex(from_block),
        "toBlock": "latest",
        "topics": [TRANSFER_TOPIC, w_padded],
    }
    r = rpc("eth_getLogs", [params])
    logs = r.get("result") or []
    # dedupe by tx hash (one sell tx may emit multiple Transfer events)
    seen = set()
    out = []
    for l in logs:
        h = l["transactionHash"]
        if h in seen: continue
        seen.add(h)
        out.append((int(l["blockNumber"], 16), h))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="24h", help="window: 6h, 24h, 2d")
    args = ap.parse_args()
    unit = args.since[-1]; n = int(args.since[:-1])
    mult = {"s":1,"m":60,"h":3600,"d":86400}[unit]
    cutoff = int(time.time()) - n * mult

    rows = list(csv.DictReader(open('/data/bsc-meme-mev/trader_live/live_log.csv')))
    buys = [r for r in rows
            if int(r.get('ts_unix_ns',0) or 0) >= cutoff * 10**9
            and r.get('broadcast') == 'true'
            and int(r.get('bnb_in_wei','0') or '0') > 0]

    print(f"=== per-trade PnL: last {args.since} ({len(buys)} BUYs) ===")
    print()
    fmt = "{:5s} {:14s} {:>8s} {:>8s} {:>9s} {:>9s} {:>9s}  {}"
    print(fmt.format("kol", "token", "bnb_in", "bnb_out", "pnl_bnb", "pnl_usd", "pnl_pct", "status"))
    print("-" * 100)

    bnb_usd = 715.53  # rough; could fetch live
    total_pnl_bnb = 0.0
    closed = 0
    open_ = 0

    for r in buys:
        tok = r['token_address']
        kol = r.get('kol_name', '?')
        bnb_in = int(r['bnb_in_wei']) / 1e18
        buy_tx = r['tx_hash']

        # Gas cost from buy tx
        buy_receipt = get_receipt(buy_tx)
        gas_buy = 0
        if buy_receipt:
            gas_used = int(buy_receipt.get("gasUsed", "0x0"), 16)
            gas_price = int(buy_receipt.get("effectiveGasPrice", "0x0"), 16)
            gas_buy = gas_used * gas_price / 1e18

        # Current token balance
        bal = balance_of(tok, WALLET)
        if bal > 0:
            status = f"OPEN bal={bal:.2e}"
            bnb_out = 0.0
            total_gas = gas_buy
        else:
            status = "CLOSED"
            buy_block = int(buy_receipt.get("blockNumber", "0x0"), 16) if buy_receipt else 0
            sells = find_our_sell_txs(tok, buy_block)
            bnb_out = 0.0
            total_gas = gas_buy
            for blk, sell_tx in sells:
                rcpt = get_receipt(sell_tx)
                if not rcpt: continue
                if int(rcpt.get("status", "0x0"), 16) != 1: continue  # reverted
                # Sum BNB returned from this sell
                bnb_out += parse_bnb_returned(rcpt, WALLET) / 1e18
                gas_used = int(rcpt.get("gasUsed", "0x0"), 16)
                gas_price = int(rcpt.get("effectiveGasPrice", "0x0"), 16)
                total_gas += gas_used * gas_price / 1e18
            # If no WBNB Withdrawal/Transfer events found, this was likely a
            # Four.Meme sell that pays out as internal BNB call (no event).
            # Fall back to: balance-delta around the sell tx block.
            if bnb_out == 0.0 and sells:
                for blk, sell_tx in sells:
                    pre = bnb_balance_at(blk - 1)
                    post = bnb_balance_at(blk)
                    rcpt = get_receipt(sell_tx)
                    gas_used = int(rcpt.get("gasUsed", "0x0"), 16) if rcpt else 0
                    gas_price = int(rcpt.get("effectiveGasPrice", "0x0"), 16) if rcpt else 0
                    gas_wei = gas_used * gas_price
                    # post = pre - gas + received
                    received = max(0, (post + gas_wei) - pre)
                    bnb_out += received / 1e18

        pnl_bnb = bnb_out - bnb_in - total_gas
        pnl_usd = pnl_bnb * bnb_usd
        pnl_pct = (pnl_bnb / bnb_in * 100) if bnb_in > 0 else 0
        print(fmt.format(
            kol, tok[:14]+"...",
            f"{bnb_in:.4f}", f"{bnb_out:.4f}",
            f"{pnl_bnb:+.4f}", f"{pnl_usd:+.2f}", f"{pnl_pct:+.1f}%",
            status,
        ))
        if status == "CLOSED":
            total_pnl_bnb += pnl_bnb
            closed += 1
        else:
            open_ += 1

    print("-" * 100)
    print(f"CLOSED: {closed}  OPEN: {open_}")
    print(f"REALIZED PnL: {total_pnl_bnb:+.4f} BNB = ${total_pnl_bnb * bnb_usd:+.2f}")

if __name__ == "__main__":
    main()
