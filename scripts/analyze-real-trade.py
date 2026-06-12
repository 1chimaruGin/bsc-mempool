#!/usr/bin/env python3
"""
Reconstruct REAL fill price, REAL peak, and REAL low-before-peak for a
given trade — from on-chain data only (no reliance on the runner's
journal).

Method:
  1. BUY tx receipt → tokens received + BNB paid → real_entry_price
  2. SELL tx receipt → BNB received + tokens sold → real_exit_price
  3. Held window = [buy_block, sell_block]
     Scan ALL price-moving events in that window:
       - Four.Meme launchpad TradeBuy/TradeSell on this token
       - PancakeSwap V2 pair Swap events (if pair exists)
     For each, compute a spot price.
     Track running peak; record running low-since-buy until peak block.

Output: clean table comparing journal vs reality.
"""
import json, sys, argparse, time
from collections import defaultdict
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

NODEREAL = "https://bsc-mainnet.nodereal.io/v1/3bed06fc28e04f73a64a54da9c575a47"
LAUNCHPAD = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
WBNB      = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"

TRADE_BUY_TOPIC  = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
TRADE_SELL_TOPIC = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"
TRANSFER_TOPIC   = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
V2_SWAP_TOPIC    = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"  # Swap(sender,amount0In,amount1In,amount0Out,amount1Out,to)
V2_SYNC_TOPIC    = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

def rpc(method, params, retries=3):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":1}).encode()
    last = None
    for i in range(retries):
        try:
            req = Request(NODEREAL, data=body, headers={"Content-Type":"application/json"})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                if "error" in data:
                    raise RuntimeError(data["error"].get("message","?"))
                return data.get("result")
        except (HTTPError, URLError, json.JSONDecodeError, RuntimeError) as e:
            last = e; time.sleep(2 ** i)
    raise RuntimeError(f"rpc {method} failed: {last}")

def get_receipt(tx):    return rpc("eth_getTransactionReceipt", [tx])
def get_tx(tx):         return rpc("eth_getTransactionByHash", [tx])
def call(to, data, block="latest"):
    return rpc("eth_call", [{"to": to, "data": data}, block])

def get_v2_pair(token):
    # getPair(WBNB, token)
    sel = "0xe6a43905"
    addr1 = WBNB[2:].rjust(64, "0")
    addr2 = token[2:].rjust(64, "0")
    r = call(V2_FACTORY, f"0x{sel.strip('0x')}{addr1}{addr2}")
    if not r or r in ("0x", "0x"+"0"*64): return None
    addr = "0x" + r[-40:]
    return addr if int(addr, 16) != 0 else None

# ── decoders ───────────────────────────────────────────────────────

def decode_4meme(log, is_buy):
    """4meme TradeBuy/TradeSell — token at word 0, tokens at word 3,
    bnb at word 4, fee at word 5. Returns (token, price BNB-wei/raw-token)."""
    d = log["data"][2:]
    if len(d) < 32*6*2: return None
    w = lambda i: d[i*64:(i+1)*64]
    token = "0x" + w(0)[24:]
    try:
        tokens = int(w(3), 16)
        bnb_net = int(w(4), 16)
        fee = int(w(5), 16)
    except ValueError: return None
    if tokens < 10**12: return None
    bnb_gross = bnb_net + fee
    if bnb_gross == 0: return None
    return token.lower(), bnb_gross / tokens, "4meme_buy" if is_buy else "4meme_sell"

def decode_v2_swap(log, token_is_0):
    """V2 Swap event. amount0In,amount1In,amount0Out,amount1Out.
    Compute price = bnb_amount / token_amount."""
    d = log["data"][2:]
    if len(d) < 64*4: return None
    a0in  = int(d[0:64], 16)
    a1in  = int(d[64:128], 16)
    a0out = int(d[128:192], 16)
    a1out = int(d[192:256], 16)
    # token0 ≡ WBNB or token; if token_is_0 means our token is at slot 0
    if token_is_0:
        bnb_in, tok_in, bnb_out, tok_out = a1in, a0in, a1out, a0out
    else:
        bnb_in, tok_in, bnb_out, tok_out = a0in, a1in, a0out, a1out
    # If bnb_in > 0: someone bought (BNB in, tokens out)
    # If bnb_out > 0: someone sold (tokens in, BNB out)
    if bnb_in > 0 and tok_out > 0:
        return bnb_in / tok_out, "v2_buy"
    if tok_in > 0 and bnb_out > 0:
        return bnb_out / tok_in, "v2_sell"
    return None

# ── per-trade reconstruction ───────────────────────────────────────

def reconstruct(token, buy_tx, sell_tx, wallet, bnb_usd=600):
    token = token.lower()
    wallet = wallet.lower()

    print(f"\n=== Reconstructing trade ===")
    print(f"  token:  {token}")
    print(f"  buy:    {buy_tx}")
    print(f"  sell:   {sell_tx}")

    # ── BUY side
    buy_tx_data = get_tx(buy_tx)
    buy_rec     = get_receipt(buy_tx)
    if not buy_rec:
        print("  ⚠ buy receipt missing"); return
    buy_block   = int(buy_rec["blockNumber"], 16)
    buy_status  = buy_rec["status"]

    # Tokens received by our wallet
    pad = wallet[2:].rjust(64, "0").lower()
    tokens_recv = 0
    for log in buy_rec["logs"]:
        if log["address"].lower() != token: continue
        if log["topics"][0].lower() != TRANSFER_TOPIC: continue
        if log["topics"][2].lower().endswith(pad):
            tokens_recv += int(log["data"], 16)

    bnb_paid_wei = int(buy_tx_data["value"], 16) if buy_tx_data else 0
    real_buy_price = bnb_paid_wei / tokens_recv if tokens_recv > 0 else 0

    print(f"\n  ── BUY ({buy_tx[:18]}…)")
    print(f"     block:   {buy_block}  status: {buy_status}")
    print(f"     paid:    {bnb_paid_wei/1e18:.6f} BNB (${bnb_paid_wei/1e18 * bnb_usd:.2f})")
    print(f"     got:     {tokens_recv/1e18:.2f} tokens")
    if real_buy_price > 0:
        mcap_usd = real_buy_price * 1e27 / 1e18 * bnb_usd
        print(f"     PRICE:   {real_buy_price:.4e} BNB/raw (mcap ≈ ${mcap_usd:,.0f})")

    # ── SELL side
    sell_tx_data = get_tx(sell_tx)
    sell_rec     = get_receipt(sell_tx)
    if not sell_rec:
        print("  ⚠ sell receipt missing")
        return
    sell_block  = int(sell_rec["blockNumber"], 16)
    sell_status = sell_rec["status"]

    # Tokens sent BY our wallet (Transfer FROM us)
    tokens_sent = 0
    for log in sell_rec["logs"]:
        if log["address"].lower() != token: continue
        if log["topics"][0].lower() != TRANSFER_TOPIC: continue
        if log["topics"][1].lower().endswith(pad):
            tokens_sent += int(log["data"], 16)

    # BNB received by our wallet: native transfer OR WBNB Transfer TO us
    bnb_recv_wei = 0
    # 1. Try WBNB Transfer TO us
    for log in sell_rec["logs"]:
        if log["address"].lower() != WBNB.lower(): continue
        if log["topics"][0].lower() != TRANSFER_TOPIC: continue
        if log["topics"][2].lower().endswith(pad):
            bnb_recv_wei += int(log["data"], 16)
    # 2. If nothing on WBNB path, look for raw BNB call traces (would need debug_trace).
    #    Use the post-sell wallet balance diff as fallback (approximate).
    if bnb_recv_wei == 0:
        # Native BNB sells (Four.Meme) — balance diff
        try:
            pre  = int(rpc("eth_getBalance",  [wallet, hex(sell_block - 1)]), 16)
            post = int(rpc("eth_getBalance",  [wallet, hex(sell_block)]),     16)
            # Subtract gas spent (gasUsed × effectiveGasPrice)
            gas_used = int(sell_rec["gasUsed"], 16)
            gas_price = int(sell_rec.get("effectiveGasPrice", sell_tx_data.get("gasPrice","0x0")), 16)
            gas_cost = gas_used * gas_price
            bnb_recv_wei = max(0, post - pre + gas_cost)
        except Exception as e:
            print(f"  ⚠ balance-diff fallback failed: {e}")
            bnb_recv_wei = 0

    real_sell_price = bnb_recv_wei / tokens_sent if tokens_sent > 0 else 0

    print(f"\n  ── SELL ({sell_tx[:18]}…)")
    print(f"     block:   {sell_block}  status: {sell_status}")
    print(f"     sold:    {tokens_sent/1e18:.2f} tokens")
    print(f"     got:     {bnb_recv_wei/1e18:.6f} BNB (${bnb_recv_wei/1e18 * bnb_usd:.2f})")
    if real_sell_price > 0:
        mcap_usd = real_sell_price * 1e27 / 1e18 * bnb_usd
        print(f"     PRICE:   {real_sell_price:.4e} BNB/raw (mcap ≈ ${mcap_usd:,.0f})")

    # ── Realized return
    if real_buy_price > 0 and real_sell_price > 0:
        ratio = real_sell_price / real_buy_price
        pnl_bnb = (bnb_recv_wei - bnb_paid_wei) / 1e18
        print(f"\n  ── REALIZED")
        print(f"     ratio:   {ratio:.4f}x  ({(ratio-1)*100:+.1f}%)")
        print(f"     net BNB: {pnl_bnb:+.6f}  (${pnl_bnb * bnb_usd:+.2f})")

    # ── Scan held window for peak / low
    print(f"\n  ── Held window: blocks [{buy_block}, {sell_block}] = {sell_block - buy_block + 1} blocks")

    # Pair detection
    pair = get_v2_pair(token)
    print(f"     V2 pair: {pair if pair else '(none, still on Four.Meme curve)'}")
    token_is_0 = False
    if pair:
        # token0() = 0x0dfe1681
        t0 = call(pair, "0x0dfe1681")
        if t0:
            t0_addr = "0x" + t0[-40:]
            token_is_0 = (t0_addr.lower() == token.lower())

    # Gather all price-setting events in window
    prices = []  # list of (block, price, src)

    # 4meme TradeBuy / TradeSell on this token (scan launchpad, filter token)
    from_blk = buy_block
    to_blk   = sell_block
    for topic, is_buy in [(TRADE_BUY_TOPIC, True), (TRADE_SELL_TOPIC, False)]:
        try:
            logs = rpc("eth_getLogs", [{
                "address": LAUNCHPAD, "fromBlock": hex(from_blk),
                "toBlock": hex(to_blk), "topics":[topic],
            }]) or []
        except Exception as e:
            print(f"     ⚠ scan launchpad/{topic[:10]} failed: {e}")
            continue
        for log in logs:
            r = decode_4meme(log, is_buy)
            if r and r[0] == token:
                prices.append((int(log["blockNumber"],16), r[1], r[2]))

    # V2 Swaps if pair exists
    if pair:
        try:
            logs = rpc("eth_getLogs", [{
                "address": pair, "fromBlock": hex(from_blk),
                "toBlock": hex(to_blk), "topics":[V2_SWAP_TOPIC],
            }]) or []
        except Exception as e:
            print(f"     ⚠ scan V2 pair failed: {e}")
            logs = []
        for log in logs:
            r = decode_v2_swap(log, token_is_0)
            if r:
                prices.append((int(log["blockNumber"],16), r[0], r[1]))

    if not prices:
        print(f"     ⚠ no price-setting events in held window — entry/exit must be only data points")
        return

    prices.sort(key=lambda x: x[0])
    # Compute peak and low-before-peak
    peak_block = peak_price = 0.0
    low_block  = low_price  = float("inf")
    # Walk forward, ratchet peak; track low while peak hasn't been beaten
    running_low = float("inf")
    running_low_blk = None
    for blk, p, src in prices:
        if p > peak_price:
            peak_price = p
            peak_block = blk
            # snapshot the low *up to* this new peak
            low_price = running_low if running_low < float("inf") else p
            low_block = running_low_blk if running_low_blk else blk
            running_low = float("inf")  # reset for any future deeper peak
        else:
            if p < running_low:
                running_low = p
                running_low_blk = blk

    print(f"     events seen: {len(prices)}  ({sum(1 for x in prices if x[2].endswith('buy'))} buys, {sum(1 for x in prices if x[2].endswith('sell'))} sells)")
    print(f"\n  ── PEAK")
    print(f"     block:   {peak_block} (held block {peak_block - buy_block})")
    print(f"     price:   {peak_price:.4e}  (mcap ≈ ${peak_price * 1e27 / 1e18 * bnb_usd:,.0f})")
    if real_buy_price > 0:
        print(f"     vs entry: {peak_price/real_buy_price:.2f}x  ({(peak_price/real_buy_price-1)*100:+.0f}%)")
    print(f"\n  ── LOW BEFORE PEAK")
    print(f"     block:   {low_block} (held block {low_block - buy_block if low_block else '-'})")
    print(f"     price:   {low_price:.4e}  (mcap ≈ ${low_price * 1e27 / 1e18 * bnb_usd:,.0f})")
    if real_buy_price > 0:
        print(f"     vs entry: {low_price/real_buy_price:.2f}x  ({(low_price/real_buy_price-1)*100:+.0f}%)")
    if peak_price > 0:
        print(f"     vs peak:  {low_price/peak_price:.2f}x  ({(low_price/peak_price-1)*100:+.0f}%)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", required=True)
    ap.add_argument("--buy_tx", required=True)
    ap.add_argument("--sell_tx", required=True)
    ap.add_argument("--wallet", default="0x530306684A29E23676d30fA80dC6100e80b042ea")
    ap.add_argument("--bnb_usd", type=float, default=600)
    a = ap.parse_args()
    reconstruct(a.token, a.buy_tx, a.sell_tx, a.wallet, a.bnb_usd)
