#!/usr/bin/env python3
"""
Backtest `signal_cascade` — a downside vote rule using real per-block
event data (mirror of the upside signal_vote we already ship).

For each trade, fetch real Four.Meme TradeBuy/TradeSell events block-by-
block from N+1 to N+40 and compute features:

  F1  sell_dominance:  sell_count_3blk > buy_count_3blk × 2
  F2  buy_collapse:    bv3 / bv10 < 0.3   (buy velocity collapsed)
  F3  whale_sell:      max single sell in last 3 blocks > 0.5 BNB
  F4  net_outflow:     net BNB flow last 3 blocks < -0.5 BNB

Variants tested (each fires only if vote count ≥ threshold):
  CA  3-of-4 strict             — only fire on overwhelming bear signal
  CB  3-of-4 + need price <-10% — guards against false positives
  CC  2-of-4 strict             — more sensitive
  CD  2-of-4 + need price <-15% — moderate threshold + price guard

CRITICAL CHECK: for each WINNING trade (actual realized > 0), report
whether the variant would have fired BEFORE the actual exit and how
much that would have cost us.
"""
import json, os, re, subprocess, sys, urllib.request
from collections import defaultdict

RPC = os.environ.get("NODEREAL_RPC_URL") or sys.exit("set NODEREAL_RPC_URL")
FOURMEME = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
TOKEN_INFOS_SEL = "0xe684626b"
WALLET = "0x530306684a29e23676d30fa80dc6100e80b042ea"
BNB_USD = 586.0
TRACE_WINDOW = 40
SELL_SLIPPAGE = 0.10

TRADE_BUY_TOPIC  = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
TRADE_SELL_TOPIC = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"

def rpc(m, p):
    req = urllib.request.Request(RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode(),
        headers={"content-type":"application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]

def to_int(h): return int(h, 16) if isinstance(h, str) else h

def decode_trade_data(data_hex):
    """Returns (token_addr, bnb_gross_wei). Mirror of four_meme_price.rs::decode_trade_buy.
    Each word is 32 bytes = 64 hex chars. Layout per the Rust decoder:
      Word 0: token (last 20 bytes = hex chars 24-64)
      Word 3: tokens received
      Word 4: bnb_net (hex chars 256-320)
      Word 5: fee     (hex chars 320-384)
    bnb_gross = bnb_net + fee
    """
    h = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if len(h) < 64 * 6: return None, None
    token_addr = "0x" + h[24:64]
    bnb_net = int(h[256:320], 16) if h[256:320] else 0
    fee     = int(h[320:384], 16) if h[320:384] else 0
    return token_addr, bnb_net + fee

def curve_bnb(token, block):
    data = TOKEN_INFOS_SEL + token.lower().lstrip("0x").zfill(64)
    try:
        v = rpc("eth_call", [{"to": FOURMEME, "data": data}, hex(block)])
    except Exception:
        return None
    if not v or v == "0x": return None
    h = v.lstrip("0x")
    if len(h) < 9*64: return None
    return int(h[8*64:9*64], 16)

# Parse journal
print("Parsing journal …", file=sys.stderr)
log = subprocess.run(["journalctl","-u","bsc-runner","--since","2026-06-01","--no-pager"],
    capture_output=True, text=True).stdout.splitlines()
our_buy_tx, our_sell_tx, exit_reason = {}, {}, {}
re_buy = re.compile(r'BROADCAST kol=D token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')
re_sell = re.compile(r'SELL BROADCAST kol=TRAIL_([a-z_]+) token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')
for line in log:
    if 'BROADCAST kol=D' in line:
        m = re_buy.search(line)
        if m: our_buy_tx[m.group(1).lower()] = m.group(2)
    elif 'SELL BROADCAST kol=TRAIL_' in line:
        m = re_sell.search(line)
        if m:
            exit_reason[m.group(2).lower()] = m.group(1)
            our_sell_tx[m.group(2).lower()] = m.group(3)

trades = []
for tok, buy_h in our_buy_tx.items():
    if tok in our_sell_tx:
        trades.append({"token": tok, "buy_h": buy_h, "sell_h": our_sell_tx[tok], "reason": exit_reason[tok]})
trades = trades[-30:]
print(f"Backtesting {len(trades)} trades", file=sys.stderr)

# Fetch trade data + per-block event stats
results = []
for i, t in enumerate(trades):
    print(f"  [{i+1}/{len(trades)}] {t['token'][:10]}", file=sys.stderr)
    try:
        br = rpc("eth_getTransactionReceipt", [t["buy_h"]])
        bt = rpc("eth_getTransactionByHash", [t["buy_h"]])
        sr = rpc("eth_getTransactionReceipt", [t["sell_h"]])
        if br is None or sr is None or br["status"] != "0x1": continue
        buy_block  = to_int(br["blockNumber"])
        sell_block = to_int(sr["blockNumber"])
        buy_value  = to_int(bt["value"])
        buy_gas    = to_int(br["gasUsed"]) * to_int(br.get("effectiveGasPrice","0x0"))
        sell_gas   = to_int(sr["gasUsed"]) * to_int(sr.get("effectiveGasPrice","0x0"))
        bal_before = int(rpc("eth_getBalance", [WALLET, hex(sell_block-1)]), 16)
        bal_after  = int(rpc("eth_getBalance", [WALLET, hex(sell_block)]), 16)
        actual_sell_proceeds = (bal_after - bal_before) + sell_gas
        actual_realized = actual_sell_proceeds - (buy_value + buy_gas + sell_gas)
        entry_curve = curve_bnb(t["token"], buy_block)
        if entry_curve is None or entry_curve == 0: continue

        # Fetch TradeBuy and TradeSell events for whole window
        fromBlk = buy_block + 1
        toBlk   = buy_block + TRACE_WINDOW
        buys  = rpc("eth_getLogs", [{"fromBlock": hex(fromBlk), "toBlock": hex(toBlk),
                                      "address": FOURMEME, "topics": [TRADE_BUY_TOPIC]}])
        sells = rpc("eth_getLogs", [{"fromBlock": hex(fromBlk), "toBlock": hex(toBlk),
                                      "address": FOURMEME, "topics": [TRADE_SELL_TOPIC]}])

        token_lc = t["token"].lower()
        # Aggregate per-block stats for this token
        per_block = defaultdict(lambda: {"buy_n": 0, "sell_n": 0, "buy_bnb": 0, "sell_bnb": 0, "max_sell_bnb": 0})
        for lg in buys:
            tok_in_log, bnb_wei = decode_trade_data(lg["data"])
            if tok_in_log is None or tok_in_log.lower() != token_lc: continue
            b = to_int(lg["blockNumber"])
            per_block[b]["buy_n"]   += 1
            per_block[b]["buy_bnb"] += bnb_wei
        for lg in sells:
            tok_in_log, bnb_wei = decode_trade_data(lg["data"])
            if tok_in_log is None or tok_in_log.lower() != token_lc: continue
            b = to_int(lg["blockNumber"])
            per_block[b]["sell_n"]   += 1
            per_block[b]["sell_bnb"] += bnb_wei
            per_block[b]["max_sell_bnb"] = max(per_block[b]["max_sell_bnb"], bnb_wei)
        # Curve state for price
        curves = {fromBlk + off: curve_bnb(t["token"], fromBlk + off) for off in range(TRACE_WINDOW)}

        results.append({
            **t, "buy_block": buy_block, "sell_block": sell_block,
            "buy_value": buy_value, "buy_gas": buy_gas, "sell_gas": sell_gas,
            "actual_realized": actual_realized,
            "actual_pct": actual_realized / (buy_value+buy_gas+sell_gas) * 100,
            "entry_curve": entry_curve, "per_block": dict(per_block), "curves": curves,
            "from_block": fromBlk,
        })
    except Exception as e:
        print(f"    err: {e}", file=sys.stderr)

# --- Simulate variants -------------------------------------------------------
def features_at(r, block):
    pb = r["per_block"]
    # 3-block window
    b3 = [block-2, block-1, block]
    buy_n_3 = sum(pb.get(b, {}).get("buy_n", 0) for b in b3)
    sell_n_3 = sum(pb.get(b, {}).get("sell_n", 0) for b in b3)
    buy_bnb_3 = sum(pb.get(b, {}).get("buy_bnb", 0) for b in b3)
    sell_bnb_3 = sum(pb.get(b, {}).get("sell_bnb", 0) for b in b3)
    max_sell_3 = max((pb.get(b, {}).get("max_sell_bnb", 0) for b in b3), default=0)
    # 10-block window
    b10 = list(range(block-9, block+1))
    buy_n_10 = sum(pb.get(b, {}).get("buy_n", 0) for b in b10)
    # price
    cur = r["curves"].get(block)
    if cur is None or r["entry_curve"] == 0: return None
    pct_drop = (cur - r["entry_curve"]) / r["entry_curve"]
    # Features
    F1 = (sell_n_3 > buy_n_3 * 2)
    bv3 = buy_n_3 / 3.0
    bv10 = buy_n_10 / 10.0
    F2 = (bv10 > 0 and (bv3 / bv10) < 0.3)
    F3 = (max_sell_3 / 1e18 > 0.5)
    F4 = ((buy_bnb_3 - sell_bnb_3) / 1e18 < -0.5)
    return {"F1": F1, "F2": F2, "F3": F3, "F4": F4, "pct_drop": pct_drop,
            "votes": int(F1) + int(F2) + int(F3) + int(F4)}

variants = {
    "CA_3of4_strict":    lambda f: f["votes"] >= 3,
    "CB_3of4_p<-10%":    lambda f: f["votes"] >= 3 and f["pct_drop"] <= -0.10,
    "CC_2of4_strict":    lambda f: f["votes"] >= 2,
    "CD_2of4_p<-15%":    lambda f: f["votes"] >= 2 and f["pct_drop"] <= -0.15,
}

def simulate_cascade(r, decide):
    """Find first block in window where decide() fires."""
    for off in range(2, TRACE_WINDOW):  # skip first 2 blocks (no 3-block history)
        b = r["from_block"] + off
        f = features_at(r, b)
        if f is None: continue
        if decide(f):
            return off, f["pct_drop"]
    return None, None

def hypothetical_pnl(r, exit_pct):
    sell_proceeds = r["buy_value"] * (1 + exit_pct) * (1 - SELL_SLIPPAGE)
    return sell_proceeds - (r["buy_value"] + r["buy_gas"] + r["sell_gas"])

# --- Report ------------------------------------------------------------------
print(f"\n=== Per-trade analysis ({len(results)} trades) ===\n")
print(f"{'token':<12} {'actual':>9} | {'CA':>16} {'CB':>16} {'CC':>16} {'CD':>16}")
print("-" * 100)
totals = {k: 0.0 for k in variants}
totals["ACTUAL"] = sum(r["actual_realized"] for r in results) / 1e18

winners_killed = {k: [] for k in variants}
for r in results:
    is_winner = r["actual_realized"] > 0
    row = f"{r['token'][:10]:<12} ${r['actual_realized']/1e18*BNB_USD:>+7.2f}"
    if is_winner: row = row[:0] + "⭐ " + row  # mark winners
    else: row = "   " + row
    for name, decide in variants.items():
        off, pct = simulate_cascade(r, decide)
        if off is None:
            # Cascade never fired — keep actual outcome
            pnl_usd = r["actual_realized"]/1e18 * BNB_USD
            cell = f"hold(actual)"
        else:
            pnl_usd = hypothetical_pnl(r, pct) / 1e18 * BNB_USD
            actual_pnl_usd = r["actual_realized"]/1e18 * BNB_USD
            delta = pnl_usd - actual_pnl_usd
            # Check if this killed a winner
            if is_winner and pnl_usd < actual_pnl_usd - 0.5:
                winners_killed[name].append((r["token"][:10], actual_pnl_usd, pnl_usd, off))
            mark = "↑" if delta > 0.5 else ("↓" if delta < -0.5 else " ")
            cell = f"{mark}${pnl_usd:>+5.2f}@b{off}"
        totals[name] += pnl_usd / BNB_USD
        row += f" {cell:>16}"
    print(row)

print("\n=== Totals ===")
print(f"  ACTUAL:           ${totals['ACTUAL']*BNB_USD:>+7.2f}")
for k in variants:
    d = (totals[k] - totals['ACTUAL']) * BNB_USD
    mark = "↑↑" if d > 5 else ("↑" if d > 0.5 else ("↓" if d < -0.5 else " "))
    print(f"  {k:<22} ${totals[k]*BNB_USD:>+7.2f}   Δ {d:>+6.2f}  {mark}")

print(f"\n=== ⚠ WINNERS KILLED by each variant ===")
for k in variants:
    killed = winners_killed[k]
    if not killed:
        print(f"  {k}: NONE ✓")
    else:
        print(f"  {k}: {len(killed)} winners killed:")
        for tok, actual, hyp, blk in killed:
            print(f"    {tok}: actual +${actual:.2f} → cascade -${hyp:.2f} at block N+{blk} (lost ${actual - hyp:.2f})")
