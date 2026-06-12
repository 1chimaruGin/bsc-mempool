#!/usr/bin/env python3
"""
Pre-trade signal investigation. For each of the last 30 closed D trades,
compute four candidate signals and check correlation with realized PnL:

1. D's streak (running win/loss count of D's previous N trades)
2. Holder count at our entry block (proxies rug risk — fewer holders = riskier)
3. Bot composition in D's block (% of buyers that are known MEV/Telegram bots)
4. Dev wallet history (has this token's deployer launched other rugs/winners?)

We compute, correlate with realized PnL, and report whether each signal
would profitably gate entry.
"""
import json, os, re, subprocess, sys, urllib.request
from collections import defaultdict, Counter

RPC = os.environ.get("NODEREAL_RPC_URL") or sys.exit("set NODEREAL_RPC_URL")
FOURMEME    = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
TOKEN_INFOS_SEL = "0xe684626b"
GMGN_PROXY  = "0x1de460f363af910f51726def188f9004276bf4bc"
PCS_V2      = "0x10ed43c718714eb63d5aa57b78b54704e256024e"
WALLET      = "0x530306684a29e23676d30fa80dc6100e80b042ea"
D_WALLET    = "0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRADE_BUY_TOPIC = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
TOKEN_CREATE_TOPIC = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"
BNB_USD = 586.0

# Well-known BSC bot/router routers and Telegram-bot proxies
KNOWN_BOTS = {
    GMGN_PROXY:                                     "GMGN_proxy",
    "0x9595dc23a5d4f0d6750dc4fcae3aa5b3ddd5b1b9": "maestro_router",  # placeholder
    "0xa64ed1b66cb2838ef2a198d8345c0ce6967a2a3c": "banana_router",   # placeholder
    "0xc60e71bd0f2e6a8832fe5a99b5b2e4c11ef93b97": "unibot",          # placeholder
}

def rpc(m, p):
    req = urllib.request.Request(RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode(),
        headers={"content-type":"application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]

def to_int(h): return int(h, 16) if isinstance(h, str) else h

# --- Parse journal for D's full trade history ---------------------------------
print("Parsing journal …", file=sys.stderr)
log = subprocess.run(["journalctl","-u","bsc-runner","--since","2026-06-01","--no-pager"],
    capture_output=True, text=True).stdout.splitlines()

our_buy_tx, our_sell_tx, exit_reason = {}, {}, {}
d_buy_value = {}
re_buy = re.compile(r'BROADCAST kol=D token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')
re_sell = re.compile(r'SELL BROADCAST kol=TRAIL_([a-z_]+) token=(0x[0-9a-f]+).*tx_hash=(0x[0-9a-f]+)')
re_d_obs = re.compile(r'kol_name=D\b.*side="BUY".*token=(0x[0-9a-f]+).*value_bnb=([\d.]+)')

for line in log:
    if 'BROADCAST kol=D' in line:
        m = re_buy.search(line)
        if m: our_buy_tx[m.group(1).lower()] = m.group(2)
    elif 'SELL BROADCAST kol=TRAIL_' in line:
        m = re_sell.search(line)
        if m:
            reason, tok, h = m.group(1), m.group(2).lower(), m.group(3)
            exit_reason[tok] = reason; our_sell_tx[tok] = h
    elif 'side="BUY"' in line and 'kol_name=D' in line and 'method_label' in line:
        m = re_d_obs.search(line)
        if m: d_buy_value[m.group(1).lower()] = float(m.group(2))

# Build ordered trade list (preserves chronology)
trades = []
for tok, buy_h in our_buy_tx.items():
    if tok in our_sell_tx:
        trades.append({"token": tok, "buy_h": buy_h, "sell_h": our_sell_tx[tok],
                       "reason": exit_reason[tok], "d_bnb": d_buy_value.get(tok)})
trades = trades[-30:]
print(f"Investigating {len(trades)} trades", file=sys.stderr)

# --- Step 1: realized PnL per trade -------------------------------------------
print("Step 1/4: realized PnL …", file=sys.stderr)
for t in trades:
    try:
        br = rpc("eth_getTransactionReceipt", [t["buy_h"]])
        bt = rpc("eth_getTransactionByHash", [t["buy_h"]])
        sr = rpc("eth_getTransactionReceipt", [t["sell_h"]])
        if br is None or sr is None or br["status"] != "0x1":
            t["realized_usd"] = None; continue
        buy_block  = to_int(br["blockNumber"])
        sell_block = to_int(sr["blockNumber"])
        buy_value  = to_int(bt["value"])
        buy_gas    = to_int(br["gasUsed"]) * to_int(br.get("effectiveGasPrice","0x0"))
        sell_gas   = to_int(sr["gasUsed"]) * to_int(sr.get("effectiveGasPrice","0x0"))
        bal_before = int(rpc("eth_getBalance", [WALLET, hex(sell_block-1)]), 16)
        bal_after  = int(rpc("eth_getBalance", [WALLET, hex(sell_block)]), 16)
        proceeds   = (bal_after - bal_before) + sell_gas
        realized   = proceeds - (buy_value + buy_gas + sell_gas)
        t["realized_usd"] = realized / 1e18 * BNB_USD
        t["buy_block"] = buy_block
        t["d_block"] = buy_block - 1  # D landed in N+0
    except Exception as e:
        t["realized_usd"] = None
        print(f"  err: {e}", file=sys.stderr)

# --- Signal 1: D's streak (running L's from D's PREVIOUS trades) --------------
# Note: "win/loss" here is OUR realized outcome, not D's — but a strong proxy
# since they're highly correlated when our entry is close to D's
print("Signal 1: D's streak", file=sys.stderr)
streak = 0
for t in trades:
    t["streak_in"] = streak  # streak BEFORE this trade
    if t["realized_usd"] is None: continue
    if t["realized_usd"] < 0:
        streak = streak - 1 if streak < 0 else -1
    else:
        streak = streak + 1 if streak > 0 else 1

# --- Signal 2: Holder count at entry block -------------------------------------
# Count unique addresses that received the token via Transfer events from
# block-of-token-create to our entry block. Proxy for "how distributed"
# the token is — fresh tokens with 1-3 holders are pure-curve early.
print("Signal 2: holder count at entry …", file=sys.stderr)
def addr_topic_to_hex(t):
    return "0x" + t[-40:]

for i, t in enumerate(trades):
    if "buy_block" not in t:
        t["holders"] = None; continue
    try:
        # Pull last 30 blocks of Transfer events for this token (capped)
        logs = rpc("eth_getLogs", [{
            "fromBlock": hex(t["buy_block"] - 50),
            "toBlock":   hex(t["buy_block"]),
            "address":   t["token"],
            "topics":    [ERC20_TRANSFER_TOPIC],
        }])
        recipients = set()
        for lg in logs:
            if len(lg["topics"]) >= 3:
                recipients.add(addr_topic_to_hex(lg["topics"][2]))
        # Exclude the curve contract and zero address
        recipients.discard("0x" + "0"*40)
        recipients.discard(FOURMEME)
        t["holders"] = len(recipients)
    except Exception as e:
        t["holders"] = None

# --- Signal 3: bot composition in D's block ----------------------------------
# Look at D's block (N+0) and count buyers using known router proxies.
# High bot % = high cascade risk.
print("Signal 3: bot composition in D's block …", file=sys.stderr)
for t in trades:
    if "d_block" not in t: continue
    try:
        # Get all txs in D's block
        blk = rpc("eth_getBlockByNumber", [hex(t["d_block"]), True])
        bot_count = 0
        total_routers = 0
        for tx in blk["transactions"]:
            to_addr = (tx.get("to") or "").lower()
            if to_addr in (GMGN_PROXY, PCS_V2) or to_addr in KNOWN_BOTS:
                total_routers += 1
                if to_addr in KNOWN_BOTS:
                    bot_count += 1
        t["bot_pct"] = (bot_count / total_routers * 100) if total_routers > 0 else 0
        t["router_count"] = total_routers
    except Exception as e:
        t["bot_pct"] = None
        t["router_count"] = None

# --- Signal 4: dev wallet history (from sniper journal) -----------------------
# For each token, find its creator from TokenCreate events (we have these in
# journal from sniper). Then count how many OTHER tokens that creator has
# deployed and what fraction died (proxy for rug-deployer).
print("Signal 4: dev wallet history …", file=sys.stderr)
# Parse sniper TokenCreate log into dev → tokens map
dev_tokens = defaultdict(list)
token_dev  = {}
re_create = re.compile(r'TokenCreate observed token=(0x[0-9a-f]+) dev=(0x[0-9a-f]+)')
for line in log:
    if "TokenCreate observed" in line:
        m = re_create.search(line)
        if m:
            tok, dev = m.group(1).lower(), m.group(2).lower()
            token_dev[tok] = dev
            dev_tokens[dev].append(tok)

# For each trade's token, look up dev + dev's history
for t in trades:
    dev = token_dev.get(t["token"])
    if dev is None:
        t["dev_history"] = None
        t["dev_tokens"] = None
        continue
    t["dev_history"] = dev
    t["dev_tokens"] = len(dev_tokens[dev])

# --- Report -------------------------------------------------------------------
print(f"\n{'token':<12} {'reason':<13} {'realized':>9} | {'streak':>6} {'holders':>8} {'bot%':>5} {'routers':>7} {'dev_toks':>8}")
print("-" * 100)
for t in trades:
    rp = f"{t.get('realized_usd', 0):+.2f}" if t.get("realized_usd") is not None else "    n/a"
    st = str(t.get("streak_in", "n/a"))
    h = str(t.get("holders", "?"))
    bp = f"{t.get('bot_pct',0):.0f}" if t.get("bot_pct") is not None else "n/a"
    rc = str(t.get("router_count", "?"))
    dt = str(t.get("dev_tokens", "?"))
    print(f"{t['token'][:10]:<12} {t['reason']:<13} ${rp:>8} | {st:>6} {h:>8} {bp:>5} {rc:>7} {dt:>8}")

# Compute correlations: hit rate by signal bucket
print("\n=== Hit-rate by signal bucket ===")
def bucketize(trades, key, ranges):
    """Return [(range_label, count, win_rate, avg_pnl)]."""
    out = []
    for label, lo, hi in ranges:
        sub = [t for t in trades if t.get(key) is not None and lo <= t[key] < hi and t.get("realized_usd") is not None]
        n = len(sub)
        wins = sum(1 for t in sub if t["realized_usd"] > 0)
        avg = sum(t["realized_usd"] for t in sub) / n if n > 0 else 0
        out.append((label, n, wins, avg))
    return out

for key, ranges in [
    ("streak_in", [("≤-3",-999,-2),("-2 to -1",-2,0),("0 to +1",0,2),("≥+2",2,999)]),
    ("holders",   [("0-5",0,6),("6-15",6,16),("16-30",16,31),("≥31",31,99999)]),
    ("bot_pct",   [("0",0,0.001),("1-25",0.001,25),("25-50",25,50),("≥50",50,101)]),
    ("dev_tokens",[("1",1,2),("2-3",2,4),("4-10",4,11),("≥11",11,99999)]),
]:
    print(f"\n--- {key} ---")
    for label, n, wins, avg in bucketize(trades, key, ranges):
        if n == 0: continue
        wr = wins / n * 100
        print(f"  {label:<12} n={n:>2}  win_rate={wr:>4.0f}%  avg_pnl=${avg:>+5.2f}")
