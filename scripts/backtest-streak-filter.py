#!/usr/bin/env python3
"""
PROPERLY backtest the D-streak filter with recursive streak updating.

When we skip a trade, the streak doesn't update (we didn't take it).
When we take a trade and it wins/loses, streak updates accordingly.
This is the correct simulation — my earlier analysis used a naive (non-
recursive) streak that assumed we took every trade.

Also: verify NO winners are killed by each filter variant.
"""
import json, os, re, subprocess, sys, urllib.request

RPC = os.environ.get("NODEREAL_RPC_URL") or sys.exit("set NODEREAL_RPC_URL")
WALLET = "0x530306684a29e23676d30fa80dc6100e80b042ea"
BNB_USD = 586.0

def rpc(m, p):
    req = urllib.request.Request(RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode(),
        headers={"content-type":"application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())["result"]

def to_int(h): return int(h, 16) if isinstance(h, str) else h

# Parse journal
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

trades_raw = []
for tok, buy_h in our_buy_tx.items():
    if tok in our_sell_tx:
        trades_raw.append({"token": tok, "buy_h": buy_h, "sell_h": our_sell_tx[tok], "reason": exit_reason[tok]})
trades_raw = trades_raw[-30:]
print(f"Backtesting {len(trades_raw)} trades", file=sys.stderr)

# Compute realized PnL
trades = []
for i, t in enumerate(trades_raw):
    try:
        br = rpc("eth_getTransactionReceipt", [t["buy_h"]])
        bt = rpc("eth_getTransactionByHash", [t["buy_h"]])
        sr = rpc("eth_getTransactionReceipt", [t["sell_h"]])
        if br is None or sr is None or br["status"] != "0x1": continue
        sell_block = to_int(sr["blockNumber"])
        buy_value  = to_int(bt["value"])
        buy_gas    = to_int(br["gasUsed"]) * to_int(br.get("effectiveGasPrice","0x0"))
        sell_gas   = to_int(sr["gasUsed"]) * to_int(sr.get("effectiveGasPrice","0x0"))
        bal_before = int(rpc("eth_getBalance", [WALLET, hex(sell_block-1)]), 16)
        bal_after  = int(rpc("eth_getBalance", [WALLET, hex(sell_block)]), 16)
        proceeds   = (bal_after - bal_before) + sell_gas
        realized   = (proceeds - (buy_value + buy_gas + sell_gas)) / 1e18 * BNB_USD
        trades.append({**t, "realized_usd": realized})
    except Exception as e:
        print(f"  err: {e}", file=sys.stderr)

# Recursive filter simulation
def simulate(filter_fn):
    """Apply filter recursively. Streak updates only on TAKEN trades."""
    streak = 0
    kept, skipped, winners_killed = [], [], []
    for t in trades:
        if filter_fn(streak):
            skipped.append({**t, "streak_at_decision": streak})
            if t["realized_usd"] > 0:
                winners_killed.append({**t, "streak_at_decision": streak})
        else:
            kept.append({**t, "streak_at_decision": streak})
            if t["realized_usd"] > 0:   streak = streak + 1 if streak > 0 else 1
            elif t["realized_usd"] < 0: streak = streak - 1 if streak < 0 else -1
    return kept, skipped, winners_killed

filters = {
    "actual (no filter)":         lambda s: False,
    "F1: skip if streak ≥ +1":    lambda s: s >= 1,
    "F2: skip if streak ≥ 0":     lambda s: s >= 0,
    "F3: skip if s ≥ +1 or ≤ -3": lambda s: s >= 1 or s <= -3,
    "F4: skip if s ≥ +2 or ≤ -4": lambda s: s >= 2 or s <= -4,
}

print(f"\n{'Filter':<30} {'kept':>5} {'skipped':>8} {'total_PnL':>10} {'winners_killed':>17}")
print("-" * 80)
for name, fn in filters.items():
    kept, skipped, killed = simulate(fn)
    total_pnl = sum(t["realized_usd"] for t in kept)
    win_kill_loss = sum(t["realized_usd"] for t in killed)
    killed_count = len(killed)
    killed_str = f"{killed_count} (-${win_kill_loss:.2f})" if killed_count else "0 ✓"
    print(f"  {name:<28} {len(kept):>5} {len(skipped):>8} {'$'+f'{total_pnl:+.2f}':>10} {killed_str:>17}")

print()
print("=== Per-trade decisions under best filter (F1: skip if streak ≥ +1) ===\n")
kept, skipped, killed = simulate(filters["F1: skip if streak ≥ +1"])
state = "kept"  # for display
streak = 0
for t in trades:
    decision = "SKIP" if streak >= 1 else "TAKE"
    note = ""
    if decision == "SKIP" and t["realized_usd"] > 0:
        note = f"  ⚠ would have lost +${t['realized_usd']:.2f} winner"
    print(f"  streak={streak:+2}  {t['token'][:10]}  ${t['realized_usd']:>+6.2f} {t['reason']:<14} {decision}{note}")
    if decision == "TAKE":
        if t["realized_usd"] > 0:   streak = streak + 1 if streak > 0 else 1
        elif t["realized_usd"] < 0: streak = streak - 1 if streak < 0 else -1
