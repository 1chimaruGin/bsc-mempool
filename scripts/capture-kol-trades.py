#!/usr/bin/env python3
"""
Watch the live `bsc-runner` journal and capture the NEXT 3 KOL trades in
each visibility path (public / private). For each captured trade, compute
the entry mcap and ALL exit mcaps (the KOL usually scales out across
multiple sells) directly from chain receipts.

Methodology (matches what GMGN displays):
  spot_price (BNB-wei / raw-token-unit) = derived from the actual tx
    receipt — tx.value / tokens_received for buys, WBNB-out / tokens-in
    for sells. Works for Four.Meme/flap bonding curves AND graduated V2.
  total_supply = read once via eth_call(token, "0x18160ddd").
  mcap_usd = (spot_price × total_supply / 1e18) × BNB_USD

Hold window for KOL's exit sequence: 1 hour. If KOL hasn't fully exited
by the window end, the row is finalized with what we have so far.

Output: a structured table per finalized trade — go check on GMGN.

  scripts/capture-kol-trades.py [N]   # default N=3 per path
"""
import json, os, re, signal, subprocess, sys, threading, time
import urllib.request, urllib.error
from collections import defaultdict


# ── env ────────────────────────────────────────────────────────────────────
def load_env(p="/data/bsc-meme-mev/.env"):
    out = {}
    if not os.path.exists(p):
        return out
    with open(p) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out


ENV = load_env()
NODEREAL = ENV.get("NODEREAL_RPC_URL") or os.environ.get("NODEREAL_RPC_URL", "")
LOCAL = "http://127.0.0.1:8545"
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BNB_USD_FEED = "0x0567f2323251f0aab15c8dfb1967e4e8a7d42aee"  # Chainlink BNB/USD
HOLD_SECS = 3600  # 1 hour
N_PER_PATH_DEFAULT = 3
OUTPUT_JSONL = "/data/bsc-meme-mev/kol_trades_captured.jsonl"


# ── RPC ────────────────────────────────────────────────────────────────────
def rpc(url, method, params, retries=2):
    body = json.dumps(
        {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    ).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    for a in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read()).get("result")
        except Exception:
            if a < retries:
                time.sleep(0.5)
                continue
            return None


def hx(h):
    if h is None or h == "" or h == "0x":
        return 0
    if isinstance(h, int):
        return h
    try:
        return int(h, 16) if h.startswith("0x") else int(h)
    except (ValueError, AttributeError):
        return 0


# ── caches ─────────────────────────────────────────────────────────────────
_supply = {}
_bnb_usd = {"value": 0.0, "ts": 0}


def supply(token):
    t = token.lower()
    if t in _supply:
        return _supply[t]
    r = rpc(LOCAL, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"])
    if not r and NODEREAL:
        r = rpc(NODEREAL, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"])
    _supply[t] = hx(r) if r else 0
    return _supply[t]


def bnb_usd_now():
    """Chainlink BNB/USD latestAnswer (8 decimals). Cached 60s."""
    now = time.time()
    if now - _bnb_usd["ts"] < 60 and _bnb_usd["value"] > 0:
        return _bnb_usd["value"]
    r = rpc(LOCAL, "eth_call",
            [{"to": BNB_USD_FEED, "data": "0x50d25bcd"}, "latest"])
    if r and len(r) >= 4:
        val = hx(r) / 1e8
        if val > 0:
            _bnb_usd["value"] = val
            _bnb_usd["ts"] = now
    return _bnb_usd["value"] or 650.0  # safe-ish fallback


# ── tx → spot price (BNB-wei / raw-token-unit) ─────────────────────────────
def price_from_tx(tx_hash, token_lc, is_buy):
    r = rpc(LOCAL, "eth_getTransactionReceipt", [tx_hash])
    if not r and NODEREAL:
        r = rpc(NODEREAL, "eth_getTransactionReceipt", [tx_hash])
    if not r or r.get("status") != "0x1":
        return None, None
    tok_amt = wbnb_amt = 0
    for lg in r.get("logs") or []:
        topics = lg.get("topics") or []
        if not topics or (topics[0] or "").lower() != TRANSFER:
            continue
        amt = hx(lg.get("data"))
        addr = (lg.get("address") or "").lower()
        if addr == token_lc:
            tok_amt = max(tok_amt, amt)
        elif addr == WBNB:
            wbnb_amt = max(wbnb_amt, amt)
    if tok_amt == 0:
        return None, None
    blk = hx(r.get("blockNumber"))
    if is_buy:
        tx = rpc(LOCAL, "eth_getTransactionByHash", [tx_hash]) or \
             (NODEREAL and rpc(NODEREAL, "eth_getTransactionByHash", [tx_hash]))
        if not tx:
            return None, blk
        v = hx(tx.get("value"))
        bnb_wei = v if v > 0 else wbnb_amt
    else:
        if wbnb_amt > 0:
            bnb_wei = wbnb_amt
        else:
            gas_used = hx(r.get("gasUsed"))
            gas_price = hx(r.get("effectiveGasPrice"))
            tx = rpc(LOCAL, "eth_getTransactionByHash", [tx_hash]) or \
                 (NODEREAL and rpc(NODEREAL, "eth_getTransactionByHash", [tx_hash]))
            if not tx:
                return None, blk
            d = tx.get("from")
            if not d:
                return None, blk
            ba = rpc(LOCAL, "eth_getBalance", [d, hex(blk)])
            bb = rpc(LOCAL, "eth_getBalance", [d, hex(blk - 1)])
            if (ba is None or bb is None) and NODEREAL:
                ba = rpc(NODEREAL, "eth_getBalance", [d, hex(blk)])
                bb = rpc(NODEREAL, "eth_getBalance", [d, hex(blk - 1)])
            if ba is None or bb is None:
                return None, blk
            proceeds = (hx(ba) - hx(bb)) + gas_used * gas_price
            if proceeds <= 0:
                return None, blk
            bnb_wei = proceeds
    if bnb_wei <= 0 or tok_amt <= 0:
        return None, blk
    return (bnb_wei / tok_amt), blk


def mcap_usd(spot_price, token, bnb_usd):
    """spot_price in BNB-wei/raw → USD mcap."""
    sup = supply(token)
    if not sup or not spot_price:
        return 0.0
    return (spot_price * sup / 1e18) * bnb_usd


# ── parser for journal lines ───────────────────────────────────────────────
KV = re.compile(r'(\w+)=(?:"([^"]*)"|(\S+))')
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_ansi(s):
    return ANSI.sub("", s)


def parse_kv(line):
    out = {}
    for m in KV.finditer(strip_ansi(line)):
        out[m.group(1)] = m.group(2) if m.group(2) is not None else m.group(3)
    return out


# ── trade state ────────────────────────────────────────────────────────────
class Trade:
    def __init__(self, kol, token, sym, visibility, entry_tx, entry_block):
        self.kol = kol
        self.token = token
        self.sym = sym
        self.visibility = visibility   # 'public' | 'private'
        self.entry_tx = entry_tx
        self.entry_block = entry_block
        self.entry_ts = time.time()
        self.entry_mcap = None         # USD
        self.entry_price = None        # BNB-wei/raw
        self.exits = []                # list of {tx, block, mcap_usd, price, bnb_received}
        self.finalized = False

    def finalize_if_due(self):
        if self.finalized:
            return False
        if time.time() - self.entry_ts >= HOLD_SECS:
            self.finalized = True
            return True
        return False

    def to_dict(self):
        return {
            "kol": self.kol,
            "visibility": self.visibility,
            "symbol": self.sym,
            "token": self.token,
            "entry": {
                "block": self.entry_block,
                "tx": self.entry_tx,
                "mcap_usd": self.entry_mcap,
            },
            "exits": [{
                "block": e["block"], "tx": e["tx"],
                "mcap_usd": e["mcap_usd"],
                "bnb_received": e["bnb_received"],
            } for e in self.exits],
            "exit_count": len(self.exits),
        }


def print_trade(t):
    print()
    print("=" * 78)
    print(f"  {t.visibility.upper():7}  KOL={t.kol}   symbol={t.sym}")
    print(f"  TOKEN  {t.token}")
    print(f"  ENTRY  blk={t.entry_block}  tx={t.entry_tx}")
    em = f"${t.entry_mcap:,.0f}" if t.entry_mcap is not None else "(unresolved)"
    print(f"         mcap_usd = {em}")
    if not t.exits:
        print(f"  EXITS  (none observed within {HOLD_SECS//60}min hold window)")
    else:
        print(f"  EXITS  ({len(t.exits)} tranche{'s' if len(t.exits)>1 else ''}):")
        for i, e in enumerate(t.exits, 1):
            print(f"    [{i}] blk={e['block']}  tx={e['tx']}")
            print(f"         mcap_usd = ${e['mcap_usd']:,.0f}   "
                  f"bnb_received = {e['bnb_received']:.5f}")
    print(f"  → appended to {OUTPUT_JSONL}")
    print("=" * 78)
    sys.stdout.flush()
    # Persist for manual GMGN cross-check.
    try:
        with open(OUTPUT_JSONL, "a") as f:
            f.write(json.dumps(t.to_dict()) + "\n")
    except Exception as e:
        print(f"  [warn] could not append JSONL: {e}", file=sys.stderr)


# ── main capture loop ──────────────────────────────────────────────────────
def main():
    N = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else N_PER_PATH_DEFAULT
    print(f"[capture] watching for next {N} BUYs in PUBLIC and {N} in PRIVATE…",
          file=sys.stderr)
    print(f"[capture] hold window per trade = {HOLD_SECS//60} min "
          f"(KOL may sell in multiple tranches)", file=sys.stderr)

    open_trades = {}  # (kol, token) -> Trade
    captured_done = []
    counts = defaultdict(int)  # 'public_buys', 'private_buys'

    def maybe_finalize_overdue():
        """Drain timed-out trades into the done list and print them."""
        done_keys = []
        for k, t in open_trades.items():
            if t.finalize_if_due():
                print_trade(t)
                captured_done.append(t)
                done_keys.append(k)
        for k in done_keys:
            del open_trades[k]

    # `journalctl -f` from now, plain output. Tail bsc-runner only.
    # Wrap in stdbuf -oL so journalctl line-buffers its output (default is
    # block-buffer when stdout is a pipe — stalls our reader for minutes).
    # SYSTEMD_COLORS=0 strips ANSI codes injected by journalctl on the -f
    # path (breaks the key=value parser otherwise).
    env = {**os.environ, "SYSTEMD_COLORS": "0", "NO_COLOR": "1"}
    p = subprocess.Popen(
        ["stdbuf", "-oL",
         "journalctl", "-u", "bsc-runner", "-f", "--no-pager",
         "--since", "now", "-o", "cat"],
        stdout=subprocess.PIPE, bufsize=1, text=True, env=env,
    )

    try:
        for line in p.stdout:
            line = strip_ansi(line.rstrip("\n"))

            # Drain overdue every line (cheap).
            maybe_finalize_overdue()

            # Termination check
            if (counts["public_buys"] >= N and counts["private_buys"] >= N
                    and not open_trades):
                break

            if "KOL tx CONFIRMED" not in line:
                continue
            kv = parse_kv(line)
            kol = kv.get("kol_name")
            side = kv.get("side", "").strip('"')
            visibility = kv.get("visibility", "").strip('"')
            tx_hash = kv.get("tx_hash")
            token = kv.get("token", "").lower()
            if not (kol and side and visibility and tx_hash and token):
                continue

            # Need symbol — try token_symbol from the line, else fallback later.
            sym = kv.get("symbol") or kv.get("token_symbol") or token[:8]

            if side == "BUY":
                if counts[f"{visibility}_buys"] >= N:
                    continue
                if (kol, token) in open_trades:
                    continue  # already tracking
                t = Trade(kol, token, sym, visibility, tx_hash, hx(kv.get("mined_block")))
                # Resolve entry mcap immediately (run async to not block the tail)
                def resolve_entry(trade=t):
                    price, _blk = price_from_tx(trade.entry_tx, trade.token, True)
                    if price:
                        trade.entry_price = price
                        trade.entry_mcap = mcap_usd(price, trade.token, bnb_usd_now())
                threading.Thread(target=resolve_entry, daemon=True).start()
                open_trades[(kol, token)] = t
                counts[f"{visibility}_buys"] += 1
                print(f"[capture] +ENTRY  {visibility:7} kol={kol}  sym={sym}  "
                      f"tok={token}  tx={tx_hash}", file=sys.stderr)

            elif side == "SELL":
                t = open_trades.get((kol, token))
                if not t:
                    continue
                # Resolve sell mcap async
                def resolve_exit(trade=t, sell_tx=tx_hash,
                                  sell_blk=hx(kv.get("mined_block"))):
                    price, _ = price_from_tx(sell_tx, trade.token, False)
                    if price is None:
                        return
                    mc = mcap_usd(price, trade.token, bnb_usd_now())
                    # bnb_received = price × tokens_sent. Read tokens_sent
                    # from the receipt's Transfer log of THIS token (any
                    # amount works — bonding-curve sells emit a Transfer
                    # FROM=KOL TO=launchpad). Falls back to WBNB scan if
                    # there's no token-Transfer (graduated V2 path).
                    r = (rpc(LOCAL, "eth_getTransactionReceipt", [sell_tx]) or
                         (NODEREAL and rpc(NODEREAL,
                                            "eth_getTransactionReceipt", [sell_tx])))
                    tokens_sent_raw = 0
                    wbnb_received_wei = 0
                    if r:
                        for lg in r.get("logs") or []:
                            tps = lg.get("topics") or []
                            if not tps or (tps[0] or "").lower() != TRANSFER:
                                continue
                            addr = (lg.get("address") or "").lower()
                            amt = hx(lg.get("data"))
                            if addr == trade.token:
                                tokens_sent_raw = max(tokens_sent_raw, amt)
                            elif addr == WBNB:
                                wbnb_received_wei = max(wbnb_received_wei, amt)
                    if wbnb_received_wei > 0:
                        bnb_received = wbnb_received_wei / 1e18
                    elif tokens_sent_raw > 0 and price > 0:
                        # Native-BNB sell — derive proceeds from price × tokens.
                        bnb_received = (price * tokens_sent_raw) / 1e18
                    else:
                        bnb_received = 0.0
                    trade.exits.append({
                        "tx": sell_tx,
                        "block": sell_blk,
                        "mcap_usd": mc,
                        "price": price,
                        "bnb_received": bnb_received,
                    })
                threading.Thread(target=resolve_exit, daemon=True).start()
                print(f"[capture] +EXIT   {visibility:7} kol={kol}  sym={t.sym}  "
                      f"tx={tx_hash}", file=sys.stderr)
    finally:
        # Give pending resolvers a moment, finalize all, print rest.
        time.sleep(3)
        for k, t in list(open_trades.items()):
            t.finalized = True
            print_trade(t)
            captured_done.append(t)
            del open_trades[k]
        p.terminate()

    print()
    print(f"[capture] DONE — captured {sum(1 for t in captured_done if t.visibility=='public')} "
          f"public + {sum(1 for t in captured_done if t.visibility=='private')} private "
          f"trades", file=sys.stderr)


if __name__ == "__main__":
    main()
