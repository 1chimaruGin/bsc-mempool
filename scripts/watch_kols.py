#!/usr/bin/env python3
"""Clean live KOL trade tape — see watch-kols.sh for usage."""
import json
import os
import re
import subprocess
import sys
import urllib.request

RPC = "http://127.0.0.1:8545"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
RPC_TIMEOUT = 3

# Runner logs UTC (ISO …Z). Display offset in hours; default JST (+9, no
# DST). Override: WATCH_TZ=+2 (CEST), WATCH_TZ=0 (UTC), etc.
def _tz():
    v = os.environ.get("WATCH_TZ", "+9").strip().replace("UTC", "") or "+9"
    try:
        return int(v)
    except ValueError:
        return 9


TZ_OFF = _tz()
TZ_LABEL = "UTC" if TZ_OFF == 0 else f"UTC{TZ_OFF:+d}"
_TS_RE = re.compile(r"T(\d{2}):(\d{2}):(\d{2})")


def localize(line: str) -> str:
    """HH:MM:SS of the line's leading UTC timestamp, shifted by TZ_OFF."""
    m = _TS_RE.search(line[:40])
    if not m:
        return "??:??:??"
    h, mi, s = (int(x) for x in m.groups())
    h = (h + TZ_OFF) % 24
    return f"{h:02d}:{mi:02d}:{s:02d}"

# ---- args -------------------------------------------------------------------
args = sys.argv[1:]
show_all = "--all" in args
hist_min = 0
if "--hist" in args:
    i = args.index("--hist")
    hist_min = int(args[i + 1]) if i + 1 < len(args) else 30
    del args[i : i + 2]
args = [a for a in args if a != "--all"]
flt = args[0].lower() if args else ""

# ---- token symbol cache -----------------------------------------------------
_sym: dict[str, str] = {}


def symbol(addr: str) -> str:
    a = addr.lower()
    if a in _sym:
        return _sym[a]
    out = addr[:10]
    try:
        req = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": addr, "data": "0x95d89b41"}, "latest"],
                "id": 1,
            }
        ).encode()
        r = urllib.request.urlopen(
            urllib.request.Request(
                RPC, data=req, headers={"Content-Type": "application/json"}
            ),
            timeout=RPC_TIMEOUT,
        )
        h = json.load(r).get("result", "")
        s = ""
        if h and len(h) > 130:
            # Standard ABI string: offset(32) | len(32) | utf-8 bytes.
            ln = int(h[66 : 66 + 64], 16)
            s = bytes.fromhex(h[130 : 130 + ln * 2]).decode("utf-8", "replace")
        elif h and len(h) >= 66:
            # Legacy bytes32-packed symbol (older BEP20).
            s = bytes.fromhex(h[2:66]).rstrip(b"\x00").decode("utf-8", "replace")
        s = "".join(c for c in s if c.isprintable()).strip()
        if s:
            out = s[:12]
    except Exception:
        pass
    _sym[a] = out
    return out


# ---- market cap -------------------------------------------------------------
# mcap = pool spot price (BNB/token from PancakeV2 WBNB pair) × totalSupply
#        × BNB-USD.  Computed at the trade's block when known (CONFIRMED
#        lines carry mined_block) so it's the mcap AT THAT TRADE, not "now".
V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
CHAINLINK_BNBUSD = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE"
_dec: dict[str, int] = {}
_mcap: dict[tuple, float | None] = {}   # (token, block) -> usd | None
_bnb: list = [0.0, 0.0]                 # [price, fetched_at]


def _call(to: str, data: str, block: str = "latest"):
    try:
        req = json.dumps({"jsonrpc": "2.0", "method": "eth_call",
                          "params": [{"to": to, "data": data}, block],
                          "id": 1}).encode()
        r = urllib.request.urlopen(urllib.request.Request(
            RPC, data=req, headers={"Content-Type": "application/json"}),
            timeout=RPC_TIMEOUT)
        return json.load(r).get("result", "") or ""
    except Exception:
        return ""


def _u(hexword: str) -> int:
    try:
        return int(hexword, 16)
    except ValueError:
        return 0


def bnb_usd() -> float:
    import time
    if _bnb[0] and time.time() - _bnb[1] < 30:
        return _bnb[0]
    h = _call(CHAINLINK_BNBUSD, "0x50d25bcd")
    v = _u(h) / 1e8 if h else 0.0
    if v > 0:
        _bnb[0], _bnb[1] = v, time.time()
    return _bnb[0]


def _decimals(token: str) -> int:
    if token in _dec:
        return _dec[token]
    h = _call(token, "0x313ce567")
    d = _u(h) if h else 18
    _dec[token] = d if 0 < d <= 36 else 18
    return _dec[token]


TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


def _rpc(method: str, params: list):
    try:
        req = json.dumps({"jsonrpc": "2.0", "method": method,
                          "params": params, "id": 1}).encode()
        r = urllib.request.urlopen(urllib.request.Request(
            RPC, data=req, headers={"Content-Type": "application/json"}),
            timeout=RPC_TIMEOUT)
        return json.load(r).get("result")
    except Exception:
        return None


_last_price: dict[str, float] = {}  # token -> last known BNB/token price
_val_by_tx: dict[str, float] = {}   # txhash -> value_bnb (from PENDING line)


def mcap_calc(txh: str, token: str, side: str, value_bnb: float):
    """Executed mcap = price(BNB/token) × totalSupply × BNB-USD.

    BNB leg:
      • BUY  → native BNB spent = `value_bnb` (always correct, even when
        the router uses native BNB with no WBNB log).
      • SELL → WBNB ERC20 Transfer in the receipt; if the BNB leg is
        native (no log), fall back to the last known price for the token.
    Token leg: largest `token` Transfer in the receipt.
    Venue-agnostic (flap / Four.Meme / PancakeV2). Cached per tx."""
    key = (token.lower(), txh.lower())
    if key in _mcap:
        return _mcap[key]
    out = None
    try:
        rc = _rpc("eth_getTransactionReceipt", [txh])
        if rc and rc.get("logs"):
            tok = token.lower()
            tok_amt = wbnb_amt = 0
            for lg in rc["logs"]:
                if not lg["topics"] or lg["topics"][0].lower() != TRANSFER_TOPIC:
                    continue
                amt = _u(lg["data"])
                a = lg["address"].lower()
                if a == tok:
                    tok_amt = max(tok_amt, amt)
                elif a == WBNB.lower():
                    wbnb_amt = max(wbnb_amt, amt)
            dec = _decimals(token)
            if tok_amt:
                tok_whole = tok_amt / 10 ** dec
                bnb = 0.0
                if side == "BUY" and value_bnb > 0:
                    bnb = value_bnb
                elif wbnb_amt:
                    bnb = wbnb_amt / 1e18
                if bnb > 0 and tok_whole > 0:
                    price = bnb / tok_whole
                    _last_price[tok] = price
                elif tok in _last_price:
                    price = _last_price[tok]          # SELL w/ native BNB leg
                else:
                    price = 0.0
                if price > 0:
                    sup = _u(_call(token, "0x18160ddd"))
                    if sup:
                        out = price * (sup / 10 ** dec) * bnb_usd()
    except Exception:
        out = None
    _mcap[key] = out
    return out


def fmt_mcap(v) -> str:
    if v is None or v <= 0:
        return "—"
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    if v >= 1e6:
        return f"${v/1e6:.2f}M"
    if v >= 1e3:
        return f"${v/1e3:.0f}k"
    return f"${v:.0f}"


# ---- line parsing -----------------------------------------------------------
def fld(line: str, key: str) -> str:
    m = re.search(rf'{key}=("?)([^\s"]*)\1', line)
    return m.group(2) if m else ""


G, R, DIM, B, RST = "\x1b[32m", "\x1b[31m", "\x1b[2m", "\x1b[1m", "\x1b[0m"


def link(url: str, text: str) -> str:
    # OSC 8 terminal hyperlink: clickable in VS Code / iTerm2 / kitty /
    # GNOME Terminal / Windows Terminal. Text stays the full hash so it's
    # also copy-pasteable where OSC 8 isn't supported.
    return f"\x1b]8;;{url}\x1b\\{text}\x1b]8;;\x1b\\"


def dwidth(s: str) -> int:
    # East-Asian wide chars (CJK token names) render double-width.
    w = 0
    for ch in s:
        o = ord(ch)
        w += 2 if (0x1100 <= o <= 0x115F or 0x2E80 <= o <= 0xA4CF
                   or 0xAC00 <= o <= 0xD7A3 or 0xF900 <= o <= 0xFAFF
                   or 0xFF00 <= o <= 0xFF60 or 0x20000 <= o <= 0x3FFFD) else 1
    return w


def pad(s: str, n: int) -> str:
    return s + " " * max(0, n - dwidth(s))


HEADER = (
    f"{'TIME ' + TZ_LABEL:13}  {'KOL':3} {'SIDE':4} {'TOKEN':12} "
    f"{'MCAP':8} {'ST':2} {'DETAIL':24} TX"
)


def emit(line: str):
    if "KOL tx observed" in line:
        kind = "pending"
    elif "KOL tx CONFIRMED" in line:
        kind = "confirmed"
    else:
        return

    name = fld(line, "kol_name")
    side = fld(line, "side")
    token = fld(line, "token")
    mid = fld(line, "method_id")
    txh = fld(line, "tx_hash")
    txn = txh if txh.startswith("0x") else f"0x{txh}"
    ts = localize(line)

    # value_bnb only appears on the PENDING line — cache it for the
    # CONFIRMED line of the same tx (pending fires ~ms earlier).
    _vb_raw = fld(line, "value_bnb")
    if _vb_raw:
        try:
            _val_by_tx[txn] = float(_vb_raw)
        except ValueError:
            pass

    is_swap = mid == "0x4d819a2a" or token.startswith("0x")
    if not show_all and not is_swap:
        return  # suppress approve/transfer noise

    if flt in ("buy", "sell") and side.lower() != flt:
        return
    if flt and flt not in ("buy", "sell") and name != args[0]:
        return

    sym = symbol(token) if token.startswith("0x") else "-"
    scol = G if side == "BUY" else R if side == "SELL" else ""
    side_cell = f"{scol}{side:4}{RST}" if scol else f"{side:4}"
    if kind == "pending":
        st, detail, detail_plain, line_dim = "⏳", "pending", "pending", DIM
        # No receipt while pending → mcap unknown; the ✓ line ~ms later
        # carries the real executed mcap.
        mc = "·"
    else:
        blk = fld(line, "mined_block")
        vis = fld(line, "visibility")
        if vis == "private":
            # Never seen pending — direct-to-builder / private RPC. No
            # lead-time or block-delta exist (zero advance notice).
            st = "🔒"
            detail_plain = f"PRIVATE  blk {blk}"
            detail = f"{R}PRIVATE{RST}  {B}blk {blk}{RST}"
            line_dim = ""
        else:
            lead = fld(line, "lead_ms")
            delta = fld(line, "block_delta")
            st = "✓ "
            detail_plain = f"blk {blk}  +{lead}ms  Δ{delta}"
            dcol = R if delta == "0" else ""
            detail = f"{B}blk {blk}{RST}  +{lead}ms  {dcol}Δ{delta}{RST}"
            line_dim = ""
        # Executed mcap from the tx receipt + native value_bnb (cached
        # from the pending line of this tx).
        _vbnb = _val_by_tx.get(txn, 0.0)
        mc = (
            fmt_mcap(mcap_calc(txn, token, side, _vbnb))
            if token.startswith("0x")
            else "—"
        )

    detail += " " * max(0, 24 - len(detail_plain))
    tx_full = txh if txh.startswith("0x") else f"0x{txh}"
    tx_cell = link(f"https://bscscan.com/tx/{tx_full}", tx_full)
    row = (
        f"{line_dim}{ts:13}{RST}  {name:3} {side_cell} {pad(sym,12)} "
        f"{mc:8} {st:2} {detail} {DIM}{tx_cell}{RST}"
    )
    print(row)
    sys.stdout.flush()


# ---- replay history then follow --------------------------------------------
print(HEADER)
print("-" * len(HEADER))

if hist_min:
    raw = subprocess.run(
        [
            "journalctl",
            "-u",
            "bsc-runner.service",
            "--since",
            f"{hist_min} min ago",
            "--no-pager",
            "-o",
            "cat",
        ],
        capture_output=True,
        text=True,
    ).stdout
    for ln in ANSI.sub("", raw).splitlines():
        emit(ln)

proc = subprocess.Popen(
    ["journalctl", "-u", "bsc-runner.service", "-f", "-n", "0", "-o", "cat"],
    stdout=subprocess.PIPE,
    text=True,
)
try:
    for ln in proc.stdout:
        emit(ANSI.sub("", ln.rstrip("\n")))
except KeyboardInterrupt:
    proc.terminate()
