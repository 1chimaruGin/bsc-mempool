#!/usr/bin/env python3
"""
Recompute paper PnL with EXACT chain-derived prices AND multi-sell ladder
exits. For every closed-trade row we:

  1. Resolve the KOL's actual entry tx (by Transfer log with TO=KOL_addr
     in the entry block window), read his receipt → entry price + mcap.
  2. Read our +1 entry price (V2 pool reserves at N+1 if pool exists,
     else the median price of real swaps in N+1..N+5).
  3. Scan up to 1 HOUR (~7200 blocks) for every SELL by the KOL of this
     token (Transfer with FROM=KOL_addr). For each tranche T_i:
        - read his sell receipt → KOL exec price + mcap at sell block
        - read our +1 V2 price at sell_block+1 → our fill price
        - close the proportional fraction of OUR position (T_i / T_kol_total)
     and accumulate our BNB proceeds.
  4. If KOL still holds at window end, FORCE-CLOSE the remainder at the
     V2 pool price at window_end (mirrors the 24h timeout in the live
     trader, applied to the 1h analysis window).

New CSV columns added (existing readers ignore extras):
  kol_exit_count          — number of KOL sells we mirrored
  kol_exit_mcap_first_usd — mcap at his first sell
  kol_exit_mcap_last_usd  — mcap at his last sell
  our_avg_exit_mcap_usd   — token-weighted avg of our +1 exit mcaps
  fill_source             — "v3_ladder" | "v3_force_close" | "unresolvable"

Sidecar full-detail JSONL:  closed_trades_details.jsonl  per-row drilldown.

  scripts/recompute-v3.py             # report TODAY UTC (read-only)
  scripts/recompute-v3.py --all       # all-time
  scripts/recompute-v3.py --backfill  # OVERWRITE closed_trades.csv + JSONL
"""
import csv, datetime, json, os, sys, time
from collections import defaultdict
import urllib.request, urllib.error


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
NODEREAL = ENV.get("NODEREAL_RPC_URL") or os.environ.get("NODEREAL_RPC_URL")
LOCAL = "http://127.0.0.1:8545"
if not NODEREAL:
    print("ERROR: NODEREAL_RPC_URL missing", file=sys.stderr)
    sys.exit(1)

WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
FACTORY_V2 = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
HOLD_WINDOW_BLOCKS = 7200  # ≈ 1h at 0.5s blocks
GETLOGS_MAX_RANGE = 5000   # NodeReal free plan max blocks per query
DETAILS_PATH = "/data/bsc-meme-mev/closed_trades_details.jsonl"


# ── KOL address book ──────────────────────────────────────────────────────
def load_kols(p="/data/bsc-meme-mev/config/kols.toml"):
    out, cur = {}, None
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line.startswith("address"):
                cur = line.split("=", 1)[1].strip().strip('"').lower()
            elif line.startswith("name") and cur:
                out[line.split("=", 1)[1].strip().strip('"')] = cur
                cur = None
    return out


KOLS = load_kols()


# ── RPC + budget ──────────────────────────────────────────────────────────
_last_archive_ns = [0]
_MIN_GAP_NS = 400_000_000
_archive_calls = [0]
_ARCHIVE_CAP = 50000  # full backfill — NodeReal free plan has ~3M CUs/day


def now_ns():
    return time.time_ns()


def rpc(url, method, params, archive=False, retries=2):
    if archive:
        if _archive_calls[0] >= _ARCHIVE_CAP:
            return None
        delta = now_ns() - _last_archive_ns[0]
        if delta < _MIN_GAP_NS:
            time.sleep((_MIN_GAP_NS - delta) / 1e9)
        _last_archive_ns[0] = now_ns()
        _archive_calls[0] += 1
    body = json.dumps(
        {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    ).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read()).get("result")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
            if attempt < retries:
                time.sleep(0.5)
                continue
            return None
        except Exception:
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
_supply, _pair, _price = {}, {}, {}


def supply(token):
    t = token.lower()
    if t in _supply:
        return _supply[t]
    r = rpc(LOCAL, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"])
    if not r or len(r) < 4:
        r = rpc(NODEREAL, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"], archive=True)
    _supply[t] = hx(r) if r else 0
    return _supply[t]


def v2_pair(token):
    t = token.lower()
    if t in _pair:
        return _pair[t]
    data = "0xe6a43905" + WBNB[2:].rjust(64, "0") + token[2:].rjust(64, "0")
    r = rpc(LOCAL, "eth_call", [{"to": FACTORY_V2, "data": data}, "latest"])
    if not r:
        _pair[t] = None
        return None
    addr = "0x" + r[-40:]
    _pair[t] = None if int(addr, 16) == 0 else addr
    return _pair[t]


def v2_price(token, block):
    """BNB-wei per raw-token-unit at `block` (end of block state). None if no pool/state."""
    pair = v2_pair(token)
    if not pair:
        return None
    k = (pair, block)
    if k in _price:
        return _price[k]
    r = rpc(LOCAL, "eth_call", [{"to": pair, "data": "0x0902f1ac"}, hex(block)])
    if not r or len(r) < 130 or r == "0x":
        r = rpc(NODEREAL, "eth_call", [{"to": pair, "data": "0x0902f1ac"}, hex(block)], archive=True)
    if not r or len(r) < 130 or r == "0x":
        _price[k] = None
        return None
    h = r[2:]
    r0, r1 = int(h[0:64], 16), int(h[64:128], 16)
    wbnb_r, tok_r = (r0, r1) if int(WBNB, 16) < int(token.lower(), 16) else (r1, r0)
    _price[k] = (wbnb_r / tok_r) if tok_r else None
    return _price[k]


# ── per-tx pricing (with archive balance fallback) ─────────────────────────
def price_from_tx(tx_hash, token_lc, is_buy):
    r = rpc(LOCAL, "eth_getTransactionReceipt", [tx_hash])
    if not r:
        r = rpc(NODEREAL, "eth_getTransactionReceipt", [tx_hash], archive=True)
    if not r or r.get("status") != "0x1":
        return None
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
        return None
    if is_buy:
        tx = rpc(LOCAL, "eth_getTransactionByHash", [tx_hash]) or \
             rpc(NODEREAL, "eth_getTransactionByHash", [tx_hash], archive=True)
        if not tx:
            return None
        v = hx(tx.get("value"))
        bnb_wei = v if v > 0 else wbnb_amt
    else:
        if wbnb_amt > 0:
            bnb_wei = wbnb_amt
        else:
            blk = hx(r.get("blockNumber"))
            gas_used = hx(r.get("gasUsed"))
            gas_price = hx(r.get("effectiveGasPrice"))
            tx = rpc(LOCAL, "eth_getTransactionByHash", [tx_hash]) or \
                 rpc(NODEREAL, "eth_getTransactionByHash", [tx_hash], archive=True)
            if not tx:
                return None
            d = tx.get("from")
            if not d:
                return None
            ba = rpc(LOCAL, "eth_getBalance", [d, hex(blk)])
            bb = rpc(LOCAL, "eth_getBalance", [d, hex(blk - 1)])
            if ba is None or bb is None:
                ba = rpc(NODEREAL, "eth_getBalance", [d, hex(blk)], archive=True)
                bb = rpc(NODEREAL, "eth_getBalance", [d, hex(blk - 1)], archive=True)
            if ba is None or bb is None:
                return None
            proceeds = (hx(ba) - hx(bb)) + gas_used * gas_price
            if proceeds <= 0:
                return None
            bnb_wei = proceeds
    return (bnb_wei / tok_amt) if (bnb_wei > 0 and tok_amt > 0) else None


# ── KOL position helpers ──────────────────────────────────────────────────
def get_kol_entry_tx(token, kol_addr, opened_block):
    """Earliest Transfer to KOL_addr around opened_block ± 2 → his entry tx."""
    if not kol_addr:
        return None
    filt = [{
        "address": token,
        "fromBlock": hex(max(0, opened_block - 2)),
        "toBlock": hex(opened_block + 1),
        "topics": [TRANSFER, None,
                   "0x" + kol_addr[2:].rjust(64, "0")],
    }]
    logs = rpc(NODEREAL, "eth_getLogs", filt, archive=True) or []
    if not logs:
        return None
    logs.sort(key=lambda l: (hx(l.get("blockNumber")), hx(l.get("logIndex"))))
    return logs[0].get("transactionHash")


def kol_balance_after_entry(token, kol_addr, block):
    """KOL's token balance at end of `block` — eth_call balanceOf."""
    if not kol_addr:
        return 0
    # balanceOf(address): selector 0x70a08231
    data = "0x70a08231" + kol_addr[2:].rjust(64, "0")
    r = rpc(NODEREAL, "eth_call", [{"to": token, "data": data}, hex(block)], archive=True)
    return hx(r)


def find_kol_sells(token, kol_addr, from_blk, to_blk):
    """All Transfer logs with FROM=kol_addr in [from_blk, to_blk]. Chunked
    to respect NodeReal's max range."""
    out = []
    if not kol_addr:
        return out
    cur = from_blk
    while cur <= to_blk:
        end = min(cur + GETLOGS_MAX_RANGE, to_blk)
        filt = [{
            "address": token,
            "fromBlock": hex(cur),
            "toBlock": hex(end),
            "topics": [TRANSFER,
                       "0x" + kol_addr[2:].rjust(64, "0"),
                       None],
        }]
        logs = rpc(NODEREAL, "eth_getLogs", filt, archive=True) or []
        out.extend(logs)
        cur = end + 1
    # sort + dedupe by tx_hash (a tx may have multiple Transfer logs)
    by_tx = {}
    for lg in sorted(out, key=lambda l: (hx(l.get("blockNumber")),
                                          hx(l.get("logIndex")))):
        txh = lg.get("transactionHash")
        if not txh:
            continue
        if txh in by_tx:
            # Sum amounts within the same tx (multi-leg sells)
            by_tx[txh]["amt"] += hx(lg.get("data"))
        else:
            by_tx[txh] = {
                "tx": txh,
                "block": hx(lg.get("blockNumber")),
                "amt": hx(lg.get("data")),
                "to": "0x" + (lg.get("topics", [None, None, None])[2] or "")[-40:],
            }
    return sorted(by_tx.values(), key=lambda x: x["block"])


# ── per-row resolver ───────────────────────────────────────────────────────
def resolve(row):
    try:
        bnb_in_wei = int(row["bnb_in_wei"])
        opened = int(row["opened_at_block"])
        token = row["token_address"]
        kol_name = row["kol_name"]
        bnb_usd = float(row.get("bnb_usd_close") or 0)
    except (KeyError, ValueError):
        return None
    if opened == 0 or not token.startswith("0x"):
        return None
    bnb_in = bnb_in_wei / 1e18
    kol_addr = KOLS.get(kol_name)
    sup = supply(token)

    # KOL's entry price + tokens received
    entry_tx = get_kol_entry_tx(token, kol_addr, opened)
    kol_entry_px = price_from_tx(entry_tx, token.lower(), True) if entry_tx else None
    # KOL's total tokens of this asset right after entry
    kol_total = kol_balance_after_entry(token, kol_addr, opened + 1)
    if kol_total == 0 and entry_tx:
        # fallback: tokens transferred in his entry receipt
        r = rpc(LOCAL, "eth_getTransactionReceipt", [entry_tx]) or \
            rpc(NODEREAL, "eth_getTransactionReceipt", [entry_tx], archive=True)
        if r:
            for lg in r.get("logs") or []:
                tps = lg.get("topics") or []
                if not tps or (tps[0] or "").lower() != TRANSFER:
                    continue
                if (lg.get("address") or "").lower() != token.lower():
                    continue
                to = "0x" + (tps[2] or "")[-40:]
                if to.lower() == kol_addr:
                    kol_total += hx(lg.get("data"))

    # Our +1 entry price
    our_entry_px = v2_price(token, opened + 1)
    if not our_entry_px:
        # Bonding-curve case: try median of real swaps in N+1..N+5
        filt = [{
            "address": token,
            "fromBlock": hex(opened + 1),
            "toBlock": hex(opened + 5),
            "topics": [TRANSFER],
        }]
        logs = rpc(NODEREAL, "eth_getLogs", filt, archive=True) or []
        seen, prices = set(), []
        for lg in logs[:15]:
            tx = lg.get("transactionHash")
            if not tx or tx in seen:
                continue
            seen.add(tx)
            p = price_from_tx(tx, token.lower(), True)
            if p and p > 0:
                prices.append(p)
        if prices:
            prices.sort()
            our_entry_px = prices[len(prices) // 2]
    if not our_entry_px:
        our_entry_px = kol_entry_px  # last resort: KOL's price (slight bias)
    if not our_entry_px or our_entry_px <= 0 or kol_total <= 0:
        return None

    our_tokens = bnb_in / our_entry_px  # in raw token units

    # KOL's full sell sequence over the 1h window
    sells = find_kol_sells(token, kol_addr,
                            opened + 1, opened + HOLD_WINDOW_BLOCKS)

    # Mirror each KOL sell proportionally
    our_bnb_out_wei = 0.0
    cum_kol_sold = 0
    fills = []
    for s in sells:
        if cum_kol_sold >= kol_total:
            break
        kol_sell_amt = min(s["amt"], kol_total - cum_kol_sold)
        if kol_sell_amt <= 0:
            continue
        # KOL's exec price for this sell
        kol_sell_px = price_from_tx(s["tx"], token.lower(), False)
        # Our +1 exit price (V2 pool reserves at sell_block+1)
        our_exit_px = v2_price(token, s["block"] + 1)
        if not our_exit_px:
            # Bonding fallback: median in sell_block..sell_block+5
            filt = [{
                "address": token,
                "fromBlock": hex(s["block"]),
                "toBlock": hex(s["block"] + 5),
                "topics": [TRANSFER],
            }]
            logs = rpc(NODEREAL, "eth_getLogs", filt, archive=True) or []
            seen, prices = set(), []
            for lg in logs[:15]:
                tx = lg.get("transactionHash")
                if not tx or tx in seen:
                    continue
                seen.add(tx)
                p = price_from_tx(tx, token.lower(), False)
                if p and p > 0:
                    prices.append(p)
            if prices:
                prices.sort()
                our_exit_px = prices[len(prices) // 2]
        if not our_exit_px:
            our_exit_px = kol_sell_px or our_entry_px  # last resort
        frac = kol_sell_amt / kol_total
        our_tokens_sold = our_tokens * frac
        bnb_out_i = our_tokens_sold * our_exit_px
        our_bnb_out_wei += bnb_out_i
        cum_kol_sold += kol_sell_amt
        # Mcap = price(BNB-wei/raw) × supply(raw) × bnb_usd ÷ 1e18 (wei→BNB).
        kol_mcap = (kol_sell_px * sup * bnb_usd / 1e18) if (kol_sell_px and sup) else 0
        our_mcap = (our_exit_px * sup * bnb_usd / 1e18) if (our_exit_px and sup) else 0
        fills.append({
            "block": s["block"],
            "kol_sell_tx": s["tx"],
            "kol_sell_tokens": kol_sell_amt,
            "our_tokens_sold": our_tokens_sold,
            "kol_exit_px": kol_sell_px,
            "our_exit_px": our_exit_px,
            "kol_exit_mcap_usd": kol_mcap,
            "our_exit_mcap_usd": our_mcap,
            "bnb_out_wei_i": bnb_out_i,
        })

    # Force-close any remainder at window end
    force_closed = False
    if cum_kol_sold < kol_total:
        force_blk = opened + HOLD_WINDOW_BLOCKS
        force_px = v2_price(token, force_blk)
        if not force_px:
            # Try a recent block we have a price for — last sell block, or last chain swap
            if fills:
                force_px = fills[-1]["our_exit_px"]
            else:
                force_px = our_entry_px  # 0% PnL on remainder
        remainder_frac = (kol_total - cum_kol_sold) / kol_total
        remainder_tokens = our_tokens * remainder_frac
        bnb_out_force = remainder_tokens * force_px
        our_bnb_out_wei += bnb_out_force
        force_closed = True
        kol_mcap_f = (force_px * sup * bnb_usd / 1e18) if sup else 0
        fills.append({
            "block": force_blk,
            "kol_sell_tx": None,
            "kol_sell_tokens": kol_total - cum_kol_sold,
            "our_tokens_sold": remainder_tokens,
            "kol_exit_px": None,
            "our_exit_px": force_px,
            "kol_exit_mcap_usd": kol_mcap_f,
            "our_exit_mcap_usd": kol_mcap_f,
            "bnb_out_wei_i": bnb_out_force,
            "force_close": True,
        })

    bnb_out = our_bnb_out_wei  # already in BNB units (price was wei/raw)
    pnl_bnb = bnb_out - bnb_in
    pnl_usd = pnl_bnb * bnb_usd

    # mcaps
    kol_entry_mcap = (kol_entry_px * sup * bnb_usd / 1e18) if (kol_entry_px and sup) else 0
    our_entry_mcap = (our_entry_px * sup * bnb_usd / 1e18) if sup else 0
    kol_exit_mcaps = [f["kol_exit_mcap_usd"] for f in fills if f.get("kol_exit_mcap_usd")]
    our_avg_exit_mcap = (
        sum(f["our_exit_mcap_usd"] * f["our_tokens_sold"] for f in fills) /
        sum(f["our_tokens_sold"] for f in fills)
    ) if fills and sum(f["our_tokens_sold"] for f in fills) > 0 else 0

    return {
        "bnb_out_wei": int(max(bnb_out, 0) * 1e18),
        "pnl_wei": int(pnl_bnb * 1e18),
        "pnl_bnb": pnl_bnb,
        "pnl_usd": pnl_usd,
        "pnl_pct": (pnl_bnb / bnb_in) if bnb_in > 0 else 0.0,
        "kol_entry_mcap_usd": kol_entry_mcap,
        "our_entry_mcap_usd": our_entry_mcap,
        "kol_exit_count": len([f for f in fills if not f.get("force_close")]),
        "kol_exit_mcap_first_usd": kol_exit_mcaps[0] if kol_exit_mcaps else 0,
        "kol_exit_mcap_last_usd": kol_exit_mcaps[-1] if kol_exit_mcaps else 0,
        "our_avg_exit_mcap_usd": our_avg_exit_mcap,
        "fill_source": ("v3_force_close" if force_closed and not kol_exit_mcaps
                         else "v3_ladder"),
        "fills": fills,
    }


# ── CSV io ─────────────────────────────────────────────────────────────────
NEW_COLS = ["kol_exit_count", "kol_exit_mcap_first_usd", "kol_exit_mcap_last_usd",
            "our_avg_exit_mcap_usd"]


def load_csv(path, label, today_only):
    if not os.path.exists(path):
        return []
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    out = []
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                ts = int(r["ts_unix_ns"])
            except (KeyError, ValueError):
                continue
            if today_only:
                d = datetime.datetime.fromtimestamp(
                    ts / 1e9, datetime.timezone.utc
                ).strftime("%Y-%m-%d")
                if d != today:
                    continue
            r["_path"] = label
            out.append(r)
    return out


def backfill(path, details_fh):
    if not os.path.exists(path):
        return {}
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = f"{path}.pre-v3-{ts}"
    with open(path) as f:
        with open(backup, "w") as g:
            g.write(f.read())
    print(f"  {path}\n    backed up → {backup}", file=sys.stderr)
    with open(path) as f:
        rdr = csv.DictReader(f)
        fields = list(rdr.fieldnames)
        rows = list(rdr)
    for c in NEW_COLS:
        if c not in fields:
            fields.append(c)
    counts = defaultdict(int)
    for i, r in enumerate(rows, 1):
        if i % 25 == 0:
            print(f"    {i}/{len(rows)}  (archive: {_archive_calls[0]})",
                  file=sys.stderr)
        res = resolve(r)
        if res is None:
            counts["unresolvable"] += 1
            r["close_reason"] = "unresolvable"
            r["bnb_out_wei"] = r["bnb_in_wei"]
            r["pnl_wei"] = "0"; r["pnl_bnb"] = "0.000000"
            r["pnl_usd"] = "0.00"; r["pnl_pct"] = "0.000000"
            for c in NEW_COLS:
                r.setdefault(c, "")
            continue
        counts[res["fill_source"]] += 1
        r["bnb_out_wei"] = str(res["bnb_out_wei"])
        r["pnl_wei"] = str(res["pnl_wei"])
        r["pnl_bnb"] = f"{res['pnl_bnb']:.6f}"
        r["pnl_usd"] = f"{res['pnl_usd']:.2f}"
        r["pnl_pct"] = f"{res['pnl_pct']:.6f}"
        r["d_mcap_usd"] = f"{res['kol_entry_mcap_usd']:.0f}"
        r["our_entry_mcap_usd"] = f"{res['our_entry_mcap_usd']:.0f}"
        r["kol_exit_count"] = str(res["kol_exit_count"])
        r["kol_exit_mcap_first_usd"] = f"{res['kol_exit_mcap_first_usd']:.0f}"
        r["kol_exit_mcap_last_usd"] = f"{res['kol_exit_mcap_last_usd']:.0f}"
        r["our_avg_exit_mcap_usd"] = f"{res['our_avg_exit_mcap_usd']:.0f}"
        if r.get("close_reason") in ("price_unavailable", "no_liquidity"):
            r["close_reason"] = "kol_sell" if res["kol_exit_count"] > 0 else "timeout"
        details_fh.write(json.dumps({
            "ts_unix_ns": r["ts_unix_ns"],
            "kol": r["kol_name"],
            "token_addr": r["token_address"],
            "token_symbol": r["token_symbol"],
            "portfolio": r["portfolio"],
            "fills": res["fills"],
        }, default=str) + "\n")
    tmp = f"{path}.tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})
    os.replace(tmp, path)
    return counts


def report(rows, scope):
    by_path = defaultdict(lambda: {"n": 0, "bnb": 0.0, "usd": 0.0, "wins": 0,
                                    "fc": 0, "unres": 0,
                                    "avg_kol_exits": 0, "rows_with_exits": 0})
    by_kol = defaultdict(lambda: {"pub_n": 0, "pub_usd": 0.0,
                                   "prv_n": 0, "prv_usd": 0.0})
    for i, r in enumerate(rows, 1):
        if i % 20 == 0:
            print(f"  {i}/{len(rows)}  (archive: {_archive_calls[0]})",
                  file=sys.stderr)
        res = resolve(r)
        path = r["_path"]
        if res is None:
            by_path[path]["unres"] += 1
            continue
        if res["fill_source"] == "v3_force_close":
            by_path[path]["fc"] += 1
        by_path[path]["n"] += 1
        by_path[path]["bnb"] += res["pnl_bnb"]
        by_path[path]["usd"] += res["pnl_usd"]
        if res["pnl_usd"] > 0:
            by_path[path]["wins"] += 1
        if res["kol_exit_count"] > 0:
            by_path[path]["avg_kol_exits"] += res["kol_exit_count"]
            by_path[path]["rows_with_exits"] += 1
        kol = r["kol_name"]
        if path == "public":
            by_kol[kol]["pub_n"] += 1
            by_kol[kol]["pub_usd"] += res["pnl_usd"]
        else:
            by_kol[kol]["prv_n"] += 1
            by_kol[kol]["prv_usd"] += res["pnl_usd"]
    print()
    print(f"\033[1mRECOMPUTED PAPER REPORT v3\033[0m  scope={scope}  "
          f"(multi-sell ladder, 1h window)")
    print("=" * 88)
    print(f"{'PATH':9} {'closed':>6} {'netBNB':>11} {'netUSD':>9} "
          f"{'win':>5} {'forced':>7} {'unres':>6} {'avg_exits':>10}")
    print("-" * 80)
    for path in ("public", "private"):
        a = by_path[path]
        wr = (a["wins"] / a["n"] * 100) if a["n"] else 0
        avg_x = (a["avg_kol_exits"] / a["rows_with_exits"]) if a["rows_with_exits"] else 0
        col = "\033[32m" if a["usd"] >= 0 else "\033[31m"
        print(f"{path:9} {a['n']:6d} {a['bnb']:11.5f} "
              f"{col}{a['usd']:9.2f}\033[0m {wr:4.0f}% "
              f"{a['fc']:7d} {a['unres']:6d} {avg_x:10.2f}")
    print()
    print(f"{'KOL':4} {'pub#':>5} {'pubUSD':>9} {'prv#':>5} {'prvUSD':>9}  best")
    print("-" * 48)
    rank = sorted(by_kol.items(),
                  key=lambda kv: -(kv[1]["pub_usd"] + kv[1]["prv_usd"]))
    for kol, a in rank:
        best = "private" if a["prv_usd"] >= a["pub_usd"] else "public"
        print(f"{kol:4} {a['pub_n']:5d} {a['pub_usd']:9.2f} "
              f"{a['prv_n']:5d} {a['prv_usd']:9.2f}  {best}")
    if rank:
        top_k, top = rank[0]
        print(f"\n🏆 {top_k} leads (${top['pub_usd'] + top['prv_usd']:.2f} combined)")
    pp = by_path["public"]["usd"]
    pr = by_path["private"]["usd"]
    print(f"🥇 path: public ${pp:.2f}  vs  private ${pr:.2f}  → "
          f"{'private' if pr >= pp else 'public'}")
    print(f"\n[archive calls used: {_archive_calls[0]} / {_ARCHIVE_CAP}]",
          file=sys.stderr)


def main():
    if "--backfill" in sys.argv:
        print("BACKFILL v3 — multi-sell ladder, 1h window, sidecar JSONL…",
              file=sys.stderr)
        details = open(DETAILS_PATH, "w")
        try:
            a = backfill("/data/bsc-meme-mev/trader/closed_trades.csv", details)
            b = backfill("/data/bsc-meme-mev/trader_private/closed_trades.csv", details)
        finally:
            details.close()
        merged = defaultdict(int)
        for d in (a, b):
            for k, v in d.items():
                merged[k] += v
        print("\nfill_source totals:", file=sys.stderr)
        for k in ("v3_ladder", "v3_force_close", "unresolvable"):
            print(f"  {k:18s}: {merged.get(k,0)}", file=sys.stderr)
        print(f"sidecar JSONL: {DETAILS_PATH}", file=sys.stderr)
        print(f"[archive calls used: {_archive_calls[0]} / {_ARCHIVE_CAP}]",
              file=sys.stderr)
        return

    today_only = "--all" not in sys.argv
    pub = load_csv("/data/bsc-meme-mev/trader/closed_trades.csv", "public", today_only)
    prv = load_csv("/data/bsc-meme-mev/trader_private/closed_trades.csv", "private", today_only)
    rows = pub + prv
    if not rows:
        print("no rows in scope")
        return
    print(f"Recomputing {len(rows)} rows v3 (multi-sell ladder, 1h window)…",
          file=sys.stderr)
    label = (datetime.datetime.now(datetime.timezone.utc).strftime("TODAY %Y-%m-%d")
             if today_only else "ALL-TIME")
    report(rows, label)


if __name__ == "__main__":
    main()
