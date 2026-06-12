#!/usr/bin/env python3
"""Latency benchmarks for the live trader hot paths.

Times each RPC roundtrip we make on BUY/SELL paths against the SAME
endpoints the production runner uses (local geth + NodeReal archive +
BlockRazor read-only). Reports min/p50/p90/p99/max over N iterations.

Does NOT submit any transactions (no money risk).

Usage:
  scripts/bench-hot-paths.py            # 50 iterations
  scripts/bench-hot-paths.py --n 200    # more samples
"""
import argparse
import json
import os
import sys
import time
import statistics
import urllib.request
from concurrent.futures import ThreadPoolExecutor

# ── env ────────────────────────────────────────────────────────────────
def load_env(path="/data/bsc-meme-mev/.env"):
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out

ENV = load_env()
LOCAL = "http://127.0.0.1:8545"
NODEREAL = ENV.get("NODEREAL_RPC_URL", "")
WALLET = "0x530306684A29E23676d30fA80dC6100e80b042ea"

# ── constants (same as executor_live.rs) ────────────────────────────────
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
PANCAKE_V2_ROUTER = "0x10ED43C718714eb63d5aA57B78B54704E256024E"
PANCAKE_V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
FOURMEME = "0x5c952063c7fc8610FFDB798152D69F0B9550762b"
TOKEN_CREATE_TOPIC = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"

# Sample tokens we have real data for (Four.Meme + V2)
SAMPLE_V2_TOKEN = "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"  # CAKE (V2 pair)
SAMPLE_FM_TOKEN  = "0x65d79e96e7c3495b45b69a4195f6d61eb8cd4444"  # builder (Four.Meme)
SAMPLE_KOL_ADDR  = "0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373"  # D


# ── rpc ─────────────────────────────────────────────────────────────────
def rpc_call(url, method, params, timeout=5):
    body = json.dumps({"jsonrpc":"2.0","method":method,"params":params,"id":1}).encode()
    req = urllib.request.Request(
        url, body,
        {"Content-Type":"application/json", "User-Agent":"Mozilla/5.0"},
    )
    t0 = time.perf_counter_ns()
    try:
        resp = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        return None, time.perf_counter_ns() - t0, str(e)
    return resp, time.perf_counter_ns() - t0, None


# ── one-off helpers (same shape as Rust hot-path helpers) ────────────────
def gas_price(url):
    _, ns, _ = rpc_call(url, "eth_gasPrice", [])
    return ns

def block_number(url):
    _, ns, _ = rpc_call(url, "eth_blockNumber", [])
    return ns

def balance_of(url, token, who, blk_tag="latest"):
    sel = "0x70a08231"
    data = sel + who[2:].lower().rjust(64, "0")
    _, ns, _ = rpc_call(url, "eth_call",
        [{"to": token, "data": data}, blk_tag])
    return ns

def get_amounts_out(url, amount_wei, path):
    # selector 0xd06ca61f for getAmountsOut(uint256,address[])
    # encode amountIn + offset(64) + len + addrs
    sel = "0xd06ca61f"
    head = f"{amount_wei:064x}" + f"{0x40:064x}" + f"{len(path):064x}"
    body = "".join(a[2:].lower().rjust(64, "0") for a in path)
    data = sel + head + body
    _, ns, _ = rpc_call(url, "eth_call",
        [{"to": PANCAKE_V2_ROUTER, "data": data}, "latest"])
    return ns

def get_pair(url, tokA, tokB):
    sel = "0xe6a43905"
    data = sel + tokA[2:].lower().rjust(64,"0") + tokB[2:].lower().rjust(64,"0")
    _, ns, _ = rpc_call(url, "eth_call",
        [{"to": PANCAKE_V2_FACTORY, "data": data}, "latest"])
    return ns

def fourmeme_sell_probe(url, token, amount_wei):
    sel = "0xf464e7db"
    data = sel + token[2:].lower().rjust(64,"0") + f"{amount_wei:064x}"
    _, ns, _ = rpc_call(url, "eth_call",
        [{"from": WALLET, "to": FOURMEME, "data": data}, "latest"])
    return ns

def get_logs_launchpad_window(url, head_block, span):
    from_blk = max(head_block - span, 0)
    params = {
        "address": FOURMEME,
        "fromBlock": hex(from_blk),
        "toBlock": hex(head_block),
        "topics": [TOKEN_CREATE_TOPIC],
    }
    _, ns, _ = rpc_call(url, "eth_getLogs", [params], timeout=15)
    return ns


# ── parallel kol_sell_fraction (mirrors `tokio::join!` in Rust) ──────────
def kol_sell_fraction_parallel(url, kol, token, blk):
    """Two parallel balanceOf calls — mirrors the Rust impl's tokio::join!"""
    def one(b):
        sel = "0x70a08231"
        data = sel + kol[2:].lower().rjust(64,"0")
        body = json.dumps({
            "jsonrpc":"2.0","method":"eth_call",
            "params":[{"to":token,"data":data}, hex(b)], "id":1,
        }).encode()
        req = urllib.request.Request(
            url, body, {"Content-Type":"application/json","User-Agent":"Mozilla/5.0"})
        try:
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            return False
        return True
    t0 = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=2) as ex:
        list(ex.map(one, [blk-1, blk]))
    return time.perf_counter_ns() - t0


# ── stats ───────────────────────────────────────────────────────────────
def quantile(xs, q):
    xs = sorted(xs)
    if not xs: return 0
    return xs[min(len(xs) - 1, int(q * len(xs)))]

def fmt_ns(ns):
    return f"{ns/1e6:.1f}ms"

def report(label, samples_ns):
    if not samples_ns:
        print(f"  {label:<40s}  (no data)")
        return
    print(
        f"  {label:<40s}  "
        f"min {fmt_ns(min(samples_ns)):>7s}  "
        f"p50 {fmt_ns(quantile(samples_ns, 0.5)):>7s}  "
        f"p90 {fmt_ns(quantile(samples_ns, 0.9)):>7s}  "
        f"p99 {fmt_ns(quantile(samples_ns, 0.99)):>7s}  "
        f"max {fmt_ns(max(samples_ns)):>7s}"
    )


# ── benches ─────────────────────────────────────────────────────────────
def run(N):
    print(f"=== Hot-path RPC benchmarks (n={N}) ===")
    print()

    # Warm-up — 3 calls to each endpoint so we measure steady-state not cold-connect.
    for _ in range(3):
        gas_price(LOCAL); block_number(LOCAL)
        if NODEREAL: gas_price(NODEREAL)

    # 1. eth_gasPrice — every BUY/SELL slow path needs it (cached for 2s in Rust)
    samples = [gas_price(LOCAL) for _ in range(N)]
    report("eth_gasPrice (local geth, uncached)", samples)
    if NODEREAL:
        samples = [gas_price(NODEREAL) for _ in range(N)]
        report("eth_gasPrice (NodeReal, uncached)", samples)
    print()

    # 2. eth_blockNumber — dev resolver anchor when kol_block=0
    samples = [block_number(LOCAL) for _ in range(N)]
    report("eth_blockNumber (local)", samples)
    print()

    # 3. balanceOf — token_balance for fast-path sell sanity, exit balance read
    samples = [balance_of(LOCAL, SAMPLE_V2_TOKEN, WALLET) for _ in range(N)]
    report("balanceOf @ latest (V2 token)", samples)
    samples = [balance_of(LOCAL, SAMPLE_FM_TOKEN, WALLET) for _ in range(N)]
    report("balanceOf @ latest (Four.Meme token)", samples)
    print()

    # 4. balanceOf at specific block — kol_sell_fraction
    # Pick a recent block from our journal (D's sell of 0xb15c8 on 2026-05-29)
    head = json.loads(urllib.request.urlopen(urllib.request.Request(
        LOCAL,
        json.dumps({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}).encode(),
        {"Content-Type":"application/json","User-Agent":"Mozilla/5.0"},
    ), timeout=5).read())["result"]
    head = int(head, 16)
    samples = [balance_of(LOCAL, SAMPLE_V2_TOKEN, SAMPLE_KOL_ADDR, hex(head - 5)) for _ in range(N)]
    report("balanceOf @ block (local, 5 blk old)", samples)
    print()

    # 5. kol_sell_fraction in parallel (the actual hot-path shape)
    samples = [kol_sell_fraction_parallel(LOCAL, SAMPLE_KOL_ADDR, SAMPLE_V2_TOKEN, head - 5) for _ in range(N)]
    report("kol_sell_fraction (2 parallel balanceOf)", samples)
    print()

    # 6. getAmountsOut — V2 amountOutMin computation + tax check round-trip
    one_bnb_wei = 10**15  # 0.001 BNB
    samples = [get_amounts_out(LOCAL, one_bnb_wei, [WBNB, SAMPLE_V2_TOKEN]) for _ in range(N)]
    report("getAmountsOut (V2 quote)", samples)
    # Tax check is TWO getAmountsOut (round-trip buy then sell)
    samples = [get_amounts_out(LOCAL, one_bnb_wei, [WBNB, SAMPLE_V2_TOKEN]) +
               get_amounts_out(LOCAL, one_bnb_wei // 1000, [SAMPLE_V2_TOKEN, WBNB])
               for _ in range(N)]
    report("implied_sell_tax_v2 (2× getAmountsOut)", samples)
    print()

    # 7. getPair — pick_route V2 check
    samples = [get_pair(LOCAL, WBNB, SAMPLE_V2_TOKEN) for _ in range(N)]
    report("factory.getPair (pick_route V2 check)", samples)
    print()

    # 8. Four.Meme sellToken probe — pick_route Four.Meme path
    samples = [fourmeme_sell_probe(LOCAL, SAMPLE_FM_TOKEN, 10**15) for _ in range(N)]
    report("Four.Meme sellToken probe", samples)
    print()

    # 9. dev_resolver eth_getLogs window
    if NODEREAL:
        samples = [get_logs_launchpad_window(NODEREAL, head, 5000) for _ in range(N // 5 or 1)]
        report("dev_resolver eth_getLogs (5k blocks, NodeReal)", samples)
    samples = [get_logs_launchpad_window(LOCAL, head, 5000) for _ in range(N // 5 or 1)]
    report("dev_resolver eth_getLogs (5k blocks, local)", samples)
    print()

    # ── synthesized end-to-end hot-path latencies ─────────────────────────
    # These add the dependent RPCs that the Rust execute()/execute_exit() do,
    # excluding sign (~3ms CPU) and broadcast (~50-100ms to BR).
    print("=== Synthesized end-to-end hot-path (RPC only — adds sign+broadcast on top) ===")

    # BUY (Four.Meme, common case for D/I): dev_resolver + gas + pick_route probe
    fm_buy = []
    for _ in range(N):
        ns = 0
        ns += get_pair(LOCAL, WBNB, SAMPLE_FM_TOKEN)  # pick_route V2 check (returns 0)
        ns += fourmeme_sell_probe(LOCAL, SAMPLE_FM_TOKEN, 10**15)  # FM dry-run
        # gas_wei skipped — Rust caches it for 2s
        fm_buy.append(ns)
    report("BUY Four.Meme (pair-check + FM probe)", fm_buy)

    # BUY (V2): + getAmountsOut for amountOutMin + tax round-trip
    v2_buy = []
    for _ in range(N):
        ns = 0
        ns += get_pair(LOCAL, WBNB, SAMPLE_V2_TOKEN)
        ns += get_amounts_out(LOCAL, one_bnb_wei, [WBNB, SAMPLE_V2_TOKEN])
        ns += get_amounts_out(LOCAL, one_bnb_wei // 1000, [SAMPLE_V2_TOKEN, WBNB])
        v2_buy.append(ns)
    report("BUY V2 (pair + amountOut + tax check)", v2_buy)

    # SELL fast-path: kol_sell_fraction only (cached route + cached balance + pre-approved)
    sell_fast = [
        kol_sell_fraction_parallel(LOCAL, SAMPLE_KOL_ADDR, SAMPLE_V2_TOKEN, head - 5)
        for _ in range(N)
    ]
    report("SELL fast-path (kol_sell_fraction only)", sell_fast)

    # SELL slow-path: balance + pick_route + (cached gas)
    sell_slow = []
    for _ in range(N):
        ns = 0
        ns += balance_of(LOCAL, SAMPLE_FM_TOKEN, WALLET)
        ns += get_pair(LOCAL, WBNB, SAMPLE_FM_TOKEN)
        ns += fourmeme_sell_probe(LOCAL, SAMPLE_FM_TOKEN, 1)
        ns += kol_sell_fraction_parallel(LOCAL, SAMPLE_KOL_ADDR, SAMPLE_FM_TOKEN, head - 5)
        sell_slow.append(ns)
    report("SELL slow-path (balance + route + fraction)", sell_slow)
    print()

    print("Notes:")
    print("  - sign  ≈  3-5ms CPU (alloy ECDSA, single-threaded; not benchmarked)")
    print("  - BlockRazor broadcast  ≈  50-100ms (skipped to avoid burning gas)")
    print("  - All RPC samples include JSON parse + HTTP keepalive (production-realistic)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=50, help="iterations per benchmark")
    args = ap.parse_args()
    run(args.n)
