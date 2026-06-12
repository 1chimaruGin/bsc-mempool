#!/usr/bin/env python3
"""
For each of our recent D-public BUY broadcasts, list every Four.Meme
launchpad tx in the SAME block and the BLOCK BEFORE (where D usually
lands). Show effective_gas_price, tx_index, and who. Tests hypothesis:
we use 10 gwei → we land last.
"""
import json, os, urllib.request

RPC = os.environ.get("NODEREAL_RPC_URL") or "http://127.0.0.1:8545"
FOURMEME = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
GMGN_PROXY = "0x1de460f363af910f51726def188f9004276bf4bc"
PCS_V2 = "0x10ed43c718714eb63d5aa57b78b54704e256024e"
D_WALLET = "0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373"
OUR_WALLET = "0x530306684a29e23676d30fa80dc6100e80b042ea"

# (our_tx_hash, token_address, our_gas_gwei)
BROADCASTS = [
    ("0x951af14f4ba7096709b155f97846230b68ca7997e9eba91f474731cf7ac68054", "0x28c86673956d6236bf0ddaffaf0b41d345f57777", 10),
    ("0xf5f61d681a81062abd3edaf7294c5db30184429f364a776ad3edd1210455847e", "0x947af604e08b4278de287cc3df8be84b57f04444", 10),
    ("0xe4f4155d2b6b1edda018af076f156f113e23e8fb5ddc717d2fc20da3520fa771", "0x4a5d434c48ac852d96a75472f9151a43da654444", 10),
    ("0x6ea7bedb0c040820a8faaafc819f1d54959fefde65090cc5972360ccb451ac4f", "0x4a8e2a4952317660207aebc21cb4faf232da4444", 10),
    ("0x1a0b2bdf2d17b5e2273e364a3ecea12e1689992b22f387d770871c5c1afe6d53", "0x2f232c341166b43c7d51b01290bbde8cacdc4444", 10),
    ("0xee563611498d8cc90239ce15ff2340387cc8f2022c4c61c24d3ddd5fcc8527bd", "0x8a598d1bedb4c40220cb9a0b3b65cf6a46954444", 10),
    ("0x488b484e5d0e60bfca8ad2284ecab344b2f50ee6ef5328ae02ca5a334354c580", "0x6b6bb3a02b131741f5706bdc3edf4dfcb1aa4444", 10),
    ("0x4f104a5e0cb05661dd22230d5325c13772b99b20e0512d4efe056b51aee8008c", "0x6693bfe2b2911420273274ada16abda393b44444", 10),
]

def rpc(m, p):
    b = json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode()
    req = urllib.request.Request(RPC, data=b, headers={"content-type":"application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]

def to_int(h):
    return int(h, 16) if isinstance(h, str) else h

VENUES = {FOURMEME, GMGN_PROXY, PCS_V2}

def venue_label(addr):
    a = (addr or "").lower()
    if a == FOURMEME:    return "FOURMEME"
    if a == GMGN_PROXY:  return "GMGN_PRX"
    if a == PCS_V2:      return "PCS_V2  "
    return "        "

def label_who(addr):
    a = (addr or "").lower()
    if a == D_WALLET:   return "  [D]"
    if a == OUR_WALLET: return " [US]"
    return ""

def analyze(our_tx, token):
    our_receipt = rpc("eth_getTransactionReceipt", [our_tx])
    if not our_receipt:
        return
    our_block = to_int(our_receipt["blockNumber"])
    our_status = our_receipt["status"]
    our_idx = to_int(our_receipt["transactionIndex"])

    # Pull our block + previous block
    rows_by_block = {}
    for bn in [our_block - 1, our_block]:
        blk = rpc("eth_getBlockByNumber", [hex(bn), True])
        rows = []
        for tx in blk["transactions"]:
            to_addr = (tx.get("to") or "").lower()
            from_addr = (tx.get("from") or "").lower()
            # Keep: any launchpad/router tx, or D's tx, or our tx
            is_venue = to_addr in VENUES
            is_d = (from_addr == D_WALLET)
            is_us = (from_addr == OUR_WALLET)
            if not (is_venue or is_d or is_us):
                continue
            # Effective gas price from receipt for true landed value
            rcpt = rpc("eth_getTransactionReceipt", [tx["hash"]])
            eff_gp = to_int(rcpt.get("effectiveGasPrice") or tx.get("gasPrice"))
            rows.append({
                "idx": to_int(tx["transactionIndex"]),
                "gwei": eff_gp / 1e9,
                "from": from_addr,
                "to": to_addr,
                "status": rcpt["status"],
                "is_d": is_d, "is_us": is_us,
                "venue": venue_label(to_addr),
                "hash": tx["hash"],
            })
        rows.sort(key=lambda r: r["idx"])
        rows_by_block[bn] = rows

    # D usually lands in our_block-1; sometimes same block
    d_block = None
    for bn in [our_block - 1, our_block]:
        for r in rows_by_block[bn]:
            if r["is_d"]:
                d_block = bn
                break
        if d_block: break

    print(f"\n=== Token {token[:10]}…  Our block {our_block}  D block {d_block} (gap {our_block - d_block if d_block else '?'})  Our status={our_status} ===")
    for bn in [our_block - 1, our_block]:
        rows = rows_by_block[bn]
        if not rows:
            continue
        # Stats over the whole filtered block
        gweis = [r["gwei"] for r in rows]
        print(f"  Block {bn}: {len(rows)} relevant tx  gwei[min={min(gweis):.1f} median={sorted(gweis)[len(gweis)//2]:.1f} max={max(gweis):.1f}]")
        # Show only D, US, and the 3 around our_idx (or D's idx)
        anchor_idx = None
        for r in rows:
            if (bn == our_block and r["is_us"]) or (bn == d_block and r["is_d"]):
                anchor_idx = r["idx"]
        for r in rows:
            show = r["is_d"] or r["is_us"] or (anchor_idx is not None and abs(r["idx"] - anchor_idx) <= 3)
            if not show: continue
            tag = label_who(r["from"])
            st = "OK" if r["status"] == "0x1" else "RV"
            print(f"    idx={r['idx']:>3} {r['venue']} gwei={r['gwei']:>6.2f} st={st} from={r['from'][:14]}{tag}")

for our_tx, token, _ in BROADCASTS:
    try:
        analyze(our_tx, token)
    except Exception as e:
        print(f"err {our_tx[:10]}: {e}")
