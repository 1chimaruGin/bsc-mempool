# bsc-meme-mev — self-hosted BSC mempool + trading bot

Pivot from `eth-meme-mev` (Ethereum mainnet). The ETH stack
([github.com/1chimaruGin/eth-mempool](https://github.com/1chimaruGin/eth-mempool))
is frozen as a reference; this is a clean BSC build.

## Why BSC

| Constraint | Ethereum | BSC |
|---|---|---|
| Gas per swap | $5–50 | $0.10–0.30 |
| Block time | 12 s | 3 s |
| $50 ticket gas overhead | 10–100% | 0.2–0.6% |
| Memecoin churn | low | high (Four.Meme, PancakeSwap, KOL flow) |
| Liquidator competition | OEV-auctioned | open (Venus, Radiant) |
| Self-hosted node | Reth (1 TB pruned) | bsc-geth (~600 GB pruned) |

For a $450 risk-capital operator, BSC's cost structure is fundamentally
more accommodating. The same patterns from the ETH stack port over —
mempool listener → bus → KOL filter → paper trader → on-chain executor —
but the unit economics actually work at this size.

## Stack

```
                              bsc-geth (PoSA full node, pruned)
                                       │ IPC
                  ┌────────────────────┼─────────────────────┐
                  │                    │                     │
              bsc-core            bsc-sources            bsc-telemetry
              (PendingTx,         (IPC + WSS raw         (Prometheus,
               RLP decode,         pending-tx              capture,
               signer recovery)    subscription)           block oracle)
                  │                    │                     │
                  └────────────────────┼─────────────────────┘
                                       │
                                  bsc-bus
                          (decoder → dedupe → fanout)
                                       │
                  ┌────────────────────┼─────────────────────┐
                  │                    │                     │
              bsc-kol             bsc-trader            bsc-liquidator
              (KOL watchlist,     (PancakeSwap V2/V3,    (Venus + Radiant
               Telegram alerts)    paper trader)          health-factor poll)
```

All wired by `bsc-runner` (binary).

## Chain constants — BSC mainnet

| | Address |
|---|---|
| Chain ID | `56` |
| Native gas token | BNB (18 dec) |
| WBNB | `0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c` |
| PancakeSwap V2 Router | `0x10ED43C718714eb63d5aA57B78B54704E256024E` |
| PancakeSwap V2 Factory | `0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73` |
| PancakeSwap V3 Router | `0x13f4EA83D0bd40E75C8222255bc855a974568Dd4` |
| PancakeSwap V3 Factory | `0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865` |
| PancakeSwap V3 QuoterV2 | `0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997` |
| Multicall3 | `0xcA11bde05977b3631167028862bE2a173976CA11` |
| USDT (18 dec on BSC!) | `0x55d398326f99059fF775485246999027B3197955` |
| USDC | `0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d` |
| Venus Comptroller | `0xfD36E2c2a6789Db23113685031d7F16329158384` |
| Venus vBNB | `0xA07c5b74C9B40447a954e1466938b865b6BBea36` |

(Pinned to BSC mainnet docs — verify on each launch with `bsc-runner verify-addresses`.)

## Quick start (eventual)

```bash
# 1. Install + sync bsc-geth (separate step — multi-day initial sync)
sudo bash scripts/install-bsc-geth.sh
systemctl enable --now bsc-geth.service
journalctl -u bsc-geth -f   # wait for ~head

# 2. Build runner
cargo build --release -p bsc-runner

# 3. Configure
cp config/default.toml /etc/bsc-meme-mev.toml
$EDITOR /etc/bsc-meme-mev.toml   # fill IPC path, KOL list, Telegram

# 4. Start
./scripts/run.sh
```

## Roadmap

| Day | Milestone |
|---|---|
| **Day 0** | Scaffold (this commit) — workspace, configs, install scripts |
| **Day 1** | bsc-geth sync started; mempool IPC reachable; first PendingTx logged |
| **Day 2** | KOL watcher + Telegram (port the ETH-side `kol_watch.rs` + `kol_confirm.rs`) |
| **Day 3** | PancakeSwap V2/V3 quoter; paper trader same-block vs next-block portfolios |
| **Day 4** | Venus liquidation observer (V2-style accrue interest then poll borrowers) |
| **Day 5** | Memecoin sniping: Four.Meme bonding-curve detector |
| **Day 6** | Live trading switch (paper → real, guarded by a single config flag) |
| **Day 7+** | Tune, measure, iterate |

## What is NOT being ported from ETH

- `mempool-beacon` — BSC has no separate CL; PoSA consensus is in-process to bsc-geth
- Day-2A Chainlink oracle-update detector — BSC uses different oracle flows (Binance Oracle, partial Chainlink). Re-derive only if/when Venus liquidations are wired up
- Aave V3 day-3 flash-loan contract — Aave is not on BSC; Venus is the equivalent and the architecture differs

## Sources of strategy ideas

- Four.Meme bonding-curve launches (BSC equivalent of Solana's Pump.fun)
- KOL trades via GMGN proxy (`0x1de460f363AF910f51726DEf188F9004276Bf4bc` — flagged earlier)
- PancakeSwap V3 LP rebalance bots
- Venus liquidations
- BSC-specific MEV (slot leaders are 21 rotating validators; private flow share is high)

## Security note

This is a personal research project. **No funds are exposed until the live-trading
flag is flipped in `config`.** Until then everything is paper-mode + observability.
