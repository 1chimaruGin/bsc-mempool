# bsc-meme-mev — self-hosted BSC memecoin trading bot

Live KOL-following meme trader on BSC mainnet. Watches whitelisted KOL wallets
in the mempool, copy-trades their Four.Meme / PancakeSwap V2 BUYs, and exits via
a peak-trail + signal_vote state machine.

Originally a pivot from `eth-meme-mev` ([github.com/1chimaruGin/eth-mempool](https://github.com/1chimaruGin/eth-mempool));
the ETH stack is frozen as a reference and this is a clean BSC build.

---

## Status

**Live trading** at $10 per BUY, following KOL D on public mempool.
- Block gap from D's tx: consistently **N+1** (down from baseline N+2)
- Race-submit across 3 paths: local geth + BlockRazor MEV-Boost + Puissant (48 Club)
- Nonce-drift fix (disk persistence) protects against multi-path mempool divergence

See [docs/SESSION-LOG.md](docs/SESSION-LOG.md) for what shipped, what was rejected,
and the EV-floor finding.

---

## Documentation

Start here:

| Doc | Purpose |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System overview, module inventory, data flow, boot sequence |
| [docs/TRADER.md](docs/TRADER.md) | Trader deep-dive: race-submit, bundle paths, nonce manager, adaptive trail |
| [docs/gap-reduction.md](docs/gap-reduction.md) | Catalog of latency / gap-reduction levers ranked by impact and effort |
| [docs/SCRIPTS.md](docs/SCRIPTS.md) | Analysis + backtest scripts catalog |
| [docs/SESSION-LOG.md](docs/SESSION-LOG.md) | 2026-06-08 to 2026-06-10 development log |

---

## Quick start (cold install)

```bash
# 1. Install + sync bsc-geth (multi-day initial sync; separate machine fine)
sudo bash scripts/install-bsc-geth.sh
systemctl enable --now bsc-geth.service
journalctl -u bsc-geth -f                   # wait for sync to ~head

# 2. Build runner
cargo build --release -p bsc-runner

# 3. Configure
cp config/default.toml /etc/bsc-meme-mev.toml
$EDITOR /etc/bsc-meme-mev.toml              # set IPC, KOL list, Telegram, etc.

# Secrets (NEVER commit these):
cat > .env <<EOF
WALLET_PRIVATE_KEY=0x...
WALLET_ADDRESS=0x...
BLOCKRAZOR_AUTH_KEY=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
NODEREAL_RPC_URL=...                        # OPTIONAL — for analysis scripts only
EOF
chmod 600 .env

# 4. Run live trading config (initially shadow-only, then full)
$EDITOR config/limits.toml                  # [phase] shadow=true to start
./scripts/install-runner.sh                 # installs systemd unit
systemctl start bsc-runner
journalctl -u bsc-runner -f
```

To go live: edit `config/limits.toml`, set `[phase] full = true, shadow = false`, restart.

---

## Stack

```
                            bsc-geth (full node, ~5.5ms RTT to BlockRazor)
                                         │
                       ┌─────────────────┼─────────────────┐
                       │                 │                 │
                  WS pending          WS newHeads       HTTP RPC
                       │                 │                 │
                       └─────────────────┼─────────────────┘
                                         ▼
                                    bsc-runner
                          ┌──────────────────────────┐
                          │  kol_watch  →  strategy  │
                          │       │           │      │
                          │       ▼           ▼      │
                          │  adaptive_trail        │
                          │           │            │
                          │           ▼            │
                          │   executor_live        │
                          │     (race-submit)      │
                          └────────┬───────────────┘
                                   │
                  ┌────────────────┼────────────────┐
                  │                │                │
              local geth      BlockRazor         Puissant
              eth_sendRaw     eth_sendBundle     eth_sendPuissant
                  │                │                │
                  └────────────────┴────────────────┘
                                   │
                            BSC validators
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full data flow.

---

## Why BSC

| Constraint | Ethereum | BSC |
|---|---|---|
| Gas per swap | $5-50 | $0.10-0.30 |
| Block time | 12 s | 0.45 s (post-Fermi) |
| $10 ticket gas overhead | 50-500% | 15-25% |
| Memecoin churn | low | high (Four.Meme, KOL flow) |
| Self-hosted node | Reth (1 TB pruned) | bsc-geth (~1.5 TB pruned) |

For a small-cap operator BSC's cost structure works at this size where ETH doesn't.

---

## Chain constants — BSC mainnet

| | Address |
|---|---|
| Chain ID | `56` |
| Native gas token | BNB (18 dec) |
| WBNB | `0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c` |
| PancakeSwap V2 Router | `0x10ED43C718714eb63d5aA57B78B54704E256024E` |
| PancakeSwap V2 Factory | `0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73` |
| **Four.Meme launchpad** | `0x5c952063c7fc8610FFDB798152D69F0B9550762b` |
| GMGN router | `0x1de460f363AF910f51726DEf188F9004276Bf4bc` |
| Multicall3 | `0xcA11bde05977b3631167028862bE2a173976CA11` |
| USDT (18 dec on BSC!) | `0x55d398326f99059fF775485246999027B3197955` |
| USDC | `0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d` |

---

## Subsystems

- **KOL trader** — copy-trades whitelisted KOLs (currently D only) on public mempool sub
- **Dev sniper** — paper-mode early-buy of tokens deployed by whitelisted devs (2 devs currently)
- **KOL paper-trading scoreboard** — parallel paper book per KOL × visibility for ongoing evaluation
- **Four.Meme price oracle** — TradeBuy/TradeSell event stream → per-block stats used by signal_vote

See [docs/TRADER.md](docs/TRADER.md) for the trader deep-dive.

---

## Security

- `.env` (with private key + auth tokens) is gitignored and chmod 600
- `config/limits.toml::[phase]` is the master kill switch:
  - `shadow = true` — sign locally, never broadcast
  - `tiny = true` — broadcast but capped at 0.001 BNB
  - `full = true` — full production sizing
- `LimitsRuntime::check()` runs on every trade with daily-loss halt and per-trade caps
- Token blacklist (hot-reloadable) prevents trades on known scams/stables

---

## Repository layout

```
bsc-meme-mev/
├── README.md                              this file
├── Cargo.toml / Cargo.lock                Rust workspace
├── crates/bsc-runner/                     main binary
│   └── src/
│       ├── main.rs / wiring.rs            entry + boot
│       ├── kol_watch.rs                   pending tx sub
│       ├── kol_confirm.rs                 mined tx sub
│       ├── four_meme_price.rs             curve oracle + per-block stats
│       ├── token_flow.rs                  per-held-token flow tracking
│       ├── dev_sniper.rs                  dev-launchpad paper sniper
│       ├── bnb_price.rs                   BNB/USD oracle
│       └── trader/                        trader subsystem
│           ├── executor_live.rs           ⭐ race-submit + bundle paths
│           ├── nonce.rs                   ⭐ disk-persisted nonce manager
│           ├── adaptive_trail.rs          trail/signal_vote/sl exit machine
│           ├── strategy.rs                entry gate
│           ├── limits.rs                  risk limits
│           ├── live_ledger.rs             CSV ledger
│           ├── blacklist.rs               token blacklist
│           ├── kol_budget.rs              paper-trading per-KOL pots
│           ├── wallet.rs                  trader wallet
│           └── ... (paper/types/sim/etc.)
├── config/
│   ├── default.toml                       main config
│   ├── limits.toml                        ⭐ risk limits + phase
│   ├── kols.toml                          KOL whitelist
│   ├── dev_whitelist*.toml                dev whitelists
│   └── token_blacklist.toml               token blacklist
├── docs/                                  ⭐ documentation
│   ├── ARCHITECTURE.md
│   ├── TRADER.md
│   ├── SCRIPTS.md
│   ├── SESSION-LOG.md
│   ├── gap-reduction.md
│   └── port-plan.md                       legacy from ETH port
├── scripts/                               analysis + ops
│   ├── analyze-*.py                       per-trade forensics
│   ├── backtest-*.py                      strategy backtests
│   ├── kol-paper-report.py                KOL scoreboard
│   └── systemd/                           service units
├── trader/                                paper trading state (KOL pots)
├── trader_private/                        paper trading state (private visibility)
└── trader_live/                           live trading state + wallet_nonce
```

(⭐ = key files added/refactored in the 2026-06-08 → 2026-06-10 session.)
