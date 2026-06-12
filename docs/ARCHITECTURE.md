# Architecture — bsc-meme-mev

System-level overview of the runner, modules, and data flow.
For trading-strategy specifics see [TRADER.md](TRADER.md).
For gap-reduction history see [gap-reduction.md](gap-reduction.md).

---

## 1. Top-level data flow

```
                                  bsc-geth (PoSA full node, ~5.5ms RTT to BlockRazor)
                                              │
                          ┌───────────────────┼───────────────────┐
                          │                   │                   │
                       WS pending          WS newHeads        HTTP RPC
                          │                   │                   │
                          ▼                   ▼                   ▼
                     ┌──────────────────────────────────────────────┐
                     │           bsc-runner (binary)                │
                     │                                              │
                     │  ┌───────────────┐    ┌──────────────────┐  │
                     │  │ kol_watch     │───►│ kol_confirm      │  │
                     │  │ (pending sub) │    │ (block-mine sub) │  │
                     │  └──────┬────────┘    └──────────────────┘  │
                     │         │                                    │
                     │         ▼                                    │
                     │  ┌───────────────┐    ┌──────────────────┐  │
                     │  │ four_meme_    │    │ token_flow       │  │
                     │  │ price (curve  │    │ (held position   │  │
                     │  │  + stats)     │    │  buy/sell sub)   │  │
                     │  └──────┬────────┘    └──────────────────┘  │
                     │         │                                    │
                     │         ▼                                    │
                     │  ┌───────────────────────────────────────┐  │
                     │  │ trader::strategy                      │  │
                     │  │  - whitelist gate (D + I)             │  │
                     │  │  - per-KOL budget gate                │  │
                     │  │  - limits gate (per_trade_max, etc.)  │  │
                     │  └──────┬────────────────────────────────┘  │
                     │         │                                    │
                     │         ▼                                    │
                     │  ┌───────────────────────────────────────┐  │
                     │  │ trader::adaptive_trail                │  │
                     │  │  - position state machine             │  │
                     │  │  - peak trail / signal_vote / sl      │  │
                     │  └──────┬────────────────────────────────┘  │
                     │         │                                    │
                     │         ▼                                    │
                     │  ┌───────────────────────────────────────┐  │
                     │  │ trader::executor_live                 │  │
                     │  │  - sign (NonceManager.reserve)        │  │
                     │  │  - race-submit:                       │  │
                     │  │    • local geth  eth_sendRawTx        │  │
                     │  │    • BR bundle   eth_sendBundle       │  │
                     │  │    • Puissant    eth_sendPuissant     │  │
                     │  │  - bg approve + fast-path SELL        │  │
                     │  └──────┬────────────────────────────────┘  │
                     │         │                                    │
                     └─────────┼────────────────────────────────────┘
                               ▼
                  ┌────────────────────────────┐
                  │ BlockRazor + Puissant      │
                  │ + local geth peer mesh     │
                  │ → BSC validators           │
                  └────────────────────────────┘
```

---

## 2. Module inventory (`crates/bsc-runner/src/`)

### Core mempool / event subs

| Module | Purpose |
|---|---|
| `kol_watch.rs` | Subscribes to pending tx WSS, filters by whitelisted KOL wallets, decodes Four.Meme/V2/GMGN buys |
| `kol_confirm.rs` | Subscribes to newHeads + receipts, confirms KOL tx mined (with lead_ms, slot_remaining_ms, detect_ms instrumentation) |
| `four_meme_price.rs` | Subscribes to Four.Meme `TradeBuy` + `TradeSell` events; maintains per-block stats (buy_count, sell_count, BNB flow) used by adaptive_trail's voting features and signal_vote/signal_dump exits |
| `token_flow.rs` | Subscribes to per-held-token Transfer events; surfaces "FLOW on held token" log lines for awareness |
| `held_tokens.rs` | Registry of currently-held token addresses (for token_flow subscription routing) |

### Pricing / oracle

| Module | Purpose |
|---|---|
| `bnb_price.rs` | BNB/USD price oracle (sampled every N seconds from a price feed); used for USD-denominated sizing |
| `price_oracle.rs` | Generic V2 pool price helper (`getAmountsOut`) |
| `mcap.rs` | Market-cap utilities (token_supply × curve_price) |
| `gmgn.rs` | Decodes GMGN router calldata to extract underlying token + BNB amount |
| `venus.rs` | Venus protocol decode helpers (legacy from ETH stack, unused in active code paths) |

### Sniper

| Module | Purpose |
|---|---|
| `dev_sniper.rs` | Subscribes to Four.Meme `TokenCreate` events, fires PAPER snipe when creator is in `config/dev_whitelist_sniper.toml`, runs sniper-specific trail state machine |

### Trader subsystem (`crates/bsc-runner/src/trader/`)

| Module | Purpose |
|---|---|
| `mod.rs` | Wires strategy + adaptive_trail + executor_live; bootstraps NonceManager with disk persistence |
| `types.rs` | Decision, Position, CloseReason enums; portfolio definitions |
| `position.rs` | Open-position tracking |
| `ledger.rs` | Generic trade ledger (paper) |
| `paper.rs` | Paper-mode trade execution with slippage simulation |
| `sim.rs` | Trade simulator |
| `strategy.rs` | Entry gating: whitelist, min_buy_bnb, USD sizing policy |
| `kol_budget.rs` | Per-KOL closed-loop budget (paper-trading pots: $200 per KOL × 2 visibility) |
| `dev_resolver.rs` | Dev whitelist resolver for "trust list" sizing bonus (currently disabled) |
| `wallet.rs` | TraderWallet (loads private key from env, signs txs) |
| `nonce.rs` | NonceManager with disk persistence + max-of-two resync (see [TRADER.md](TRADER.md)) |
| `limits.rs` | Live-trading risk limits (daily_loss, per_trade_max, max_open, etc.) |
| `blacklist.rs` | Hot-loadable token blacklist (stables/majors/known scams) |
| `live_ledger.rs` | Live-trading CSV ledger (writes to `trader_live/live_log.csv`) |
| `live_only.rs` | Live-only execution path (no paper) |
| `adaptive_trail.rs` | Exit state machine: peak trail + signal_vote (3-of-4 upside dump detection) + hard_sl + timeout |
| `executor_live.rs` | Signed-tx broadcast: race-submit (BR + local geth + bundle relays); bg approve; fast-path SELL |

### Config / wiring

| Module | Purpose |
|---|---|
| `config.rs` | Config struct (TOML deserialization for `limits.toml`, `default.toml`, etc.) |
| `wiring.rs` | App boot: read config → init oracles → init trader → spawn subs |
| `main.rs` | Entry point, tracing init |

---

## 3. Config files (`config/`)

| File | Purpose |
|---|---|
| `default.toml` | Main config (RPC URLs, KOL groups, adaptive_trail params, sniper params, audit dir) |
| `limits.toml` | Live-trading risk limits (phase: full/tiny/shadow, daily_loss, per_trade_max, slippage, gas cap, USD sizing) |
| `kols.toml` | KOL whitelist (D, I, A, etc.) with wallet addresses and visibility tags |
| `dev_whitelist.toml` | Dev wallet whitelist for trader's "trust list" sizing bonus (currently 10 devs; bonus path off) |
| `dev_whitelist_sniper.toml` | Separate dev whitelist for dev_sniper paper test (currently 2 devs) |
| `token_blacklist.toml` | Hot-loadable list of token addresses to never trade (stables, majors, known scams) |

---

## 4. Runtime state directories

| Path | Purpose |
|---|---|
| `trader/` | Paper-mode (KOL trader) state: `kol_budgets.json`, `open_positions.json`, `closed_trades.csv` |
| `trader_private/` | Paper-mode private-visibility variant of the above |
| `trader_live/` | Live trading state: `live_log.csv` (one row per broadcast), `wallet_nonce` (disk-persisted next-nonce for restart safety) |

---

## 5. Scripts (`scripts/`)

See [SCRIPTS.md](SCRIPTS.md) for full catalog. High level:
- `analyze-*.py` — per-trade forensics (mcap reconstruction, block-by-block competition analysis)
- `backtest-*.py` — strategy backtests (entry filters, exit variants, voted-SL, streak filter, signal_cascade)
- `kol-paper-report.py` — KOL paper-trading scoreboard
- `systemd/` — systemd unit files for `bsc-geth.service` + `bsc-runner.service`

---

## 6. External dependencies

| Dependency | Role |
|---|---|
| **bsc-geth** (local, pruned) | Mempool source via WSS; broadcast endpoint via HTTP RPC; 80 peers (good propagation) |
| **NodeReal RPC** | Backup for analysis scripts; archive-node queries (historical eth_getBalance, eth_call at past blocks). NOT used by live trader |
| **BlockRazor** (`bsc.blockrazor.xyz`) | MEV-Boost bundle relay (`eth_sendBundle`); 5.5ms RTT |
| **Puissant Network** (`puissant-bsc.48.club`) | 48 Club's MEV-Boost relay (`eth_sendPuissant`); free public endpoint |
| **Four.Meme launchpad** (`0x5c952063…`) | Bonding-curve venue for memecoin trading |
| **PancakeSwap V2** (`0x10ED43C7…`) | DEX fallback when token has graduated from Four.Meme curve |
| **GMGN proxy** (`0x1de460f3…`) | Router used by D and other KOLs; we decode its calldata to extract underlying tx intent |

---

## 7. Boot sequence

1. `main.rs` initializes tracing (compact format, log-level filter)
2. `wiring.rs::run`:
   - Reads `config/default.toml` + `config/limits.toml`
   - Constructs `BnbPrice` oracle + spawns its background refresh
   - Constructs `FourMemePriceCache` + `FourMemeStatsCache` + spawns event subs
   - Constructs `LimitsRuntime`, `LiveLedger`, `BlacklistRuntime`
   - Loads `TraderWallet` from `WALLET_PRIVATE_KEY` + `WALLET_ADDRESS` env
   - Constructs `NonceManager::new(rpc_url, wallet_addr, Some(disk_path))`:
     - Reads disk-persisted next-nonce
     - Reads chain pending nonce
     - Takes `max(disk, chain_pending)` as bootstrap
   - Constructs `LiveExecutor` with all of the above
   - Spawns `kol_watch` + `kol_confirm` + `dev_sniper` + `token_flow`
   - Spawns adaptive_trail loop with trail/signal_vote handling
   - Logs `LiveExecutor attached ...` and is ready

---

## 8. Failure modes + recovery

| Failure | Effect | Recovery |
|---|---|---|
| bsc-geth WS disconnect | KOL watcher logs "stream ended; reconnecting" and re-subscribes | Automatic; no action needed |
| bsc-geth down | Race-submit's geth leg fails; broadcast errors | Restart bsc-geth; bundle relays may still land tx |
| BlockRazor 5xx | Bundle leg fails; race-submit local_geth still lands | None needed |
| Puissant 5xx | Puissant leg fails; other paths still land | None needed |
| Nonce drift after restart | `max(disk, chain_pending)` bootstrap prevents reuse | Auto-handled by NonceManager design |
| Wallet balance below `min_wallet_bnb` | All BUYs skip with `wallet_floor` reason | Top up wallet |
| Daily loss limit hit | `LimitsRuntime` triggers full HALT; all trades skip | Manual restart after review |
