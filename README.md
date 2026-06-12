# bsc-mempool

> Live KOL-following meme trader on BSC mainnet.

A self-hosted Rust trading bot that watches whitelisted KOL ("key opinion leader") wallets in the BNB Smart Chain mempool, copy-trades their Four.Meme / PancakeSwap V2 BUYs, and manages exits through a peak-trail + `signal_vote` state machine. It runs on a low-latency pipeline fed by a local `bsc-geth` node, with shadow / tiny / full phase gating so it can be validated dry before risking funds.

![Rust](https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white)
![alloy](https://img.shields.io/badge/alloy-2.x-orange)
![Tokio](https://img.shields.io/badge/Tokio-async-blue?logo=rust&logoColor=white)
![BNB Chain](https://img.shields.io/badge/BNB%20Chain-mainnet-F0B90B?logo=binance&logoColor=white)

## Features

- **KOL mempool copy-trader** — subscribes to pending transactions and matches them against a `from`-address whitelist (router/selector agnostic), catching GMGN-routed, Four.Meme launchpad, and PancakeSwap V2 BUYs.
- **Adaptive exit state machine** — positions exit on price action via a peak-trail with arm threshold, break-even ratchet, hard stop-loss, block timeout, and a `signal_vote` leading-exit rule derived from per-block Four.Meme trade stats.
- **Phase-gated risk controls** — a master `[phase]` switch (`shadow` / `tiny` / `full`) plus per-trade caps, daily-loss halt, gas ceiling, slippage limits, token-age and sell-tax checks, and a hot-reloadable token blacklist.
- **Live execution** — disk-persisted nonce manager and submission via a primary MEV relay with a local-node fallback.
- **Paper-trading scoreboard** — parallel per-KOL closed-loop budgets for ongoing evaluation, with separate public-mempool and private-confirmed strategy books.
- **Dev-launchpad sniper** — optional paper-mode early-buy of Four.Meme tokens deployed by whitelisted devs, with its own adaptive trail tuning.

## Installation

Requires a recent stable Rust toolchain (Rust 2024 edition, see `rust-toolchain.toml`) and access to a synced `bsc-geth` full node for IPC/WS/RPC.

```bash
git clone git@github.com:1chimaruGin/bsc-mempool.git
cd bsc-mempool
cargo build --release -p bsc-runner
```

## Configuration

Runtime behavior is driven by TOML files under `config/` (`default.toml`, `limits.toml`, `kols.toml`, blacklists). Any value can be overridden via environment using the `BSC_MEME_MEV_<SECTION>__<KEY>` convention.

Secrets are loaded from a gitignored `.env` (never commit it). Reference by name with placeholder values:

```dotenv
WALLET_PRIVATE_KEY=0x<your-trader-key>
WALLET_ADDRESS=0x<your-trader-address>
BLOCKRAZOR_AUTH_KEY=<relay-auth-key>
TELEGRAM_BOT_TOKEN=<telegram-bot-token>
TELEGRAM_CHAT_ID=<telegram-chat-id>
NODEREAL_RPC_URL=<optional-rpc-url>   # analysis scripts only
```

Key config files:

- `config/kols.toml` — the KOL wallet whitelist (`[[kol]]` entries).
- `config/limits.toml` — `[phase]` master switch, per-trade/daily limits, strategy gates, submission relay.
- `config/default.toml` — node endpoints (IPC/WS/RPC), pipeline sizing, trader/DEX addresses.

## Usage

```bash
# Start the live mempool runner with a config file
./target/release/bsc-runner run --config config/default.toml

# Replay a previously captured mempool segment
./target/release/bsc-runner replay path/to/segment.bincode.zst --speed 1.0

# Print build + chain info
./target/release/bsc-runner version
```

Start with `shadow = true` (signs locally, never broadcasts), validate behavior, then graduate to `tiny` and `full` in `config/limits.toml`.

## Project Structure

```
crates/
  bsc-runner/      main binary: mempool watch, trader, exit machine, oracles
    src/trader/    execution, nonce manager, adaptive trail, risk limits, ledgers
  bsc-bus/         pipeline: subscription, decode, dedupe, fanout
  bsc-sources/     mempool sources (IPC / WSS / relay)
  bsc-dex/         PancakeSwap V2/V3 + multicall helpers
  bsc-core/        shared types + decoders
  bsc-telemetry/   metrics, capture, replay
config/            TOML config (KOLs, limits, blacklists)
docs/              architecture, trader deep-dive, scripts catalog
scripts/           analysis + backtest tooling, systemd units
```

## Disclaimer

This is experimental trading software provided as-is, with no warranty. On-chain trading carries substantial risk of total loss. Nothing here is financial advice. Use at your own risk, and only with funds you can afford to lose.
