# ETH → BSC port plan

This file tracks what carries over from `eth-meme-mev`, what needs adaptation,
and what's new. Updated as crates land.

## Crates: status

| ETH crate | BSC crate | Status | Notes |
|---|---|---|---|
| `mempool-core` | `bsc-core` | **port verbatim** | RLP decode + signer recovery are chain-agnostic. `SourceId` enum just gets BSC labels. `SlotContext` becomes `BlockContext` (BSC has no slots) — drop slot/epoch fields, keep block_number + parent_hash. |
| `mempool-beacon` | — | **dropped** | BSC PoSA has no separate Beacon API. Validator rotation is on-EL. |
| `mempool-bus` | `bsc-bus` | **port verbatim** | Decoder pool + dedupe + fanout patterns are chain-agnostic. The only ETH-specific thing was the head-state seeded from CL — replaced by EL-driven block oracle. |
| `mempool-sources` | `bsc-sources` | **port + adapt** | WSS source works as-is (BSC supports `eth_subscribe newPendingTransactions, true` extension via bsc-geth). IPC source path differs (`/data/bsc-meme-mev/bsc-geth/geth.ipc`). Drop devp2p stub. |
| `mempool-telemetry` | `bsc-telemetry` | **port + adapt** | Prometheus / capture / replay are chain-agnostic. Block oracle: replace CL-driven with EL-driven (newHeads + eth_getBlockByHash). |
| `mempool-runner` | `bsc-runner` | **rewrite** | Wiring is mostly the same but config is BSC-specific. Drop CL consumer. Add PancakeSwap V2 path. |
| (new) | `bsc-dex` | **new code** | PancakeSwap V2/V3 quoter + factory + Multicall3 bindings. V2 quoter is `getAmountsOut`; V3 quoter is `QuoterV2`. |

## Modules within `mempool-runner` → `bsc-runner`

| ETH module | BSC equivalent | Notes |
|---|---|---|
| `kol_watch.rs` | port | swap WETH refs → WBNB, ETH-only label strings; KOL list moves to `config/kols.toml` |
| `kol_confirm.rs` | port | swap `eth_getBlockByNumber` pagination — BSC blocks are 4× denser, may need batching |
| `receipt_decoder.rs` | port + extend | add PancakeSwap V2 Swap event decoding; the V3 path is already there |
| `token_resolver.rs` | port verbatim | ERC20 symbol/decimals lookup is identical |
| `trader/` | port + adapt | sim.rs needs V2 path FIRST (V2 is dominant on BSC), then V3 fallback; strategy.rs threshold becomes BNB-denominated |
| `liquidator/` | rewrite for Venus | Venus is a Compound-V2 fork — `accrueInterest()` + `getAccountSnapshot(borrower)` per market. Multicall3 to batch. Day 4 of roadmap. |
| `liquidator/oracle.rs` | TBD | Venus uses ChainlinkOracle + BinanceOracle + ResilientOracle. Different mempool detection pattern; defer to Day 5+. |

## Address constants to swap

| Symbol | ETH mainnet | BSC mainnet |
|---|---|---|
| Native wrapper | `0xC02aaA39…` (WETH) | `0xbb4CdB9C…` (WBNB) |
| Uniswap V2 Router | — | `0x10ED43C7…` PancakeSwap V2 |
| Uniswap V3 Factory | `0x1F98431c…` | `0x0BFbCF9f…` PancakeSwap V3 |
| Uniswap V3 QuoterV2 | `0x61fFE014…` | `0xB048Bbc1…` |
| Multicall3 | `0xcA11bde0…` | `0xcA11bde0…` (same canonical) |
| Lending pool | `0x87870Bca…` Aave V3 | `0xfD36E2c2…` Venus Comptroller |

## What still doesn't carry

- **Day-2A Chainlink oracle-update mempool detector**. BSC's oracle landscape is different: Venus uses Binance Oracle (off-chain push) + ResilientOracle fallback. Re-derive the mempool detection pattern only after Venus liquidator is live and we know what oracle txs to watch.
- **Day-3 Aave flash-loan contract**. Venus has its own flash-loan API (different ABI). New Solidity contract.
- **Beacon SSE head-state seeding**. BSC has no equivalent — `newHeads` over WS is the substitute.

## What's genuinely NEW on BSC

- **Four.Meme bonding-curve sniping** (BSC Pump.fun analogue). No ETH equivalent in current stack.
- **GMGN proxy decoding** for KOL D's wallet (`0x1de460f363AF910f51726DEf188F9004276Bf4bc`). Same proxy address appeared earlier in research; flagged for BSC focus.
- **21-validator slot leadership**. Lower MEV competition density than ETH; potentially favorable for a $450 operator.

## Estimated effort (calendar hours, not LOC)

| Crate / module | Hours |
|---|---|
| Workspace scaffold + Cargo deps wired | 1 |
| `bsc-core` (port + adapt) | 2 |
| `bsc-bus` (port verbatim) | 1 |
| `bsc-sources` (WSS + IPC) | 3 |
| `bsc-telemetry` (replace beacon block-oracle) | 3 |
| `bsc-dex` (PancakeSwap V2 + V3 + Multicall3) | 6 |
| `bsc-runner` skeleton (config + wiring) | 4 |
| KOL watcher port | 2 |
| KOL confirm watcher port | 2 |
| Paper trader (V2-first) | 6 |
| Receipt decoder PancakeSwap V2 extension | 3 |
| Discovery script for BSC KOLs | 2 |
| **Total to Day 3 milestone (paper trading)** | **~35 hours** |

Venus liquidator + Four.Meme sniping are Day 4-5 work, additional ~30 hours.
