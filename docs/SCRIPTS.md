# Scripts catalog

All analysis, backtest, and operational scripts in `scripts/`.

---

## Live operations

| Script | Purpose |
|---|---|
| `kol-paper-report.py` | Generates KOL paper-trading scoreboard from `trader/closed_trades.csv` and `trader_private/closed_trades.csv` |
| `kol-wins-devs.py` | Cross-references winning trades against token deployers (dev wallet analysis) |
| `systemd/bsc-geth.service` | systemd unit for bsc-geth full node |
| `systemd/bsc-runner.service` | systemd unit for bsc-runner binary |

---

## Trade forensics (per-trade analysis)

| Script | Purpose |
|---|---|
| `analyze-real-trade.py` | Reconstructs a single trade end-to-end from on-chain data: BUY/SELL receipts, scans launchpad TradeBuy/Sell + V2 Swap events in held window. Used to verify entry/exit mcap claims |
| `analyze-block-gas.py` | For each of our recent broadcasts, list every Four.Meme launchpad tx in the same block + N-1; compare effective_gas_price + tx_index to characterize our position in the block ordering |
| `analyze-exit-candidates.py` | 8-strategy backtest framework for exit rule comparison (no slippage) |
| `analyze-exit-candidates-realistic.py` | Same with 0%/3%/5%/7%/10% slippage haircuts |
| `analyze-partial-vote.py` | Walks every block in cached paths testing partial-on-vote moonbag variants (9 variants tested, all underperformed single-sell) |
| `prototype-graduation-gate.py` | Tested simple/sticky/peak-progress/moonbag variants for Four.Meme graduation gating; all rejected due to pre-graduation volatility |

---

## Filter/exit backtests (2026-06-10 session)

| Script | Purpose | Verdict |
|---|---|---|
| `backtest-entry-filter.py` | Pre-trade filter backtest: pre-D flow, D-block non-D flow, mcap >2x at entry | All net-negative |
| `backtest-exit-variants.py` | 7 exit-side variants on per-block trajectory data | All worse than current |
| `backtest-voted-sl.py` | Voting-based stop-loss using curve-state features | All worse; staged voting +$23 (misleading — would lose with trail enabled) |
| `backtest-pretrade-signals.py` | 4 pre-trade signals: D streak, holder count, bot %, dev token count | Signals identified; only streak looked promising naively |
| `backtest-signal-cascade.py` | Voted DOWNSIDE exit using real event data (mirror of signal_vote) | CB variant (3-of-4 + price <-10%) doesn't kill winners but still net-negative |
| `backtest-streak-filter.py` | PROPER recursive streak filter backtest | Killed 8 winners (-$74.92); **ABORTED ship** |

**Key finding across all backtests:** with curve-state + event features only, current logic (peak trail + -30% hard_sl + 3-of-4 signal_vote) is at or near the EV floor for D-following on the available 30-trade sample.

---

## Infra / setup

| Script | Purpose |
|---|---|
| `install-bsc-geth.sh` | Install bsc-geth full node from source |
| `install-runner.sh` | Build + install bsc-runner systemd unit |
| `post-download.sh` | Extracts snapshot + starts bsc-geth |
| `post-restore.sh` | Post-restore chain warmup |
| `snapshot-restore.sh` | Snapshot restore tooling |
| `verify-all.sh` | Comprehensive sanity check (chain head, RPC, mempool, wallet balance) |
| `run.sh` | Convenience runner script |
| `sync-status.sh` | bsc-geth sync dashboard |

---

## Historical data dumps (in repo root)

Large analysis JSONs produced by various microstructure scripts:

| File | Size | Description |
|---|---|---|
| `d_block_competition.csv` | 87 KB | Per-block competitor analysis when D bought |
| `d_microstructure.csv` | 41 KB | Single-shot microstructure dump |
| `d_microstructure_paths.json` | 8.8 MB | Full per-block traces for D's trades |
| `d_microstructure_30day.csv` | 152 KB | 30-day summary |
| `d_microstructure_30day_paths.json` | 26 MB | 30-day full traces |
| `d_microstructure_v2.csv` | 104 KB | V2 dump with refined metrics |
| `d_microstructure_v2_paths.json` | 87 MB | V2 full traces (largest file) |
| `d_trades_30day.csv` | 107 KB | D's 30-day trade list |
| `d_trades_30day_v2.csv` | 108 KB | V2 of same |
| `d_trades_30day_v2_paths.json` | 5.9 MB | V2 trade paths |
| `d_trades_analysis.csv` | 2.8 KB | Aggregate analysis |
| `kol_trades_captured.jsonl` | 2.6 KB | Captured KOL trades (jsonl) |
