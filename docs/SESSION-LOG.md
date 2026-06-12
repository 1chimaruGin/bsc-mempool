# Session log — 2026-06-08 through 2026-06-10

A focused 3-day sprint on gap reduction and exit-strategy refinement.
What shipped, what got measured, what got rejected.

---

## Day 1 (2026-06-08): infrastructure + bundle paths

### Shipped
- **C2: Race-submit (BR + local geth in parallel)** — biggest free latency win.
  - First-success wins; loser keeps running so the tx reaches both validator pools
  - Idempotency: "already known" treated as success
  - Telemetry: `winner=<endpoint> rtt_ms=<N>`
  - In production: local geth wins every race at rtt_ms=0-2

- **D2: BlockRazor MEV-Boost bundle (`eth_sendBundle`)** — gap-killer.
  - Targets `current_block + 1` (= D's block)
  - Fire-and-forget; doesn't gate broadcast return
  - Atomic same-block landing when BR's builder produces N+1

### Documented
- `docs/gap-reduction.md` — comprehensive lever catalog ranked by impact/effort
- Probed `bsc.blockrazor.xyz` from current host: **5.5ms ping, 6.5ms TCP connect** (effectively co-located; A1 lever is moot)

### Diagnosed
- Realized losses are 27-50% on hard_sl, not the tagged -30% — actual fill includes curve impact + gas
- Tagged hard_sl ≠ realized loss

---

## Day 2 (2026-06-09): bundle ecosystem + critical bug

### Shipped
- **Puissant Network bundle relay (`eth_sendPuissant`)** — 48 Club's free MEV-Boost relay.
  - Different validator subset from BlockRazor
  - Combined coverage approaches ~50-70% of BSC blocks
  - Free public endpoint (no auth required)

- **Stripped BR sendRaw from race-submit** — unblocked the bundle path.
  - BR's relay was deduplicating against its own sendRaw queue
  - 4/4 BUYs failed bundle with `"bundle already exist"` until removed
  - Local geth was winning every race-submit anyway
  - After fix: BR bundle ACCEPTED cleanly

- **Trade size $20 → $10** (limits.toml).
  - Reduced per-trade risk while race-submit data collected
  - `per_trade_max_bnb: 0.040 → 0.025`
  - `daily_loss_usd: 40.0 → 25.0`

### CRITICAL bug found + fixed

**Nonce drift bug.** After shipping race-submit, the NonceManager bootstrapped from `eth_getTransactionCount(addr, "pending")` on local geth. But geth's pending pool only sees txs that passed through it. Txs accepted by BR/Puissant but not yet known to local geth were invisible. Post-restart, "pending" returned a stale value, and every subsequent BUY/SELL failed with `nonce too low: state 496, tx 494`.

This bug was created by race-submit itself and broke trading silently for ~12 hours.

**Fix (nonce.rs):**
- Persist next-nonce to disk on every `reserve()` (fire-and-forget atomic write)
- On bootstrap: `initial = max(disk_value, chain_pending)`
- `resync()`: `new = max(chain_pending, local)` — never go backwards
- Disk path: `trader_live/wallet_nonce`

Verified: trader resumed clean trading after fix; no more nonce errors observed.

### Production observations
- Block gap from D's tx dropped from typical **N+2 → N+1** with new architecture
- 4 D 92-gwei BUYs in 35 seconds all hard_sl'd (-26% to -50% realized) — pump-dump pattern by D

---

## Day 3 (2026-06-10): exhaustive backtests + final verdict

### Backtest #1: Entry-side flow filters (`backtest-entry-filter.py`)

Tested 3 filters across 30 closed D trades:

| Filter | Realized | Δ vs actual | Skipped |
|---|---|---|---|
| Actual (no filter) | -$8.93 | — | 0 |
| Pre-D 2-block net flow <0 | -$12.88 | -$3.95 | 3 |
| D-block non-D flow <0 | -$8.93 | $0.00 | 0 (never fires — D's BUY always pushes flow positive) |
| Mcap >2x at entry | -$12.32 | -$3.39 | 18 |

**Verdict:** none beat current logic. Swarm-trap hypothesis falsified — big swarms produce both rockets AND rugs.

### Backtest #2: Exit-side variants (`backtest-exit-variants.py`)

Tested 6 variants on per-block curve trajectory:

| Variant | Realized | Δ vs current |
|---|---|---|
| V1 -20% hard_sl (tighter) | -$65.48 | -$56.56 |
| V2 -50% hard_sl (wider) | -$17.60 | -$8.67 |
| V3 -15% in 1 block (velocity) | -$126.45 | -$117.53 |
| V4 -25% over 2 blocks | -$73.43 | -$64.50 |
| V5 peak ratchet -20% | -$117.30 | -$108.37 |
| V6 peak ratchet -50% | -$52.04 | -$43.11 |

**Verdict:** every variant worse than current. Current exit logic (peak trail + signal_vote + -30% hard_sl) is at the local optimum.

### Backtest #3: Voted hard_sl (`backtest-voted-sl.py`)

Tested 5 conditional-SL variants. V4 (staged voting at -20%/-40%/-60% with different vote thresholds) showed +$23.63 improvement — but the gain was illusory. V4 removed trail/signal_vote in the simulation, letting winners "ride" to N+40. In production those winners would have exited at signal_vote anyway.

**Verdict:** with curve-state features only, no voted-SL beats fixed -30% hard_sl when combined with trail + signal_vote.

### Backtest #4: Pre-trade signals (`backtest-pretrade-signals.py`)

4 signals correlated against realized PnL:

| Signal | Best bucket | Avg PnL | Note |
|---|---|---|---|
| **D's streak** | -2 to -1 | **+$2.29** | Looks promising — but invalid (see #6) |
| Holders at entry | 16-30 | -$0.19 | Marginal |
| Bot % in D's block | ≥50% | -$0.08 | 93% of trades fall here — not actionable |
| Dev's prior deploys | ≥11 (veteran) | +$0.44 | Real signal but requires sniper data join |

### Backtest #5: signal_cascade with real event data (`backtest-signal-cascade.py`)

Mirror of signal_vote on the downside. 4 variants with real per-block buy/sell/flow events:

| Variant | Winners killed | Total PnL | Δ vs current |
|---|---|---|---|
| CA 3-of-4 strict | 2 (-$10.77) | -$19.71 | -$10.78 |
| CB 3-of-4 + price <-10% | **0 ✅** | -$18.47 | -$9.54 |
| CC 2-of-4 strict | 8 (-$86) ❌ | -$84.79 | -$75.86 |
| CD 2-of-4 + price <-15% | 2 | -$53.21 | -$44.29 |

**CB design works (no winners killed)** but still loses money because cascading exits on losers come too late — curve has already cratered to -40% by the time 3-of-4 features agree.

### Backtest #6: D-streak filter PROPER recursive simulation (`backtest-streak-filter.py`)

Earlier naive bucket analysis suggested +$38.62 improvement from skipping at streak ≥0. Proper recursive simulation (streak only updates on TAKEN trades) revealed the truth:

| Filter | Kept | Total PnL | Winners killed |
|---|---|---|---|
| Actual | 30 | -$8.93 | 0 |
| Skip if streak ≥+1 | 3 | +$1.00 | **8 (-$74.92)** |
| Skip if streak ≥0 | 0 | $0 | 9 (never trades) |
| Skip if ≥+1 or ≤-3 | 3 | +$1.00 | 8 |
| Skip if ≥+2 or ≤-4 | 4 | +$5.11 | 7 |

**Critical:** **wins cluster** in D's trading. After one win, streak goes to +1 and the filter would skip the next 7-8 trades — including +$29.42 signal_vote winner and +$18.62 trail winner.

**My earlier "+$38.62 improvement" was a backtest bug.** The naive bucket analysis assumed the streak followed the same path with or without the filter — it doesn't.

**ABORTED ship.**

### Final verdict

**Current trading logic is at the EV floor with available signals.** Improvements from here require:
- Larger sample data (1000+ trades vs current 30)
- Off-chain signals (D's social media, funding patterns, Telegram)
- Strategy pivot (different KOL, different venue, different style)
- Or paid private mempool subscription (BR private $500/mo or bloXroute $1500+/mo)

---

## Production state at session end

| Component | Status |
|---|---|
| Runner | Active, healthy |
| Trade size | $10 per BUY |
| Nonce drift | Fixed (disk persistence) |
| Race-submit | Local geth winning at rtt_ms=0-2 |
| BR bundle | ACCEPTED cleanly, target N+1 |
| Puissant | ACCEPTED cleanly |
| Block gap | Consistently 1 block (N+1) |
| Strategy | D-follow with -30% hard_sl + peak trail + 3-of-4 signal_vote/dump |
| Net PnL | ~ -$0.30/trade structural |

## Files changed in this session

| File | Purpose |
|---|---|
| `crates/bsc-runner/src/trader/executor_live.rs` | Race-submit refactor; BR sendRaw stripped; bundle + Puissant added |
| `crates/bsc-runner/src/trader/nonce.rs` | Disk persistence + max-of-two resync |
| `crates/bsc-runner/src/trader/mod.rs` | Pass nonce_persist_path to NonceManager::new |
| `config/limits.toml` | Trade size 20→10, daily_loss 40→25, per_trade_max 0.040→0.025 |
| `docs/gap-reduction.md` | Comprehensive lever catalog |
| `docs/ARCHITECTURE.md` | NEW system-level overview |
| `docs/TRADER.md` | NEW trader deep-dive |
| `docs/SCRIPTS.md` | NEW script catalog |
| `docs/SESSION-LOG.md` | NEW this file |
| `scripts/backtest-*.py` | 6 new backtests |
| `scripts/analyze-block-gas.py` | Per-block tx_index/gas comparison |
