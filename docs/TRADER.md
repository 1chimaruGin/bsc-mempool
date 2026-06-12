# Trader — deep dive

Detailed reference for the live trading subsystem (`crates/bsc-runner/src/trader/`).
For system overview see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Strategy overview

**Copy-trade KOL D (with whitelisting) on Four.Meme / PCS V2.**

- Watch D's pending tx via mempool WSS sub
- When D buys a token D-routed (GMGN proxy or direct), decode amount + token
- Apply entry gate (limits, blacklist, sizing policy)
- Sign + broadcast our BUY across 3 parallel paths (race-submit)
- Once mined, track position with adaptive_trail state machine
- Exit on: peak-trail (-30% from peak after +10% arm), signal_vote (3-of-4 upside dump detection), hard_sl (-30% from entry), timeout (4000 blocks)

Current trade size: **$10 per BUY** (was $20 briefly, reverted 2026-06-09).

---

## 2. Race-submit architecture (executor_live.rs::broadcast)

When the strategy signals a BUY/SELL, `broadcast()` fires the same signed tx through **three parallel paths**:

```
            signed tx
                │
       ┌────────┼────────────────┬───────────────────┐
       │        │                │                   │
       ▼        ▼                ▼                   ▼
  Local geth  BlockRazor      BlockRazor          Puissant
  eth_sendRaw eth_sendBundle  eth_sendBundle      eth_sendPuissant
   (gate)     (gap-killer)    target=N+1          (48 Club relay)
       │
       │ first ACK wins; bundle paths fire-and-forget
       │ (no return-gate)
       ▼
   broadcast() returns Ok
```

### Path 1: Local geth `eth_sendRawTransaction` — sole acceptance gate

- Submits to `http://127.0.0.1:8545` (local pruned geth)
- Local geth has ~80 peers including many BSC validators directly
- Typical RTT: **0-2ms** (essentially loopback)
- This is the **only path that gates broadcast()'s return** — Ok iff geth accepts

### Path 2: BlockRazor `eth_sendBundle` — atomic backrun

- Targets `current_block + 1` (the block D's BUY will land in)
- If BR's connected builder produces that block, our tx is included **atomically alongside D's** at tx_index right after
- This is the only path that can produce **gap=0** (same block as D)
- Free service via `bsc.blockrazor.xyz`; auth via `BLOCKRAZOR_AUTH_KEY` env

### Path 3: Puissant `eth_sendPuissant` — 48 Club relay

- Same intent as BR bundle but routes through 48 Club's validator subset
- Uses Puissant-specific API (different from Flashbots `eth_sendBundle`)
- Free public endpoint: `https://puissant-bsc.48.club/`
- Submits with `maxTimestamp` deadline (we use +30s) and `acceptReverting: []`
- Covers blocks BR's builder doesn't win → expands total atomic-backrun coverage

### Why BR `sendRaw` was removed (2026-06-09)

Originally the architecture had 4 paths: BR sendRaw + local geth sendRaw + BR bundle + Puissant. But BR's relay deduplicates against its own sendRaw queue — when we submit the same signed tx via both `sendRaw` and `sendBundle`, the bundle gets rejected with `"bundle already exist"` because the tx is "already known" via sendRaw. 4/4 BUYs hit this on shipping day.

Fix: dropped BR sendRaw. Local geth's race-submit at rtt_ms=0-2 was winning every race anyway, so removing BR sendRaw cost nothing AND unblocked the bundle path. After the fix, BR bundle accepts cleanly.

---

## 3. NonceManager — disk persistence + max-of-two resync (nonce.rs)

### Problem (2026-06-09 discovery)

`eth_getTransactionCount(addr, "pending")` against local geth only returns nonces visible to **its own** mempool. After we ship race-submit, txs accepted by BR but not yet seen by local geth are invisible. After a restart, local geth's "pending" reports a stale value and we reuse nonces already in flight → every subsequent BUY/SELL fails with `nonce too low`.

This bug was created by the race-submit refactor itself and broke trading silently for ~12 hours.

### Fix

```rust
NonceManager::new(rpc_url, address, persist_path) {
  chain_n = fetch_pending_nonce(rpc, address)      // local geth pending
  disk_n  = read(persist_path).unwrap_or(0)        // last reserved next-nonce
  initial = max(chain_n, disk_n)                   // never reuse
  AtomicU64::new(initial)
}

reserve() -> u64 {
  n = current.fetch_add(1)
  spawn(async { fs::write(persist_path, (n+1).to_string()) })  // fire-and-forget
  n
}

resync() -> u64 {
  chain_n = fetch_pending_nonce(...)
  local = current.load()
  new = max(chain_n, local)         // NEVER go backwards
  current.store(new)
  new
}
```

Disk path: `/data/bsc-meme-mev/trader_live/wallet_nonce` (single integer).

**Key invariant:** the local counter only ever moves UP. Restart safe.

---

## 4. Adaptive trail — exit state machine (adaptive_trail.rs)

State per position:

```
pub struct TrailState {
    armed:            bool,   // crossed +arm_pct threshold (e.g. +10%) since open
    peak_price:       f64,    // running max since open
    last_price:       f64,    // most recent price seen
    breakeven_locked: bool,   // crossed breakeven_at_pct → +breakeven_lock_pct lock
    history_count:    u8,     // rolling buffer for vel_10 + dist_from_local_max
    history_idx:      u8,
    price_history:    [f64; PRICE_HISTORY_LEN],
}
```

### Exit reasons (in priority order)

| Reason | Trigger | Tag |
|---|---|---|
| `signal_vote` | 3-of-4 upside-dump features fire AFTER arm | `TRAIL_signal_vote` |
| `signal_dump` | Legacy "SignalDump" rule (dist_from_local_max>0.30 AND vel_10<-0.01) | `TRAIL_signal_dump` |
| `trail` | Price drops `trail_pct` from `peak_price` after armed | `TRAIL_trail` |
| `be_locked` | Price hits `breakeven_at_pct` then drops back to `breakeven_lock_pct` above entry | `TRAIL_be_locked` |
| `hard_sl` | Price drops `hard_sl_pct` from entry (never armed) | `TRAIL_hard_sl` |
| `timeout` | Age > `max_hold_blocks` (4000 ≈ 30 min on BSC) | `TRAIL_timeout` |

Config (`[adaptive_trail]` in `default.toml`):
- `arm_pct = 0.10` — arm at +10%
- `trail_pct = 0.30` — trail at -30% from peak after armed
- `hard_sl_pct = 0.30` — hard stop at -30% from entry
- `max_hold_blocks = 4000`
- `breakeven_at_pct = 0.15` — start break-even ratchet at +15%
- `breakeven_lock_pct = 0.05` — lock at +5% above entry once ratcheted

### signal_vote feature set (3-of-4 must fire)

Implemented in `compute_vote_signals()`:

1. `dist_from_local_max > 0.30` — current price dropped 30% from running local max
2. `vel_10 < -0.01` — 10-block price velocity is negative
3. `buy_velocity_collapse < 0.5` — bv3 / bv10 < 0.5 (buy velocity collapsed by >50%)
4. `net_flow_3blk_bnb < -1.0` — net BNB flow in last 3 blocks is more than -1 BNB

Data source: `four_meme_price::FourMemeStatsCache` (per-block `BlockStats`).

This rule fires on real peak rollovers — caught both observed +140% and +85.8% winners in production.

---

## 5. Position lifecycle (BUY path)

```
1. kol_watch detects D's BUY (pending tx)
   ↓
2. Strategy gate: whitelist=D? min_buy_bnb=0.1? wallet_floor OK?
   ↓
3. Size: $10 USD → BNB at current BNB/USD ($586) → 0.01706 BNB
   ↓
4. NonceManager.reserve() → nonce N
   ↓
5. Build TransactionRequest, sign with TraderWallet
   ↓
6. executor_live::broadcast(signed_tx)
   - spawn(submit_raw to local geth)    ← gate
   - spawn(submit_bundle to BlockRazor) ← fire-and-forget
   - spawn(submit_puissant to 48 Club)  ← fire-and-forget
   - wait for geth ACK
   - log "broadcast accepted (race-submit) winner=local_geth rtt_ms=X"
   ↓
7. Log BROADCAST line to live_log.csv
   ↓
8. spawn bg_finalize_position():
   - wait 2 blocks
   - call sellToken(token, 0) dry-run to confirm token is sellable
   - if sellable: submit_approve_bg() to pre-approve MAX allowance
   - log "bg_finalize: token PRE-APPROVED, sell fast-path armed"
   ↓
9. PositionEntry registered in self.positions HashMap
   ↓
10. adaptive_trail loop polls price, updates TrailState
   ↓
11. On exit trigger: execute_exit() uses fast-path (pre-approved) for sub-100ms SELL
```

---

## 6. Position lifecycle (SELL fast-path)

When adaptive_trail signals exit, `execute_exit()` runs a **separate path** from `broadcast()`:

```
1. Lookup PositionEntry from self.positions
2. Lock per-token exit mutex (prevents oversell on rapid re-trigger)
3. Determine sell amount: balanceOf(token) for our wallet
4. Choose route: V2 if pair exists (cached) else Four.Meme curve
5. Build sellToken tx with route-specific calldata
6. Sign with nonce.reserve()
7. submit_raw to BR submit_url (single path — was historically the fast-path)
   - For consistency in 2026-06-09 refactor, SELL still uses single-path submit
   - Future: could migrate to same 3-path race-submit, but submit_rtt_ms is already ~12-20ms
8. Log "SELL BROADCAST kol=TRAIL_xxx ... total_ms=N submit_rtt_ms=M"
```

Pre-approval enables the fast-path: skipping the `approve(spender, MAX)` step at exit time. Approve is done in background after BUY mines (`bg_finalize_position`).

---

## 7. Risk limits (limits.rs + limits.toml)

Every trade passes `LimitsRuntime::check()` before signing.

| Limit | Value | Purpose |
|---|---|---|
| `phase = "full"` | Live broadcasting (vs `shadow`/`tiny`) | Master kill-switch |
| `daily_loss_usd = 25.0` | Stop trading after $25 loss in a day | Hard halt |
| `per_trade_max_bnb = 0.025` | Max BNB per trade (~$15 ceiling) | Per-trade cap |
| `min_wallet_bnb = 0.01` | Skip trades if wallet < 0.01 BNB | Bankroll floor |
| `max_open_positions = 10` | Cap on concurrent positions | Concentration risk |
| `max_trades_per_day = 60` | Daily trade count cap | Burn-rate limit |
| `max_gas_price_gwei = 10` | Walk away if gas > 10 gwei | Cost guard |
| `slippage_bps = 500` | 5% max slippage on V2 amountOutMin | Sandwich protection |
| `cooldown_after_loss_ms = 5000` | Pause 5s after a -$3+ loss | Tilt protection |

`phase.shadow` = sign everything but never broadcast (~0 risk). Used during development.

---

## 8. Trade ledger (live_ledger.rs + live_log.csv)

Each broadcast appends one row to `trader_live/live_log.csv`:

```
ts_unix_ns,phase,kol_name,visibility,token_address,token_symbol,bnb_in_wei,gas_gwei,nonce,tx_hash,wallet_bnb,broadcast,limit_skip_reason
```

- `phase` = `full`/`tiny`/`shadow`
- `kol_name` for BUYs is the KOL who triggered (`D`, `I`, `A`, etc.); for exits is `TRAIL_<reason>`
- `visibility` = `public` / `private` / `exit`
- `broadcast = true` iff actual broadcast happened; `false` for limit-skips/dry-runs
- `limit_skip_reason` = empty on success, else `NotKolHit`/`wallet_floor`/`daily_loss_halt`/etc.

This CSV is the source of truth for PnL reconstruction and backtests.

---

## 9. Per-KOL paper trading (kol_budget.rs)

Parallel paper-trading scoreboard (independent of live trader):

- 15 KOLs (from `kols.toml`)
- 2 visibility variants each (public-only vs public+private)
- Closed-loop $200 budget per KOL × visibility = 30 paper pots
- Position sizing: `position_pct_bps = 1000` (10% of remaining pot per trade)
- Dust floor: 0.001 BNB

State files: `trader/kol_budgets.json` + `open_positions.json` + `closed_trades.csv`.

Reporting: `scripts/kol-paper-report.py` produces the scoreboard.

This runs alongside live trading and is the source for ongoing KOL evaluation.

---

## 10. Dev sniper (dev_sniper.rs)

Independent paper-mode subsystem:

- Subscribes to Four.Meme `TokenCreate` events (topic `0x396d5e90…`)
- When deployer is in `dev_whitelist_sniper.toml` (currently 2 devs), fires a PAPER snipe
- Trade size: 0.0271 BNB pinned (≈$18 @ $670/BNB historical)
- Runs dedicated `sniper_trail` (separate config from KOL trader):
  - `arm_pct = 0.10`
  - `trail_pct = 0.30`
  - `hard_sl_pct = 0.30`
  - `max_hold_blocks = 4000`

41 paper snipes since 2026-06-01. Net PnL roughly -$60 to -$160 (rough estimate; `dead_token` exits at -100% dominate).

**Conclusion:** with 2 devs, paper EV is negative. Strategy not promising as-is. See [SESSION-LOG.md](SESSION-LOG.md) for analysis.

---

## 11. What's gated by config flags

- `[phase] full = true` — broadcast for real (vs shadow logging only)
- `[strategy] kol_whitelist = ["D"]` — only follow these KOLs
- `[strategy] public_only = true` — skip private-mempool KOL signals (we can't see most of them anyway)
- `[dev_sniper] enabled = true, mode = "paper"` — sniper runs but never broadcasts
- `[adaptive_trail] enabled = true` — use trail logic (vs legacy single-point TP/SL)
- `[sniper_trail] enabled = true` — same for sniper

To freeze trading: edit `limits.toml` and set `[phase] full = false, shadow = true`.

---

## 12. Telemetry hooks

| Event | Log target | Format |
|---|---|---|
| KOL pending tx observed | `kol` | `KOL tx observed kol_name=X tx_hash=Y from=Z value_bnb=V gas_price_gwei=G` |
| KOL tx confirmed mined | `kol` | `KOL tx CONFIRMED ... mined_block=N ms_into_block=M slot_remaining_ms=R lead_ms=L detect_ms=D` |
| Strategy skip | `trader_live` | `skip: strategy-gate kol=X reason=Y` |
| Sized trade | `trader_live` | `sized trade by USD policy ... sized_bnb_wei=V` |
| Pre-broadcast | `trader_live` | `BROADCAST kol=X token=Y tx_hash=Z route=R nonce=N gas_gwei=G bnb=V` |
| Race-submit winner | `trader_live` | `broadcast accepted (race-submit) winner=W rtt_ms=M` |
| BR bundle accept | `trader_live` | `bundle ACCEPTED by BlockRazor target_block=N rtt_ms=M` |
| Puissant accept | `trader_live` | `puissant ACCEPTED rtt_ms=M` |
| Bundle/Puissant fail | `trader_live` | `WARN ... failed; race-submit fallback in effect error=E rtt_ms=M` |
| Trail exit | `trader_live` | `SELL BROADCAST kol=TRAIL_X ... fast_path=true total_ms=M submit_rtt_ms=R` |
| Nonce drift detected | `trader_live` | `WARN nonce resynced from chain local_before=A chain_pending=B local_after=C` |
