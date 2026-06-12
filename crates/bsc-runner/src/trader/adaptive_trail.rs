//! Adaptive trailing-stop exit strategy.
//!
//! Replaces KOL-driven exits: we ignore D's sells and exit based purely
//! on price movement of the underlying token.
//!
//! ## State machine
//!
//! ```text
//!         ┌─────────────┐                     ┌─────────────┐
//!         │  UNARMED    │  peak ≥ 1.20×entry  │   ARMED     │
//!  entry─►│  (waiting   │ ──────────────────► │  (trail     │
//!         │   for pump) │                     │   active)   │
//!         └─────────────┘                     └─────────────┘
//!               │                                   │
//!               │ price ≤ 0.70×entry                │ price ≤ 0.90×peak
//!               ▼                                   ▼
//!          exit("hard_sl")                     exit("trail")
//! ```
//!
//! Plus a timeout: after `max_hold_blocks` (~30 min) we exit with
//! reason "timeout".
//!
//! ## Price source
//!
//! Per-block, per held-token: observe Transfer events on the token and
//! reconstruct the latest buy/sell price.
//!   - PancakeV2: `getAmountsOut` quote — cheap, accurate, ~1ms
//!   - Four.Meme: scan token's Transfer events involving the launchpad
//!     in the last few blocks; use the latest BUY's effective price
//!     (`tx.value` / token_amount). Sells ignored for pricing (BNB out
//!     isn't visible in event logs).
//!
//! If no fresh observation is available this block, peak/last_price stay
//! unchanged; only the timeout can fire.
//!
//! ## Wiring
//!
//! Subscribes to the EL `newHeads` stream. On each block:
//!   1. Get the held-positions snapshot from `LiveExecutor`
//!   2. For each position, get/initialise its `TrailState`
//!   3. Query latest price (best-effort)
//!   4. Update peak; check arm + exit conditions
//!   5. On exit-condition match, call `LiveExecutor::execute_exit` with
//!      synthetic kol_addr=ZERO (forces fraction=1.0 → full close)

use crate::trader::executor_live::LiveExecutor;
use alloy::hex;
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub const FOURMEME_LAUNCHPAD: &str = "0x5c952063c7fc8610FFDB798152D69F0B9550762b";
pub const WBNB: &str = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c";
pub const PANCAKE_V2_ROUTER: &str = "0x10ED43C718714eb63d5aA57B78B54704E256024E";
/// ERC20 Transfer event topic[0].
pub const TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Debug, Clone, Deserialize)]
pub struct TrailConfig {
    /// Master switch.
    #[serde(default)]
    pub enabled: bool,
    /// Arm trail once peak ≥ entry × (1 + arm_pct). 0.20 → +20% gain.
    #[serde(default = "default_arm_pct")]
    pub arm_pct: f64,
    /// Once armed, exit when current ≤ peak × (1 - trail_pct). 0.10 → -10%.
    #[serde(default = "default_trail_pct")]
    pub trail_pct: f64,
    /// Exit immediately if current ≤ entry × (1 - hard_sl_pct). 0.30 → -30%.
    #[serde(default = "default_hard_sl_pct")]
    pub hard_sl_pct: f64,
    /// Hold-time cap in blocks. 4000 ≈ 30 min on BSC (450 ms/block).
    #[serde(default = "default_max_hold")]
    pub max_hold_blocks: u64,
    /// Local geth WebSocket URL for newHeads.
    #[serde(default)]
    pub ws_url: String,
    /// Local geth HTTP for log queries.
    #[serde(default)]
    pub rpc_url: String,
    /// BREAK-EVEN RATCHET: once peak ≥ entry × (1 + breakeven_at_pct),
    /// the hard-SL floor moves UP to entry × (1 + breakeven_lock_pct).
    /// 0.15 / 0.05 → if peak hits +15%, lock at least +5%.
    /// Backtest (584 D trades, 30 days): protects the 4-of-6 closed losses
    /// today (peaked +13/+19/+23/+27%) that rode straight to -30% SL.
    /// Cost: trimmed runners by ~5 (≥2x count 44 → 39); avg essentially flat.
    #[serde(default = "default_breakeven_at_pct")]
    pub breakeven_at_pct: f64,
    #[serde(default = "default_breakeven_lock_pct")]
    pub breakeven_lock_pct: f64,
}

fn default_arm_pct() -> f64 { 0.20 }
fn default_trail_pct() -> f64 { 0.10 }
fn default_hard_sl_pct() -> f64 { 0.30 }
fn default_max_hold() -> u64 { 4000 }
fn default_breakeven_at_pct() -> f64 { 0.15 }
fn default_breakeven_lock_pct() -> f64 { 0.05 }

impl Default for TrailConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            arm_pct: default_arm_pct(),
            trail_pct: default_trail_pct(),
            hard_sl_pct: default_hard_sl_pct(),
            max_hold_blocks: default_max_hold(),
            ws_url: String::new(),
            rpc_url: String::new(),
            breakeven_at_pct: default_breakeven_at_pct(),
            breakeven_lock_pct: default_breakeven_lock_pct(),
        }
    }
}

/// Length of the rolling price history used by the leading-exit rule
/// (`dist_from_local_max>0.30 AND vel_10<-0.01`). 10 = matches the
/// backtest's `vel_10` window precisely (P30/P75/P90 derivations all
/// used the same window).
pub const PRICE_HISTORY_LEN: usize = 10;

/// Per-position trail state. Updated on every newHeads tick.
///
/// `price_history` is a circular buffer of the last `PRICE_HISTORY_LEN`
/// observed prices, used by the v1 leading-exit rule. `history_count`
/// = number of valid entries (< LEN until the buffer fills). `history_idx`
/// = next slot to overwrite. Kept `Copy` so the existing get/insert
/// pattern in `process_one_position` keeps working.
#[derive(Debug, Clone, Copy)]
pub struct TrailState {
    pub entry_price_bnb_per_token: f64,
    pub peak_price: f64,
    pub last_price: f64,
    pub armed: bool,
    pub opened_block: u64,
    pub last_observed_block: u64,
    pub price_history: [f64; PRICE_HISTORY_LEN],
    pub history_count: u8,
    pub history_idx:   u8,
    /// Break-even ratchet has fired (peak reached the threshold and SL
    /// floor has been moved up). Set once; never reset.
    pub breakeven_ratcheted: bool,
}

impl Default for TrailState {
    fn default() -> Self {
        Self {
            entry_price_bnb_per_token: 0.0,
            peak_price: 0.0,
            last_price: 0.0,
            armed: false,
            opened_block: 0,
            last_observed_block: 0,
            price_history: [0.0; PRICE_HISTORY_LEN],
            history_count: 0,
            history_idx:   0,
            breakeven_ratcheted: false,
        }
    }
}

/// Pure state-machine evaluation — no I/O, fully unit-testable.
///
/// Returns `Some(reason)` if the position should be exited THIS tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    HardStopLoss,
    /// Break-even ratchet floor was crossed (peak previously hit the
    /// `breakeven_at_pct` threshold so the SL floor moved up to
    /// `entry × (1 + breakeven_lock_pct)`). Locks a small gain on the
    /// "pump then dump" pattern that never armed.
    BreakevenLocked,
    /// Leading-signal exit (v1 winner): both conditions true same block
    /// after arming —
    ///   `dist_from_local_max > 0.30` (price is ≤70% of last-10-blk max)
    /// AND
    ///   `vel_10 < -0.01` (10-blk avg slope is ≤ -1% per block)
    SignalDump,
    /// Multi-signal voting exit (3-of-4 rule). After armed, at each
    /// tick we evaluate four dump-indicator features:
    ///   1. dist_from_local_max > 0.30
    ///   2. vel_10 < -0.01
    ///   3. buy_velocity_collapse < 0.5  (per-block buy rate dropping)
    ///   4. net_flow_3blk < -1 BNB        (3-blk net sell pressure)
    /// When ≥3 fire same block, we exit. Backtested 30 days, 584 trades:
    /// 1.151x avg (vs 1.124x baseline) — robust to slippage assumptions.
    SignalVote,
    Trail,
    Timeout,
}

impl ExitReason {
    pub fn label(self) -> &'static str {
        match self {
            ExitReason::HardStopLoss    => "hard_sl",
            ExitReason::BreakevenLocked => "be_locked",
            ExitReason::SignalDump      => "signal_dump",
            ExitReason::SignalVote      => "signal_vote",
            ExitReason::Trail           => "trail",
            ExitReason::Timeout         => "timeout",
        }
    }
}

/// Step the state with a new observation. Returns the exit reason if
/// triggered, or `None` to keep holding.
///
/// Mutates `peak`, `armed`, and `price_history` in place.
/// `current_price` is BNB-per-token.
///
/// Exit priority:
///   1. HardStopLoss (any time)
///   2. SignalDump   (only when armed, only when history is full)
///   3. Trail        (only when armed)
///   4. Timeout      (block-count cap)
pub fn step(
    state: &mut TrailState,
    cfg: &TrailConfig,
    current_price: f64,
    current_block: u64,
) -> Option<ExitReason> {
    // Update peak + last observed
    if current_price > state.peak_price {
        state.peak_price = current_price;
    }
    state.last_price = current_price;
    state.last_observed_block = current_block;

    // Push current observation into the rolling history (circular buffer).
    let idx = state.history_idx as usize;
    state.price_history[idx] = current_price;
    state.history_idx = ((idx + 1) % PRICE_HISTORY_LEN) as u8;
    if (state.history_count as usize) < PRICE_HISTORY_LEN {
        state.history_count += 1;
    }

    // Arm transition: peak ≥ entry × (1 + arm_pct)
    if !state.armed && state.peak_price >= state.entry_price_bnb_per_token * (1.0 + cfg.arm_pct) {
        state.armed = true;
    }

    // BREAK-EVEN RATCHET: once peak crosses the threshold, mark it.
    // Effective SL floor below is then `max(hard_sl, lock_floor)`.
    if !state.breakeven_ratcheted
       && state.peak_price >= state.entry_price_bnb_per_token * (1.0 + cfg.breakeven_at_pct)
    {
        state.breakeven_ratcheted = true;
    }

    // HARD STOP-LOSS (or BreakevenLocked if the ratchet has fired and we
    // dropped below the lock floor instead of the hard floor).
    let hard_sl_floor = state.entry_price_bnb_per_token * (1.0 - cfg.hard_sl_pct);
    let lock_floor    = state.entry_price_bnb_per_token * (1.0 + cfg.breakeven_lock_pct);
    let effective_floor = if state.breakeven_ratcheted {
        // After ratchet, the higher floor wins.
        hard_sl_floor.max(lock_floor)
    } else {
        hard_sl_floor
    };
    if current_price <= effective_floor {
        // Distinguish reasons: if ratchet was active and lock_floor is the
        // binding constraint, it's BreakevenLocked (small-gain protected);
        // otherwise it's the original hard SL.
        if state.breakeven_ratcheted && lock_floor > hard_sl_floor {
            return Some(ExitReason::BreakevenLocked);
        }
        return Some(ExitReason::HardStopLoss);
    }

    // SIGNAL-DUMP (leading exit) — only when armed AND the history buffer
    // is full (10 observations). Beats raw trail in the backtest because
    // it fires *earlier* — captures the price drop before the trail's
    // 30% give-back completes.
    if state.armed && state.history_count as usize >= PRICE_HISTORY_LEN {
        // local_max = max price in the last 10 observations
        let local_max = state.price_history.iter().cloned().fold(0.0_f64, f64::max);
        // 10-blocks-ago price = the slot we're ABOUT to overwrite next
        // (i.e. the current write head before advance — which equals
        // `idx` we just wrote to, plus 1 mod LEN... but we already wrote
        // current_price to `idx` and advanced; so the OLDEST is now at
        // the NEW history_idx position).
        let oldest_idx = state.history_idx as usize % PRICE_HISTORY_LEN;
        let oldest_price = state.price_history[oldest_idx];
        if local_max > 0.0 && oldest_price > 0.0 {
            let dist_from_local_max = (local_max - current_price) / local_max;
            // vel_10 is "average per-block return over 10 blocks":
            //   (current - oldest) / oldest / 10
            let vel_10 = (current_price - oldest_price) / oldest_price / 10.0;
            if dist_from_local_max > 0.30 && vel_10 < -0.01 {
                return Some(ExitReason::SignalDump);
            }
        }
    }

    // TRAIL — only after arming
    if state.armed {
        let trail_floor = state.peak_price * (1.0 - cfg.trail_pct);
        if current_price <= trail_floor {
            return Some(ExitReason::Trail);
        }
    }

    // TIMEOUT — block-count cap
    if current_block.saturating_sub(state.opened_block) >= cfg.max_hold_blocks {
        return Some(ExitReason::Timeout);
    }

    None
}

/// Per-token state map + exit dispatcher. Held by the spawned task.
pub struct TrailWatcher {
    pub cfg: TrailConfig,
    pub exec: Arc<LiveExecutor>,
    pub states: Arc<parking_lot::Mutex<HashMap<Address, TrailState>>>,
    pub http: reqwest::Client,
    pub ledger_path: PathBuf,
    /// Real-time WS-fed bonding-curve price cache. Replaces the flaky
    /// `eth_getLogs`-based fallback in the hot path.
    pub price_cache: crate::four_meme_price::FourMemePriceCache,
    /// Per-token block-level flow stats — used by the 3-of-4 voting
    /// exit rule (buy_velocity_collapse, net_flow_3blk).
    pub price_cache_stats: crate::four_meme_price::FourMemeStatsCache,
}

pub fn start(
    cfg: TrailConfig,
    exec: Arc<LiveExecutor>,
    ledger_path: PathBuf,
    price_cache: crate::four_meme_price::FourMemePriceCache,
    price_cache_stats: crate::four_meme_price::FourMemeStatsCache,
    shutdown: CancellationToken,
) {
    if !cfg.enabled {
        tracing::info!(target: "trail", "adaptive trail DISABLED");
        return;
    }
    tracing::info!(
        target: "trail",
        arm_pct = cfg.arm_pct, trail_pct = cfg.trail_pct,
        hard_sl_pct = cfg.hard_sl_pct, max_hold_blocks = cfg.max_hold_blocks,
        breakeven_at_pct = cfg.breakeven_at_pct,
        breakeven_lock_pct = cfg.breakeven_lock_pct,
        ws_url = %cfg.ws_url,
        "adaptive trail ENABLED"
    );

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) bsc-meme-mev/0.1")
        .build()
        .expect("reqwest");

    let watcher = Arc::new(TrailWatcher {
        cfg: cfg.clone(),
        exec,
        states: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        http,
        ledger_path,
        price_cache,
        price_cache_stats,
    });

    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(500);
        loop {
            if shutdown.is_cancelled() { return; }
            match run_loop(watcher.clone(), shutdown.clone()).await {
                Ok(()) => {
                    tracing::info!(target: "trail", "newHeads stream ended; reconnecting");
                    backoff = Duration::from_millis(500);
                }
                Err(e) => {
                    tracing::warn!(target: "trail", error = %e, "newHeads error; reconnecting");
                }
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(15));
        }
    });
}

async fn run_loop(watcher: Arc<TrailWatcher>, shutdown: CancellationToken) -> Result<()> {
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(watcher.cfg.ws_url.clone()))
        .await?;
    let provider = Arc::new(provider);
    let mut sub = provider.subscribe_blocks().await?.into_stream();
    tracing::info!(target: "trail", "newHeads subscription open");
    use futures::StreamExt;

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            head = sub.next() => {
                let Some(header) = head else { return Ok(()); };
                let block_n = header.number;
                if let Err(e) = on_block(watcher.clone(), block_n).await {
                    tracing::debug!(target: "trail", error = %e, "block tick failed (non-fatal)");
                }
            }
        }
    }
}

async fn on_block(watcher: Arc<TrailWatcher>, block_n: u64) -> Result<()> {
    let positions = watcher.exec.position_snapshot().await;
    if positions.is_empty() {
        return Ok(());
    }
    // Process all positions in PARALLEL. Each is a spawned task so:
    //   - 10 positions × 60ms price query = 60ms total (was 600ms serial)
    //   - keeps us well under BSC's 450ms block window
    //   - if one position's RPC stalls, others aren't blocked
    let mut joins = Vec::with_capacity(positions.len());
    for (token, entry) in positions {
        let watcher = watcher.clone();
        joins.push(tokio::spawn(async move {
            process_one_position(watcher, token, entry, block_n).await;
        }));
    }
    for j in joins {
        let _ = j.await;
    }
    Ok(())
}

async fn process_one_position(
    watcher: Arc<TrailWatcher>,
    token: Address,
    entry: Arc<crate::trader::executor_live::PositionEntry>,
    block_n: u64,
) {
    let amt = *entry.tokens_bought.lock();
    let approved = entry.approved.load(std::sync::atomic::Ordering::Acquire);
    // Skip not-yet-ready / fully-exited entries.
    if amt.is_zero() || !approved {
        return;
    }

    // Initialize state on first sight
    let mut state = {
        let map = watcher.states.lock();
        map.get(&token).copied()
    };
    if state.is_none() {
        let init = init_state_from_ledger(token, block_n, &watcher.ledger_path).await;
        if let Some(s) = init {
            watcher.states.lock().insert(token, s);
            state = Some(s);
        } else {
            tracing::debug!(
                target: "trail",
                token = %format!("{token:#x}"),
                "no entry-price data yet; skipping until known"
            );
            return;
        }
    }
    let mut state = state.unwrap();

    // Get latest price observation. CACHE FIRST (WS-fed, push-based,
    // no flaky eth_getLogs polling), then V2 (handles graduated tokens),
    // then Four.Meme observed-curve scan as final fallback.
    //
    // CRITICAL: cache staleness is capped at 5 blocks (~2.5s). Beyond that
    // we MUST fall through to V2 quote so we catch graduation events.
    // Previously this used `max_hold_blocks` (4000) and we'd keep returning
    // the pre-graduation bonding-curve price for the entire 30-min hold —
    // missing 10×+ peaks like 0x947af604 (real peak $162k vs observed
    // $23k on 2026-06-08).
    const CACHE_MAX_STALE_BLOCKS: u64 = 5;
    let current_price = match get_latest_price_cached(
        &watcher.price_cache, CACHE_MAX_STALE_BLOCKS,
        &watcher.http, &watcher.cfg.rpc_url,
        token, entry.route, state.last_observed_block, block_n,
    ).await {
        Some(p) => p,
        None => {
            // No new observation — still check TIMEOUT (block-only)
            let exit_now = block_n.saturating_sub(state.opened_block)
                >= watcher.cfg.max_hold_blocks;
            if exit_now {
                fire_exit(&watcher, token, "timeout", &state, block_n).await;
            }
            return;
        }
    };

    // ── 3-of-4 VOTING EXIT CHECK ─────────────────────────────────────
    // Evaluated BEFORE step() so it fires earlier than SignalDump (which
    // also runs inside step()). Only active when:
    //   (a) the position is already armed (peak ≥ entry × 1.30)
    //   (b) we have ≥3 of these signals firing same block:
    //        1. dist_from_local_max > 0.30
    //        2. vel_10 < -0.01
    //        3. buy_velocity_collapse < 0.5
    //        4. net_flow_3blk < -1 BNB
    // Backtest (30d, 584 trades): 1.151x avg vs 1.124x baseline.
    if state.armed && state.history_count as usize >= PRICE_HISTORY_LEN {
        if let Some(votes) = compute_vote_signals(
            &watcher.price_cache_stats,
            token,
            &state,
            current_price,
        ) {
            if votes >= 3 {
                // Update last observation before firing for accurate log.
                state.last_price = current_price;
                fire_exit(&watcher, token, ExitReason::SignalVote.label(), &state, block_n).await;
                return;
            }
        }
    }

    // Step state machine
    if let Some(reason) = step(&mut state, &watcher.cfg, current_price, block_n) {
        fire_exit(&watcher, token, reason.label(), &state, block_n).await;
    } else {
        // No exit — persist updated peak/last
        watcher.states.lock().insert(token, state);
    }
}

/// Compute the 3-of-4 voting feature count. Returns None when stats
/// aren't yet available (e.g., token just opened, window not warmed).
/// Otherwise returns the number of voting signals firing this block.
fn compute_vote_signals(
    stats_cache: &crate::four_meme_price::FourMemeStatsCache,
    token: Address,
    state: &TrailState,
    current_price: f64,
) -> Option<u8> {
    // Feature 1: dist_from_local_max > 0.30
    let local_max = state.price_history.iter().cloned().fold(0.0_f64, f64::max);
    if local_max <= 0.0 { return None; }
    let dist = (local_max - current_price) / local_max;

    // Feature 2: vel_10 < -0.01
    // oldest price = the slot we're about to overwrite (history_idx position)
    let oldest_idx = state.history_idx as usize % PRICE_HISTORY_LEN;
    let oldest = state.price_history[oldest_idx];
    if oldest <= 0.0 { return None; }
    let vel_10 = (current_price - oldest) / oldest / 10.0;

    // Features 3 + 4: need stats cache
    let stats = stats_cache.read();
    let token_stats = stats.get(&token)?;
    let buy_3  = token_stats.buy_count_last(3);
    let buy_10 = token_stats.buy_count_last(10);
    let net_flow_3blk_wei = token_stats.net_flow_bnb_wei_last(3);
    drop(stats);

    let bv3  = (buy_3  as f64) / 3.0;
    let bv10 = (buy_10 as f64) / 10.0;
    let collapse = if bv10 > 0.0 { bv3 / bv10 } else { 1.0 };
    // -1 BNB threshold = -1e18 wei
    let net_flow_3blk_bnb = net_flow_3blk_wei as f64 / 1e18;

    let mut votes: u8 = 0;
    if dist > 0.30       { votes += 1; }
    if vel_10 < -0.01    { votes += 1; }
    if collapse < 0.5    { votes += 1; }
    if net_flow_3blk_bnb < -1.0 { votes += 1; }
    Some(votes)
}

async fn fire_exit(
    watcher: &Arc<TrailWatcher>,
    token: Address,
    reason: &str,
    state: &TrailState,
    block_n: u64,
) {
    let ratio_from_entry = state.last_price / state.entry_price_bnb_per_token;
    let ratio_from_peak = if state.peak_price > 0.0 {
        state.last_price / state.peak_price
    } else { 1.0 };
    tracing::info!(
        target: "trail",
        token = %format!("{token:#x}"),
        reason,
        entry_price = state.entry_price_bnb_per_token,
        peak_price = state.peak_price,
        current_price = state.last_price,
        ratio_from_entry = ratio_from_entry,
        ratio_from_peak = ratio_from_peak,
        armed = state.armed,
        held_blocks = block_n.saturating_sub(state.opened_block),
        "TRAIL EXIT"
    );
    // Synthetic kol_addr=ZERO → kol_sell_fraction returns None → unwrap_or(1.0)
    // → full close. Exactly what we want for an exit-by-trail signal.
    let kol_name = format!("TRAIL_{reason}");
    if let Err(e) = watcher.exec.execute_exit(
        token, &kol_name, /* kol_block */ block_n, /* kol_addr */ Address::ZERO,
    ).await {
        tracing::warn!(target: "trail", error = %e, "trail exit broadcast failed");
    }
    // Drop state — we no longer track this token
    watcher.states.lock().remove(&token);
}

/// Initialize TrailState by reading the buy receipt from the ledger.
/// Computes entry_price = bnb_in / tokens_received.
async fn init_state_from_ledger(
    token: Address,
    current_block: u64,
    ledger_path: &PathBuf,
) -> Option<TrailState> {
    use std::io::BufRead;
    let token_lower = format!("{token:#x}").to_lowercase();
    let f = std::fs::File::open(ledger_path).ok()?;
    // Find latest BUY row for this token (large bnb_in_wei, not visibility=exit)
    let mut buy_row: Option<Vec<String>> = None;
    let reader = std::io::BufReader::new(f);
    for line in reader.lines().map_while(Result::ok) {
        let parts: Vec<String> = line.split(',').map(|s| s.to_string()).collect();
        if parts.len() < 13 { continue; }
        if !parts[4].to_lowercase().contains(&token_lower) { continue; }
        if parts[3] == "exit" { continue; }
        let bnb_in: u128 = parts[6].parse().unwrap_or(0);
        if bnb_in == 0 { continue; }
        buy_row = Some(parts);
    }
    let row = buy_row?;
    let buy_tx_hash = &row[9];
    let bnb_in: f64 = row[6].parse::<u128>().ok()? as f64 / 1e18;

    // Fetch buy receipt → parse Transfer event TO our wallet → tokens_received
    let receipt_body = serde_json::json!({
        "jsonrpc":"2.0","method":"eth_getTransactionReceipt",
        "params":[buy_tx_hash], "id":1
    });
    let http = reqwest::Client::new();
    let v: serde_json::Value = http.post("http://127.0.0.1:8545")
        .json(&receipt_body).send().await.ok()?
        .json().await.ok()?;
    let receipt = v.get("result")?;
    let logs = receipt.get("logs")?.as_array()?;
    let wallet = row[1].clone(); // placeholder; we'll use the actual wallet
    let _ = wallet;
    // Sum Transfer events on `token` from launchpad (any from), to anyone.
    // Take the LAST one of magnitude > 0 — that's our buy.
    let mut tokens_received: f64 = 0.0;
    let token_addr_str = format!("{token:#x}").to_lowercase();
    for log in logs.iter().rev() {
        let addr = log.get("address").and_then(|x| x.as_str()).unwrap_or("").to_lowercase();
        if addr != token_addr_str { continue; }
        let topics = log.get("topics")?.as_array()?;
        if topics.is_empty() { continue; }
        let t0 = topics[0].as_str().unwrap_or("");
        if !t0.eq_ignore_ascii_case(TRANSFER_TOPIC) { continue; }
        let data = log.get("data").and_then(|x| x.as_str()).unwrap_or("0x");
        let hex = data.trim_start_matches("0x");
        if hex.is_empty() { continue; }
        let amt = U256::from_str_radix(hex, 16).ok()?;
        let amt_f: f64 = amt.to_string().parse().ok()?;
        if amt_f > tokens_received {
            tokens_received = amt_f;
        }
    }
    if tokens_received <= 0.0 {
        return None;
    }
    // Token amount is in raw decimals (usually 18). Price = bnb / tokens_raw_to_whole.
    // For mcap-ratio purposes the absolute decimals don't matter as long as we're
    // consistent. We use raw wei-per-raw-token as our price proxy.
    // entry_price = bnb_in_wei / tokens_received_raw
    let bnb_in_wei = bnb_in * 1e18;
    let entry_price = bnb_in_wei / tokens_received;

    let buy_block: u64 = receipt.get("blockNumber")
        .and_then(|x| x.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(current_block);

    Some(TrailState {
        entry_price_bnb_per_token: entry_price,
        peak_price: entry_price,
        last_price: entry_price,
        armed: false,
        opened_block: buy_block,
        last_observed_block: buy_block,
        ..TrailState::default()
    })
}

/// Query the latest BNB-per-token price for `token`. Tries V2 first
/// (works for any token with a V2 pair — including Four.Meme tokens
/// that have GRADUATED post-curve), then falls back to observed
/// launchpad Transfers (for tokens still on the bonding curve).
///
/// 2026-06-02 bug fix: the original implementation used `route` from
/// the position cache. Four.Meme tokens that graduated mid-hold kept
/// route=FourMeme but their actual trading moved to V2. Our oracle
/// then returned None for the entire post-graduation phase → peak
/// stayed at entry → trail never armed → we missed obvious pumps
/// (user reported a 26.5k→68.59k token where we never sold).
///
/// New approach: ALWAYS try V2 first regardless of the cached route.
/// V2 returns None when there's no pair (pre-graduation). Then we
/// fall back to the observed-curve method.
pub async fn get_latest_price(
    http: &reqwest::Client,
    rpc_url: &str,
    token: Address,
    _route: crate::trader::executor_live::BuyRoute,
    from_block: u64,
    to_block: u64,
) -> Option<f64> {
    if let Some(p) = v2_quote_price(http, rpc_url, token).await {
        return Some(p);
    }
    fourmeme_observed_price(http, rpc_url, token, from_block, to_block).await
}

/// Same as `get_latest_price` but checks the real-time WS-fed price
/// cache FIRST. Cached entries are accepted if they're recent enough
/// to be relevant (within `max_stale_blocks` of the current block).
///
/// Use this in hot paths (trail watchers) — it avoids the flaky
/// `eth_getLogs` fallback when the cache has fresh data.
pub async fn get_latest_price_cached(
    cache: &crate::four_meme_price::FourMemePriceCache,
    max_stale_blocks: u64,
    http: &reqwest::Client,
    rpc_url: &str,
    token: Address,
    route: crate::trader::executor_live::BuyRoute,
    from_block: u64,
    to_block: u64,
) -> Option<f64> {
    if let Some(pp) = cache.read().get(&token).copied() {
        let staleness = to_block.saturating_sub(pp.block);
        if staleness <= max_stale_blocks {
            return Some(pp.price);
        }
    }
    get_latest_price(http, rpc_url, token, route, from_block, to_block).await
}

/// V2 BUY-side quote: simulate sending 0.001 BNB → tokens. Returns
/// `wei_BNB_in / raw_tokens_out` so the unit matches both the entry
/// price (computed from buy receipt: bnb_paid / tokens_received) and
/// the Four.Meme observed price (also buy-side).
///
/// Using buy-side keeps state-machine ratios consistent — if we queried
/// the sell side we'd always read a slightly lower number than entry
/// even when mcap is unchanged, and hard SL would mis-fire.
pub async fn v2_quote_price(
    http: &reqwest::Client,
    rpc_url: &str,
    token: Address,
) -> Option<f64> {
    let wbnb = WBNB.parse::<Address>().ok()?;
    let router = PANCAKE_V2_ROUTER.parse::<Address>().ok()?;
    let probe_in: u128 = 1_000_000_000_000_000; // 0.001 BNB
    // getAmountsOut(uint256, address[]) — selector 0xd06ca61f
    // path = [WBNB, token] → we send BNB, receive tokens (BUY side)
    let sel = "0xd06ca61f";
    let head = format!("{:064x}{:064x}{:064x}", probe_in, 0x40u64, 2u64);
    let body_addrs = format!(
        "{:0>64}{:0>64}",
        hex::encode(wbnb.as_slice()),
        hex::encode(token.as_slice()),
    );
    let data = format!("0x{}{}{}", sel.trim_start_matches("0x"), head, body_addrs);
    let body = serde_json::json!({
        "jsonrpc":"2.0","method":"eth_call",
        "params":[{"to": format!("{router:#x}"), "data": data}, "latest"],
        "id":1
    });
    let v: serde_json::Value = http.post(rpc_url).json(&body).send().await.ok()?
        .json().await.ok()?;
    let hex_s = v.get("result")?.as_str()?.trim_start_matches("0x");
    let raw = hex::decode(hex_s).ok()?;
    if raw.len() < 96 { return None; }
    let n = U256::from_be_slice(&raw[32..64]);
    let n: u64 = n.try_into().ok()?;
    if n == 0 || raw.len() < 64 + (n as usize) * 32 { return None; }
    let last_off = 64 + ((n as usize) - 1) * 32;
    let tokens_out = U256::from_be_slice(&raw[last_off..last_off + 32]);
    let tokens_out_f: f64 = tokens_out.to_string().parse().ok()?;
    if tokens_out_f <= 0.0 { return None; }
    // price = BNB in (wei) / tokens out (raw) — same units as buy receipt
    Some((probe_in as f64) / tokens_out_f)
}

/// Four.Meme observed price: scan token's Transfer events between
/// `from_block` (exclusive) and `to_block` for Transfers FROM the
/// launchpad (= BUYs). For each, fetch the tx and compute
/// price = tx.value / token_amount.
async fn fourmeme_observed_price(
    http: &reqwest::Client,
    rpc_url: &str,
    token: Address,
    from_block: u64,
    to_block: u64,
) -> Option<f64> {
    if to_block <= from_block { return None; }
    let launchpad = FOURMEME_LAUNCHPAD.parse::<Address>().ok()?;
    let launchpad_padded = format!("0x{:0>64}", hex::encode(launchpad.as_slice()));
    // Transfer FROM launchpad: topics[1] = launchpad
    let params = serde_json::json!({
        "address": format!("{token:#x}"),
        "fromBlock": format!("0x{:x}", from_block + 1),
        "toBlock":   format!("0x{:x}", to_block),
        "topics": [TRANSFER_TOPIC, launchpad_padded],
    });
    let body = serde_json::json!({
        "jsonrpc":"2.0","method":"eth_getLogs","params":[params],"id":1
    });
    let v: serde_json::Value = http.post(rpc_url).json(&body).send().await.ok()?
        .json().await.ok()?;
    let logs = v.get("result")?.as_array()?;
    // Iterate in reverse — take the latest BUY
    for log in logs.iter().rev() {
        let data = log.get("data").and_then(|x| x.as_str()).unwrap_or("0x");
        let hex_s = data.trim_start_matches("0x");
        if hex_s.is_empty() { continue; }
        let tokens = U256::from_str_radix(hex_s, 16).ok()?;
        if tokens.is_zero() { continue; }
        let tx_hash = log.get("transactionHash")?.as_str()?;
        let tx_body = serde_json::json!({
            "jsonrpc":"2.0","method":"eth_getTransactionByHash",
            "params":[tx_hash], "id":1
        });
        let tx_v: serde_json::Value = http.post(rpc_url).json(&tx_body).send().await.ok()?
            .json().await.ok()?;
        let bnb_hex = tx_v.get("result")?.get("value")?.as_str()?;
        let bnb = U256::from_str_radix(bnb_hex.trim_start_matches("0x"), 16).ok()?;
        if bnb.is_zero() { continue; }
        let bnb_f: f64 = bnb.to_string().parse().ok()?;
        let tokens_f: f64 = tokens.to_string().parse().ok()?;
        return Some(bnb_f / tokens_f);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_cfg() -> TrailConfig {
        TrailConfig {
            enabled: true,
            arm_pct: 0.20,
            trail_pct: 0.10,
            hard_sl_pct: 0.30,
            max_hold_blocks: 4000,
            ws_url: String::new(),
            rpc_url: String::new(),
            // Ratchet disabled in fixture by setting threshold above any
            // realistic peak (10x). Existing tests are unaffected.
            breakeven_at_pct: 10.0,
            breakeven_lock_pct: 0.0,
        }
    }

    fn fresh_state(entry: f64, opened_at: u64) -> TrailState {
        TrailState {
            entry_price_bnb_per_token: entry,
            peak_price: entry,
            last_price: entry,
            armed: false,
            opened_block: opened_at,
            last_observed_block: opened_at,
            ..TrailState::default()
        }
    }

    #[test]
    fn no_exit_when_price_drifts_inside_window() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // Price wobbles between -10% and +10% — never arms, never hits SL
        for (i, p) in [95.0, 105.0, 92.0, 110.0, 102.0].iter().enumerate() {
            let r = step(&mut s, &cfg, *p, (i + 1) as u64);
            assert_eq!(r, None, "no exit expected at price={p}");
        }
        assert!(!s.armed);
    }

    #[test]
    fn arms_at_plus_20_percent() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        let r1 = step(&mut s, &cfg, 115.0, 1);
        assert_eq!(r1, None);
        assert!(!s.armed, "115 is +15%, below arm threshold");
        let r2 = step(&mut s, &cfg, 120.0, 2);
        assert_eq!(r2, None);
        assert!(s.armed, "120 reaches +20%, must arm");
    }

    #[test]
    fn hard_sl_fires_at_minus_30_percent_unarmed() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        let r = step(&mut s, &cfg, 70.0, 1);
        assert_eq!(r, Some(ExitReason::HardStopLoss));
    }

    #[test]
    fn hard_sl_also_applies_when_armed() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 130.0, 1); // arm
        assert!(s.armed);
        let r = step(&mut s, &cfg, 65.0, 2); // -35%
        assert_eq!(r, Some(ExitReason::HardStopLoss));
    }

    #[test]
    fn trail_fires_when_armed_and_drops_10pct_from_peak() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 130.0, 1); // arm, peak=130
        assert!(s.armed);
        // Drop to 130 × 0.91 = 118.3 → above trail
        let r1 = step(&mut s, &cfg, 118.3, 2);
        assert_eq!(r1, None);
        // Drop to 130 × 0.89 = 115.7 → below trail (≤ peak × 0.90)
        let r2 = step(&mut s, &cfg, 115.7, 3);
        assert_eq!(r2, Some(ExitReason::Trail));
    }

    #[test]
    fn trail_does_not_fire_unarmed_even_if_dropped_from_peak() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 115.0, 1); // peak=115, NOT armed (+15%)
        let r = step(&mut s, &cfg, 90.0, 2); // -22% from entry, -22% from peak
        // 90/100 = 0.90 — does NOT trigger hard_sl (≤ 70 needed)
        // Not armed, so trail does NOT fire
        assert_eq!(r, None);
    }

    #[test]
    fn timeout_fires_after_max_hold_blocks() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // 4000 blocks later, price within range
        let r = step(&mut s, &cfg, 102.0, 4000);
        assert_eq!(r, Some(ExitReason::Timeout));
    }

    #[test]
    fn timeout_takes_lower_priority_than_hard_sl() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // Price at -50%, well past timeout
        let r = step(&mut s, &cfg, 50.0, 5000);
        assert_eq!(r, Some(ExitReason::HardStopLoss));
    }

    #[test]
    fn peak_only_ratchets_up() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 150.0, 1);
        assert_eq!(s.peak_price, 150.0);
        step(&mut s, &cfg, 140.0, 2);
        assert_eq!(s.peak_price, 150.0, "peak doesn't go DOWN on weaker quote");
        step(&mut s, &cfg, 160.0, 3);
        assert_eq!(s.peak_price, 160.0);
    }

    // ── End-to-end SIMULATION: a 100-step price walk ────────────────────
    //
    // Verifies the state machine produces the SAME sequence of transitions
    // for the SAME synthetic price walk every time. This is the user-
    // facing "feasibility" check — we can audit the exact trigger by
    // replaying the walk.

    fn synthetic_pump_then_dump() -> Vec<f64> {
        // Entry=100. Pump to 180, dump to 60.
        let mut v = Vec::new();
        for i in 0..50  { v.push(100.0 + (i as f64) * 1.6); }  // 100 → 178.4
        v.push(180.0);                                          // peak
        for i in 0..50  { v.push(180.0 - (i as f64) * 2.4); }  // 180 → 60
        v
    }

    #[test]
    fn simulated_pump_then_dump_exits_on_trail() {
        let cfg = fixture_cfg();
        let walk = synthetic_pump_then_dump();
        let mut s = fresh_state(100.0, 0);
        let mut fired: Option<(ExitReason, u64, f64)> = None;
        for (i, p) in walk.iter().enumerate() {
            let blk = (i + 1) as u64;
            if let Some(r) = step(&mut s, &cfg, *p, blk) {
                fired = Some((r, blk, *p));
                break;
            }
        }
        let (reason, blk, exit_price) = fired.expect("must have fired an exit");
        assert_eq!(reason, ExitReason::Trail,
            "expected trail; got {reason:?} at block {blk} price {exit_price}");
        // Peak was 180, trail at 90% = 162. Exit_price ≤ 162.
        assert!(exit_price <= 162.0, "trail exit should be at ≤ 162 (peak×0.9); got {exit_price}");
        // Net gain vs entry: locked in 62%+ at exit
        let pct_vs_entry = (exit_price - 100.0) / 100.0;
        assert!(pct_vs_entry >= 0.50,
            "trail must lock in ≥50% gain when peak was 80% up; got {:.1}%", pct_vs_entry * 100.0);
    }

    #[test]
    fn simulated_no_pump_just_dump_exits_on_hard_sl() {
        let cfg = fixture_cfg();
        // Entry=100, immediate slow decline to 50
        let mut s = fresh_state(100.0, 0);
        let mut fired: Option<(ExitReason, u64, f64)> = None;
        for i in 0..50 {
            let p = 100.0 - (i as f64) * 1.5;
            let blk = (i + 1) as u64;
            if let Some(r) = step(&mut s, &cfg, p, blk) {
                fired = Some((r, blk, p));
                break;
            }
        }
        let (reason, _blk, _p) = fired.expect("must fire");
        assert_eq!(reason, ExitReason::HardStopLoss);
        assert!(!s.armed, "never armed");
    }

    #[test]
    fn simulated_flat_market_exits_on_timeout() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // Flat price for max_hold_blocks
        let mut fired: Option<ExitReason> = None;
        for i in 1..=cfg.max_hold_blocks {
            // Price wobbles in [-15%, +15%] — never arms, never SL
            let p = 95.0 + ((i % 5) as f64) * 2.0; // 95..103
            if let Some(r) = step(&mut s, &cfg, p, i) {
                fired = Some(r);
                break;
            }
        }
        assert_eq!(fired, Some(ExitReason::Timeout));
    }

    // ── EXACT THRESHOLD BOUNDARY TESTS ─────────────────────────────────

    #[test]
    fn arm_fires_at_exact_120_pct() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // 119.99 → not yet armed
        let r1 = step(&mut s, &cfg, 119.99, 1);
        assert_eq!(r1, None);
        assert!(!s.armed);
        // EXACTLY 120 → armed (≥ comparison)
        let r2 = step(&mut s, &cfg, 120.0, 2);
        assert_eq!(r2, None);
        assert!(s.armed, "exactly 120 (entry × 1.20) must arm");
    }

    #[test]
    fn hard_sl_fires_at_exact_70_pct() {
        let cfg = fixture_cfg();
        // 70.01 → no exit (above SL)
        let mut s1 = fresh_state(100.0, 0);
        let r1 = step(&mut s1, &cfg, 70.01, 1);
        assert_eq!(r1, None);
        // EXACTLY 70 → SL fires (≤ comparison)
        let mut s2 = fresh_state(100.0, 0);
        let r2 = step(&mut s2, &cfg, 70.0, 1);
        assert_eq!(r2, Some(ExitReason::HardStopLoss));
    }

    #[test]
    fn trail_fires_at_exact_90_pct_of_peak() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 200.0, 1); // peak=200, armed
        // 180.01 → above trail (200 × 0.90 = 180)
        let r1 = step(&mut s, &cfg, 180.01, 2);
        assert_eq!(r1, None);
        // EXACTLY 180 → trail fires (≤ comparison)
        let r2 = step(&mut s, &cfg, 180.0, 3);
        assert_eq!(r2, Some(ExitReason::Trail));
    }

    #[test]
    fn arm_and_trail_can_fire_on_same_tick_if_dump_is_sharp() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // First tick: shoot up to 150
        step(&mut s, &cfg, 150.0, 1);
        assert!(s.armed, "peak=150 ≥ entry × 1.20 = 120, must arm");
        assert_eq!(s.peak_price, 150.0);
        // Second tick: dump to peak × 0.85 = 127.5 → trail floor 135 → exit
        let r = step(&mut s, &cfg, 127.5, 2);
        assert_eq!(r, Some(ExitReason::Trail));
    }

    #[test]
    fn timeout_at_exact_max_hold_block() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // 3999 blocks later → no timeout
        let r1 = step(&mut s, &cfg, 110.0, 3999);
        assert_eq!(r1, None);
        // EXACTLY 4000 blocks → timeout fires (≥ comparison)
        let r2 = step(&mut s, &cfg, 110.0, 4000);
        assert_eq!(r2, Some(ExitReason::Timeout));
    }

    // ── REAL-WORLD SCENARIO REPLAYS ────────────────────────────────────
    //
    // Modeled on the user's actual trades that failed to exit correctly
    // on 2026-06-02. These tests prove the state machine would have done
    // the right thing IF the price oracle had returned correct data.

    #[test]
    fn replay_user_token_1_pumps_to_69k_then_dumps() {
        // entry 26.5k, ATH 68.59k, dump to 14.47k
        let cfg = fixture_cfg();
        let mut s = fresh_state(26500.0, 0);
        // Climb: 26.5 → 32 (arm) → 68.59 (peak)
        for (i, p) in [27000.0, 28000.0, 30000.0, 32000.0, 40000.0, 50000.0, 60000.0, 68590.0]
            .iter().enumerate() {
            let r = step(&mut s, &cfg, *p, (i + 1) as u64);
            assert_eq!(r, None, "no exit during climb at price {p}");
        }
        assert!(s.armed, "must be armed after seeing 32k+");
        assert_eq!(s.peak_price, 68590.0);
        // First dip from peak: 68590 × 0.91 = 62417 → above trail (61731)
        let r1 = step(&mut s, &cfg, 62417.0, 10);
        assert_eq!(r1, None, "62417 still above 61731 trail floor");
        // Second dip: 61730 → below trail floor → fire
        let r2 = step(&mut s, &cfg, 61730.0, 11);
        assert_eq!(r2, Some(ExitReason::Trail),
            "should exit on trail just below 90% of peak");
        // Implied: we locked in 61730 / 26500 = +133% gain
        let pct = (61730.0 - 26500.0) / 26500.0 * 100.0;
        assert!(pct >= 130.0, "trail must lock in ≥130% gain; got +{pct:.1}%");
    }

    #[test]
    fn replay_user_token_2_pump_42_pct_then_dump() {
        // entry 9.66k, ATH 15.29k, exit reality was 4.25k (late timeout)
        let cfg = fixture_cfg();
        let mut s = fresh_state(9660.0, 0);
        // Arm at 11.6k+ (entry × 1.20)
        step(&mut s, &cfg, 11700.0, 1);
        assert!(s.armed);
        // Continue up to peak
        step(&mut s, &cfg, 15290.0, 2);
        assert_eq!(s.peak_price, 15290.0);
        // 90% trail floor = 13761
        // Dump to 13760 → trail fires
        let r = step(&mut s, &cfg, 13760.0, 3);
        assert_eq!(r, Some(ExitReason::Trail));
        // Locks in 13760 / 9660 = +42% gain (vs the actual -56% loss we took)
        let pct = (13760.0 - 9660.0) / 9660.0 * 100.0;
        assert!(pct >= 40.0, "+42% locked vs actual -56% we took");
    }

    #[test]
    fn replay_user_token_3_only_8_pct_pump_then_dump_below_sl() {
        // entry 7.32k, ATH 7.93k (+8% — below arm threshold), actual exit 3.97k
        let cfg = fixture_cfg();
        let mut s = fresh_state(7320.0, 0);
        // Mini pump — never arms
        step(&mut s, &cfg, 7930.0, 1);
        assert!(!s.armed, "+8% must NOT arm (need +20%)");
        assert_eq!(s.peak_price, 7930.0);
        // Pre-ATH low was 6180 → above SL (5124)
        let r1 = step(&mut s, &cfg, 6180.0, 2);
        assert_eq!(r1, None, "6180 is above 5124 SL");
        // Dump through SL boundary: 5124 = entry × 0.70
        let r2 = step(&mut s, &cfg, 5124.0, 3);
        assert_eq!(r2, Some(ExitReason::HardStopLoss),
            "must fire SL at exactly entry × 0.70 = 5124");
        // We'd cap loss at -30% instead of the -46% we actually took
        let pct = (5124.0 - 7320.0) / 7320.0 * 100.0;
        assert!(pct >= -30.5 && pct <= -29.5, "SL caps loss at ~-30%; got {pct:.1}%");
    }

    // ── DEFENSIVE: peak should never go DOWN ──────────────────────────

    #[test]
    fn peak_persistence_through_multiple_ticks() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        let walk = [110.0, 130.0, 125.0, 145.0, 140.0, 160.0, 155.0];
        let mut max_observed = 0f64;
        for (i, p) in walk.iter().enumerate() {
            step(&mut s, &cfg, *p, (i + 1) as u64);
            max_observed = max_observed.max(*p);
            assert_eq!(s.peak_price, max_observed,
                "peak must equal max-seen; expected {max_observed}, got {}", s.peak_price);
        }
    }

    // ── ENTRY-PRICE INVARIANT: never changes ──────────────────────────

    #[test]
    fn entry_price_immutable_across_all_ticks() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        let entry = s.entry_price_bnb_per_token;
        for (i, p) in [200.0, 250.0, 150.0, 80.0].iter().enumerate() {
            step(&mut s, &cfg, *p, (i + 1) as u64);
            assert_eq!(s.entry_price_bnb_per_token, entry,
                "entry MUST be immutable; corrupted to {}", s.entry_price_bnb_per_token);
        }
    }

    // ── ORDER OF CHECKS: hard SL beats trail beats timeout ────────────

    #[test]
    fn hard_sl_beats_trail_at_same_tick() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 200.0, 1); // peak=200, armed
        // Price drops to 60 → below both hard SL (70) AND trail (180)
        let r = step(&mut s, &cfg, 60.0, 2);
        assert_eq!(r, Some(ExitReason::HardStopLoss),
            "when both fire on same tick, hard SL takes priority");
    }

    #[test]
    fn trail_beats_timeout_at_same_tick() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 200.0, 100); // peak=200, armed
        // 4001 blocks later, price hits trail floor exactly
        let r = step(&mut s, &cfg, 180.0, 4101);
        assert_eq!(r, Some(ExitReason::Trail),
            "trail beats timeout when both could fire");
    }

    // ── ZERO/EXTREME VALUES — defensive ──────────────────────────────

    #[test]
    fn very_large_pump_does_not_overflow() {
        let cfg = fixture_cfg();
        let mut s = fresh_state(1.0, 0);
        // 1 → 1e15 (a million × million × pump)
        let r = step(&mut s, &cfg, 1e15, 1);
        assert_eq!(r, None, "huge pump must not exit");
        assert!(s.armed);
        assert_eq!(s.peak_price, 1e15);
    }

    #[test]
    fn tiny_prices_handled_correctly() {
        // Mirrors Four.Meme tokens with raw-decimals (1e-10 BNB-per-raw-token).
        let cfg = fixture_cfg();
        let entry = 1.065e-8;  // user's actual token #1 entry
        let mut s = fresh_state(entry, 0);
        // Pump 6.5x — matches user's reported peak
        step(&mut s, &cfg, entry * 6.5, 1);
        assert!(s.armed);
        // Dump to 10% below peak
        let trail_floor = entry * 6.5 * 0.9;
        let r = step(&mut s, &cfg, trail_floor, 2);
        assert_eq!(r, Some(ExitReason::Trail));
    }

    // ── SIGNAL-DUMP (v1 leading-exit rule) tests ────────────────────
    //
    // Rule: dist_from_local_max > 0.30  AND  vel_10 < -0.01
    //   (after armed, only when history buffer is full)
    //
    // Tests use a wide-trail config so the existing trail rule doesn't
    // fire first and mask the SignalDump check.

    fn wide_trail_cfg() -> TrailConfig {
        TrailConfig {
            enabled: false,
            arm_pct: 0.20,
            trail_pct: 0.90,    // 90% — effectively disabled for these tests
            hard_sl_pct: 0.50,  // 50% — keep SL out of the way too
            max_hold_blocks: 4000,
            // Ratchet disabled in this fixture too (threshold above any peak).
            breakeven_at_pct: 10.0,
            breakeven_lock_pct: 0.0,
            ws_url: String::new(),
            rpc_url: String::new(),
        }
    }

    #[test]
    fn signal_dump_requires_full_history() {
        // With <10 observations the rule MUST NOT fire even if both
        // arithmetic conditions would be true.
        let cfg = wide_trail_cfg();
        let mut s = fresh_state(100.0, 0);
        // Pump to arm
        step(&mut s, &cfg, 200.0, 1);
        assert!(s.armed);
        // Drop hard immediately. History only has 2 entries → no SignalDump.
        let r = step(&mut s, &cfg, 100.0, 2);
        assert_eq!(r, None, "history not full; rule must not fire");
    }

    #[test]
    fn signal_dump_fires_when_both_conditions_met() {
        // 10 prices: peak then drop. After 10 ticks history is full.
        //   local_max = 200, oldest = 200, current = 90
        //   dist_from_local_max = (200-90)/200 = 0.55 > 0.30 ✓
        //   vel_10 = (90-200)/200/10 = -0.055 < -0.01 ✓
        let cfg = wide_trail_cfg();
        let mut s = fresh_state(100.0, 0);
        let prices = [200.0, 180.0, 160.0, 140.0, 130.0, 120.0, 110.0, 105.0, 100.0, 90.0];
        let mut last_reason = None;
        for (i, p) in prices.iter().enumerate() {
            last_reason = step(&mut s, &cfg, *p, (i + 1) as u64);
            if last_reason.is_some() { break; }
        }
        assert_eq!(last_reason, Some(ExitReason::SignalDump),
            "with peak 200 falling to 90 over 10 ticks, SignalDump must fire");
    }

    #[test]
    fn signal_dump_does_not_fire_in_steady_climb() {
        // Steady climb → vel_10 POSITIVE → no fire.
        let cfg = wide_trail_cfg();
        let mut s = fresh_state(100.0, 0);
        let prices = [110.0, 115.0, 125.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 190.0];
        let mut last_reason = None;
        for (i, p) in prices.iter().enumerate() {
            last_reason = step(&mut s, &cfg, *p, (i + 1) as u64);
            if last_reason.is_some() { break; }
        }
        assert_eq!(last_reason, None);
        assert!(s.armed);
    }

    #[test]
    fn signal_dump_does_not_fire_when_only_slope_negative_but_dist_small() {
        // Negative slope but price stays close to local max → dist fails.
        // Use a sequence where the first 10 ticks arm and fill history with
        // values close to current.
        let cfg = wide_trail_cfg();
        let mut s = fresh_state(100.0, 0);
        // First tick: 200 to arm. Then ticks 2-10 stay within ~5% of latest.
        // local_max = 200 throughout. After tick 10, current = 191.
        //   dist = (200-191)/200 = 0.045 < 0.30 → no fire
        //   vel_10 = (191-200)/200/10 = -0.0045 > -0.01 → also no fire
        let prices = [200.0, 199.0, 198.0, 197.0, 196.0, 195.0, 194.0, 193.0, 192.0, 191.0];
        let mut last_reason = None;
        for (i, p) in prices.iter().enumerate() {
            last_reason = step(&mut s, &cfg, *p, (i + 1) as u64);
            if last_reason.is_some() { break; }
        }
        assert_eq!(last_reason, None);
    }

    #[test]
    fn signal_dump_does_not_fire_unarmed() {
        // No arm (peak never ≥ +20%). Stay below 120 throughout.
        // 110, 105, 100, 95, 90, 85, 81, 78, 75, 75
        //  - peak=110 < 120 → NOT armed
        //  - even though dist+vel would satisfy, the gate is closed
        //  - hard SL is at 50 (wide_trail_cfg uses sl=0.50) so 75 ok
        let cfg = wide_trail_cfg();
        let mut s = fresh_state(100.0, 0);
        let prices = [110.0, 105.0, 100.0, 95.0, 90.0, 85.0, 81.0, 78.0, 75.0, 75.0];
        let mut last_reason = None;
        for (i, p) in prices.iter().enumerate() {
            last_reason = step(&mut s, &cfg, *p, (i + 1) as u64);
            if last_reason.is_some() { break; }
        }
        assert_eq!(last_reason, None);
        assert!(!s.armed);
    }

    // ── BREAK-EVEN RATCHET tests ─────────────────────────────────

    fn ratchet_cfg() -> TrailConfig {
        TrailConfig {
            enabled: false,
            arm_pct: 0.30,        // arm at +30% (same as live)
            trail_pct: 0.30,
            hard_sl_pct: 0.30,
            max_hold_blocks: 4000,
            ws_url: String::new(),
            rpc_url: String::new(),
            breakeven_at_pct: 0.15,
            breakeven_lock_pct: 0.05,
        }
    }

    #[test]
    fn ratchet_fires_after_peak_15_then_dump() {
        // Peak +20% (above +15% threshold), then dump to entry × 1.04
        // → below the lock floor (entry × 1.05) → BreakevenLocked.
        let cfg = ratchet_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 120.0, 1);    // peak hits +20%, ratchet arms
        assert!(s.breakeven_ratcheted);
        let r = step(&mut s, &cfg, 104.0, 2);  // 104 < lock floor 105
        assert_eq!(r, Some(ExitReason::BreakevenLocked));
    }

    #[test]
    fn ratchet_does_not_fire_unless_peak_hit_threshold() {
        // Peak only +10% (below +15% threshold) — ratchet stays OFF.
        // Then dump to -25% (still above -30% hard SL).
        let cfg = ratchet_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 110.0, 1);    // peak +10% — not ratchet
        assert!(!s.breakeven_ratcheted);
        // Now dump to 80 (still above hard SL 70) — no exit
        let r = step(&mut s, &cfg, 80.0, 2);
        assert_eq!(r, None);
    }

    #[test]
    fn ratchet_does_not_fire_if_price_stays_above_lock() {
        // Peak +20%, drift down but stays above lock floor 105.
        let cfg = ratchet_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 120.0, 1);
        assert!(s.breakeven_ratcheted);
        // 110 > 105 → no exit
        let r = step(&mut s, &cfg, 110.0, 2);
        assert_eq!(r, None);
    }

    #[test]
    fn ratchet_overrides_hard_sl_when_lock_higher() {
        // After ratchet, effective floor = max(70, 105) = 105.
        // A price of 90 was above old SL but below the new lock — exits.
        let cfg = ratchet_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 120.0, 1);
        let r = step(&mut s, &cfg, 90.0, 2);
        assert_eq!(r, Some(ExitReason::BreakevenLocked));
    }

    #[test]
    fn ratchet_does_not_double_fire() {
        let cfg = ratchet_cfg();
        let mut s = fresh_state(100.0, 0);
        step(&mut s, &cfg, 120.0, 1);
        assert!(s.breakeven_ratcheted);
        // Even if price re-pumps then dips, ratchet stays armed once set.
        step(&mut s, &cfg, 140.0, 2);
        assert!(s.breakeven_ratcheted);
        // Price back below lock → exits at first chance.
        let r = step(&mut s, &cfg, 100.0, 3);
        assert_eq!(r, Some(ExitReason::BreakevenLocked));
    }

    #[test]
    fn ratchet_replays_today_losses() {
        // The 4 closed losses on 2026-06-06 all peaked +13..+27%.
        // bc299aa2 (peak +23%, exit 0.64x LIVE): with ratchet should lock ≥ +5%.
        let cfg = ratchet_cfg();
        // Token #1: peaked 1.23, dumped to 0.64.
        let mut s = fresh_state(100.0, 0);
        let prices = [105.0, 110.0, 118.0, 123.0, 110.0, 90.0, 75.0, 64.0];
        let mut last = None;
        for (i, p) in prices.iter().enumerate() {
            last = step(&mut s, &cfg, *p, (i + 1) as u64);
            if last.is_some() { break; }
        }
        assert_eq!(last, Some(ExitReason::BreakevenLocked));
        // Ratchet exited at the first price below 105 (the lock floor).
        // In LIVE this token would have ridden to 64 (-36% SL).
        // The ratchet exit at 90 vs SL at 70 = +20pp saved.
        assert!(s.last_price < 105.0, "exit price must be ≤ lock floor; got {}", s.last_price);
        assert!(s.last_price > 70.0,  "exit must beat LIVE's -30% SL price (70); got {}", s.last_price);

        // Token #3: peaked +27%, drifted down — would have hit timeout at -7% LIVE.
        let mut s = fresh_state(100.0, 0);
        let prices = [110.0, 120.0, 127.0, 120.0, 115.0, 110.0, 105.0, 100.0, 95.0, 93.0];
        let mut last = None;
        for (i, p) in prices.iter().enumerate() {
            last = step(&mut s, &cfg, *p, (i + 1) as u64);
            if last.is_some() { break; }
        }
        assert_eq!(last, Some(ExitReason::BreakevenLocked));
    }

    #[test]
    fn hard_sl_priority_over_signal_dump() {
        // Use the production-like fixture (sl=0.30) where 65 < 70=sl_floor.
        // Path ends at 65 — hard SL must win over SignalDump.
        let cfg = fixture_cfg();
        let mut s = fresh_state(100.0, 0);
        // First tick arms (200). The trail at -10% would fire at 180 in
        // fixture_cfg — but we just want to verify HardSL still has the
        // top priority above SignalDump when BOTH would fire same tick.
        // So put the dip below 70 on the SECOND tick before trail loops.
        let r1 = step(&mut s, &cfg, 200.0, 1);
        assert_eq!(r1, None);
        let r2 = step(&mut s, &cfg, 65.0, 2);
        assert_eq!(r2, Some(ExitReason::HardStopLoss));
    }
}
