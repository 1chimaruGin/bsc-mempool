//! Dev-launchpad sniper — buys new Four.Meme tokens at creation time
//! when the creator is on our trusted-dev whitelist.
//!
//! ## Why this exists
//!
//! Copy-trading KOLs forces us to fill AFTER the KOL → we pay the curve
//! impact they just created. ~40% N+1 slippage on hot memes ate our
//! edge in the 2026-05/06 experiment. Sniping at creation flips the
//! dynamic: WE are at curve point 0, the KOLs arrive later and push the
//! curve up FOR us.
//!
//! ## Loop
//!
//! 1. Subscribe to launchpad logs (filter: address=FOURMEME, topic[0]=TokenCreate)
//! 2. On each match, decode the token address from event DATA
//! 3. Look up the tx's `from` to learn the creator
//! 4. If creator ∈ whitelist → fire a BUY (paper or live, config-gated)
//! 5. Cache the position; existing kol_confirm exit pipeline handles sells
//!
//! ## Stop-loss / take-profit / timeout
//!
//! A separate periodic task walks held sniper positions every 30s and
//! force-exits if any threshold is hit:
//!   - +profit_take_pct → close (lock in gain)
//!   - −stop_loss_pct → close (cap downside)
//!   - elapsed > timeout_secs → close (avoid bag-holding duds)
//!
//! Mark-to-market via a single sellToken eth_call dry-run for cheapness.

use crate::config::DevSniperConfig as SniperConfig;
use crate::four_meme_price::FourMemePriceCache;
use crate::trader::adaptive_trail::{
    ExitReason, TrailConfig, TrailState, get_latest_price_cached, step as trail_step,
};
use crate::trader::executor_live::{BuyRoute, LiveExecutor};
use crate::trader::types::Decision;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Filter;
use anyhow::Result;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub const FOURMEME_LAUNCHPAD: &str = "0x5c952063c7fc8610FFDB798152D69F0B9550762b";
/// Four.Meme `TokenCreate` event topic[0] — verified empirically.
pub const TOKEN_CREATE_TOPIC: &str =
    "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20";

#[derive(Debug, Clone, Deserialize)]
struct DevsFile {
    devs: Vec<String>,
}

/// One in-flight snipe. State machine (`trail`) is updated per-block by
/// `sniper_trail_loop`. `entry_price` is filled lazily on the first
/// successful price query after BUY (V2 quote OR Four.Meme observed).
#[derive(Debug)]
struct SnipePosition {
    token: Address,
    dev: Address,
    bnb_in_wei: u128,
    opened_at: Instant,
    opened_block: u64,
    /// Filled on first successful price query post-buy. `None` while
    /// we haven't yet seen any price (curve totally idle right after
    /// creation), in which case we skip stepping but DO check timeout.
    trail: parking_lot::Mutex<Option<TrailState>>,
}

pub fn start(
    cfg: SniperConfig,
    trail_cfg: TrailConfig,
    price_cache: FourMemePriceCache,
    live_exec: Option<Arc<LiveExecutor>>,
    bnb_price: Arc<crate::bnb_price::BnbPrice>,
    shutdown: CancellationToken,
) {
    if !cfg.enabled {
        tracing::info!(target: "sniper", "dev launchpad sniper DISABLED");
        return;
    }
    // Load whitelist
    let devs = match load_devs(Path::new(&cfg.dev_whitelist_file)) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            tracing::error!(
                target: "sniper",
                error = %e,
                file = %cfg.dev_whitelist_file,
                "dev whitelist load failed; sniper disabled"
            );
            return;
        }
    };
    let pinned_bnb_wei = parse_bnb_wei(&cfg.trade_size_bnb_wei);
    tracing::info!(
        target: "sniper",
        devs = devs.len(),
        mode = %cfg.mode,
        trade_size_usd = cfg.trade_size_usd,
        trade_size_bnb_wei = pinned_bnb_wei,
        trail_enabled = trail_cfg.enabled,
        trail_arm_pct = trail_cfg.arm_pct,
        trail_pct = trail_cfg.trail_pct,
        trail_hard_sl_pct = trail_cfg.hard_sl_pct,
        trail_max_hold = trail_cfg.max_hold_blocks,
        ws_url = %cfg.ws_url,
        "dev launchpad sniper ENABLED"
    );

    let positions: Arc<Mutex<HashMap<Address, Arc<SnipePosition>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Detection loop
    {
        let cfg = cfg.clone();
        let devs = devs.clone();
        let positions = positions.clone();
        let live_exec = live_exec.clone();
        let bnb_price = bnb_price.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut backoff_ms = 500u64;
            loop {
                if shutdown.is_cancelled() { return; }
                match run_detection_loop(
                    &cfg, &devs, &positions, live_exec.clone(),
                    bnb_price.clone(), shutdown.clone(),
                ).await {
                    Ok(()) => {
                        tracing::info!(target: "sniper", "WS subscription ended; reconnecting");
                        backoff_ms = 500;
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "sniper", error = %e,
                            backoff_ms,
                            "WS subscription error; reconnecting with backoff"
                        );
                    }
                }
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                }
                backoff_ms = (backoff_ms * 2).min(15_000);
            }
        });
    }

    // Adaptive-trail loop (parallel-per-position, newHeads-driven).
    // Replaces the old per-30s timeout-only sweep.
    if trail_cfg.enabled {
        let positions = positions.clone();
        let price_cache = price_cache.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                if shutdown.is_cancelled() { return; }
                match sniper_trail_loop(positions.clone(), trail_cfg.clone(), price_cache.clone(), shutdown.clone()).await {
                    Ok(()) => {
                        tracing::info!(target: "sniper", "trail newHeads stream ended; reconnecting");
                        backoff = Duration::from_millis(500);
                    }
                    Err(e) => {
                        tracing::warn!(target: "sniper", error = %e, "trail newHeads error; reconnecting");
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
    } else {
        // Trail disabled — fall back to legacy timeout-only sweep so we at
        // least log overdue paper positions.
        let cfg = cfg.clone();
        let positions = positions.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(cfg.eval_interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await;
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    _ = tick.tick() => {
                        let positions = positions.lock().await;
                        for (token, p) in positions.iter() {
                            let age = p.opened_at.elapsed();
                            if age >= Duration::from_secs(cfg.timeout_secs) {
                                tracing::info!(
                                    target: "sniper",
                                    token = %format!("{token:#x}"),
                                    dev = %format!("{:#x}", p.dev),
                                    age_secs = age.as_secs(),
                                    "sniper position TIMED OUT; manual review or sweep-dust suggested"
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}

/// Parse `trade_size_bnb_wei` string. Empty / "0" → None (fall back to USD).
fn parse_bnb_wei(s: &str) -> Option<u128> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "0" { return None; }
    trimmed.parse::<u128>().ok().filter(|v| *v > 0)
}

/// newHeads-driven trail evaluation. On each new block:
///   1. snapshot the positions map
///   2. spawn one tokio task per position → parallel price query + state step
///   3. closed positions are removed inline by their task
async fn sniper_trail_loop(
    positions: Arc<Mutex<HashMap<Address, Arc<SnipePosition>>>>,
    trail_cfg: TrailConfig,
    price_cache: FourMemePriceCache,
    shutdown: CancellationToken,
) -> Result<()> {
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(trail_cfg.ws_url.clone()))
        .await?;
    let provider = Arc::new(provider);
    let mut sub = provider.subscribe_blocks().await?.into_stream();
    tracing::info!(target: "sniper", "trail newHeads subscription open");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .user_agent("Mozilla/5.0 bsc-meme-mev/sniper-trail")
        .build()?;

    use futures::StreamExt;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            block = sub.next() => {
                let Some(block) = block else { return Ok(()); };
                let block_n = block.number;
                let snapshot: Vec<(Address, Arc<SnipePosition>)> = {
                    let map = positions.lock().await;
                    map.iter().map(|(k, v)| (*k, v.clone())).collect()
                };
                if snapshot.is_empty() { continue; }
                let mut joins = Vec::with_capacity(snapshot.len());
                for (token, pos) in snapshot {
                    let trail_cfg = trail_cfg.clone();
                    let http = http.clone();
                    let positions = positions.clone();
                    let price_cache = price_cache.clone();
                    joins.push(tokio::spawn(async move {
                        process_sniper_position(
                            token, pos, block_n, trail_cfg, http, price_cache, positions,
                        ).await;
                    }));
                }
                for j in joins { let _ = j.await; }
            }
        }
    }
}

async fn process_sniper_position(
    token: Address,
    pos: Arc<SnipePosition>,
    block_n: u64,
    trail_cfg: TrailConfig,
    http: reqwest::Client,
    price_cache: FourMemePriceCache,
    positions: Arc<Mutex<HashMap<Address, Arc<SnipePosition>>>>,
) {
    // Has the trail state been initialized? If not, we need to scan
    // wider (from opened_block forward) to catch the first buyer.
    // Once initialized, we only need the last few blocks.
    let already_inited = pos.trail.lock().is_some();
    let scan_floor = pos.opened_block.max(block_n.saturating_sub(trail_cfg.max_hold_blocks));
    let from = if already_inited {
        block_n.saturating_sub(4)
    } else {
        scan_floor
    };
    // 5-block cap on cache staleness — see comment in adaptive_trail.rs.
    // Without this the cache holds pre-graduation prices for the whole hold
    // and we miss post-V2 movement entirely.
    const CACHE_MAX_STALE_BLOCKS: u64 = 5;
    let price = get_latest_price_cached(
        &price_cache, CACHE_MAX_STALE_BLOCKS,
        &http, &trail_cfg.rpc_url, token, BuyRoute::FourMeme, from, block_n,
    ).await;

    // Defensive eviction: if we've held > max_hold blocks and never got
    // a price observation, the token is a dead launch (dev minted, no
    // one bought). Drop it so the positions map doesn't grow forever.
    if !already_inited && price.is_none() {
        let age_blocks = block_n.saturating_sub(pos.opened_block);
        if age_blocks >= trail_cfg.max_hold_blocks {
            tracing::info!(
                target: "sniper",
                token = %format!("{token:#x}"),
                dev = %format!("{:#x}", pos.dev),
                age_blocks,
                "TRAIL EXIT (paper) reason=dead_token (no buyer activity in {} blocks)",
                age_blocks
            );
            positions.lock().await.remove(&token);
            return;
        }
    }

    // Lazy entry-price init on first successful observation. Tight scope
    // so the parking_lot guard is dropped before any `.await`.
    let exit_info = {
        let mut state_guard = pos.trail.lock();
        if state_guard.is_none() {
            let Some(p) = price else { return; }; // no price yet → wait
            *state_guard = Some(TrailState {
                entry_price_bnb_per_token: p,
                peak_price: p,
                last_price: p,
                armed: false,
                opened_block: pos.opened_block.max(block_n.saturating_sub(1)),
                last_observed_block: block_n,
                ..TrailState::default()
            });
            drop(state_guard);
            tracing::info!(
                target: "sniper",
                token = %format!("{token:#x}"),
                dev = %format!("{:#x}", pos.dev),
                entry_price = p,
                opened_block = pos.opened_block,
                "trail: entry price captured"
            );
            return;
        }
        let state = state_guard.as_mut().expect("checked is_none above");
        let current_price = price.unwrap_or(state.last_price);
        trail_step(state, &trail_cfg, current_price, block_n).map(|reason| (
            reason,
            state.entry_price_bnb_per_token,
            state.peak_price,
            current_price,
            state.armed,
            block_n.saturating_sub(state.opened_block),
        ))
    };

    if let Some((reason, entry, peak, exit_price, armed, held)) = exit_info {
        let pnl_ratio = (exit_price - entry) / entry;
        let bnb_in = pos.bnb_in_wei as f64 / 1e18;
        let pnl_bnb = bnb_in * pnl_ratio;
        tracing::info!(
            target: "sniper",
            token = %format!("{token:#x}"),
            dev = %format!("{:#x}", pos.dev),
            reason = reason.label(),
            entry_price = entry,
            peak_price = peak,
            exit_price = exit_price,
            pnl_ratio = pnl_ratio,
            pnl_bnb = pnl_bnb,
            armed = armed,
            held_blocks = held,
            "TRAIL EXIT (paper)"
        );
        positions.lock().await.remove(&token);
    }
}

async fn run_detection_loop(
    cfg: &SniperConfig,
    devs: &Arc<HashSet<Address>>,
    positions: &Arc<Mutex<HashMap<Address, Arc<SnipePosition>>>>,
    live_exec: Option<Arc<LiveExecutor>>,
    bnb_price: Arc<crate::bnb_price::BnbPrice>,
    shutdown: CancellationToken,
) -> Result<()> {
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(cfg.ws_url.clone()))
        .await?;
    let provider = Arc::new(provider);
    let launchpad = FOURMEME_LAUNCHPAD.parse::<Address>()?;
    let topic = B256::from_str(TOKEN_CREATE_TOPIC)?;
    let filter = Filter::new()
        .address(launchpad)
        .event_signature(topic);
    let mut sub = provider.subscribe_logs(&filter).await?.into_stream();
    tracing::info!(target: "sniper", "WS log subscription open (Four.Meme TokenCreate)");

    use futures::StreamExt;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            log = sub.next() => {
                let Some(log) = log else { return Ok(()); };
                if let Err(e) = handle_log(
                    log, cfg, devs, positions, &live_exec, &bnb_price, &provider
                ).await {
                    tracing::debug!(target: "sniper", error = %e, "log handler failed (non-fatal)");
                }
            }
        }
    }
}

async fn handle_log(
    log: alloy::rpc::types::Log,
    cfg: &SniperConfig,
    devs: &Arc<HashSet<Address>>,
    positions: &Arc<Mutex<HashMap<Address, Arc<SnipePosition>>>>,
    live_exec: &Option<Arc<LiveExecutor>>,
    bnb_price: &Arc<crate::bnb_price::BnbPrice>,
    provider: &Arc<impl Provider + ?Sized + 'static>,
) -> Result<()> {
    // Creator = `from` of the tx that emitted this log. This is reliable —
    // the tx that called Four.Meme's `createToken` is signed by the dev.
    let Some(tx_hash) = log.transaction_hash else {
        anyhow::bail!("log has no tx hash");
    };
    let tx = provider.get_transaction_by_hash(tx_hash).await?;
    let Some(tx) = tx else {
        anyhow::bail!("tx receipt fetch returned none");
    };
    let creator = tx.inner.signer();

    // Token address is in the event DATA (not topics), but the offset
    // varies. Earlier attempt used data[12..32] which turned out to be
    // the CREATOR (matching tx.signer()) — wrong layout assumption.
    //
    // Resilient extraction: scan 32-byte chunks looking for "address word"
    // shapes (12 zero high bytes + 20-byte address-like body), and pick
    // the first that's neither the creator nor the launchpad itself.
    let data: &[u8] = log.inner.data.data.as_ref();
    if data.len() < 32 {
        anyhow::bail!("event data too short ({} bytes)", data.len());
    }
    let launchpad_addr = FOURMEME_LAUNCHPAD.parse::<Address>()?;
    let token = data
        .chunks_exact(32)
        .filter_map(|chunk| {
            // First 12 bytes must be zero (address slot is right-aligned).
            if chunk[0..12].iter().any(|b| *b != 0) {
                return None;
            }
            // High bytes of address part should be non-zero (real address,
            // not a small uint masquerading as an address slot).
            if chunk[12..16].iter().all(|b| *b == 0) {
                return None;
            }
            Some(Address::from_slice(&chunk[12..32]))
        })
        .find(|a| *a != creator && *a != launchpad_addr)
        .ok_or_else(|| anyhow::anyhow!("no token address found in event data"))?;

    let in_whitelist = devs.contains(&creator);

    tracing::info!(
        target: "sniper",
        token = %format!("{token:#x}"),
        dev = %format!("{creator:#x}"),
        in_whitelist,
        block = log.block_number.unwrap_or(0),
        tx = %format!("{tx_hash:#x}"),
        "TokenCreate observed"
    );

    if !in_whitelist {
        return Ok(());
    }

    // ── BUY decision ─────────────────────────────────────────────────
    // Pinned-BNB sizing (when configured) is preferred over USD: backtest
    // results were calibrated at a fixed BNB amount, so floating with
    // BNB/USD would invalidate the sizing.
    let bnb_in_wei = if let Some(pinned) = parse_bnb_wei(&cfg.trade_size_bnb_wei) {
        pinned
    } else {
        let bnb_usd = bnb_price.get().await.unwrap_or(0.0);
        if bnb_usd <= 0.0 {
            tracing::warn!(target: "sniper", "BNB/USD oracle unavailable; skipping snipe");
            return Ok(());
        }
        ((cfg.trade_size_usd / bnb_usd) * 1e18) as u128
    };
    let bnb_in = U256::from(bnb_in_wei);

    // Dup-guard via internal map (the executor's own dup-guard would
    // also catch this but we log here for visibility).
    {
        let map = positions.lock().await;
        if map.contains_key(&token) {
            tracing::info!(
                target: "sniper",
                token = %format!("{token:#x}"),
                "skip: already sniped this token"
            );
            return Ok(());
        }
    }

    let decision = Decision::Enter {
        kol_name: format!("SNIPE_{}", &format!("{creator:#x}")[..10]),
        token,
        bnb_amount: bnb_in,
        kol_bnb_input: U256::ZERO,
        kol_block: log.block_number.unwrap_or(0),
        kol_tx: tx_hash,
    };

    match cfg.mode.as_str() {
        "live" => {
            let Some(exec) = live_exec.as_ref() else {
                tracing::warn!(target: "sniper", "mode=live but live executor not available");
                return Ok(());
            };
            let n_open = exec.open_positions();
            // visibility="public" treats it as a public-channel entry
            // (we DID see the launch event publicly via launchpad logs).
            if let Err(e) = exec.execute(decision, "public", n_open).await {
                tracing::warn!(target: "sniper", error = %e, "snipe execute failed");
                return Ok(());
            }
        }
        _ => {
            tracing::info!(
                target: "sniper",
                token = %format!("{token:#x}"),
                dev = %format!("{creator:#x}"),
                size_usd = cfg.trade_size_usd,
                size_bnb_wei = bnb_in_wei,
                "PAPER snipe (no broadcast)"
            );
        }
    }

    positions.lock().await.insert(token, Arc::new(SnipePosition {
        token,
        dev: creator,
        bnb_in_wei,
        opened_at: Instant::now(),
        opened_block: log.block_number.unwrap_or(0),
        trail: parking_lot::Mutex::new(None),
    }));

    Ok(())
}

fn load_devs(path: &Path) -> Result<HashSet<Address>> {
    let s = std::fs::read_to_string(path)?;
    let f: DevsFile = toml::from_str(&s)?;
    let set: HashSet<Address> = f.devs.iter()
        .filter_map(|h| h.parse::<Address>().ok())
        .collect();
    Ok(set)
}
