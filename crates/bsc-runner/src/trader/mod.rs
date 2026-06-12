//! BSC paper trader — orchestrator.
//!
//! Wires together: kol_watch sink → strategy → executor → ledger.
//!
//! Three background tasks per running trader:
//!   1. **run_consumer**: drains the KOL-hit channel, runs Strategy, calls
//!      executor.
//!   2. **run_sweeper**: every `sweep_interval_secs`, closes positions older
//!      than `hold_timeout_secs` via timeout sim.
//!   3. **run_daily_summary**: at `daily_summary_utc_hour`, fires a single
//!      Telegram summary (open positions + last 24h closed PnL).

pub mod adaptive_trail;
pub mod blacklist;
pub mod dev_resolver;
pub mod executor_live;
pub mod kol_budget;
pub mod ledger;
pub mod limits;
pub mod live_ledger;
pub mod nonce;
pub mod paper;
pub mod position;
pub mod sim;
pub mod strategy;
pub mod types;
pub mod wallet;

pub use executor_live::LiveExecutor;
pub use ledger::Ledger;
pub use limits::{LimitsConfig, LimitsRuntime};
pub use nonce::NonceManager;
pub use paper::{Costs, PaperExecutor, TelegramAlerter};
pub use wallet::TraderWallet;
pub use position::PositionBook;
pub use sim::{QuoteResult, QuoteVenue, Simulator};
pub use strategy::{Strategy, StrategyConfig, extract_target_token};
pub use types::{
    CloseReason, ClosedTrade, Decision, OpenPosition, PortfolioMode, PositionKey, Side,
    SkipReason, Token, DEFAULT_HOLD_TIMEOUT,
};

use crate::config::TraderConfig;
use crate::kol_watch::KolHit;
use crate::token_resolver::TokenResolver;
use alloy::primitives::U256;
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Handles returned by `start()` so the wiring layer can plug the trader
/// into kol_watch's `Sinks { trader: Some(hit_tx) }` slot.
pub struct TraderHandles {
    pub hit_tx: mpsc::Sender<KolHit>,
    /// Shared held-token set — wiring passes this to the Phase 2 flow
    /// watcher and Phase 3 price/liquidity oracle.
    pub held: Arc<crate::held_tokens::HeldTokens>,
}

/// Spawn the trader's three background tasks. Returns `None` if disabled.
///
/// When `live_enabled = true`, additionally tries to load
/// `config/limits.toml` + wallet from env and constructs a `LiveExecutor`
/// that shadows the paper trader on every Decision::Enter. In SHADOW
/// phase (the default in `config/limits.toml`) the live executor signs
/// each tx but never broadcasts. Only the PUBLIC-path trader gets this.
pub async fn start(
    cfg: TraderConfig,
    resolver: Arc<TokenResolver>,
    shutdown: CancellationToken,
    live_enabled: bool,
) -> Option<TraderHandles> {
    if !cfg.enabled {
        tracing::info!(target: "trader", "trader disabled in config; skipping");
        return None;
    }
    if cfg.mode != "paper" {
        tracing::warn!(
            target: "trader",
            mode = %cfg.mode,
            "only 'paper' mode is implemented; refusing to start"
        );
        return None;
    }

    let ledger_dir: PathBuf = cfg.ledger_dir.clone();
    let ledger = match Ledger::new(&ledger_dir) {
        Ok(l) => Arc::new(l),
        Err(e) => {
            tracing::error!(target: "trader", error = %e, "ledger init failed");
            return None;
        }
    };

    let telegram = if cfg.telegram.enabled
        && !cfg.telegram.bot_token.is_empty()
        && cfg.telegram.chat_id != 0
    {
        Some(Arc::new(TelegramAlerter::new(
            cfg.telegram.bot_token.clone(),
            cfg.telegram.chat_id,
        )))
    } else {
        if cfg.telegram.enabled {
            tracing::warn!(target: "trader",
                "trader telegram enabled but bot_token/chat_id missing; alerts off"
            );
        }
        None
    };

    let held = crate::held_tokens::HeldTokens::new();
    let bnb_price = Arc::new(crate::bnb_price::BnbPrice::new(cfg.rpc_url.clone()));
    let costs = Costs {
        slip_v2_bps: cfg.slippage_bps_v2,
        slip_bonding_bps: cfg.slippage_bps_bonding,
        gas_per_trade_usd: cfg.gas_per_trade_usd,
    };
    // Closed-loop per-KOL budget book (recommended). Empty / 0 budget
    // disables; positions fall back to strategy-supplied bnb_amount.
    let budget_wei: u128 = cfg
        .per_kol_budget_bnb_wei
        .parse::<u128>()
        .unwrap_or(0);
    let kol_budgets = if budget_wei > 0 {
        let dust: u128 = cfg
            .dust_floor_wei
            .parse::<u128>()
            .unwrap_or(1_000_000_000_000_000);
        let path = ledger_dir.join("kol_budgets.json");
        let book = kol_budget::KolBudgetBook::load_or_new(
            path.clone(),
            budget_wei,
            cfg.position_pct_bps,
            dust,
        );
        tracing::info!(
            target: "trader",
            initial_bnb_wei = budget_wei,
            position_pct_bps = cfg.position_pct_bps,
            dust_floor_wei = dust,
            book_path = %path.display(),
            "per-KOL budget enabled (closed-loop)"
        );
        Some(book)
    } else {
        None
    };
    let executor = Arc::new(PaperExecutor::new(
        cfg.rpc_url.clone(),
        resolver,
        ledger,
        telegram,
        held.clone(),
        bnb_price.clone(),
        cfg.archive_rpc_url.clone(),
        costs,
        kol_budgets,
    ));

    // Empty filter = ALL watched KOLs (the set is already bounded by
    // kols.toml — only the 15 GOAT wallets are ever delivered here).
    // min_buy_usd is taken verbatim from config — `0.0` is "disabled",
    // not "use legacy default". Strategy already treats 0 as no-gate.
    let kol_filter = cfg.kol_filter.clone();
    let min_buy_usd = cfg.min_buy_usd;
    let strategy = Arc::new(Strategy::new(StrategyConfig {
        kol_filter: kol_filter.clone(),
        min_buy_usd,
        min_buy_bnb: 0.0,
        size_fraction: cfg.size_fraction,
    }));
    // bnb_price already constructed above (shared with the executor).

    let (hit_tx, hit_rx) = mpsc::channel::<KolHit>(256);

    // LiveExecutor — only attach to the PUBLIC trader, only when caller
    // opts in. Failure to load any piece (limits.toml / wallet / nonce)
    // leaves the paper trader unaffected.
    let live: Option<Arc<LiveExecutor>> = if live_enabled {
        build_live_executor(&cfg.rpc_url).await
    } else {
        None
    };

    // Consumer task.
    {
        let executor = executor.clone();
        let strategy = strategy.clone();
        let bnb_price = bnb_price.clone();
        let live = live.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_consumer(strategy, executor, bnb_price, live, hit_rx, shutdown).await;
        });
    }

    // Sweeper task.
    {
        let executor = executor.clone();
        let hold = Duration::from_secs(cfg.hold_timeout_secs);
        let interval = Duration::from_secs(cfg.sweep_interval_secs.max(60));
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_sweeper(executor, hold, interval, shutdown).await;
        });
    }

    // Daily summary task.
    if cfg.daily_summary_utc_hour < 24 {
        let executor = executor.clone();
        let hour = cfg.daily_summary_utc_hour;
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_daily_summary(executor, hour, shutdown).await;
        });
    }

    tracing::info!(
        target: "trader",
        mode = %cfg.mode,
        kol_filter = ?kol_filter,
        min_buy_usd,
        size_fraction = cfg.size_fraction,
        ledger_dir = %ledger_dir.display(),
        hold_timeout_secs = cfg.hold_timeout_secs,
        daily_summary_utc_hour = cfg.daily_summary_utc_hour,
        slip_v2_bps = costs.slip_v2_bps,
        slip_bonding_bps = costs.slip_bonding_bps,
        gas_per_trade_usd = costs.gas_per_trade_usd,
        "BSC paper trader up — scoped copy (D/$400) + exit-follow"
    );

    Some(TraderHandles { hit_tx, held })
}

async fn run_consumer(
    strategy: Arc<Strategy>,
    executor: Arc<PaperExecutor>,
    bnb_price: Arc<crate::bnb_price::BnbPrice>,
    live: Option<Arc<LiveExecutor>>,
    mut rx: mpsc::Receiver<KolHit>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::info!(target: "trader", "consumer shutdown");
                return;
            }
            maybe = rx.recv() => {
                let Some(hit) = maybe else { return };
                // BNB/USD for the USD entry gate. Fail closed: if price is
                // unavailable we pass 0.0 so BUYs can't pass min_buy_usd
                // (SELL/exit path doesn't use price, so exits still work).
                let bnb_usd = bnb_price.get().await.unwrap_or(0.0);
                let decision = strategy.evaluate(&hit, bnb_usd);
                if matches!(decision, Decision::Skip { .. }) {
                    continue;
                }
                // PAPER side first (always).
                let paper_decision = decision.clone();
                if let Err(e) = executor.execute(decision, &hit.calldata).await {
                    tracing::warn!(target: "trader", error = %e, "execute failed");
                }
                // LIVE shadow side — only when attached AND it's a public-
                // visibility entry (this fn runs in the public-path trader).
                if let Some(le) = &live {
                    let n_open = executor.book.lock().await.len() as u32;
                    if let Err(e) = le.execute(paper_decision, "public", n_open).await {
                        tracing::warn!(target: "trader_live", error = %e,
                            "live executor error");
                    }
                }
            }
        }
    }
}

/// Build a `LiveExecutor` from `config/limits.toml` + wallet env vars.
/// Returns None if anything's missing or the wallet check fails — paper
/// trader continues unaffected.
async fn build_live_executor(rpc_url: &str) -> Option<Arc<LiveExecutor>> {
    use std::path::Path;
    let limits_path = Path::new("config/limits.toml");
    let cfg = match limits::load_config(limits_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "trader_live", error = %e,
                "no live config; LiveExecutor disabled");
            return None;
        }
    };
    let wallet = match TraderWallet::from_env("WALLET_PRIVATE_KEY", "WALLET_ADDRESS") {
        Ok(w) => Arc::new(w),
        Err(e) => {
            tracing::error!(target: "trader_live", error = %e,
                "wallet load failed; LiveExecutor disabled");
            return None;
        }
    };
    // Persist next-nonce to disk on every reserve() so a restart
    // doesn't reuse nonces invisible to local geth's pending pool (txs
    // submitted only via BR/Puissant land in their relay's pool, not
    // ours). Survives restarts cleanly.
    let nonce_persist_path = std::path::Path::new(&cfg.audit.log_dir)
        .join("wallet_nonce");
    let nonce = match NonceManager::new(
        rpc_url.to_string(),
        wallet.address(),
        Some(nonce_persist_path),
    ).await {
        Ok(n) => Arc::new(n),
        Err(e) => {
            tracing::error!(target: "trader_live", error = %e,
                "nonce sync failed; LiveExecutor disabled");
            return None;
        }
    };
    let ledger_dir = std::path::Path::new(&cfg.audit.log_dir).to_path_buf();
    let ledger = match live_ledger::LiveLedger::new(&ledger_dir) {
        Ok(l) => Arc::new(l),
        Err(e) => {
            tracing::error!(target: "trader_live", error = %e,
                "live ledger init failed; LiveExecutor disabled");
            return None;
        }
    };
    let auth = std::env::var(&cfg.submission.primary_auth_env).unwrap_or_default();
    let bnb_price = Arc::new(crate::bnb_price::BnbPrice::new(rpc_url.to_string()));
    // ── Dev resolver REMOVED 2026-05-29 ─────────────────────────────────
    // Bench results showed eth_getLogs against the launchpad on NodeReal
    // takes p50 ≈ 200ms (local geth can't index that range; times out at
    // 15s+). With a 150ms sync timeout, the resolver was usually returning
    // `Unknown` → defaulting to $10 sizing anyway, while still costing
    // ~150ms on the hot path. Net EV negative vs the rare $10 bonus hit.
    //
    // Module + config file (`config/dev_whitelist.toml`) are kept on disk
    // so we can re-wire if we move resolution off-hot-path later (e.g.
    // pre-warm cache from block scanner).
    let dev_resolver = {
        tracing::info!(
            target: "trader_live",
            "dev resolver DISABLED — every trade sized at trade_size_usd; bonus path off"
        );
        None
    };
    // Hot-loadable blacklist (TOML). Missing file is non-fatal — falls back
    // to the const list baked into blacklist.rs.
    let blacklist_path = std::path::Path::new(&cfg.tokens.blacklist_file);
    let blacklist = match blacklist::BlacklistRuntime::load(blacklist_path) {
        Ok(b) => Some(b),
        Err(e) => {
            tracing::warn!(target: "trader_live", error = %e,
                "blacklist load failed; using hardcoded fallback");
            None
        }
    };
    let exec = match LiveExecutor::new(
        wallet.clone(),
        nonce.clone(),
        Arc::new(LimitsRuntime::new(cfg.clone())),
        ledger.clone(),
        rpc_url.to_string(),
        cfg.submission.primary_url.clone(),
        auth,
        bnb_price,
        dev_resolver,
        blacklist,
    ) {
        Ok(e) => Arc::new(e),
        Err(e) => {
            tracing::error!(target: "trader_live", error = %e,
                "LiveExecutor build failed");
            return None;
        }
    };
    tracing::info!(
        target: "trader_live",
        address = %format!("{:#x}", wallet.address()),
        shadow = cfg.phase.shadow,
        tiny = cfg.phase.tiny,
        full = cfg.phase.full,
        whitelist = ?cfg.strategy.kol_whitelist,
        per_trade_max_bnb = cfg.limits.per_trade_max_bnb,
        daily_loss_usd = cfg.limits.daily_loss_usd,
        ledger_dir = %ledger_dir.display(),
        "LiveExecutor attached"
    );
    Some(exec)
}

/// Standalone live consumer — runs the strategy + LiveExecutor on its
/// own KolHit channel. Use when the paper trader is disabled.
///
/// Returns `Some(KolHit sender)` to plug into `Sinks::live`, or `None`
/// if any piece of the live setup failed.
pub async fn start_live_only(
    trader_cfg: &TraderConfig,
    trail_cfg: crate::trader::adaptive_trail::TrailConfig,
    price_cache: crate::four_meme_price::FourMemePriceCache,
    price_cache_stats: crate::four_meme_price::FourMemeStatsCache,
    shutdown: CancellationToken,
) -> Option<mpsc::Sender<KolHit>> {
    let trail_active = trail_cfg.enabled;
    let exec = build_live_executor(&trader_cfg.rpc_url).await?;

    // Adaptive trail watcher (price-driven exits). Started here so it has
    // direct access to the LiveExecutor for `position_snapshot` + `execute_exit`.
    if trail_active {
        let ledger_path = std::path::Path::new("/data/bsc-meme-mev/trader_live/live_log.csv")
            .to_path_buf();
        adaptive_trail::start(
            trail_cfg,
            exec.clone(),
            ledger_path,
            price_cache,
            price_cache_stats,
            shutdown.clone(),
        );
    }
    // Strategy uses limits.toml as the live source of truth (whitelist +
    // gates + size). trader_cfg supplies only rpc_url here.
    let sc = exec.limits().config().strategy.clone();
    let strategy = Arc::new(Strategy::new(StrategyConfig {
        kol_filter: sc.kol_whitelist,
        min_buy_usd: sc.min_buy_usd,
        min_buy_bnb: sc.min_buy_bnb,
        size_fraction: sc.size_fraction,
    }));
    tracing::info!(
        target: "trader_live",
        min_buy_bnb = sc.min_buy_bnb,
        min_buy_usd = sc.min_buy_usd,
        size_fraction = sc.size_fraction,
        "strategy gates configured"
    );
    let bnb_price = Arc::new(crate::bnb_price::BnbPrice::new(trader_cfg.rpc_url.clone()));
    // 1024 capacity (was 256). Under burst KOL activity (D + I + retry sweep
    // all firing during a hot meme run), 256 could overflow → `try_send` from
    // kol_confirm would drop with the `forward_or_log` warning. 1024 is ~5s
    // of worst-case backpressure at 200 hits/s.
    let (tx, mut rx) = mpsc::channel::<KolHit>(1024);

    // ── Daily-loss kill switch (wallet-baseline polling) ─────────────────
    // Snapshot wallet BNB at startup. Every 30s, read current balance + BNB/USD,
    // compute (baseline - current) × bnb_usd = realized_loss_usd, push into
    // LimitsRuntime. When loss crosses `daily_loss_usd`, LimitsRuntime.check()
    // returns DailyLossHalt → next trade is rejected. Manual restart resets.
    //
    // This is wallet-cash-based, not per-trade-PnL-based. Captures slippage,
    // gas, sandwich losses, EVERYTHING that drains the wallet. Doesn't track
    // unrealized PnL of open positions (intentional — kill switch is for
    // cash drain, not mark-to-market drawdown).
    {
        let exec = exec.clone();
        let bnb_price = bnb_price.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            // Wait briefly for the wallet read to be reliable post-startup.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let initial = match exec.wallet_balance_bnb_public().await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(target: "trader_live", error = %e,
                        "wallet baseline read failed; daily-loss kill switch DEGRADED");
                    return;
                }
            };
            fn utc_day_now() -> u64 {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() / 86_400)
                    .unwrap_or(0)
            }
            let mut baseline = initial;
            let mut baseline_day = utc_day_now();
            tracing::info!(
                target: "trader_live",
                baseline_bnb = baseline,
                baseline_utc_day = baseline_day,
                "daily-loss kill switch armed"
            );
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // skip immediate fire
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    _ = tick.tick() => {
                        let now_bnb = exec.wallet_balance_bnb_public().await.unwrap_or(baseline);
                        let bnb_usd = bnb_price.get().await.unwrap_or(0.0);
                        if bnb_usd <= 0.0 { continue; }
                        // UTC midnight rollover: re-snapshot baseline + reset
                        // the limits-runtime day counter so today's loss starts
                        // at zero (and the halt clears for the new day).
                        let today = utc_day_now();
                        if today != baseline_day {
                            tracing::info!(
                                target: "trader_live",
                                prev_baseline_bnb = baseline,
                                new_baseline_bnb = now_bnb,
                                prev_utc_day = baseline_day,
                                new_utc_day = today,
                                "kill switch: UTC midnight rollover — resetting baseline"
                            );
                            baseline = now_bnb;
                            baseline_day = today;
                            exec.limits_runtime().sync_realized_loss(0.0);
                            continue;
                        }
                        let loss_usd = ((baseline - now_bnb).max(0.0)) * bnb_usd;
                        exec.limits_runtime().sync_realized_loss(loss_usd);
                    }
                }
            }
        });
    }

    // ── Retry sweep REMOVED 2026-05-29 ──────────────────────────────────
    // Previous implementation indiscriminately called execute_exit on any
    // position older than 5 min, causing premature exits while KOL D was
    // still holding/scaling out (real money loss on token
    // 0xb15c87115ad5ba15add8a0c089fe1fdd38fb4444 — held 5.5 min, exit
    // fired by the sweep; D didn't actually sell until 89 min later, at
    // 4× higher mcap). Stuck positions are now handled exclusively by:
    //   - KOL exit-follow on `Decision::Exit` (kol_confirm + kol_watch)
    //   - manual `scripts/sweep-dust.py` for "GW" graduation-window cases

    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::info!(target: "trader_live", "consumer shutdown");
                    return;
                }
                maybe = rx.recv() => {
                    let Some(hit) = maybe else { return };
                    let bnb_usd = bnb_price.get().await.unwrap_or(0.0);
                    let decision = strategy.evaluate(&hit, bnb_usd);
                    match decision {
                        Decision::Skip { reason } => {
                            tracing::info!(
                                target: "trader_live",
                                kol = %hit.kol_name,
                                side = hit.side.unwrap_or("-"),
                                value_bnb = hit.value_bnb,
                                value_usd = hit.value_bnb * bnb_usd,
                                token = %hit.token.map(|t| format!("{t:#x}"))
                                    .unwrap_or_else(|| "-".into()),
                                reason = ?reason,
                                "skip: strategy-gate"
                            );
                            metrics::counter!(
                                "bsc_trader_live_strategy_skipped_total",
                                "reason" => format!("{:?}", reason),
                            ).increment(1);
                        }
                        Decision::Exit { kol_name, token, kol_block, kol_addr, .. } => {
                            // When the adaptive-trail strategy is active we
                            // IGNORE KOL sell signals — exits are price-driven
                            // by the trail watcher, not by D's behaviour.
                            // Toggle via `[adaptive_trail] enabled = true`.
                            if trail_active {
                                tracing::debug!(
                                    target: "trader_live",
                                    kol = %kol_name,
                                    token = %format!("{token:#x}"),
                                    "skip exit: trail strategy active (KOL-sell signals ignored)"
                                );
                                continue;
                            }
                            // KOL sold this token; we sell PROPORTIONALLY (the
                            // same fraction KOL just sold), not 100%. The
                            // executor handles short-circuit on zero balance.
                            if let Err(e) = exec.execute_exit(token, &kol_name, kol_block, kol_addr).await {
                                tracing::warn!(target: "trader_live", error = %e,
                                    "exit failed");
                            }
                        }
                        Decision::Enter { .. } => {
                            // Visibility on entry decides position size:
                            //   public  → trade_size_usd
                            //   private → trade_size_private_usd
                            // Sourced from the KolHit so kol_confirm and
                            // kol_watch can populate it correctly.
                            let n_open = exec.open_positions();
                            if let Err(e) = exec.execute(decision, hit.visibility, n_open).await {
                                tracing::warn!(target: "trader_live", error = %e,
                                    "live exec failed");
                            }
                        }
                    }
                }
            }
        }
    });
    Some(tx)
}

async fn run_sweeper(
    executor: Arc<PaperExecutor>,
    hold_timeout: Duration,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // First tick fires immediately; skip it to avoid a spurious sweep at startup.
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {
                executor.sweep_timeouts(hold_timeout).await;
            }
        }
    }
}

async fn run_daily_summary(
    executor: Arc<PaperExecutor>,
    utc_hour: u8,
    shutdown: CancellationToken,
) {
    loop {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let seconds_today = now % 86400;
        let target = u64::from(utc_hour).saturating_mul(3600);
        let wait_secs = if seconds_today < target {
            target - seconds_today
        } else {
            86400 - seconds_today + target
        };
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(Duration::from_secs(wait_secs)) => {}
        }

        let book_snapshot = {
            let book = executor.book.lock().await;
            book.snapshot()
        };
        if let Some(tg) = &executor.telegram {
            let body = format_daily_summary(&book_snapshot);
            tg.send(body).await;
        }
        tracing::info!(target: "trader", open_positions = book_snapshot.len(), "daily summary fired");
    }
}

fn format_daily_summary(open: &[OpenPosition]) -> String {
    let mut lines = String::new();
    lines.push_str("📊 <b>TRADER DAILY SUMMARY</b>\n\n");
    if open.is_empty() {
        lines.push_str("(no open positions)\n");
    } else {
        lines.push_str(&format!("📂 {} open positions:\n", open.len()));
        for p in open.iter().take(10) {
            lines.push_str(&format!(
                "  • <b>{sym}</b> ({port}) — in {bnb}\n",
                sym = p.token_symbol.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;"),
                port = p.portfolio.label(),
                bnb = format_bnb_short(p.bnb_in),
            ));
        }
        if open.len() > 10 {
            lines.push_str(&format!("  …and {} more\n", open.len() - 10));
        }
    }
    lines
}

fn format_bnb_short(wei: U256) -> String {
    let n: f64 = wei.to_string().parse().unwrap_or(0.0);
    let bnb = n / 1e18;
    format!("{bnb:.3} BNB")
}

fn parse_u256_dec(s: &str) -> Option<U256> {
    s.parse::<U256>().ok()
}

#[cfg(test)]
mod orchestrator_tests {
    use super::*;

    #[test]
    fn parse_u256_works() {
        assert_eq!(
            parse_u256_dec("500000000000000000"),
            Some(U256::from(500_000_000_000_000_000u128))
        );
        assert_eq!(parse_u256_dec("not a number"), None);
    }

    #[test]
    fn format_daily_summary_empty() {
        let s = format_daily_summary(&[]);
        assert!(s.contains("no open positions"));
    }
}
