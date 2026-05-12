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

pub mod ledger;
pub mod paper;
pub mod position;
pub mod sim;
pub mod strategy;
pub mod types;

pub use ledger::Ledger;
pub use paper::{PaperExecutor, TelegramAlerter};
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
}

/// Spawn the trader's three background tasks. Returns `None` if disabled.
pub fn start(
    cfg: TraderConfig,
    resolver: Arc<TokenResolver>,
    shutdown: CancellationToken,
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

    let executor = Arc::new(PaperExecutor::new(
        cfg.rpc_url.clone(),
        resolver,
        ledger,
        telegram,
    ));

    let min_buy_bnb_wei = parse_u256_dec(&cfg.min_buy_bnb_wei)
        .unwrap_or(U256::from(500_000_000_000_000_000u128));
    let strategy = Arc::new(Strategy::new(StrategyConfig {
        min_buy_bnb_wei,
        size_fraction: cfg.size_fraction,
    }));

    let (hit_tx, hit_rx) = mpsc::channel::<KolHit>(256);

    // Consumer task.
    {
        let executor = executor.clone();
        let strategy = strategy.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_consumer(strategy, executor, hit_rx, shutdown).await;
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
        min_buy_bnb_wei = %min_buy_bnb_wei,
        size_fraction = cfg.size_fraction,
        ledger_dir = %ledger_dir.display(),
        hold_timeout_secs = cfg.hold_timeout_secs,
        daily_summary_utc_hour = cfg.daily_summary_utc_hour,
        "BSC paper trader up — TWO portfolios (FastTip + NormalTip)"
    );

    Some(TraderHandles { hit_tx })
}

async fn run_consumer(
    strategy: Arc<Strategy>,
    executor: Arc<PaperExecutor>,
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
                let decision = strategy.evaluate(&hit);
                if matches!(decision, Decision::Skip { .. }) {
                    continue;
                }
                if let Err(e) = executor.execute(decision, &hit.calldata).await {
                    tracing::warn!(target: "trader", error = %e, "execute failed");
                }
            }
        }
    }
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
