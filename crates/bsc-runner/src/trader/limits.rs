//! Live-trader kill criteria — every decision passes through `check()`
//! before it can become a signed tx. The config file is read once at
//! startup; runtime counters live in-memory and reset at 00:00 UTC.
//!
//! Failure modes (return value of `check`):
//!   `Ok(())`              — trade may proceed
//!   `Err(LimitFail::...)` — trade is skipped (or system halts if Fatal)

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

// ── config ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    pub phase: PhaseConfig,
    pub limits: HardLimits,
    pub strategy: StrategyConfig,
    pub wallet: WalletConfig,
    pub submission: SubmissionConfig,
    pub tokens: TokenPolicy,
    pub audit: AuditConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PhaseConfig {
    pub shadow: bool,
    pub tiny: bool,
    pub full: bool,
}

impl PhaseConfig {
    /// True if signed txs should actually hit the wire. False = shadow.
    pub fn broadcast_enabled(&self) -> bool {
        self.tiny || self.full
    }

    /// Per-trade cap honoring tiny-mode override.
    pub fn effective_per_trade_max_bnb(&self, full_cap: f64) -> f64 {
        if self.tiny && !self.full {
            // Tiny mode hard-caps at 0.001 BNB regardless of full_cap.
            full_cap.min(0.001)
        } else {
            full_cap
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HardLimits {
    pub daily_loss_usd: f64,
    pub per_trade_max_bnb: f64,
    pub min_wallet_bnb: f64,
    pub max_open_positions: u32,
    pub max_trades_per_day: u32,
    pub max_gas_price_gwei: u64,
    pub slippage_bps: u32,
    pub cooldown_after_loss_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // `mode` is informational; only one mode shipped
pub struct StrategyConfig {
    pub mode: String,
    pub public_only: bool,
    pub size_fraction: f64,
    pub kol_whitelist: Vec<String>,
    /// Minimum KOL BUY size in BNB. 0.0 = disabled (USD gate only).
    #[serde(default)]
    pub min_buy_bnb: f64,
    /// Minimum KOL BUY size in USD. 0.0 = disabled (BNB gate only).
    /// Both gates apply when set; either alone is enough.
    #[serde(default)]
    pub min_buy_usd: f64,
    /// Default position size in USD for PUBLIC-channel entries (KOL tx
    /// observed in our public mempool, where we have full latency window
    /// to potentially same-block-land). 0.0 disables USD-sizing and falls
    /// back to the legacy `size_fraction × kol_buy` path.
    #[serde(default)]
    pub trade_size_usd: f64,
    /// Position size in USD for PRIVATE-channel entries (KOL tx observed
    /// only post-confirmation — direct-to-builder / private relay). Always
    /// land at N+1+ with worse fill than the KOL. Typically half of
    /// `trade_size_usd` so we ride D's wave with reduced capital where the
    /// edge is smaller. 0.0 ⇒ disable PRIVATE entries entirely.
    #[serde(default)]
    pub trade_size_private_usd: f64,
    /// Bonus position size in USD when the token's dev IS on the trust
    /// list. Ignored if `trade_size_usd == 0`. (Dev resolver removed on
    /// the hot path 2026-05-29; field retained for re-enable.)
    #[serde(default)]
    pub dev_bonus_size_usd: f64,
    /// Path (relative to repo root) to the dev trust list.
    #[serde(default)]
    pub dev_whitelist_file: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // keyfile reserved for filesystem-key path; we use env vars today
pub struct WalletConfig {
    pub keyfile: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // fallback_url reserved for BlockRazor-down failover (Phase B)
pub struct SubmissionConfig {
    pub primary_url: String,
    pub primary_auth_env: String,
    pub fallback_url: String,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // token-policy gates land in Phase B (honeypot detection)
pub struct TokenPolicy {
    pub min_age_seconds: u64,
    pub max_sell_tax_bps: u32,
    pub blacklist_file: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // telegram_alerts wired in Phase B
pub struct AuditConfig {
    pub log_dir: String,
    pub telegram_alerts: bool,
}

pub fn load_config(path: &Path) -> Result<LimitsConfig> {
    let s = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let c: LimitsConfig = toml::from_str(&s)
        .with_context(|| format!("parse {}", path.display()))?;
    // Sanity: exactly one phase active.
    let active = [c.phase.shadow, c.phase.tiny, c.phase.full]
        .iter()
        .filter(|b| **b)
        .count();
    if active != 1 {
        anyhow::bail!(
            "config/limits.toml: exactly one of phase.{{shadow,tiny,full}} must be true (got {active})"
        );
    }
    Ok(c)
}

// ── runtime state ──────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct DailyState {
    /// UTC day (days since epoch) this state applies to. Reset on rollover.
    day: u64,
    realized_loss_usd: f64,
    trades_today: u32,
    last_loss_unix_ms: u64,
    halted: bool,
}

pub struct LimitsRuntime {
    cfg: LimitsConfig,
    whitelist: HashSet<String>,
    state: Mutex<DailyState>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LimitFail {
    NotWhitelisted(String),
    PrivateVisibilityWhilePublicOnly,
    PerTradeCapExceeded { wanted_bnb: f64, cap_bnb: f64 },
    MaxOpenPositions(u32),
    MaxTradesPerDay(u32),
    DailyLossHalt { loss_usd: f64, cap_usd: f64 },
    WalletBelowMin { balance_bnb: f64, min_bnb: f64 },
    GasTooHigh { gwei: u64, cap: u64 },
    InCooldown { remaining_ms: u64 },
    PhaseSaysShadow, // not a fail per se, but explicit "do not broadcast"
}

impl std::fmt::Display for LimitFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LimitFail::NotWhitelisted(k) => write!(f, "kol {k} not in whitelist"),
            LimitFail::PrivateVisibilityWhilePublicOnly => write!(f, "private visibility, public_only=true"),
            LimitFail::PerTradeCapExceeded { wanted_bnb, cap_bnb } => {
                write!(f, "trade size {wanted_bnb:.4} > cap {cap_bnb:.4} BNB")
            }
            LimitFail::MaxOpenPositions(n) => write!(f, "already {n} open positions"),
            LimitFail::MaxTradesPerDay(n) => write!(f, "hit {n} trades today"),
            LimitFail::DailyLossHalt { loss_usd, cap_usd } => {
                write!(f, "daily loss ${loss_usd:.2} ≥ ${cap_usd:.2} — HALTED")
            }
            LimitFail::WalletBelowMin { balance_bnb, min_bnb } => {
                write!(f, "wallet {balance_bnb:.4} < min {min_bnb:.4} BNB")
            }
            LimitFail::GasTooHigh { gwei, cap } => write!(f, "gas {gwei} > cap {cap} gwei"),
            LimitFail::InCooldown { remaining_ms } => write!(f, "cooldown {remaining_ms}ms left"),
            LimitFail::PhaseSaysShadow => write!(f, "shadow-mode (no broadcast)"),
        }
    }
}

/// Inputs to a single trade-decision check.
pub struct TradeCheck<'a> {
    pub kol_name: &'a str,
    pub visibility: &'a str,        // "public" | "private"
    pub bnb_amount: f64,            // BNB we want to spend
    pub current_open_positions: u32,
    pub current_wallet_bnb: f64,
    pub current_gas_gwei: u64,
}

impl LimitsRuntime {
    pub fn new(cfg: LimitsConfig) -> Self {
        let whitelist = cfg.strategy.kol_whitelist.iter().cloned().collect();
        // Seed the day counter to TODAY so the first `sync_realized_loss` lives
        // in the right bucket (else maybe_reset_day would wipe it).
        let day = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 86_400)
            .unwrap_or(0);
        Self {
            cfg,
            whitelist,
            state: Mutex::new(DailyState { day, ..DailyState::default() }),
        }
    }

    pub fn config(&self) -> &LimitsConfig {
        &self.cfg
    }

    pub fn broadcast_enabled(&self) -> bool {
        self.cfg.phase.broadcast_enabled()
    }

    fn maybe_reset_day(&self, now_ms: u64) {
        let day = now_ms / (86_400 * 1_000);
        let mut s = self.state.lock();
        if s.day != day {
            *s = DailyState { day, ..DailyState::default() };
        }
    }

    /// Run every gate. Returns the FIRST failure if any, else Ok(()).
    pub fn check(&self, t: &TradeCheck<'_>) -> std::result::Result<(), LimitFail> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.maybe_reset_day(now_ms);

        let s = self.state.lock();
        if s.halted {
            return Err(LimitFail::DailyLossHalt {
                loss_usd: s.realized_loss_usd,
                cap_usd: self.cfg.limits.daily_loss_usd,
            });
        }
        if let Some(remaining) =
            cooldown_remaining(s.last_loss_unix_ms, self.cfg.limits.cooldown_after_loss_ms, now_ms)
        {
            return Err(LimitFail::InCooldown { remaining_ms: remaining });
        }
        let trades_today = s.trades_today;
        drop(s);

        if !self.whitelist.contains(t.kol_name) {
            return Err(LimitFail::NotWhitelisted(t.kol_name.to_string()));
        }
        if self.cfg.strategy.public_only && t.visibility != "public" {
            return Err(LimitFail::PrivateVisibilityWhilePublicOnly);
        }
        let cap = self
            .cfg
            .phase
            .effective_per_trade_max_bnb(self.cfg.limits.per_trade_max_bnb);
        if t.bnb_amount > cap {
            return Err(LimitFail::PerTradeCapExceeded {
                wanted_bnb: t.bnb_amount,
                cap_bnb: cap,
            });
        }
        if t.current_open_positions >= self.cfg.limits.max_open_positions {
            return Err(LimitFail::MaxOpenPositions(self.cfg.limits.max_open_positions));
        }
        if trades_today >= self.cfg.limits.max_trades_per_day {
            return Err(LimitFail::MaxTradesPerDay(self.cfg.limits.max_trades_per_day));
        }
        if t.current_wallet_bnb < self.cfg.limits.min_wallet_bnb {
            return Err(LimitFail::WalletBelowMin {
                balance_bnb: t.current_wallet_bnb,
                min_bnb: self.cfg.limits.min_wallet_bnb,
            });
        }
        if t.current_gas_gwei > self.cfg.limits.max_gas_price_gwei {
            return Err(LimitFail::GasTooHigh {
                gwei: t.current_gas_gwei,
                cap: self.cfg.limits.max_gas_price_gwei,
            });
        }
        Ok(())
    }

    /// Called after a trade actually fires. Increments today's counter.
    pub fn record_trade_fired(&self) {
        let mut s = self.state.lock();
        s.trades_today = s.trades_today.saturating_add(1);
    }

    /// SETS the running realized-loss-USD figure from the wallet-baseline
    /// poller (the only producer of this metric in production). Idempotent;
    /// triggers halt if the new value crosses the cap. The previous
    /// per-trade `record_close` adder was removed 2026-05-28 because no
    /// production code path ever called it — the wallet-baseline approach
    /// is the single source of truth and captures gas + slippage + sandwich
    /// losses all-in.
    pub fn sync_realized_loss(&self, loss_usd: f64) {
        let loss_usd = loss_usd.max(0.0);
        let mut s = self.state.lock();
        if (loss_usd - s.realized_loss_usd).abs() < f64::EPSILON {
            return;
        }
        s.realized_loss_usd = loss_usd;
        if !s.halted && s.realized_loss_usd >= self.cfg.limits.daily_loss_usd {
            s.halted = true;
            tracing::error!(
                target: "trader_live",
                loss_usd = s.realized_loss_usd,
                cap_usd  = self.cfg.limits.daily_loss_usd,
                "DAILY LOSS LIMIT REACHED (wallet-baseline check) — HALTING until manual restart"
            );
        }
    }

    pub fn is_halted(&self) -> bool {
        self.state.lock().halted
    }
}

fn cooldown_remaining(last_loss_ms: u64, cooldown_ms: u64, now_ms: u64) -> Option<u64> {
    if last_loss_ms == 0 {
        return None;
    }
    let elapsed = now_ms.saturating_sub(last_loss_ms);
    if elapsed < cooldown_ms {
        Some(cooldown_ms - elapsed)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_cfg() -> LimitsConfig {
        LimitsConfig {
            phase: PhaseConfig { shadow: true, tiny: false, full: false },
            limits: HardLimits {
                daily_loss_usd: 50.0,
                per_trade_max_bnb: 0.005,
                min_wallet_bnb: 0.05,
                max_open_positions: 10,
                max_trades_per_day: 30,
                max_gas_price_gwei: 10,
                slippage_bps: 500,
                cooldown_after_loss_ms: 5000,
            },
            strategy: StrategyConfig {
                mode: "copy_at_n_plus_1".into(),
                public_only: true,
                size_fraction: 0.01,
                kol_whitelist: vec!["O".into(), "M".into()],
                min_buy_bnb: 0.5,
                min_buy_usd: 0.0,
                trade_size_usd: 0.0,
                trade_size_private_usd: 0.0,
                dev_bonus_size_usd: 0.0,
                dev_whitelist_file: String::new(),
            },
            wallet: WalletConfig { keyfile: "/dev/null".into() },
            submission: SubmissionConfig {
                primary_url: "https://bsc.blockrazor.xyz".into(),
                primary_auth_env: "BLOCKRAZOR_AUTH_KEY".into(),
                fallback_url: "http://127.0.0.1:8545".into(),
                timeout_ms: 5000,
            },
            tokens: TokenPolicy {
                min_age_seconds: 60,
                max_sell_tax_bps: 500,
                blacklist_file: "/dev/null".into(),
            },
            audit: AuditConfig {
                log_dir: "/tmp/test".into(),
                telegram_alerts: false,
            },
        }
    }

    #[test]
    fn whitelist_excludes_unknown_kol() {
        let r = LimitsRuntime::new(fake_cfg());
        let t = TradeCheck {
            kol_name: "X", visibility: "public", bnb_amount: 0.001,
            current_open_positions: 0, current_wallet_bnb: 0.2, current_gas_gwei: 3,
        };
        match r.check(&t) {
            Err(LimitFail::NotWhitelisted(s)) => assert_eq!(s, "X"),
            x => panic!("expected NotWhitelisted, got {x:?}"),
        }
    }

    #[test]
    fn public_only_rejects_private() {
        let r = LimitsRuntime::new(fake_cfg());
        let t = TradeCheck {
            kol_name: "O", visibility: "private", bnb_amount: 0.001,
            current_open_positions: 0, current_wallet_bnb: 0.2, current_gas_gwei: 3,
        };
        assert!(matches!(r.check(&t), Err(LimitFail::PrivateVisibilityWhilePublicOnly)));
    }

    #[test]
    fn per_trade_cap_enforced() {
        let r = LimitsRuntime::new(fake_cfg());
        let t = TradeCheck {
            kol_name: "O", visibility: "public", bnb_amount: 0.999,
            current_open_positions: 0, current_wallet_bnb: 0.2, current_gas_gwei: 3,
        };
        match r.check(&t) {
            Err(LimitFail::PerTradeCapExceeded { wanted_bnb, cap_bnb }) => {
                assert!((wanted_bnb - 0.999).abs() < 1e-9);
                assert!((cap_bnb - 0.005).abs() < 1e-9);
            }
            x => panic!("expected PerTradeCapExceeded, got {x:?}"),
        }
    }

    #[test]
    fn happy_path_passes() {
        let r = LimitsRuntime::new(fake_cfg());
        let t = TradeCheck {
            kol_name: "O", visibility: "public", bnb_amount: 0.003,
            current_open_positions: 2, current_wallet_bnb: 0.2, current_gas_gwei: 3,
        };
        assert!(r.check(&t).is_ok());
    }

    #[test]
    fn daily_loss_halts_after_threshold() {
        let r = LimitsRuntime::new(fake_cfg());
        // Wallet-baseline poller produces a $51 realized loss
        r.sync_realized_loss(51.0);
        assert!(r.is_halted());
        // Even a perfect trade now fails
        let t = TradeCheck {
            kol_name: "O", visibility: "public", bnb_amount: 0.001,
            current_open_positions: 0, current_wallet_bnb: 0.2, current_gas_gwei: 3,
        };
        assert!(matches!(r.check(&t), Err(LimitFail::DailyLossHalt { .. })));
    }

    #[test]
    fn tiny_mode_caps_trade_size_below_full_cap() {
        let mut c = fake_cfg();
        c.phase = PhaseConfig { shadow: false, tiny: true, full: false };
        let r = LimitsRuntime::new(c);
        let t = TradeCheck {
            kol_name: "O", visibility: "public", bnb_amount: 0.002,
            current_open_positions: 0, current_wallet_bnb: 0.2, current_gas_gwei: 3,
        };
        // tiny mode caps at 0.001 — 0.002 should fail
        assert!(matches!(r.check(&t), Err(LimitFail::PerTradeCapExceeded { .. })));
    }
}
