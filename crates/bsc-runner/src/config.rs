#![allow(dead_code)]  // config/state fields populated via serde / Rust can't see implicit use
//! Config loading. TOML file + env-var overrides via figment.
//!
//! Env override pattern (double underscore separator):
//!   BSC_MEME_MEV_<SECTION>__<KEY>=value
//! e.g. BSC_MEME_MEV_METRICS__LISTEN_ADDR=127.0.0.1:9101

use anyhow::{Context, Result};
use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub chain: ChainConfig,
    pub metrics: MetricsConfig,
    pub capture: CaptureConfig,
    pub pipeline: PipelineConfig,
    pub sources: SourcesConfig,
    pub block_oracle: BlockOracleConfig,
    pub trader: TraderConfig,
    /// Separate, independent paper trader driven by `kol_confirm` instead
    /// of the pending mempool: enters on the KOL's PRIVATE confirmed buys
    /// (invisible pre-block), exit-follows confirmed sells. Own ledger so
    /// public-pending vs private-confirmed PnL compare cleanly.
    #[serde(default)]
    pub trader_private: TraderConfig,
    #[serde(default)]
    pub kol_watch: KolWatchConfig,
    #[serde(default)]
    pub liquidator: LiquidatorConfig,
    #[serde(default)]
    pub four_meme: FourMemeConfig,
    /// Dev launchpad sniper — buys new Four.Meme tokens at creation when
    /// the creator is on a trusted-dev whitelist. Independent from the
    /// KOL copy-trader. See `crates/bsc-runner/src/dev_sniper.rs`.
    #[serde(default)]
    pub dev_sniper: DevSniperConfig,
    /// Adaptive trailing-stop exit strategy. When enabled, replaces KOL-
    /// driven exits (KOL sell signals ignored); positions are exited
    /// based on price action vs entry+peak. See
    /// `crates/bsc-runner/src/trader/adaptive_trail.rs`.
    #[serde(default)]
    pub adaptive_trail: crate::trader::adaptive_trail::TrailConfig,
    /// SAME trail state machine, separately tuned for the dev-sniper
    /// path (looser arm at +10%, wider trail at -30%, same -30% SL,
    /// same 4000-block timeout). Backtest beat the D-Kelly default by
    /// +80% ($13.29 → $23.97/day).
    #[serde(default)]
    pub sniper_trail: crate::trader::adaptive_trail::TrailConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DevSniperConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_size_usd")]
    pub trade_size_usd: f64,
    /// If set (non-empty / non-"0"), overrides `trade_size_usd` with an
    /// absolute BNB amount in wei. Used to lock the snipe at a backtest-
    /// validated size (e.g. 0.0271 BNB) independent of BNB/USD swings.
    #[serde(default)]
    pub trade_size_bnb_wei: String,
    #[serde(default)]
    pub dev_whitelist_file: String,
    #[serde(default)]
    pub ws_url: String,
    /// HTTP RPC for trail price queries (V2 quote + Four.Meme observed).
    /// Falls back to ws_url's host:port if empty.
    #[serde(default)]
    pub rpc_url: String,
    #[serde(default = "default_profit_pct")]
    pub profit_take_pct: f64,
    #[serde(default = "default_stop_pct")]
    pub stop_loss_pct: f64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_eval_secs")]
    pub eval_interval_secs: u64,
}

fn default_mode() -> String { "paper".into() }
fn default_size_usd() -> f64 { 10.0 }
fn default_profit_pct() -> f64 { 50.0 }
fn default_stop_pct() -> f64 { 30.0 }
fn default_timeout_secs() -> u64 { 1800 }
fn default_eval_secs() -> u64 { 30 }

#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    pub id: u64,
    pub name: String,
    pub native_symbol: String,
    pub native_decimals: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    pub listen_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CaptureConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub rotate_secs: u64,
    pub retention_bytes: u64,
    pub max_age_secs: u64,
    pub zstd_level: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub raw_channel_capacity: usize,
    pub decoded_channel_capacity: usize,
    pub broadcast_capacity: usize,
    pub dedupe_capacity: usize,
    pub dedupe_ttl_secs: u64,
    pub decoder_workers: usize,
    pub janitor_interval_secs: u64,
}

impl From<&PipelineConfig> for bsc_bus::PipelineConfig {
    fn from(p: &PipelineConfig) -> Self {
        Self {
            raw_channel_capacity: p.raw_channel_capacity,
            decoded_channel_capacity: p.decoded_channel_capacity,
            broadcast_capacity: p.broadcast_capacity,
            dedupe_capacity: p.dedupe_capacity,
            dedupe_ttl_secs: p.dedupe_ttl_secs,
            decoder_workers: p.decoder_workers,
            janitor_interval_secs: p.janitor_interval_secs,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SourcesConfig {
    #[serde(default)]
    pub wss: Vec<WssEntry>,
    pub ipc: Option<IpcEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WssEntry {
    pub name: String,
    pub source_id: u8,
    pub url: String,
    pub backfill_concurrency: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IpcEntry {
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockOracleConfig {
    pub el_ws_url: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TraderConfig {
    pub enabled: bool,
    pub mode: String,
    pub min_buy_bnb_wei: String,
    /// Only act on these KOL names (empty = all). Scope: ["D"].
    #[serde(default)]
    pub kol_filter: Vec<String>,
    /// Minimum BUY size in USD to enter (value_bnb * bnb_usd). Scope: 400.
    #[serde(default)]
    pub min_buy_usd: f64,
    pub size_fraction: f64,
    pub rpc_url: String,
    /// Optional archive RPC (NodeReal) — exit-pricing FALLBACK ONLY when
    /// the pruned local node can't serve historical state. Rate-capped.
    /// Set from env (.env NODEREAL_RPC_URL). Empty = disabled.
    #[serde(default)]
    pub archive_rpc_url: String,
    pub ledger_dir: PathBuf,
    pub hold_timeout_secs: u64,
    pub sweep_interval_secs: u64,
    pub daily_summary_utc_hour: u8,
    /// Adverse-slippage haircut for V2/V3 fills, in basis points.
    /// Applied to entry tokens received AND exit BNB received.
    /// Why: we copy at +1 block — D's tx already moved the pool, so the
    /// quote we sim at "latest" is pre-D-impact and optimistic. Default
    /// 150 bps (1.5%) per leg.
    #[serde(default = "default_slip_v2")]
    pub slippage_bps_v2: u32,
    /// Adverse-slippage haircut for bonding-curve (Four.Meme / flap) fills.
    /// Bonding curves price entirely by cumulative buys/sells in the block —
    /// landing after D in N+1 hits a worse curve point. Default 500 bps (5%).
    #[serde(default = "default_slip_bonding")]
    pub slippage_bps_bonding: u32,
    /// Per-tx gas cost in USD, charged twice per round-trip (entry + exit).
    /// BSC ≈ 1-3 gwei × ~150-300k gas ≈ $0.15-0.50. Default $0.30.
    #[serde(default = "default_gas_usd")]
    pub gas_per_trade_usd: f64,
    /// Closed-loop per-KOL paper budget in BNB wei. 0 = budgeting OFF
    /// (legacy behaviour: position size = `size_fraction` × KOL buy).
    /// Recommended: 0.3 BNB (~$200 at $665/BNB). State persisted to
    /// `{ledger_dir}/kol_budgets.json` and survives restarts.
    #[serde(default)]
    pub per_kol_budget_bnb_wei: String,
    /// Position size as a percentage (basis points) of KOL's CURRENT cash.
    /// 1000 = 10%. Compounds with wins / shrinks with losses. Ignored if
    /// `per_kol_budget_bnb_wei = 0`. Default 1000 = 10%.
    #[serde(default = "default_position_pct_bps")]
    pub position_pct_bps: u32,
    /// Position-size floor in BNB wei. Below this the KOL is considered
    /// out of budget. Default 1e15 wei = 0.001 BNB (~$0.66).
    #[serde(default = "default_dust_floor_wei")]
    pub dust_floor_wei: String,
    #[serde(default)]
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub dex: TraderDexConfig,
}

fn default_position_pct_bps() -> u32 {
    1000
}
fn default_dust_floor_wei() -> String {
    "1000000000000000".to_string()
}

fn default_slip_v2() -> u32 {
    150
}
fn default_slip_bonding() -> u32 {
    500
}
fn default_gas_usd() -> f64 {
    0.30
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TraderDexConfig {
    #[serde(default)]
    pub v2_factory: String,
    #[serde(default)]
    pub v2_router: String,
    #[serde(default)]
    pub v3_factory: String,
    #[serde(default)]
    pub v3_router: String,
    #[serde(default)]
    pub v3_quoter_v2: String,
    #[serde(default)]
    pub multicall3: String,
    #[serde(default)]
    pub wbnb: String,
    #[serde(default)]
    pub v3_fee_tiers: Vec<u32>,
    #[serde(default)]
    pub prefer_v2: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TelegramConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bot_token: String,
    #[serde(default)]
    pub chat_id: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct KolWatchConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub file: String,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub telegram: TelegramConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LiquidatorConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub protocol: String,
    #[serde(default)]
    pub rpc_url: String,
    #[serde(default)]
    pub ledger_dir: Option<PathBuf>,
    #[serde(default)]
    pub comptroller: String,
    #[serde(default)]
    pub poll_interval_secs: u64,
    #[serde(default)]
    pub watch_threshold: f64,
    #[serde(default)]
    pub alert_cooldown_secs: u64,
    #[serde(default)]
    pub min_alert_bounty_usd: f64,
    #[serde(default)]
    pub digest_interval_secs: u64,
    #[serde(default)]
    pub digest_top_n: usize,
    #[serde(default)]
    pub telegram: TelegramConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FourMemeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub launchpad: String,
}

/// Load + parse the config. TOML first, then env-var overrides.
pub fn load(path: &Path) -> Result<Config> {
    let figment = Figment::new()
        .merge(Toml::file(path))
        .merge(Env::prefixed("BSC_MEME_MEV_").split("__"));
    let cfg: Config = figment
        .extract()
        .with_context(|| format!("loading config from {}", path.display()))?;
    Ok(cfg)
}
