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
    #[serde(default)]
    pub kol_watch: KolWatchConfig,
    #[serde(default)]
    pub liquidator: LiquidatorConfig,
    #[serde(default)]
    pub four_meme: FourMemeConfig,
}

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

#[derive(Debug, Clone, Deserialize)]
pub struct TraderConfig {
    pub enabled: bool,
    pub mode: String,
    pub min_buy_bnb_wei: String,
    pub size_fraction: f64,
    pub rpc_url: String,
    pub ledger_dir: PathBuf,
    pub hold_timeout_secs: u64,
    pub sweep_interval_secs: u64,
    pub daily_summary_utc_hour: u8,
    #[serde(default)]
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub dex: TraderDexConfig,
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
