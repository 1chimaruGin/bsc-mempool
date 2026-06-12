//! On-the-fly Four.Meme creator (= "dev") lookup with caching.
//!
//! We need to know each traded token's deployer because the trade-sizing
//! policy doubles the position size when the dev is on a curated trust
//! list (`config/dev_whitelist.toml`).
//!
//! ## Lookup mechanism
//!
//! The Four.Meme launchpad (`0x5c95…0762b`) emits a `TokenCreate` event
//! with topic `0x396d5e90…cad20` whenever a user calls `createToken`.
//! The token address sits in the event DATA (not topics), so we filter
//! launchpad logs by topic in a narrow block window ending at the token's
//! anchor block, find the entry whose data contains our token, then read
//! the creation tx's `from` to get the dev address.
//!
//! ## Latency budget
//!
//! Sync lookup against NodeReal archive: ~80-300ms. Capped at `TIMEOUT_MS`
//! to bound the impact on copy latency. A timeout returns "unknown" and
//! the trade falls back to default size — but a background task continues
//! resolution so the next time we see that token, the cache is populated.

use alloy::primitives::Address;
use anyhow::Result;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub const FOURMEME_LAUNCHPAD: &str = "0x5c952063c7fc8610FFDB798152D69F0B9550762b";
pub const TOKEN_CREATE_TOPIC: &str =
    "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20";
/// Sync-lookup budget. NodeReal usually returns 80-200ms; tail can hit
/// 500ms+. Beyond 150ms our copy lands a block later — adverse fill costs
/// more than the dev bonus would gain, so we default to the smaller size
/// and rely on the background fill for next hit.
pub const SYNC_TIMEOUT: Duration = Duration::from_millis(150);
/// How many blocks back from the anchor to scan. Empirically, KOLs buy
/// 0-200 blocks after token creation; 5000 is generous and still tractable.
pub const ANCHOR_WINDOW_BLOCKS: u64 = 5_000;

#[derive(Debug, Deserialize)]
struct DevsFile {
    devs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevLookup {
    /// Dev resolved AND on the trust list → use bonus size.
    WhitelistedDev,
    /// Dev resolved but NOT on the trust list → default size.
    OtherDev,
    /// Couldn't resolve in budget — default size, background continues.
    Unknown,
}

pub struct DevResolver {
    whitelist: HashSet<Address>,
    /// `None` = no Four.Meme creation found (probably not a 4meme token);
    /// `Some(addr)` = creator resolved.
    cache: Arc<Mutex<HashMap<Address, Option<Address>>>>,
    rpc_url: String,
    archive_rpc_url: String,
    http: reqwest::Client,
}

impl DevResolver {
    pub fn load(
        whitelist_file: &Path,
        rpc_url: String,
        archive_rpc_url: String,
    ) -> Result<Arc<Self>> {
        let s = std::fs::read_to_string(whitelist_file)?;
        let f: DevsFile = toml::from_str(&s)?;
        let whitelist: HashSet<Address> = f
            .devs
            .iter()
            .filter_map(|h| h.parse::<Address>().ok())
            .collect();
        tracing::info!(
            target: "trader_live",
            count = whitelist.len(),
            file = %whitelist_file.display(),
            "dev_resolver: trust list loaded"
        );
        Ok(Arc::new(Self {
            whitelist,
            cache: Arc::new(Mutex::new(HashMap::new())),
            rpc_url,
            archive_rpc_url,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .user_agent("Mozilla/5.0 (X11; Linux x86_64) bsc-meme-mev/0.1")
                .build()?,
        }))
    }

    pub fn whitelist_size(&self) -> usize {
        self.whitelist.len()
    }

    /// Sync lookup with bounded latency. On cache miss, races the resolver
    /// against `SYNC_TIMEOUT`; on timeout, schedules a background completion
    /// and returns `Unknown` so the caller can default the trade size.
    pub async fn lookup(self: &Arc<Self>, token: Address, anchor_block: u64) -> DevLookup {
        // Cache fast-path
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&token) {
                return match entry {
                    Some(dev) if self.whitelist.contains(dev) => DevLookup::WhitelistedDev,
                    Some(_) => DevLookup::OtherDev,
                    None => DevLookup::OtherDev, // confirmed not a Four.Meme token
                };
            }
        }

        // Cache miss — race resolve() with timeout
        let resolver = self.clone();
        let resolve_fut = async move { resolver.resolve(token, anchor_block).await };
        match tokio::time::timeout(SYNC_TIMEOUT, resolve_fut).await {
            Ok(Some(dev)) => {
                self.cache.lock().await.insert(token, Some(dev));
                if self.whitelist.contains(&dev) {
                    DevLookup::WhitelistedDev
                } else {
                    DevLookup::OtherDev
                }
            }
            Ok(None) => {
                self.cache.lock().await.insert(token, None);
                DevLookup::OtherDev
            }
            Err(_) => {
                // Timeout — kick off background completion so next time is fast.
                let resolver = self.clone();
                let token_copy = token;
                let cache = self.cache.clone();
                tokio::spawn(async move {
                    if let Some(dev) = resolver.resolve(token_copy, anchor_block).await {
                        cache.lock().await.insert(token_copy, Some(dev));
                    } else {
                        cache.lock().await.insert(token_copy, None);
                    }
                });
                DevLookup::Unknown
            }
        }
    }

    /// Raw resolution: scan launchpad logs in a 5k-block window ending at
    /// `anchor_block`, find the TokenCreate event whose data contains the
    /// token, fetch its tx, return `from` (the dev).
    async fn resolve(&self, token: Address, anchor_block: u64) -> Option<Address> {
        let tok_short = format!("{:x}", token);
        let from_block = anchor_block.saturating_sub(ANCHOR_WINDOW_BLOCKS);
        let to_block = anchor_block;
        let params = serde_json::json!({
            "address": FOURMEME_LAUNCHPAD,
            "fromBlock": format!("0x{:x}", from_block),
            "toBlock":   format!("0x{:x}", to_block),
            "topics":    [TOKEN_CREATE_TOPIC],
        });
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [params],
            "id": 1,
        });
        let urls = if self.archive_rpc_url.is_empty() {
            vec![self.rpc_url.as_str()]
        } else {
            vec![self.archive_rpc_url.as_str(), self.rpc_url.as_str()]
        };
        for url in urls {
            let v: serde_json::Value = match self.http.post(url).json(&body).send().await {
                Ok(r) => match r.json().await {
                    Ok(j) => j,
                    Err(_) => continue,
                },
                Err(_) => continue,
            };
            let logs = match v.get("result").and_then(|x| x.as_array()) {
                Some(a) => a,
                None => continue,
            };
            for log in logs {
                let data = log
                    .get("data")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_lowercase();
                if data.contains(&tok_short) {
                    let tx_hash = log.get("transactionHash").and_then(|x| x.as_str())?;
                    return self.tx_from(url, tx_hash).await;
                }
            }
        }
        None
    }

    async fn tx_from(&self, url: &str, tx_hash: &str) -> Option<Address> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 1,
        });
        let v: serde_json::Value = self.http.post(url).json(&body).send().await.ok()?
            .json().await.ok()?;
        let from = v.get("result")?.get("from")?.as_str()?;
        from.parse::<Address>().ok()
    }
}
