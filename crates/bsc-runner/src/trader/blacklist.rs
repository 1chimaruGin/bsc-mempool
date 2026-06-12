//! Token blacklist — addresses we will NEVER buy/track regardless of which
//! KOL touches them. Loaded from `config/token_blacklist.toml` at startup.
//!
//! The fallback hardcoded list is the SAFETY NET for tests + early-boot
//! before the runtime list is populated. Production calls go through
//! `BlacklistRuntime` which holds the loaded list.

use alloy::primitives::Address;
use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Hardcoded fallback — used by paper trader (no runtime injection) and as
/// a safety net if the TOML file is missing/empty. Production live trader
/// gets the merged superset (file ∪ this list).
pub const FALLBACK_BLACKLISTED: &[&str] = &[
    // Stables
    "0x55d398326f99059ff775485246999027b3197955",
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
    // Wrappers
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    // Majors
    "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
    "0x000ae314e2a2172a039b26378814c252734f556a",
];

/// Fallback-only check. Used by code paths without access to
/// `BlacklistRuntime` (paper trader, unit tests). Live executor should
/// hold an `Arc<BlacklistRuntime>` instead.
pub fn is_blacklisted(token: Address) -> bool {
    let t = format!("{token:#x}").to_lowercase();
    FALLBACK_BLACKLISTED
        .iter()
        .any(|s| s.eq_ignore_ascii_case(&t))
}

#[derive(Debug, Deserialize)]
struct BlacklistFile {
    addresses: Vec<String>,
}

/// Hot-loadable runtime list. Held behind `Arc<RwLock<HashSet<Address>>>`
/// so a future reload command can swap in a new list under contention.
pub struct BlacklistRuntime {
    inner: Arc<RwLock<HashSet<Address>>>,
}

impl BlacklistRuntime {
    /// Load from `config/token_blacklist.toml` and merge with the hardcoded
    /// fallback. Missing file is non-fatal — falls back to hardcoded only.
    pub fn load(path: &Path) -> Result<Arc<Self>> {
        let mut set: HashSet<Address> = FALLBACK_BLACKLISTED
            .iter()
            .filter_map(|s| s.parse::<Address>().ok())
            .collect();

        match std::fs::read_to_string(path) {
            Ok(s) => {
                let parsed: BlacklistFile = toml::from_str(&s)
                    .with_context(|| format!("parse {}", path.display()))?;
                let mut file_count = 0usize;
                for addr in &parsed.addresses {
                    if let Ok(a) = addr.parse::<Address>() {
                        if set.insert(a) {
                            file_count += 1;
                        }
                    } else {
                        tracing::warn!(
                            target: "trader_live",
                            addr = %addr,
                            "blacklist: bad address (ignored)"
                        );
                    }
                }
                tracing::info!(
                    target: "trader_live",
                    fallback_count = FALLBACK_BLACKLISTED.len(),
                    file_added = file_count,
                    total = set.len(),
                    file = %path.display(),
                    "blacklist loaded"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "trader_live",
                    file = %path.display(),
                    error = %e,
                    fallback_count = set.len(),
                    "blacklist file missing; using fallback only"
                );
            }
        }
        Ok(Arc::new(Self {
            inner: Arc::new(RwLock::new(set)),
        }))
    }

    pub fn is_blacklisted(&self, token: Address) -> bool {
        self.inner.read().contains(&token)
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn usdt_blacklisted_fallback() {
        let t = Address::from_str("0x55d398326f99059ff775485246999027b3197955").unwrap();
        assert!(is_blacklisted(t));
    }

    #[test]
    fn random_meme_not_blacklisted_fallback() {
        let t = Address::from_str("0x65d79e96e7c3495b45b69a4195f6d61eb8cd4444").unwrap();
        assert!(!is_blacklisted(t));
    }

    #[test]
    fn runtime_loads_fallback_when_file_missing() {
        let rt = BlacklistRuntime::load(Path::new("/nonexistent/path.toml")).unwrap();
        assert!(rt.len() >= FALLBACK_BLACKLISTED.len());
        let usdt = Address::from_str("0x55d398326f99059ff775485246999027b3197955").unwrap();
        assert!(rt.is_blacklisted(usdt));
    }
}
