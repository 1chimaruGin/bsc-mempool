//! BEP20/ERC20 metadata resolver with caching (BSC port).
//!
//! Looks up `symbol() / name() / decimals()` for a given token contract via
//! bsc-geth's `eth_call`. Results are cached in-process. Failures are also
//! cached briefly so we don't hammer the node for non-ERC20 contracts that
//! revert on these selectors.

use alloy::primitives::{Address, U256};
use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SELECTOR_SYMBOL: &str = "0x95d89b41";
const SELECTOR_NAME: &str = "0x06fdde03";
const SELECTOR_DECIMALS: &str = "0x313ce567";

#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub address: Address,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
}

impl TokenInfo {
    pub fn format_amount(&self, raw: U256) -> String {
        format_amount(raw, self.decimals)
    }
}

#[derive(Clone)]
enum CacheEntry {
    Ok(Arc<TokenInfo>),
    Failed(Instant),
}

pub struct TokenResolver {
    cache: DashMap<Address, CacheEntry>,
    rpc_url: String,
    client: reqwest::Client,
    failed_ttl: Duration,
}

impl TokenResolver {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            cache: DashMap::with_capacity(1024),
            rpc_url: rpc_url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(4))
                .build()
                .expect("build reqwest client"),
            failed_ttl: Duration::from_secs(120),
        }
    }

    pub async fn lookup(&self, addr: Address) -> Option<Arc<TokenInfo>> {
        if let Some(entry) = self.cache.get(&addr) {
            match entry.value() {
                CacheEntry::Ok(info) => return Some(info.clone()),
                CacheEntry::Failed(at) if at.elapsed() < self.failed_ttl => return None,
                _ => {}
            }
        }
        match self.fetch(addr).await {
            Some(info) => {
                let arc = Arc::new(info);
                self.cache.insert(addr, CacheEntry::Ok(arc.clone()));
                Some(arc)
            }
            None => {
                self.cache.insert(addr, CacheEntry::Failed(Instant::now()));
                None
            }
        }
    }

    async fn fetch(&self, addr: Address) -> Option<TokenInfo> {
        let symbol = self.eth_call_string(addr, SELECTOR_SYMBOL).await?;
        let name = self
            .eth_call_string(addr, SELECTOR_NAME)
            .await
            .unwrap_or_else(|| symbol.clone());
        let decimals = self.eth_call_u8(addr, SELECTOR_DECIMALS).await.unwrap_or(18);
        Some(TokenInfo {
            address: addr,
            symbol,
            name,
            decimals,
        })
    }

    async fn eth_call(&self, addr: Address, data: &str) -> Result<String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{
                "to": format!("{addr:#x}"),
                "data": data,
            }, "latest"],
            "id": 1,
        });
        let resp: serde_json::Value =
            self.client.post(&self.rpc_url).json(&body).send().await?.json().await?;
        Ok(resp
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("no result"))?
            .to_string())
    }

    async fn eth_call_string(&self, addr: Address, data: &str) -> Option<String> {
        let r = self.eth_call(addr, data).await.ok()?;
        let h = r.strip_prefix("0x")?;
        if h.len() < 128 {
            return None;
        }
        let length = usize::from_str_radix(&h[64..128], 16).ok()?;
        if length == 0 || length > 256 {
            return None;
        }
        let want = length * 2;
        if h.len() < 128 + want {
            return None;
        }
        let bytes = (0..length)
            .map(|i| u8::from_str_radix(&h[128 + i * 2..128 + i * 2 + 2], 16).ok())
            .collect::<Option<Vec<u8>>>()?;
        let s = String::from_utf8_lossy(&bytes).trim_end_matches('\0').to_string();
        if s.is_empty() || s.len() > 64 {
            return None;
        }
        Some(s)
    }

    async fn eth_call_u8(&self, addr: Address, data: &str) -> Option<u8> {
        let r = self.eth_call(addr, data).await.ok()?;
        let h = r.strip_prefix("0x")?;
        if h.is_empty() {
            return None;
        }
        u8::from_str_radix(&h[h.len().saturating_sub(2)..], 16).ok()
    }

    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }
}

pub fn format_amount(raw: U256, decimals: u8) -> String {
    if raw.is_zero() {
        return "0".to_string();
    }
    let s = raw.to_string();
    let dec = decimals as usize;
    if dec == 0 {
        return s;
    }
    let (whole, frac) = if s.len() <= dec {
        let pad = "0".repeat(dec - s.len());
        ("0".to_string(), format!("{pad}{s}"))
    } else {
        let split = s.len() - dec;
        (s[..split].to_string(), s[split..].to_string())
    };
    let frac_trim = frac.trim_end_matches('0');
    let whole_num: f64 = whole.parse().unwrap_or(0.0);
    let frac_digits = if whole_num >= 1_000_000.0 {
        0
    } else if whole_num >= 1_000.0 {
        1
    } else if whole_num >= 1.0 {
        3
    } else if frac_trim.is_empty() {
        0
    } else {
        6
    };
    if frac_digits == 0 || frac_trim.is_empty() {
        return thousands(&whole);
    }
    let frac_show = if frac_trim.len() > frac_digits {
        &frac_trim[..frac_digits]
    } else {
        frac_trim
    };
    format!("{}.{}", thousands(&whole), frac_show)
}

fn thousands(n: &str) -> String {
    let bytes = n.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() + bytes.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        let from_end = bytes.len() - i;
        if i > 0 && from_end % 3 == 0 {
            out.push(b',');
        }
        out.push(*b);
    }
    String::from_utf8(out).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_amount_basics() {
        let one_and_half = U256::from(1_500_000_000_000_000_000u128);
        assert_eq!(format_amount(one_and_half, 18), "1.5");

        let twelve_five_m =
            U256::from(12_500_000u128) * U256::from(10u128).pow(U256::from(18u8));
        let s = format_amount(twelve_five_m, 18);
        assert!(s.starts_with("12,500,000"), "got {s}");

        assert_eq!(format_amount(U256::ZERO, 18), "0");
    }

    #[test]
    fn thousands_separator() {
        assert_eq!(thousands("1234567"), "1,234,567");
        assert_eq!(thousands("12"), "12");
        assert_eq!(thousands("1000"), "1,000");
    }
}
