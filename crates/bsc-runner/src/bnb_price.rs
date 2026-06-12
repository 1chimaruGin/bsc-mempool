//! BNB/USD price, for the USD-denominated entry gate.
//!
//! Source: Chainlink BNB/USD aggregator on BSC mainnet
//! `0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE`, `latestAnswer()`
//! (selector `0x50d25bcd`), int256 with 8 decimals — the canonical,
//! single-call price oracle on BSC. Cached for `TTL` so a burst of KOL
//! hits doesn't hammer the node.

use std::sync::Mutex;
use std::time::{Duration, Instant};

const CHAINLINK_BNB_USD: &str = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE";
const LATEST_ANSWER: &str = "0x50d25bcd";
const TTL: Duration = Duration::from_secs(30);

pub struct BnbPrice {
    rpc_url: String,
    client: reqwest::Client,
    cache: Mutex<Option<(f64, Instant)>>,
}

impl BnbPrice {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(4))
                .build()
                .expect("build reqwest client"),
            cache: Mutex::new(None),
        }
    }

    /// Latest BNB/USD. Returns the cached value if still fresh, otherwise
    /// refetches. On RPC failure returns the stale cached value if any,
    /// else `None` (caller should fail closed — skip the trade).
    pub async fn get(&self) -> Option<f64> {
        if let Some((p, at)) = *self.cache.lock().unwrap() {
            if at.elapsed() < TTL {
                return Some(p);
            }
        }
        match self.fetch().await {
            Some(p) => {
                *self.cache.lock().unwrap() = Some((p, Instant::now()));
                Some(p)
            }
            None => self.cache.lock().unwrap().map(|(p, _)| p),
        }
    }

    async fn fetch(&self) -> Option<f64> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": CHAINLINK_BNB_USD, "data": LATEST_ANSWER}, "latest"],
            "id": 1,
        });
        let resp = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .ok()?;
        let v: serde_json::Value = resp.json().await.ok()?;
        let hex = v.get("result")?.as_str()?;
        let raw = i128::from_str_radix(hex.trim_start_matches("0x"), 16).ok()?;
        if raw <= 0 {
            return None;
        }
        // Chainlink BNB/USD has 8 decimals.
        Some(raw as f64 / 1e8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_is_sane() {
        assert!(TTL.as_secs() >= 5 && TTL.as_secs() <= 120);
    }

    #[tokio::test]
    async fn stale_cache_returned_on_fetch_failure() {
        let bp = BnbPrice::new("http://127.0.0.1:1"); // unroutable
        *bp.cache.lock().unwrap() = Some((600.0, Instant::now() - TTL * 2));
        // fetch will fail (bad port) → falls back to stale cached value
        assert_eq!(bp.get().await, Some(600.0));
    }
}
