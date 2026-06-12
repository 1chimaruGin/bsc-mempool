//! Wallet nonce tracker. Fetches initial nonce from chain on startup,
//! then increments locally per `reserve()`. Resync on RPC errors that
//! indicate drift (nonce too low / nonce too high).
//!
//! Thread-safe via atomics.
//!
//! ## Persistence (added 2026-06-09)
//!
//! Local geth's `eth_getTransactionCount(addr, "pending")` only reports
//! txs that passed through ITS mempool. With multi-path race-submit (BR
//! + local geth + bundle relays), txs accepted only by BR sit invisible
//! to local geth's pending pool. On restart we'd bootstrap from a stale
//! "pending" value and reuse nonces that are already in flight → every
//! subsequent BUY/SELL fails with "nonce too low: state 496, tx 494".
//!
//! Fix: persist the next-to-reserve value to disk on every `reserve()`.
//! On bootstrap, take `max(disk_value, chain_pending)`. The disk file
//! captures all reservations we've made regardless of which path
//! accepted; chain "pending" catches up if disk is stale (fresh wallet
//! or wiped state). The MAX guarantees we never reuse a reserved nonce.

use alloy::primitives::Address;
use anyhow::{anyhow, Context, Result};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct NonceManager {
    rpc_url: String,
    address: Address,
    current: AtomicU64,
    http:    reqwest::Client,
    /// Disk-persisted nonce path. None = persistence disabled (test mode).
    persist: Option<Arc<PathBuf>>,
}

impl NonceManager {
    /// Build a manager and prime it with the wallet's current `pending` nonce.
    /// Use `pending` (not `latest`) so we account for txs we've already
    /// submitted in this block. ALSO read the persisted next-nonce from
    /// disk (written on every reserve()) and take the max — this covers
    /// the case where we submitted txs via BR that local geth's pending
    /// pool doesn't know about.
    pub async fn new(rpc_url: String, address: Address, persist_path: Option<PathBuf>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .context("build reqwest")?;
        let chain_n = fetch_pending_nonce(&http, &rpc_url, address).await?;
        let disk_n  = persist_path.as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let n = chain_n.max(disk_n);
        tracing::info!(
            target: "trader_live",
            wallet = %format!("{address:#x}"),
            initial_nonce = n,
            chain_pending = chain_n,
            disk_persisted = disk_n,
            picked = if disk_n > chain_n { "disk" } else { "chain" },
            "nonce manager initialised"
        );
        Ok(Self {
            rpc_url,
            address,
            current: AtomicU64::new(n),
            http,
            persist: persist_path.map(Arc::new),
        })
    }

    /// Reserve the next nonce for an outgoing tx. Increments the local
    /// counter unconditionally — caller is responsible for resync on
    /// chain-rejection ("nonce too low / too high"). Persists the NEW
    /// next-value to disk so a restart bootstraps to at least this point.
    pub fn reserve(&self) -> u64 {
        let n = self.current.fetch_add(1, Ordering::Relaxed);
        // Fire-and-forget disk persistence of the NEXT nonce (n+1).
        // Best-effort: a failed write only loses crash-recovery, not
        // hot-path correctness (chain_pending still catches up usually).
        if let Some(path) = self.persist.clone() {
            tokio::spawn(async move {
                let next = n + 1;
                let _ = tokio::fs::write(&*path, next.to_string()).await;
            });
        }
        n
    }

    /// Re-pull `pending` nonce from chain and take MAX(chain, current local).
    /// Local counter is the source of truth for reservations in flight;
    /// chain pending only adds value if it has ADVANCED past us (some tx
    /// landed via a non-geth path and is now in geth's pool). Never go
    /// BACKWARDS even if chain reports a smaller number than our local.
    pub async fn resync(&self) -> Result<u64> {
        let chain_n = fetch_pending_nonce(&self.http, &self.rpc_url, self.address).await?;
        let local   = self.current.load(Ordering::Relaxed);
        let new     = chain_n.max(local);
        self.current.store(new, Ordering::Relaxed);
        tracing::warn!(
            target: "trader_live",
            local_before = local,
            chain_pending = chain_n,
            local_after = new,
            "nonce resynced from chain (max-of-two)"
        );
        Ok(new)
    }

    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }
}

async fn fetch_pending_nonce(
    http: &reqwest::Client,
    rpc_url: &str,
    address: Address,
) -> Result<u64> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getTransactionCount",
        "params": [format!("{address:#x}"), "pending"],
        "id": 1,
    });
    let v: serde_json::Value = http
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("nonce rpc post")?
        .json()
        .await
        .context("nonce rpc decode")?;
    let hex = v
        .get("result")
        .and_then(|s| s.as_str())
        .ok_or_else(|| anyhow!("no result field in response: {v}"))?;
    let s = hex.strip_prefix("0x").unwrap_or(hex);
    u64::from_str_radix(s, 16).map_err(|e| anyhow!("bad hex {hex:?}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_increments() {
        // Construct without RPC by hand
        let nm = NonceManager {
            rpc_url: "".into(),
            address: Address::ZERO,
            persist: None,
            current: AtomicU64::new(100),
            http: reqwest::Client::new(),
        };
        assert_eq!(nm.reserve(), 100);
        assert_eq!(nm.reserve(), 101);
        assert_eq!(nm.reserve(), 102);
        assert_eq!(nm.current(), 103);
    }
}
