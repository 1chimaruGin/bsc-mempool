//! Phase 1 — Venus (BSC core pool) liquidation **health watcher**.
//!
//! READ-ONLY. Never signs or sends a tx. Its only job is the honest
//! go/no-go: is there enough real liquidation flow on Venus to justify
//! building the atomic flash-loan liquidator (Phase 2)?
//!
//! Pipeline (all via the self-hosted node — no third-party API):
//!   1. `Comptroller.getAllMarkets()` → vToken list.
//!   2. `eth_getLogs` Borrow events across vTokens → active-borrower set
//!      (seed window on start, incremental per poll).
//!   3. Each poll: `Comptroller.getAccountLiquidity(addr)` →
//!      `(err, liquidity, shortfall)`. `shortfall > 0` ⇒ liquidatable.
//!   4. Rough bounty ≈ `shortfall_usd × (liquidationIncentive − 1)`.
//!      Log + count + (optional) Telegram when ≥ `min_alert_bounty_usd`.
//!
//! Venus numbers are USD scaled 1e18. Borrow event (Compound fork,
//! nothing indexed): `Borrow(address,uint256,uint256,uint256)` —
//! borrower = first 32-byte data word.

use crate::config::LiquidatorConfig;
use alloy::primitives::{keccak256, Address, B256};
use std::collections::HashSet;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn sel(sig: &str) -> String {
    let h = keccak256(sig.as_bytes());
    format!("0x{}", hex8(&h[..4]))
}
fn hex8(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
fn addr_arg(a: Address) -> String {
    format!("{:0>64}", format!("{a:x}"))
}
fn u256_hex(h: &str) -> f64 {
    u128::from_str_radix(h.trim_start_matches("0x"), 16)
        .map(|v| v as f64)
        .unwrap_or(0.0)
}
fn addr_from_word(w: &str) -> Option<Address> {
    let s = w.strip_prefix("0x").unwrap_or(w);
    if s.len() < 40 {
        return None;
    }
    let t = &s[s.len() - 40..];
    let mut b = [0u8; 20];
    for i in 0..20 {
        b[i] = u8::from_str_radix(&t[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(Address::from(b))
}

struct Rpc {
    url: String,
    c: reqwest::Client,
    id: std::sync::atomic::AtomicU64,
}
impl Rpc {
    fn new(url: String) -> Self {
        Self {
            url,
            c: reqwest::Client::builder()
                .timeout(Duration::from_secs(8))
                .build()
                .expect("reqwest"),
            id: std::sync::atomic::AtomicU64::new(1),
        }
    }
    async fn call(&self, method: &str, params: serde_json::Value) -> Option<serde_json::Value> {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let body = serde_json::json!({"jsonrpc":"2.0","method":method,"params":params,"id":id});
        let v: serde_json::Value = self
            .c
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        v.get("result").cloned()
    }
    async fn eth_call(&self, to: &str, data: String) -> Option<String> {
        self.call(
            "eth_call",
            serde_json::json!([{"to":to,"data":data},"latest"]),
        )
        .await?
        .as_str()
        .map(str::to_string)
    }
    async fn block_number(&self) -> Option<u64> {
        let r = self.call("eth_blockNumber", serde_json::json!([])).await?;
        u64::from_str_radix(r.as_str()?.trim_start_matches("0x"), 16).ok()
    }
}

pub fn start(cfg: LiquidatorConfig, shutdown: CancellationToken) {
    if !cfg.enabled || cfg.protocol != "venus" {
        tracing::info!(target: "venus", "venus watcher disabled in config; skipping");
        return;
    }
    tokio::spawn(async move {
        if let Err(e) = run(cfg, shutdown).await {
            tracing::error!(target: "venus", error = %e, "venus watcher terminated");
        }
    });
}

async fn run(cfg: LiquidatorConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    let rpc = Rpc::new(cfg.rpc_url.clone());
    let comptroller = cfg.comptroller.clone();

    // Chain guard — Venus core pool is BSC mainnet (56) only.
    if let Some(cid) = rpc
        .call("eth_chainId", serde_json::json!([]))
        .await
        .and_then(|v| v.as_str().map(str::to_string))
    {
        let n = u64::from_str_radix(cid.trim_start_matches("0x"), 16).unwrap_or(0);
        if n != 56 {
            anyhow::bail!("venus: chainId {n} != 56 (BSC mainnet); refusing");
        }
    }

    // Static params.
    let s_all_markets = sel("getAllMarkets()");
    let s_acct_liq = sel("getAccountLiquidity(address)");
    let s_liq_incentive = sel("liquidationIncentiveMantissa()");
    let s_close_factor = sel("closeFactorMantissa()");
    let borrow_topic: B256 = keccak256(b"Borrow(address,uint256,uint256,uint256)");
    let borrow_topic_hex = format!("0x{}", hex8(borrow_topic.as_slice()));

    // vToken markets.
    let markets: Vec<String> = match rpc.eth_call(&comptroller, s_all_markets.clone()).await {
        Some(h) if h.len() > 130 => {
            let body = &h[2..];
            let n = usize::from_str_radix(&body[64..128], 16).unwrap_or(0);
            (0..n)
                .filter_map(|i| {
                    let w = &body.get(128 + i * 64..128 + i * 64 + 64)?;
                    addr_from_word(w).map(|a| format!("{a:#x}"))
                })
                .collect()
        }
        _ => {
            anyhow::bail!("venus: getAllMarkets failed");
        }
    };
    let liq_incentive = rpc
        .eth_call(&comptroller, s_liq_incentive)
        .await
        .map(|h| u256_hex(&h) / 1e18)
        .unwrap_or(1.10);
    let close_factor = rpc
        .eth_call(&comptroller, s_close_factor)
        .await
        .map(|h| u256_hex(&h) / 1e18)
        .unwrap_or(0.5);
    tracing::info!(
        target: "venus",
        markets = markets.len(),
        liquidation_incentive = liq_incentive,
        close_factor,
        "venus health watcher up (READ-ONLY, Phase 1)"
    );

    // Seed borrower set from a recent window of Borrow events.
    let head = rpc.block_number().await.unwrap_or(0);
    let seed_blocks: u64 = 100_000; // ~12h of BSC blocks
    let chunk: u64 = 5_000;
    let mut borrowers: HashSet<Address> = HashSet::new();
    let mut from = head.saturating_sub(seed_blocks);
    while from < head {
        let to = (from + chunk).min(head);
        scan_borrows(&rpc, &markets, &borrow_topic_hex, from, to, &mut borrowers).await;
        from = to + 1;
        if shutdown.is_cancelled() {
            return Ok(());
        }
    }
    let mut last_scanned = head;
    tracing::info!(
        target: "venus",
        borrowers = borrowers.len(),
        seed_blocks,
        "seeded active-borrower set"
    );

    let poll = Duration::from_secs(cfg.poll_interval_secs.max(10));
    let mut tick = tokio::time::interval(poll);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }

        // Incremental borrower discovery.
        if let Some(h) = rpc.block_number().await {
            if h > last_scanned {
                let mut f = last_scanned + 1;
                while f <= h {
                    let t = (f + chunk).min(h);
                    scan_borrows(&rpc, &markets, &borrow_topic_hex, f, t, &mut borrowers)
                        .await;
                    f = t + 1;
                }
                last_scanned = h;
            }
        }

        // Health poll.
        let mut liquidatable = 0u64;
        let mut best: Option<(Address, f64)> = None;
        for acct in borrowers.iter() {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let data = format!("{s_acct_liq}{}", addr_arg(*acct));
            let Some(h) = rpc.eth_call(&comptroller, data).await else {
                continue;
            };
            // (uint err, uint liquidity, uint shortfall) — 3 words.
            let b = h.trim_start_matches("0x");
            if b.len() < 192 {
                continue;
            }
            let err = u256_hex(&b[0..64]);
            let shortfall = u256_hex(&b[128..192]) / 1e18; // USD
            if err == 0.0 && shortfall > 0.0 {
                liquidatable += 1;
                // Floor proxy: realizable bounty ≈ shortfall × (incentive−1).
                let bounty = shortfall * (liq_incentive - 1.0);
                metrics::counter!("bsc_venus_liquidatable_total").increment(1);
                metrics::histogram!("bsc_venus_bounty_usd").record(bounty);
                if best.map(|(_, x)| bounty > x).unwrap_or(true) {
                    best = Some((*acct, bounty));
                }
                if bounty >= cfg.min_alert_bounty_usd {
                    tracing::info!(
                        target: "venus",
                        account = %format!("{acct:#x}"),
                        shortfall_usd = format!("{shortfall:.2}"),
                        est_bounty_usd = format!("{bounty:.2}"),
                        "LIQUIDATABLE (read-only)"
                    );
                }
            }
        }
        tracing::info!(
            target: "venus",
            tracked_borrowers = borrowers.len(),
            liquidatable,
            best = best
                .map(|(a, b)| format!("{a:#x}=${b:.0}"))
                .unwrap_or_else(|| "-".into()),
            "venus poll"
        );
        metrics::gauge!("bsc_venus_tracked_borrowers").set(borrowers.len() as f64);
        metrics::gauge!("bsc_venus_liquidatable_now").set(liquidatable as f64);
    }
}

async fn scan_borrows(
    rpc: &Rpc,
    markets: &[String],
    topic: &str,
    from: u64,
    to: u64,
    out: &mut HashSet<Address>,
) {
    let params = serde_json::json!([{
        "fromBlock": format!("0x{from:x}"),
        "toBlock": format!("0x{to:x}"),
        "address": markets,
        "topics": [topic],
    }]);
    let Some(logs) = rpc.call("eth_getLogs", params).await else {
        return;
    };
    let Some(arr) = logs.as_array() else { return };
    for lg in arr {
        // borrower = first 32-byte data word (not indexed in Venus vBep20).
        if let Some(d) = lg.get("data").and_then(|x| x.as_str()) {
            let body = d.strip_prefix("0x").unwrap_or(d);
            if body.len() >= 64 {
                if let Some(a) = addr_from_word(&body[0..64]) {
                    if a != Address::ZERO {
                        out.insert(a);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_known_values() {
        // getAccountLiquidity(address) = 0x5ec88c79 (Compound/Venus).
        assert_eq!(sel("getAccountLiquidity(address)"), "0x5ec88c79");
        // closeFactorMantissa() = 0xe8755446
        assert_eq!(sel("closeFactorMantissa()"), "0xe8755446");
    }

    #[test]
    fn borrow_topic_is_stable() {
        let t = keccak256(b"Borrow(address,uint256,uint256,uint256)");
        assert_eq!(
            format!("0x{}", hex8(t.as_slice())),
            "0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80"
        );
    }

    #[test]
    fn addr_from_word_extracts_low20() {
        let w = format!("{:0>64}", "ab".repeat(20));
        assert_eq!(addr_from_word(&w), Some(Address::repeat_byte(0xab)));
    }
}
