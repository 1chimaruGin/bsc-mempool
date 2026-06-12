//! Real-time Four.Meme bonding-curve price feed.
//!
//! ## Why this exists
//!
//! The original price oracle in `adaptive_trail.rs` tried two paths:
//!   1. V2 quote via `getAmountsOut` — only works post-graduation
//!   2. Fallback: scan token's Transfer events via `eth_getLogs`
//!
//! Both fail for pre-graduation Four.Meme tokens:
//!   - V2 has no pool until graduation
//!   - `eth_getLogs` times out intermittently on local geth (-32002)
//!
//! Result: the trail oracle was structurally blind. On 2026-06-03 the
//! user lost 3 of 4 positions to a 4000-block timeout because peak never
//! moved off entry (FLAT prices = no observations).
//!
//! ## How this fixes it
//!
//! The Four.Meme launchpad emits a `TradeBuy` event with topic
//! `0x7db52723…` on EVERY bonding-curve buy. The event data carries the
//! full curve state — no fetch, no decode beyond a `data` slice:
//!
//! ```text
//! data[0]  → token address          (32 bytes)
//! data[3]  → tokens delivered       (uint256)
//! data[4]  → BNB paid (net of fee)  (uint256)
//! data[5]  → fee                    (uint256)
//! → price (BNB-per-raw-token) = (data[4] + data[5]) / data[3]
//! ```
//!
//! Decoded directly from the event, the price matches the receipt-based
//! `bnb_paid / tokens_received` to the wei (verified empirically against
//! the trade at block 102044910).
//!
//! A single WS log subscription on the launchpad address + event topic
//! gives us every buy chain-wide. We filter to held tokens client-side
//! and write the latest (block, price) per token into a shared
//! `Arc<RwLock<HashMap>>` that the trail watchers read on every tick.

use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Filter;
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub const FOURMEME_LAUNCHPAD: &str = "0x5c952063c7fc8610FFDB798152D69F0B9550762b";
/// The `TradeBuy` event topic[0] emitted by the Four.Meme launchpad.
/// Empirically extracted from tx `0x1c3c…b87e8` log[2].
pub const TRADE_BUY_TOPIC: &str =
    "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942";
/// The `TradeSell` event topic[0] emitted by the Four.Meme launchpad.
/// Empirically extracted from tx `0x1058…865b` log[2].
/// Same data layout as TradeBuy (token at word 0, tokens at word 3,
/// BNB net at word 4, fee at word 5) so the decoder is shared.
pub const TRADE_SELL_TOPIC: &str =
    "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19";

/// Per-token last-observed bonding-curve price.
#[derive(Debug, Clone, Copy)]
pub struct PricePoint {
    pub block: u64,
    /// BNB-per-raw-token (wei BNB / raw token units).
    /// Same unit as the buy-receipt-derived entry price.
    pub price: f64,
}

/// Per-token per-block flow stats. We keep a rolling window of these
/// in `FourMemeStatsCache` so the trail watcher can compute the
/// voting-exit features (buy_velocity_collapse, net_flow_3blk, etc.)
/// without recomputing them every tick.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockStats {
    pub block: u64,
    pub buy_count:    u32,
    pub sell_count:   u32,
    pub buy_bnb_wei:  u128,
    pub sell_bnb_wei: u128,
}

/// Per-token rolling 10-block window of stats. Pushed on the WS
/// TradeBuy/Sell stream, read by the trail watcher each tick.
pub const STATS_WINDOW: usize = 10;

#[derive(Debug, Default, Clone)]
pub struct TokenStats {
    pub blocks: std::collections::VecDeque<BlockStats>,
}

impl TokenStats {
    pub fn ingest(&mut self, block: u64, is_buy: bool, bnb_wei: u128) {
        // Reuse the last entry if same block, else push a new one
        let need_new = match self.blocks.back() {
            Some(b) => b.block != block,
            None    => true,
        };
        if need_new {
            self.blocks.push_back(BlockStats { block, ..Default::default() });
            while self.blocks.len() > STATS_WINDOW {
                self.blocks.pop_front();
            }
        }
        let last = self.blocks.back_mut().expect("just pushed");
        if is_buy {
            last.buy_count += 1;
            last.buy_bnb_wei = last.buy_bnb_wei.saturating_add(bnb_wei);
        } else {
            last.sell_count += 1;
            last.sell_bnb_wei = last.sell_bnb_wei.saturating_add(bnb_wei);
        }
    }
    /// Sum buy counts over the last N blocks (N ≤ window).
    pub fn buy_count_last(&self, n: usize) -> u32 {
        self.blocks.iter().rev().take(n).map(|b| b.buy_count).sum()
    }
    pub fn sell_count_last(&self, n: usize) -> u32 {
        self.blocks.iter().rev().take(n).map(|b| b.sell_count).sum()
    }
    pub fn net_flow_bnb_wei_last(&self, n: usize) -> i128 {
        self.blocks.iter().rev().take(n)
            .map(|b| b.buy_bnb_wei as i128 - b.sell_bnb_wei as i128)
            .sum()
    }
}

/// Shared cache of latest curve price per token. Written by the WS
/// subscriber, read by the trail watchers.
pub type FourMemePriceCache = Arc<RwLock<HashMap<Address, PricePoint>>>;
pub type FourMemeStatsCache = Arc<RwLock<HashMap<Address, TokenStats>>>;

pub fn new_cache() -> FourMemePriceCache {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn new_stats_cache() -> FourMemeStatsCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Spawn the WS subscriber. Reconnects with backoff on disconnect.
pub fn start(
    cache: FourMemePriceCache,
    stats: FourMemeStatsCache,
    ws_url: String,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(500);
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            match run_loop(cache.clone(), stats.clone(), &ws_url, shutdown.clone()).await {
                Ok(()) => {
                    tracing::info!(target: "fmprice", "WS stream ended; reconnecting");
                    backoff = Duration::from_millis(500);
                }
                Err(e) => {
                    tracing::warn!(target: "fmprice", error = %e, "WS error; reconnecting");
                }
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(15));
        }
    });
}

async fn run_loop(
    cache: FourMemePriceCache,
    stats: FourMemeStatsCache,
    ws_url: &str,
    shutdown: CancellationToken,
) -> Result<()> {
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url.to_string()))
        .await?;
    let launchpad   = FOURMEME_LAUNCHPAD.parse::<Address>()?;
    let buy_topic   = B256::from_str(TRADE_BUY_TOPIC)?;
    let sell_topic  = B256::from_str(TRADE_SELL_TOPIC)?;
    // Two parallel subscriptions (Vec form of event_signature was not
    // matching events as expected on this alloy version — observed
    // 3× DROP in event rate vs single-topic). Two sockets is robust.
    let buy_filter  = Filter::new().address(launchpad).event_signature(buy_topic);
    let sell_filter = Filter::new().address(launchpad).event_signature(sell_topic);
    let mut buy_sub  = provider.subscribe_logs(&buy_filter).await?.into_stream();
    let mut sell_sub = provider.subscribe_logs(&sell_filter).await?.into_stream();
    tracing::info!(
        target: "fmprice",
        "TradeBuy + TradeSell WS subscriptions open (launchpad={launchpad:#x})"
    );

    use futures::StreamExt;
    let mut events_buy: u64 = 0;
    let mut events_sell: u64 = 0;
    let mut events_decoded: u64 = 0;
    let mut last_report = std::time::Instant::now();
    loop {
        // Multiplex both subscriptions. tokio::select! polls whichever
        // arrives first; both feed the cache. Track whether each event
        // came from the buy or sell socket so we can populate flow stats.
        let (log_opt, is_buy) = tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            log = buy_sub.next()  => { events_buy  += 1; (log, true)  },
            log = sell_sub.next() => { events_sell += 1; (log, false) },
        };
        let Some(log) = log_opt else { return Ok(()); };
        if let Some((token, block, price, bnb_wei)) = decode_trade_buy(&log) {
            events_decoded += 1;
            cache.write().insert(token, PricePoint { block, price });
            // Stats: keep per-token rolling window of block-level flow.
            // Used by the trail watcher to compute the voting signals
            // (buy_velocity_collapse, net_flow_3blk) without recomputing
            // them at every tick.
            stats.write().entry(token).or_default().ingest(block, is_buy, bnb_wei);
        }
        if last_report.elapsed() >= Duration::from_secs(60) {
            let size = cache.read().len();
            let stats_size = stats.read().len();
            tracing::info!(
                target: "fmprice",
                events_buy, events_sell, events_decoded,
                cache_size = size, stats_tokens = stats_size,
                "feed heartbeat"
            );
            last_report = std::time::Instant::now();
        }
    }
}

/// Returns (token, block, price BNB-per-raw-token, bnb_gross_wei).
/// Works for BOTH `TradeBuy` and `TradeSell` — same data layout.
/// `bnb_gross_wei` is the BNB value of this trade (paid for buy,
/// received for sell) — used to populate per-block flow stats.
fn decode_trade_buy(log: &alloy::rpc::types::Log) -> Option<(Address, u64, f64, u128)> {
    let data = log.inner.data.data.as_ref();
    if data.len() < 32 * 6 {
        return None;
    }
    let chunk = |i: usize| &data[i * 32..(i + 1) * 32];
    let mut token_bytes = [0u8; 20];
    token_bytes.copy_from_slice(&chunk(0)[12..32]);
    let token = Address::from(token_bytes);
    let tokens = u256_from_be_bytes_f64(chunk(3));
    let bnb_net = u256_from_be_bytes_f64(chunk(4));
    let fee = u256_from_be_bytes_f64(chunk(5));
    if tokens <= 0.0 {
        return None;
    }
    let bnb_gross = bnb_net + fee;
    if bnb_gross <= 0.0 {
        return None;
    }
    let price = bnb_gross / tokens;
    let block = log.block_number.unwrap_or(0);
    let bnb_wei = bnb_gross as u128;
    Some((token, block, price, bnb_wei))
}

/// Convert big-endian 32-byte word to f64. Loses precision above ~2^53
/// but for our use (BNB amounts up to ~1e21, tokens up to ~1e30) the
/// price ratio is still meaningful — and the trail compares ratios, not
/// absolute precision.
fn u256_from_be_bytes_f64(b: &[u8]) -> f64 {
    debug_assert_eq!(b.len(), 32);
    let mut acc = 0.0f64;
    for &byte in b {
        acc = acc * 256.0 + byte as f64;
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity-check the decoder against the verified empirical data
    /// from tx 0x1c3c…b87e8 log[2] (block 102044910, token 0x9f54b1…).
    #[test]
    fn decode_matches_observed_trade() {
        // Reconstruct the event payload (8 words of 32 bytes each).
        let mut data = vec![0u8; 32 * 8];
        // data[0]: token 0x9f54b1dd9b1437c6096ce80855cd6848269f4444
        let token_hex = "9f54b1dd9b1437c6096ce80855cd6848269f4444";
        let token_bytes = alloy::hex::decode(token_hex).unwrap();
        data[12..32].copy_from_slice(&token_bytes);
        // data[3]: tokens delivered = 438189565045262000000000
        let tokens_hex = "000000000000000000000000000000000000000000005cca4dcda4d9861b0c00";
        let tokens_bytes = alloy::hex::decode(tokens_hex).unwrap();
        data[3 * 32..4 * 32].copy_from_slice(&tokens_bytes);
        // data[4]: bnb net = 15381606423893104
        let bnb_hex = "0000000000000000000000000000000000000000000000000036a57d52f89470";
        let bnb_bytes = alloy::hex::decode(bnb_hex).unwrap();
        data[4 * 32..5 * 32].copy_from_slice(&bnb_bytes);
        // data[5]: fee = 153816064238931
        let fee_hex = "00000000000000000000000000000000000000000000000000008be517dea553";
        let fee_bytes = alloy::hex::decode(fee_hex).unwrap();
        data[5 * 32..6 * 32].copy_from_slice(&fee_bytes);

        let log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: FOURMEME_LAUNCHPAD.parse().unwrap(),
                data: alloy::primitives::LogData::new_unchecked(
                    vec![B256::from_str(TRADE_BUY_TOPIC).unwrap()],
                    data.into(),
                ),
            },
            block_hash: None,
            block_number: Some(102044910),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        };
        let (token, block, price, _bnb_wei) = decode_trade_buy(&log).expect("must decode");
        assert_eq!(
            format!("{token:#x}"),
            "0x9f54b1dd9b1437c6096ce80855cd6848269f4444"
        );
        assert_eq!(block, 102044910);
        // Empirically verified: entry_price for this trade was 3.545365e-8.
        let expected = 3.545365688141697e-8;
        let rel_err = (price - expected).abs() / expected;
        assert!(
            rel_err < 1e-4,
            "decoded price {price} does not match expected {expected} (rel_err={rel_err})"
        );
    }
}
