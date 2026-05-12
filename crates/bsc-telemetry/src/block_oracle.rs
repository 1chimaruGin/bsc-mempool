//! Block-coverage oracle for BSC.
//!
//! For every new block landing on bsc-geth, fetch the tx-hash list, cross-
//! reference each hash against the `bsc-bus::Dedupe` map, and emit:
//! - `mempool_block_coverage_ratio` — fraction of mined txs whose hash we
//!   saw in the mempool prior to inclusion.
//! - `mempool_block_lead_time_seconds` — per-tx histogram of
//!   (block_timestamp - first_seen_ns).
//! - `mempool_block_lead_time_by_block_phase_seconds{phase}` — same lead-time
//!   bucketed by which quartile of the 450 ms block window the tx landed in.
//!
//! Architecture (vs the ETH version's two-source trigger):
//!
//! ```text
//!   bsc-geth WS newHeads ─► fetch block(hashes) ─► correlate with Dedupe ─► histograms
//! ```
//!
//! Single trigger source → no CL/EL dedup map needed → ~half the ETH LOC.
//! Reorg observability is left for a future iteration: bsc-geth emits reorg
//! info via the `newHeads` stream's parent-hash chain, but BSC's fast
//! finality (1.125s with Maxwell parlia) makes reorgs vanishingly rare —
//! we'd be measuring noise.

use alloy::primitives::B256;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use bsc_bus::Dedupe;
use futures::StreamExt;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

use crate::metric_names as N;

#[derive(Debug, Clone)]
pub struct BlockOracleConfig {
    /// bsc-geth WS endpoint for `newHeads` + `eth_getBlockByHash`. Must be
    /// reachable; if `None`, the oracle idles until shutdown.
    pub el_ws_url: Option<String>,
    /// Aggregate-gauge emit cadence.
    pub aggregate_interval: Duration,
}

impl Default for BlockOracleConfig {
    fn default() -> Self {
        Self {
            el_ws_url: None,
            aggregate_interval: Duration::from_secs(10),
        }
    }
}

pub struct BlockOracle {
    cfg: BlockOracleConfig,
    dedupe: Option<Arc<Dedupe>>,
    aggregate_state: Arc<RwLock<AggregateState>>,
}

#[derive(Debug, Default)]
struct AggregateState {
    blocks_seen: u64,
    txs_in_blocks: u64,
    txs_seen_in_mempool: u64,
}

impl BlockOracle {
    pub fn new(cfg: BlockOracleConfig) -> Self {
        Self {
            cfg,
            dedupe: None,
            aggregate_state: Arc::new(RwLock::new(AggregateState::default())),
        }
    }

    #[must_use]
    pub fn with_dedupe(mut self, dedupe: Arc<Dedupe>) -> Self {
        self.dedupe = Some(dedupe);
        self
    }

    pub async fn run(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let Some(url) = self.cfg.el_ws_url.clone() else {
            tracing::warn!("block_oracle: no EL WS configured; idling");
            shutdown.cancelled().await;
            return Ok(());
        };
        let Some(dedupe) = self.dedupe.clone() else {
            tracing::warn!("block_oracle: no dedupe handle; idling (would have nothing to correlate)");
            shutdown.cancelled().await;
            return Ok(());
        };

        // Aggregate gauge emitter.
        {
            let state = self.aggregate_state.clone();
            let shutdown = shutdown.clone();
            let interval = self.cfg.aggregate_interval;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => break,
                        _ = ticker.tick() => {
                            let s = state.read();
                            if s.txs_in_blocks > 0 {
                                #[allow(clippy::cast_precision_loss)]
                                let ratio = s.txs_seen_in_mempool as f64
                                    / s.txs_in_blocks as f64;
                                metrics::gauge!(N::BLOCK_COVERAGE_RATIO).set(ratio);
                            }
                        }
                    }
                }
            });
        }

        // Main loop: reconnecting WS subscription. On every newHead, fetch the
        // block and correlate.
        let mut backoff = Duration::from_millis(500);
        let max_backoff = Duration::from_secs(10);
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let res = run_once(&url, dedupe.clone(), self.aggregate_state.clone(), shutdown.clone()).await;
            if let Err(e) = res {
                tracing::warn!(error = %e, "block_oracle WS disconnected; reconnecting");
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(max_backoff);
        }
    }
}

async fn run_once(
    ws_url: &str,
    dedupe: Arc<Dedupe>,
    agg: Arc<RwLock<AggregateState>>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url.to_string()))
        .await?;
    let provider = Arc::new(provider);
    let mut sub = provider.subscribe_blocks().await?.into_stream();
    tracing::info!(url = ws_url, "block_oracle subscribed to newHeads");

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            head = sub.next() => {
                let Some(header) = head else { return Ok(()) };
                let block_hash = header.hash;
                let dedupe = dedupe.clone();
                let agg = agg.clone();
                let provider = provider.clone();
                // Spawn the block fetch + correlation so the WS loop never blocks
                // on an `eth_getBlockByHash` round-trip (which can be 5-50ms on
                // a busy node).
                tokio::spawn(async move {
                    process_block(provider, dedupe, block_hash, agg).await;
                });
            }
        }
    }
}

async fn process_block(
    provider: Arc<impl Provider + ?Sized + 'static>,
    dedupe: Arc<Dedupe>,
    block_hash: B256,
    agg: Arc<RwLock<AggregateState>>,
) {
    // Hashes-only block fetch — we don't need bodies.
    let block = match provider.get_block_by_hash(block_hash).hashes().await {
        Ok(Some(b)) => b,
        Ok(None) => return,
        Err(e) => {
            tracing::trace!(error = %e, ?block_hash, "block_oracle: getBlockByHash failed");
            return;
        }
    };
    let block_ts_ns = block.header.timestamp.saturating_mul(1_000_000_000);
    let total = block.transactions.len() as u64;
    let mut covered: u64 = 0;
    for hash in block.transactions.hashes() {
        if let Some(first_seen_ns) = dedupe.first_seen_ns(&hash) {
            if first_seen_ns < block_ts_ns {
                covered += 1;
                #[allow(clippy::cast_precision_loss)]
                let lead = (block_ts_ns - first_seen_ns) as f64 / 1e9;
                metrics::histogram!(N::BLOCK_LEAD_TIME).record(lead);
                // Per-block-phase bucketing. Uses the 450ms quartile labels
                // from metric_names.
                let ms_into_block = ms_since_unix_ns(first_seen_ns, block_ts_ns);
                let phase = crate::metric_names::block_phase_label(ms_into_block);
                metrics::histogram!(N::BLOCK_LEAD_TIME_BY_PHASE, "phase" => phase).record(lead);
            }
        }
    }
    let mut s = agg.write();
    s.blocks_seen = s.blocks_seen.saturating_add(1);
    s.txs_in_blocks = s.txs_in_blocks.saturating_add(total);
    s.txs_seen_in_mempool = s.txs_seen_in_mempool.saturating_add(covered);
}

/// ms_into_block = ms between mempool first-seen and the BLOCK BEFORE this one's
/// timestamp. Since BSC blocks are 450ms apart, we approximate as
/// `(block_ts_ns - first_seen_ns) % 450ms`. Close enough for histogram bucketing.
fn ms_since_unix_ns(first_seen_ns: u64, block_ts_ns: u64) -> u32 {
    let delta_ns = block_ts_ns.saturating_sub(first_seen_ns);
    let total_ms = delta_ns / 1_000_000;
    let mod_ms = total_ms % 450; // post-Fermi 0.45s blocks
    u32::try_from(mod_ms).unwrap_or(u32::MAX)
}

#[allow(dead_code)]
fn unix_now_ns() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ms_since_unix_ns_basic() {
        // 200 ms gap → bucket = 200 (within first quartile of 450ms block)
        let block_ts_ns = 1_700_000_000_000_000_000u64;
        let first_seen_ns = block_ts_ns - 200_000_000;
        assert_eq!(ms_since_unix_ns(first_seen_ns, block_ts_ns), 200);
    }

    #[test]
    fn ms_since_unix_ns_modulo_wraps() {
        // 1.2 s gap → 1200ms mod 450 = 300ms (in 3rd quartile)
        let block_ts_ns = 2_000_000_000_000_000_000u64;
        let first_seen_ns = block_ts_ns - 1_200_000_000;
        assert_eq!(ms_since_unix_ns(first_seen_ns, block_ts_ns), 300);
    }

    #[test]
    fn ms_since_unix_ns_handles_clock_skew() {
        // first_seen AFTER block_ts (clock skew); saturating sub → 0.
        let block_ts_ns = 1_700_000_000_000_000_000u64;
        let first_seen_ns = block_ts_ns + 1_000_000;
        assert_eq!(ms_since_unix_ns(first_seen_ns, block_ts_ns), 0);
    }

    #[test]
    fn config_default_is_idle() {
        let cfg = BlockOracleConfig::default();
        assert!(cfg.el_ws_url.is_none());
        assert_eq!(cfg.aggregate_interval, Duration::from_secs(10));
    }
}
