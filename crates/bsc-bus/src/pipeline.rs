//! Compose decoder pool + dedupe + fanout into a runnable pipeline.
//!
//! Channel topology:
//! ```text
//! sources --mpsc(N)--> decoder pool (M) --mpsc(N)--> dedupe --broadcast(K)--> subscribers
//! ```

use bsc_core::{PendingTx, RawTx};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::current_block::{CurrentBlockState, unix_now_ns};
use crate::decoder::{DecodedTx, decode_to_decoded};
use crate::dedupe::{Dedupe, DedupeOutcome};
use crate::fanout::Bus;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub raw_channel_capacity: usize,
    pub decoded_channel_capacity: usize,
    pub broadcast_capacity: usize,
    pub dedupe_capacity: usize,
    pub dedupe_ttl_secs: u64,
    pub decoder_workers: usize,
    pub janitor_interval_secs: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        // BSC has ~5× ETH's tx rate during memecoin churn — bigger queues,
        // shorter TTL (3s blocks vs 12s on ETH).
        Self {
            raw_channel_capacity: 32_768,
            decoded_channel_capacity: 32_768,
            broadcast_capacity: 65_536,
            dedupe_capacity: 524_288,
            dedupe_ttl_secs: 60,
            decoder_workers: 4,
            janitor_interval_secs: 5,
        }
    }
}

pub struct Pipeline {
    pub raw_tx_in: mpsc::Sender<RawTx>,
    pub bus: Arc<Bus>,
    pub block_state: Arc<CurrentBlockState>,
    pub dedupe: Arc<Dedupe>,
    pub shutdown: CancellationToken,
}

pub struct PipelineHandles {
    pub decoder_tasks: Vec<JoinHandle<()>>,
    pub dedupe_task: JoinHandle<()>,
    pub janitor_task: JoinHandle<()>,
}

pub fn build_pipeline(
    cfg: &PipelineConfig,
    block_state: Arc<CurrentBlockState>,
) -> (Pipeline, PipelineHandles) {
    let (raw_tx_in, raw_rx) = mpsc::channel::<RawTx>(cfg.raw_channel_capacity);
    let (decoded_tx, decoded_rx) =
        mpsc::channel::<DecodedTx>(cfg.decoded_channel_capacity);

    let bus = Arc::new(Bus::new(cfg.broadcast_capacity));
    let dedupe = Arc::new(Dedupe::new(cfg.dedupe_capacity, cfg.dedupe_ttl_secs));
    let shutdown = CancellationToken::new();

    let raw_rx = Arc::new(tokio::sync::Mutex::new(raw_rx));

    let mut decoder_tasks = Vec::with_capacity(cfg.decoder_workers);
    for worker_id in 0..cfg.decoder_workers {
        let raw_rx = raw_rx.clone();
        let decoded_tx = decoded_tx.clone();
        let shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_decoder_worker(worker_id, raw_rx, decoded_tx, shutdown).await;
        });
        decoder_tasks.push(handle);
    }
    drop(decoded_tx);

    let dedupe_task = {
        let dedupe = dedupe.clone();
        let bus = bus.clone();
        let block_state = block_state.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_dedupe(decoded_rx, dedupe, bus, block_state, shutdown).await;
        })
    };

    let janitor_task = {
        let dedupe = dedupe.clone();
        let shutdown = shutdown.clone();
        let interval = cfg.janitor_interval_secs;
        tokio::spawn(async move {
            run_janitor(dedupe, interval, shutdown).await;
        })
    };

    let pipeline = Pipeline {
        raw_tx_in,
        bus,
        block_state,
        dedupe,
        shutdown,
    };

    let handles = PipelineHandles {
        decoder_tasks,
        dedupe_task,
        janitor_task,
    };

    (pipeline, handles)
}

async fn run_decoder_worker(
    worker_id: usize,
    raw_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<RawTx>>>,
    decoded_tx: mpsc::Sender<DecodedTx>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            raw = async {
                let mut g = raw_rx.lock().await;
                g.recv().await
            } => {
                let Some(raw) = raw else { break };
                match decode_to_decoded(raw) {
                    Ok(decoded) => {
                        if decoded_tx.send(decoded).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::trace!(worker_id, error = ?e, "decode failed; dropping tx");
                    }
                }
            }
        }
    }
}

async fn run_dedupe(
    mut decoded_rx: mpsc::Receiver<DecodedTx>,
    dedupe: Arc<Dedupe>,
    bus: Arc<Bus>,
    block_state: Arc<CurrentBlockState>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            item = decoded_rx.recv() => {
                let Some(decoded) = item else { break };
                let DecodedTx { hash, from, tx, source, recv_ns, raw } = decoded;
                let outcome = dedupe.record(hash, source, recv_ns);
                let fs = match outcome {
                    DedupeOutcome::Fresh(fs) => fs,
                    DedupeOutcome::Duplicate => {
                        metrics::counter!(
                            "mempool_tx_seen_again_total",
                            "source" => source.label()
                        )
                        .increment(1);
                        continue;
                    }
                };
                // Sources record recv_ns as UNIX wall-clock ns; use the tx's
                // own arrival time so `ms_into_block` is computed against the
                // moment we actually saw it.
                let block_context = Some(block_state.block_context_for(recv_ns));
                let pending = Arc::new(PendingTx {
                    hash,
                    from,
                    tx: Arc::new(tx),
                    first_seen_ns: fs.first_ns,
                    source_seen: fs.sources,
                    raw,
                    block_context,
                });
                let receivers = bus.publish(pending);
                metrics::counter!("mempool_pending_tx_total").increment(1);
                metrics::gauge!("mempool_bus_subscribers").set(receivers as f64);
            }
        }
    }
}

async fn run_janitor(dedupe: Arc<Dedupe>, interval_secs: u64, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                let now_ns = unix_now_ns();
                let dropped = dedupe.sweep(now_ns);
                if dropped > 0 {
                    tracing::trace!(dropped, "dedupe TTL sweep");
                }
                metrics::gauge!("mempool_dedupe_map_size").set(dedupe.len() as f64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_config_defaults_match_bsc_profile() {
        let c = PipelineConfig::default();
        assert_eq!(c.raw_channel_capacity, 32_768);
        assert_eq!(c.broadcast_capacity, 65_536);
        assert_eq!(c.dedupe_capacity, 524_288);
        assert_eq!(c.dedupe_ttl_secs, 60); // 3 s blocks × ~20 buffer
        assert_eq!(c.decoder_workers, 4);
    }
}
