//! WSS pending-tx source: subscribe to `newPendingTransactions` (hash-only),
//! then fetch full bodies via `eth_getTransactionByHash` with bounded
//! concurrency. Universal path that works against any standard JSON-RPC
//! provider — Chainstack-BSC, QuickNode-BSC, NodeReal, Ankr-BSC, …
//!
//! Reconnect: jittered exponential backoff up to 10s. The outer loop never
//! returns `Ok(())` of its own accord — only shutdown can stop it.

use alloy::primitives::TxHash;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use async_trait::async_trait;
use bsc_core::{RawPayload, RawTx, SourceId};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::Source;
use crate::now_unix_ns;

#[derive(Debug, Clone)]
pub struct WssConfig {
    pub source_id: SourceId,
    pub url: String,
    pub backfill_concurrency: usize,
    pub backfill_retries: u8,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl WssConfig {
    pub fn new(source_id: SourceId, url: impl Into<String>) -> Self {
        Self {
            source_id,
            url: url.into(),
            backfill_concurrency: 32,
            backfill_retries: 2,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(10),
        }
    }
}

pub struct WssSource {
    cfg: WssConfig,
}

impl WssSource {
    pub fn new(cfg: WssConfig) -> Arc<Self> {
        Arc::new(Self { cfg })
    }
}

#[async_trait]
impl Source for WssSource {
    fn id(&self) -> SourceId {
        self.cfg.source_id
    }

    async fn run(
        self: Arc<Self>,
        out: mpsc::Sender<RawTx>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut backoff = self.cfg.initial_backoff;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                res = self.clone().run_once(out.clone(), shutdown.clone()) => {
                    match res {
                        Ok(()) => {
                            tracing::debug!(source = self.cfg.source_id.label(), "WSS subscription ended; reconnecting");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, source = self.cfg.source_id.label(), "WSS source error; reconnecting");
                        }
                    }
                    backoff_sleep(&mut backoff, self.cfg.max_backoff, &shutdown).await;
                }
            }
        }
    }
}

impl WssSource {
    async fn run_once(
        self: Arc<Self>,
        out: mpsc::Sender<RawTx>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let ws = WsConnect::new(self.cfg.url.clone());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let provider = Arc::new(provider);
        let mut sub = provider.subscribe_pending_transactions().await?.into_stream();

        let sem = Arc::new(Semaphore::new(self.cfg.backfill_concurrency));

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                next = sub.next() => {
                    let Some(hash) = next else { return Ok(()); };
                    let recv_ns = now_unix_ns();
                    let Ok(permit) = sem.clone().acquire_owned().await else { continue };
                    let cfg = self.cfg.clone();
                    let provider = provider.clone();
                    let out = out.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        backfill_one(provider, cfg, hash, recv_ns, out).await;
                    });
                }
            }
        }
    }
}

async fn backfill_one<P>(
    provider: Arc<P>,
    cfg: WssConfig,
    hash: TxHash,
    recv_ns: u64,
    out: mpsc::Sender<RawTx>,
) where
    P: Provider + ?Sized + 'static,
{
    // BSC's 0.45s blocks mean tx eviction-races are tighter than on ETH.
    // Keep the retry ladder but at shorter intervals than the ETH original.
    let delays = [
        Duration::from_millis(0),
        Duration::from_millis(100),
        Duration::from_millis(300),
    ];
    let max_attempts = (cfg.backfill_retries as usize + 1).min(delays.len());
    for delay in delays.iter().take(max_attempts) {
        if !delay.is_zero() {
            tokio::time::sleep(*delay).await;
        }
        match provider.get_transaction_by_hash(hash).await {
            Ok(Some(tx)) => {
                let envelope = tx.into_inner();
                let raw = RawTx {
                    source: cfg.source_id,
                    recv_ns,
                    hash,
                    payload: RawPayload::Decoded {
                        tx: Box::new(envelope),
                        raw: None,
                    },
                };
                if out.try_send(raw).is_err() {
                    metrics::counter!(
                        "mempool_source_backpressure_drops_total",
                        "source" => cfg.source_id.label()
                    )
                    .increment(1);
                }
                return;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::trace!(error = %e, hash = ?hash, "getTransactionByHash error");
            }
        }
    }
}

async fn backoff_sleep(current: &mut Duration, max: Duration, shutdown: &CancellationToken) {
    let delay = *current;
    *current = (*current * 2).min(max);
    tokio::select! {
        biased;
        () = shutdown.cancelled() => {}
        () = tokio::time::sleep(delay) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let cfg = WssConfig::new(SourceId::WSS_PROVIDER, "wss://example.test");
        assert_eq!(cfg.backfill_concurrency, 32);
        assert_eq!(cfg.backfill_retries, 2);
        assert_eq!(cfg.max_backoff, Duration::from_secs(10));
    }

    #[test]
    fn now_unix_ns_is_monotonic_within_one_call() {
        let a = now_unix_ns();
        let b = now_unix_ns();
        assert!(b >= a);
    }
}
