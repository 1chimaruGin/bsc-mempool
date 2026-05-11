//! Local Unix-socket JSON-RPC source (bsc-geth IPC).
//!
//! Default candidate paths:
//! - `/data/bsc-meme-mev/bsc-geth/geth.ipc`
//! - `/tmp/geth.ipc`
//! - `~/.bnb/geth.ipc`
//!
//! No-op when the socket is absent. The source struct is constructed
//! optimistically; `run` returns immediately if no IPC path is configured
//! or reachable, leaving the WSS source to do the work.
//!
//! ## Why a hand-rolled subscription
//!
//! Uses bsc-geth's `eth_subscribe newPendingTransactions, true` extension
//! (inherited from go-ethereum) to get full tx bodies in one stream — no
//! `getTransactionByHash` round-trip per pending tx, no Ok(None) race when
//! a tx is evicted between subscription and query.
//!
//! We still use alloy types (`Transaction`, `TxEnvelope`) for body decoding;
//! only the pubsub transport is custom.

use alloy::consensus::TxEnvelope;
use alloy::rpc::types::Transaction;
use async_trait::async_trait;
use bsc_core::{RawPayload, RawTx, SourceId};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::Source;
use crate::now_unix_ns;

#[derive(Debug, Clone)]
pub struct IpcConfig {
    pub path: PathBuf,
}

impl IpcConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

/// Auto-detect a usable IPC path among the standard candidates.
pub fn auto_detect() -> Option<PathBuf> {
    const CANDIDATES: &[&str] = &[
        "/data/bsc-meme-mev/bsc-geth/geth.ipc",
        "/tmp/geth.ipc",
    ];
    for c in CANDIDATES {
        if Path::new(c).exists() {
            return Some(PathBuf::from(c));
        }
    }
    if let Some(home) = dirs_home() {
        for sub in [".bnb/geth.ipc", ".bsc/geth.ipc"] {
            let p = home.join(sub);
            if p.exists() {
                return Some(p);
            }
        }
    }
    None
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

pub struct IpcSource {
    cfg: IpcConfig,
}

impl IpcSource {
    pub fn new(cfg: IpcConfig) -> Arc<Self> {
        Arc::new(Self { cfg })
    }
}

#[async_trait]
impl Source for IpcSource {
    fn id(&self) -> SourceId {
        SourceId::LOCAL_IPC
    }

    async fn run(
        self: Arc<Self>,
        out: mpsc::Sender<RawTx>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        if !self.cfg.path.exists() {
            tracing::info!(
                path = ?self.cfg.path,
                "IPC socket not present at startup; source idle"
            );
            shutdown.cancelled().await;
            return Ok(());
        }
        let mut backoff = Duration::from_millis(500);
        let max = Duration::from_secs(10);
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                res = self.clone().run_once(out.clone(), shutdown.clone()) => {
                    match res {
                        Ok(()) => tracing::info!("IPC subscription ended; reconnecting"),
                        Err(e) => tracing::warn!(error = %e, "IPC source error; reconnecting"),
                    }
                    let delay = backoff;
                    backoff = (backoff * 2).min(max);
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }
}

impl IpcSource {
    /// One subscription lifetime: connect, subscribe, stream events until the
    /// connection drops or shutdown fires.
    async fn run_once(
        self: Arc<Self>,
        out: mpsc::Sender<RawTx>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let socket = UnixStream::connect(&self.cfg.path).await?;
        let (read_half, mut write_half) = socket.into_split();
        let mut reader = BufReader::new(read_half);

        // bsc-geth honours the optional second positional `true` flag (same
        // semantics as go-ethereum upstream — full tx body in each event).
        const SUB_REQ: &[u8] =
            br#"{"jsonrpc":"2.0","method":"eth_subscribe","params":["newPendingTransactions",true],"id":1}"#;
        write_half.write_all(SUB_REQ).await?;
        write_half.write_all(b"\n").await?;
        write_half.flush().await?;

        // First response is the subscribe ack — must contain a `result` (sub id).
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            anyhow::bail!("ipc closed before subscribe ack");
        }
        let ack: serde_json::Value = serde_json::from_str(line.trim())?;
        if ack.get("error").is_some() {
            anyhow::bail!("eth_subscribe rejected: {line}");
        }
        let sub_id = ack
            .get("result")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "?".to_string());
        tracing::info!(sub_id = %sub_id, path = ?self.cfg.path, "ipc raw subscription open");

        loop {
            line.clear();
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                res = reader.read_line(&mut line) => {
                    let n = res?;
                    if n == 0 {
                        // EOF — bsc-geth closed the connection; reconnect.
                        return Ok(());
                    }
                    handle_event(line.trim(), &out);
                }
            }
        }
    }
}

/// Parse one JSON-RPC line and, if it's an `eth_subscription` event with a tx
/// body, push a RawTx to the bus.
fn handle_event(line: &str, out: &mpsc::Sender<RawTx>) {
    if line.is_empty() {
        return;
    }
    metrics::counter!("mempool_ipc_subscribe_received_total").increment(1);
    let v: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            metrics::counter!(
                "mempool_ipc_event_total",
                "outcome" => "parse_error"
            )
            .increment(1);
            tracing::debug!(error = %e, "ipc: bad json");
            return;
        }
    };
    if v.get("method").and_then(|m| m.as_str()) != Some("eth_subscription") {
        return;
    }
    let result = match v.get("params").and_then(|p| p.get("result")) {
        Some(r) => r,
        None => {
            metrics::counter!(
                "mempool_ipc_event_total",
                "outcome" => "no_result"
            )
            .increment(1);
            return;
        }
    };
    let tx: Transaction = match serde_json::from_value(result.clone()) {
        Ok(t) => t,
        Err(e) => {
            metrics::counter!(
                "mempool_ipc_event_total",
                "outcome" => "decode_error"
            )
            .increment(1);
            tracing::debug!(error = %e, "ipc: tx decode failed");
            return;
        }
    };
    let recv_ns = now_unix_ns();
    let envelope: TxEnvelope = tx.into_inner();
    let hash = *envelope.tx_hash();
    let raw = RawTx {
        source: SourceId::LOCAL_IPC,
        recv_ns,
        hash,
        payload: RawPayload::Decoded {
            tx: Box::new(envelope),
            raw: None,
        },
    };
    match out.try_send(raw) {
        Ok(_) => {
            metrics::counter!("mempool_ipc_event_total", "outcome" => "ok").increment(1);
        }
        Err(_) => {
            metrics::counter!(
                "mempool_ipc_event_total",
                "outcome" => "channel_full"
            )
            .increment(1);
            metrics::counter!(
                "mempool_source_backpressure_drops_total",
                "source" => SourceId::LOCAL_IPC.label()
            )
            .increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_detect_returns_none_when_no_socket() {
        let _ = auto_detect();
    }

    #[test]
    fn ipc_config_round_trip() {
        let cfg = IpcConfig::new("/tmp/geth.ipc");
        assert_eq!(cfg.path, PathBuf::from("/tmp/geth.ipc"));
    }
}
