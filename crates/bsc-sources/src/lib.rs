//! BSC execution-layer pending-transaction sources.
//!
//! Public surface:
//! - [`Source`] trait — every source implements this.
//! - [`wss`] module — JSON-RPC over WebSocket against a remote bsc-geth/erigon
//!   provider. Hash-only subscription with `eth_getTransactionByHash` backfill.
//! - [`ipc`] module — Unix-socket JSON-RPC for a local bsc-geth. Uses Geth's
//!   `eth_subscribe newPendingTransactions, true` extension so full tx bodies
//!   stream in one round without per-tx backfill.
//! - [`bloxroute`] module — paid BSC mempool gateway stub (kept for future
//!   premium-flow integration; not the destination per the user's "self-hosted
//!   only" rule).
//!
//! Drops `devp2p` from the ETH stack: bsc-geth doesn't expose a sentry plugin
//! the way Reth does, and devp2p on BSC peers with PoSA validator set anyway
//! — the IPC source captures the same flow with less complexity.

use async_trait::async_trait;
use bsc_core::{RawTx, SourceId};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub mod bloxroute;
pub mod ipc;
pub mod wss;

#[async_trait]
pub trait Source: Send + Sync + 'static {
    fn id(&self) -> SourceId;
    async fn run(
        self: Arc<Self>,
        out: mpsc::Sender<RawTx>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()>;
}

/// UNIX wall-clock nanoseconds. Sources stamp `RawTx::recv_ns` with this so
/// the dedupe stage can attribute first-seen latency consistently across
/// sources.
pub fn now_unix_ns() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(0)
}
