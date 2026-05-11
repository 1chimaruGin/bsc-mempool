//! Stub for BSC premium mempool gateways (bloXroute BSC, BlockRazor, MEVNet…).
//!
//! Per the operator's self-hosted-only rule this is NOT the destination, but
//! the module exists so the public API can grow into it later without a
//! version bump. The current implementation is intentionally a no-op.

use async_trait::async_trait;
use bsc_core::{RawTx, SourceId};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::Source;

#[derive(Debug, Clone)]
pub struct BloxrouteConfig {
    pub source_id: SourceId,
    pub url: String,
    pub auth_token: String,
}

pub struct BloxrouteSource {
    _cfg: BloxrouteConfig,
}

impl BloxrouteSource {
    pub fn new(cfg: BloxrouteConfig) -> Arc<Self> {
        Arc::new(Self { _cfg: cfg })
    }
}

#[async_trait]
impl Source for BloxrouteSource {
    fn id(&self) -> SourceId {
        SourceId::BLOXROUTE
    }

    async fn run(
        self: Arc<Self>,
        _out: mpsc::Sender<RawTx>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        tracing::info!("bloxroute source is a stub; idle until implemented");
        shutdown.cancelled().await;
        Ok(())
    }
}
