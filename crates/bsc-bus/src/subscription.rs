//! Subscriber-side API. Modules (KOL filter, paper trader, liquidator,
//! sniper) implement `MempoolModule` and the runner spawns each on a
//! dedicated tokio task.

use async_trait::async_trait;
use bsc_core::PendingTx;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum RecvError {
    #[error("subscription {name} lagged by {skipped} messages")]
    Lagged { name: &'static str, skipped: u64 },
    #[error("bus closed")]
    Closed,
}

pub struct Subscription {
    name: &'static str,
    rx: broadcast::Receiver<Arc<PendingTx>>,
}

impl Subscription {
    pub fn new(name: &'static str, rx: broadcast::Receiver<Arc<PendingTx>>) -> Self {
        Self { name, rx }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub async fn recv(&mut self) -> Result<Arc<PendingTx>, RecvError> {
        match self.rx.recv().await {
            Ok(tx) => Ok(tx),
            Err(broadcast::error::RecvError::Lagged(n)) => Err(RecvError::Lagged {
                name: self.name,
                skipped: n,
            }),
            Err(broadcast::error::RecvError::Closed) => Err(RecvError::Closed),
        }
    }
}

#[async_trait]
pub trait MempoolModule: Send + 'static {
    fn name(&self) -> &'static str;
    async fn run(
        self: Box<Self>,
        sub: Subscription,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()>;
}
