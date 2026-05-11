//! Broadcast bus. `Arc<PendingTx>` flows to N subscribers via
//! `tokio::sync::broadcast`; slow subscribers see `RecvError::Lagged` and
//! the bus stays unblocked.

use bsc_core::PendingTx;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::subscription::Subscription;

pub struct Bus {
    tx: broadcast::Sender<Arc<PendingTx>>,
}

impl Bus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Publish a tx. Returns the receiver count after send (for diagnostics).
    /// `Err` only if there are no subscribers — we treat that as a no-op.
    pub fn publish(&self, item: Arc<PendingTx>) -> usize {
        self.tx.send(item).unwrap_or_default()
    }

    pub fn subscribe(&self, name: &'static str) -> Subscription {
        Subscription::new(name, self.tx.subscribe())
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscriber_count_tracks_subscriptions() {
        let bus = Bus::new(8);
        assert_eq!(bus.receiver_count(), 0);
        let s1 = bus.subscribe("a");
        assert_eq!(bus.receiver_count(), 1);
        let _s2 = bus.subscribe("b");
        assert_eq!(bus.receiver_count(), 2);
        drop(s1);
        assert_eq!(bus.receiver_count(), 1);
    }
}
