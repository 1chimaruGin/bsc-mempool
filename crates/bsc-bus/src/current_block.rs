//! Read-mostly shared state: latest BSC block (number + parent hash + a
//! wall-clock anchor for measuring `ms_into_block`). Updated by a `newHeads`
//! WS consumer task on bsc-geth; read by the dedupe stage to stamp
//! `BlockContext` onto every outgoing `PendingTx`.
//!
//! Replaces the ETH stack's `CurrentHeadState`, which was CL/Beacon-driven.
//! BSC has no separate Beacon CL — block cadence is read directly from the EL.

use alloy::primitives::B256;
use bsc_core::BlockContext;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct CurrentBlock {
    pub block_number: u64,
    pub block_hash: B256,
    pub parent_block_hash: B256,
    /// Unix-ns at which we received the `newHeads` event for `block_number`.
    pub seen_at_unix_ns: u64,
}

pub struct CurrentBlockState {
    inner: RwLock<CurrentBlock>,
}

impl CurrentBlockState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(CurrentBlock::default()),
        })
    }

    pub fn snapshot(&self) -> CurrentBlock {
        self.inner.read().clone()
    }

    /// Update from a bsc-geth `newHeads` event.
    pub fn on_new_head(
        &self,
        block_number: u64,
        block_hash: B256,
        parent_block_hash: B256,
        seen_at_unix_ns: u64,
    ) {
        let mut w = self.inner.write();
        w.block_number = block_number;
        w.block_hash = block_hash;
        w.parent_block_hash = parent_block_hash;
        w.seen_at_unix_ns = seen_at_unix_ns;
    }

    /// Compute `BlockContext` for a tx observed at `unix_ns`.
    /// `ms_into_block` is wall-clock ms since the last `newHeads` event
    /// landed (so a tx seen 1.2 s into a 3 s block reports `1200`).
    pub fn block_context_for(&self, unix_ns: u64) -> BlockContext {
        let snap = self.inner.read();
        let ms = unix_ns
            .saturating_sub(snap.seen_at_unix_ns)
            .saturating_div(1_000_000);
        let ms_into_block = u32::try_from(ms).unwrap_or(u32::MAX);
        BlockContext {
            block_number: snap.block_number,
            parent_block_hash: snap.parent_block_hash,
            ms_into_block,
        }
    }
}

pub fn unix_now_ns() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_context_uses_latest_head() {
        let state = CurrentBlockState::new();
        let parent = B256::from([1u8; 32]);
        let block_hash = B256::from([2u8; 32]);
        state.on_new_head(50_000_000, block_hash, parent, 1_700_000_000_000_000_000);

        // 1.5 s after the head landed
        let ctx = state.block_context_for(1_700_000_001_500_000_000);
        assert_eq!(ctx.block_number, 50_000_000);
        assert_eq!(ctx.parent_block_hash, parent);
        assert_eq!(ctx.ms_into_block, 1_500);
    }

    #[test]
    fn ms_into_block_saturates_when_clock_jumps_back() {
        let state = CurrentBlockState::new();
        state.on_new_head(
            1,
            B256::default(),
            B256::default(),
            1_700_000_000_000_000_000,
        );
        // observed_at < seen_at → saturating sub yields 0
        let ctx = state.block_context_for(1_699_999_999_000_000_000);
        assert_eq!(ctx.ms_into_block, 0);
    }

    #[test]
    fn default_state_is_zero() {
        let state = CurrentBlockState::new();
        let snap = state.snapshot();
        assert_eq!(snap.block_number, 0);
        assert_eq!(snap.seen_at_unix_ns, 0);
    }
}
