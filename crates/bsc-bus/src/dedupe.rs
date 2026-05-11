//! Dedupe stage: owns the `DashMap<TxHash, FirstSeen>`, merges first-seen
//! observations from multiple sources, and emits exactly one `PendingTx` per
//! unique tx (the first time it's observed).
//!
//! Single-task by design — a single `Dedupe` instance is owned by the dedupe
//! task and not shared write-side. Reads (e.g. block oracle correlating
//! mined-tx hashes) are cheap from any thread because `DashMap` is already
//! sharded.

use ahash::RandomState;
use alloy::primitives::B256;
use bsc_core::{FirstSeen, SourceId, SourceSeen};
use dashmap::DashMap;

pub struct Dedupe {
    map: DashMap<B256, FirstSeen, RandomState>,
    ttl_ns: u64,
}

#[derive(Debug)]
pub enum DedupeOutcome {
    /// First time this hash was seen anywhere. Emit downstream.
    Fresh(FirstSeen),
    /// Already seen — sources updated in place; do not emit.
    Duplicate,
}

impl Dedupe {
    pub fn new(capacity: usize, ttl_secs: u64) -> Self {
        Self {
            map: DashMap::with_capacity_and_hasher(capacity, RandomState::default()),
            ttl_ns: ttl_secs.saturating_mul(1_000_000_000),
        }
    }

    pub fn record(&self, hash: B256, source: SourceId, recv_ns: u64) -> DedupeOutcome {
        match self.map.entry(hash) {
            dashmap::Entry::Vacant(v) => {
                let mut sources = SourceSeen::new();
                sources.push((source, recv_ns));
                let fs = FirstSeen {
                    first_ns: recv_ns,
                    sources,
                    expires_at_ns: recv_ns.saturating_add(self.ttl_ns),
                };
                v.insert(fs.clone());
                DedupeOutcome::Fresh(fs)
            }
            dashmap::Entry::Occupied(mut o) => {
                o.get_mut().sources.push((source, recv_ns));
                DedupeOutcome::Duplicate
            }
        }
    }

    /// Read-side lookup used by the block oracle.
    pub fn first_seen_ns(&self, hash: &B256) -> Option<u64> {
        self.map.get(hash).map(|v| v.first_ns)
    }

    pub fn snapshot(&self, hash: &B256) -> Option<FirstSeen> {
        self.map.get(hash).map(|v| v.clone())
    }

    /// Drop entries whose TTL has expired. Returns count removed.
    pub fn sweep(&self, now_ns: u64) -> usize {
        let before = self.map.len();
        self.map.retain(|_, v| v.expires_at_ns >= now_ns);
        before.saturating_sub(self.map.len())
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    #[test]
    fn first_observation_is_fresh() {
        let d = Dedupe::new(64, 60);
        let outcome = d.record(h(1), SourceId::WSS_PROVIDER, 1_000);
        match outcome {
            DedupeOutcome::Fresh(fs) => {
                assert_eq!(fs.first_ns, 1_000);
                assert_eq!(fs.sources.len(), 1);
            }
            DedupeOutcome::Duplicate => panic!("expected Fresh"),
        }
    }

    #[test]
    fn second_observation_is_duplicate_and_appends_source() {
        let d = Dedupe::new(64, 60);
        d.record(h(1), SourceId::WSS_PROVIDER, 1_000);
        let outcome = d.record(h(1), SourceId::LOCAL_IPC, 1_500);
        assert!(matches!(outcome, DedupeOutcome::Duplicate));
        let snap = d.snapshot(&h(1)).unwrap();
        assert_eq!(snap.first_ns, 1_000);
        assert_eq!(snap.sources.len(), 2);
        assert_eq!(snap.sources[0], (SourceId::WSS_PROVIDER, 1_000));
        assert_eq!(snap.sources[1], (SourceId::LOCAL_IPC, 1_500));
    }

    #[test]
    fn sweep_drops_expired_entries() {
        let d = Dedupe::new(64, 1); // 1s TTL
        d.record(h(1), SourceId::WSS_PROVIDER, 1_000_000_000);
        d.record(h(2), SourceId::WSS_PROVIDER, 1_000_000_000);
        let dropped = d.sweep(3_000_000_000);
        assert_eq!(dropped, 2);
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn sweep_preserves_unexpired() {
        let d = Dedupe::new(64, 10);
        d.record(h(1), SourceId::WSS_PROVIDER, 1_000_000_000);
        let dropped = d.sweep(2_000_000_000);
        assert_eq!(dropped, 0);
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn first_seen_ns_lookup() {
        let d = Dedupe::new(64, 60);
        d.record(h(7), SourceId::LOCAL_IPC, 999);
        assert_eq!(d.first_seen_ns(&h(7)), Some(999));
        assert_eq!(d.first_seen_ns(&h(8)), None);
    }
}
