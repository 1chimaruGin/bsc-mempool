#![allow(dead_code)]  // config/state fields populated via serde / Rust can't see implicit use
//! Shared "tokens we currently hold a paper position in".
//!
//! Written by the paper executor (insert on entry, remove on close) and
//! read by the per-token flow watcher (Phase 2) and price/liquidity
//! oracle (Phase 3). A token in here = an open position worth monitoring
//! for exit intel.

use alloy::primitives::Address;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HeldMeta {
    pub kol_name: String,
    pub symbol: String,
    pub entered_block: u64,
    pub entered_unix_ns: u64,
    /// Paper BNB cost basis (wei) summed across portfolios — for PnL ctx.
    pub bnb_in_wei: u128,
}

#[derive(Default)]
pub struct HeldTokens {
    map: DashMap<Address, HeldMeta>,
}

impl HeldTokens {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            map: DashMap::with_capacity(64),
        })
    }

    pub fn insert(&self, token: Address, meta: HeldMeta) {
        self.map.insert(token, meta);
    }

    pub fn remove(&self, token: &Address) {
        self.map.remove(token);
    }

    pub fn get(&self, token: &Address) -> Option<HeldMeta> {
        self.map.get(token).map(|e| e.clone())
    }

    pub fn contains(&self, token: &Address) -> bool {
        self.map.contains_key(token)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Snapshot of held token addresses (for the oracle's poll loop).
    pub fn addresses(&self) -> Vec<Address> {
        self.map.iter().map(|e| *e.key()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let h = HeldTokens::new();
        let a = Address::repeat_byte(9);
        assert!(!h.contains(&a));
        h.insert(
            a,
            HeldMeta {
                kol_name: "D".into(),
                symbol: "ABC".into(),
                entered_block: 1,
                entered_unix_ns: 1,
                bnb_in_wei: 0,
            },
        );
        assert!(h.contains(&a));
        assert_eq!(h.get(&a).unwrap().symbol, "ABC");
        assert_eq!(h.addresses(), vec![a]);
        h.remove(&a);
        assert!(h.is_empty());
    }
}
