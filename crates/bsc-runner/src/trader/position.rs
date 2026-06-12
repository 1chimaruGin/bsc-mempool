#![allow(dead_code)]  // paper-trader code retained for re-enable; many helpers unused while live trader is active
//! Position book — keeps track of every open paper position keyed by
//! (portfolio, kol, token). Average-in semantics on adds: a second BUY of
//! the same (portfolio, kol, token) tuple combines into the existing
//! position by summing BNB-spent and tokens-held. Close zeroes the entry.

use crate::trader::types::{OpenPosition, PortfolioMode, PositionKey};
use alloy::primitives::{Address, B256, U256};
use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Debug, Default)]
pub struct PositionBook {
    by_key: HashMap<PositionKey, OpenPosition>,
}

impl PositionBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub fn open_or_add(
        &mut self,
        portfolio: PortfolioMode,
        kol_name: String,
        token_address: Address,
        token_symbol: String,
        token_decimals: u8,
        bnb_in: U256,
        tokens_in: U256,
        block: u64,
        tx: B256,
    ) {
        let key = PositionKey {
            portfolio,
            kol_name: kol_name.clone(),
            token: token_address,
        };
        let now_ns = unix_ns();
        match self.by_key.get_mut(&key) {
            Some(pos) => {
                pos.bnb_in = pos.bnb_in.saturating_add(bnb_in);
                pos.tokens_held = pos.tokens_held.saturating_add(tokens_in);
                pos.last_added_block = block;
                pos.buy_tx_hashes.push(tx);
            }
            None => {
                self.by_key.insert(
                    key,
                    OpenPosition {
                        portfolio,
                        kol_name,
                        token_address,
                        token_symbol,
                        token_decimals,
                        bnb_in,
                        tokens_held: tokens_in,
                        opened_at_block: block,
                        opened_at_unix_ns: now_ns,
                        last_added_block: block,
                        buy_tx_hashes: vec![tx],
                        d_mcap_usd: 0.0,
                        our_entry_mcap_usd: 0.0,
                    },
                );
            }
        }
    }

    /// Stamp the entry market caps onto a position. First non-zero set
    /// wins (matches `opened_at_*` "first buy" semantics; later average-in
    /// buys don't rewrite the entry valuation).
    pub fn set_entry_mcaps(&mut self, key: &PositionKey, d_mcap: f64, our_mcap: f64) {
        if let Some(p) = self.by_key.get_mut(key) {
            if p.d_mcap_usd == 0.0 {
                p.d_mcap_usd = d_mcap;
            }
            if p.our_entry_mcap_usd == 0.0 {
                p.our_entry_mcap_usd = our_mcap;
            }
        }
    }

    pub fn get(&self, key: &PositionKey) -> Option<&OpenPosition> {
        self.by_key.get(key)
    }

    pub fn remove(&mut self, key: &PositionKey) -> Option<OpenPosition> {
        self.by_key.remove(key)
    }

    /// Partial close: shrink an open position to the remaining tokens/cost
    /// basis after a proportional sell. Used when the KOL only sold part
    /// of their stake — we close the same fraction of ours and keep the
    /// rest open. No-op if the key doesn't exist.
    pub fn shrink(&mut self, key: &PositionKey, new_tokens_held: U256, new_bnb_in: U256) {
        if let Some(p) = self.by_key.get_mut(key) {
            p.tokens_held = new_tokens_held;
            p.bnb_in = new_bnb_in;
        }
    }

    /// All positions across all portfolios opened on or before `cutoff_unix_ns`
    /// — drives the 24h timeout sweep.
    pub fn opened_before(&self, cutoff_unix_ns: u64) -> Vec<PositionKey> {
        self.by_key
            .iter()
            .filter(|(_, p)| p.opened_at_unix_ns <= cutoff_unix_ns)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// All positions for a given (kol, token) pair across all portfolios.
    /// Used when KOL sells a token and we need to close any portfolio's open
    /// position for that pair.
    pub fn keys_for_kol_token(&self, kol_name: &str, token: Address) -> Vec<PositionKey> {
        self.by_key
            .keys()
            .filter(|k| k.kol_name == kol_name && k.token == token)
            .cloned()
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PositionKey, &OpenPosition)> {
        self.by_key.iter()
    }

    pub fn snapshot(&self) -> Vec<OpenPosition> {
        self.by_key.values().cloned().collect()
    }
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token_addr(byte: u8) -> Address {
        let mut b = [0u8; 20];
        b[19] = byte;
        Address::from(b)
    }
    fn tx(byte: u8) -> B256 {
        let mut b = [0u8; 32];
        b[31] = byte;
        B256::from(b)
    }

    #[test]
    fn open_then_add_averages_in() {
        let mut book = PositionBook::new();
        let t = token_addr(1);

        book.open_or_add(
            PortfolioMode::NormalTip,
            "D".into(),
            t,
            "PEPE".into(),
            18,
            U256::from(1_000_000_000_000_000_000u128), // 1 BNB
            U256::from(100u128),
            100,
            tx(1),
        );
        book.open_or_add(
            PortfolioMode::NormalTip,
            "D".into(),
            t,
            "PEPE".into(),
            18,
            U256::from(2_000_000_000_000_000_000u128), // 2 BNB
            U256::from(150u128),
            105,
            tx(2),
        );

        let key = PositionKey {
            portfolio: PortfolioMode::NormalTip,
            kol_name: "D".into(),
            token: t,
        };
        let p = book.get(&key).unwrap();
        assert_eq!(p.bnb_in, U256::from(3_000_000_000_000_000_000u128));
        assert_eq!(p.tokens_held, U256::from(250u128));
        assert_eq!(p.opened_at_block, 100);
        assert_eq!(p.last_added_block, 105);
        assert_eq!(p.buy_tx_hashes.len(), 2);
    }

    #[test]
    fn separate_portfolios_dont_collide() {
        let mut book = PositionBook::new();
        let t = token_addr(7);
        book.open_or_add(
            PortfolioMode::NormalTip,
            "A".into(),
            t,
            "X".into(),
            18,
            U256::from(1u128),
            U256::from(10u128),
            1,
            tx(0),
        );
        book.open_or_add(
            PortfolioMode::FastTip,
            "A".into(),
            t,
            "X".into(),
            18,
            U256::from(1u128),
            U256::from(10u128),
            1,
            tx(0),
        );
        assert_eq!(book.len(), 2);
    }

    #[test]
    fn keys_for_kol_token_finds_both_portfolios() {
        let mut book = PositionBook::new();
        let t = token_addr(9);
        for mode in PortfolioMode::ALL {
            book.open_or_add(
                *mode,
                "D".into(),
                t,
                "Y".into(),
                18,
                U256::from(1u128),
                U256::from(1u128),
                1,
                tx(0),
            );
        }
        let keys = book.keys_for_kol_token("D", t);
        // Was 2 when PortfolioMode::ALL = [FastTip, NormalTip]; now ALL is
        // [FastTip] only (2026-05-25 per-KOL budget rework). Test asserts
        // the iteration matches whatever the const exposes.
        assert_eq!(keys.len(), PortfolioMode::ALL.len());
    }

    #[test]
    fn remove_zeroes_out() {
        let mut book = PositionBook::new();
        let t = token_addr(2);
        let key = PositionKey {
            portfolio: PortfolioMode::NormalTip,
            kol_name: "D".into(),
            token: t,
        };
        book.open_or_add(
            PortfolioMode::NormalTip,
            "D".into(),
            t,
            "Z".into(),
            18,
            U256::from(1u128),
            U256::from(1u128),
            1,
            tx(0),
        );
        assert!(book.remove(&key).is_some());
        assert!(book.get(&key).is_none());
    }

    #[test]
    fn opened_before_filters_by_timestamp() {
        let mut book = PositionBook::new();
        let t = token_addr(3);
        book.open_or_add(
            PortfolioMode::NormalTip,
            "D".into(),
            t,
            "Z".into(),
            18,
            U256::from(1u128),
            U256::from(1u128),
            1,
            tx(0),
        );
        let now = unix_ns();
        // Position was opened at ~now; cutoff = now + 1 should include it
        assert_eq!(book.opened_before(now + 1).len(), 1);
        // cutoff = now - 1 day should exclude it
        assert_eq!(book.opened_before(now.saturating_sub(86400_000_000_000)).len(), 0);
    }
}
