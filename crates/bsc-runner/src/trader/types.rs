#![allow(dead_code)]  // paper-trader code retained for re-enable; many helpers unused while live trader is active
//! Shared trader types. Kept dependency-light so the strategy / executor /
//! ledger / sim modules can all share without circular deps.
//!
//! Day-3 BSC port from the ETH stack's trader/types.rs. Two structural
//! diffs from the ETH version worth flagging:
//! 1. `eth_*` fields renamed to `bnb_*` (and "ETH" / "USD" wei semantics
//!    swapped for BNB / USD). 1 BNB ≈ $300-700 historically; sizing
//!    constants get re-tuned in strategy.rs.
//! 2. `Token` is self-contained (symbol + decimals + address inline) rather
//!    than wrapping a `TokenInfo` from a separate resolver crate. BSC port
//!    will add `bsc-runner/src/token_resolver.rs` as a sibling module
//!    when symbol/decimals lookups go live.

use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Direction of the KOL's swap. Drives entry vs exit decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    /// KOL spent BNB to acquire a token.
    Buy,
    /// KOL sent a token away to receive BNB.
    Sell,
}

/// Which simulated portfolio a position/trade belongs to. Both run in
/// parallel for every KOL hit so we can directly compare PnL between
/// fast-tip (next-block via high gas tip) vs normal (next-block via
/// median tip) strategies.
///
/// BSC-specific renaming from the ETH version: on BSC there's no
/// public-mempool same-block backrun pattern (no Flashbots-style relay
/// in the operator's self-hosted reach), so the comparison is "pay for
/// fast inclusion" vs "let it ride at normal gas". Both still simulate
/// at the kol_block + 1 boundary; the difference is the assumed price
/// they'd fill at given the block-phase landing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PortfolioMode {
    /// Hypothetical: lands in N+1 with a top-tier gas tip (~3× median).
    /// Entry/exit prices simulated at the BEGINNING of block N+1 (pre-
    /// other-buyers).
    FastTip,
    /// Hypothetical: lands in N+1 with normal gas. Entry/exit simulated
    /// at the END of block N+1 (after the cohort of fast-tippers).
    NormalTip,
}

impl PortfolioMode {
    /// Live set of portfolios that get a position per entry. Reduced to
    /// [FastTip] only so per-KOL budget reservation is 1:1 with closes —
    /// dual-portfolio (FastTip+NormalTip) would double-count the credit.
    /// NormalTip variant kept for ledger backwards-compat on old rows.
    pub const ALL: &'static [PortfolioMode] = &[PortfolioMode::FastTip];

    pub fn label(&self) -> &'static str {
        match self {
            PortfolioMode::FastTip => "fast_tip",
            PortfolioMode::NormalTip => "normal_tip",
        }
    }
}

/// What the strategy decided to do about a particular KOL hit. Produced by
/// `Strategy::evaluate` and consumed by `Executor::execute`.
#[derive(Debug, Clone)]
pub enum Decision {
    Skip {
        reason: SkipReason,
    },
    Enter {
        kol_name: String,
        token: Address,
        bnb_amount: U256,    // our (paper) BNB input — `size_fraction` × KOL's
        kol_bnb_input: U256, // KOL's BNB input (for diagnostics)
        kol_block: u64,
        kol_tx: B256,
    },
    Exit {
        kol_name: String,
        token: Address,
        kol_block: u64,
        kol_tx: B256,
        /// KOL's wallet address (from `KolHit.from_addr`). Used by the
        /// paper trader to query `balanceOf(kol_addr, token, kol_block-1)`
        /// and `balanceOf(kol_addr, token, kol_block)` so it can compute
        /// the KOL's actual sell fraction and close a PROPORTIONAL slice
        /// of our position — not the entire bag.
        kol_addr: Address,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    NotKolHit,
    BelowBuyThreshold,
    NotASwap,
    AlreadyHavePosition, // when a sell hit but we have no position open
    NoOpenPosition,
    UnknownToken,
    UnsupportedSide,
}

/// One live (open) paper position. Aggregated across multiple buys from the
/// same KOL on the same token (averaging in).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub portfolio: PortfolioMode,
    pub kol_name: String,
    pub token_address: Address,
    pub token_symbol: String,
    pub token_decimals: u8,
    /// Cumulative BNB "spent" across all buy fills.
    pub bnb_in: U256,
    /// Cumulative tokens accumulated.
    pub tokens_held: U256,
    /// Block of the first buy.
    pub opened_at_block: u64,
    /// Unix-ns timestamp of first buy. Used for 24h timeout.
    pub opened_at_unix_ns: u64,
    /// Block of the most recent buy (for diagnostics).
    pub last_added_block: u64,
    /// Tx hashes of the buys that opened/added to this position.
    pub buy_tx_hashes: Vec<B256>,
    /// Market cap (USD) D bought into — spot price × supply × BNB-USD at
    /// entry. 0 if no V2 pool / price unavailable.
    #[serde(default)]
    pub d_mcap_usd: f64,
    /// Market cap (USD) WE effectively entered at — our fill price ×
    /// supply × BNB-USD. Gap vs `d_mcap_usd` = copy-lag cost.
    #[serde(default)]
    pub our_entry_mcap_usd: f64,
}

impl OpenPosition {
    pub fn key(&self) -> PositionKey {
        PositionKey {
            portfolio: self.portfolio,
            kol_name: self.kol_name.clone(),
            token: self.token_address,
        }
    }
}

/// Lookup key for the position book.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionKey {
    pub portfolio: PortfolioMode,
    pub kol_name: String,
    pub token: Address,
}

/// A position that has been closed. One row per (portfolio, kol, token,
/// open-cycle) — written to CSV.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub portfolio: PortfolioMode,
    pub kol_name: String,
    pub token_address: Address,
    pub token_symbol: String,
    pub bnb_in_wei: U256,
    pub bnb_out_wei: U256,
    pub tokens_traded: U256,
    /// Net PnL in wei. Positive = profit, negative = loss.
    pub pnl_wei: i128,
    /// PnL as a fraction (e.g. 0.245 = +24.5%). Computed at close time.
    pub pnl_pct: f64,
    pub opened_at_block: u64,
    pub opened_at_unix_ns: u64,
    pub closed_at_block: u64,
    pub closed_at_unix_ns: u64,
    pub held_secs: u64,
    pub buy_tx_count: usize,
    pub close_reason: CloseReason,
    /// The KOL's sell tx that triggered our exit (None for timeout / manual).
    pub trigger_sell_tx: Option<B256>,
    /// Carried from the position: mcap (USD) D entered at.
    #[serde(default)]
    pub d_mcap_usd: f64,
    /// Carried from the position: mcap (USD) we entered at.
    #[serde(default)]
    pub our_entry_mcap_usd: f64,
    /// BNB-USD at close — for the readable `pnl_usd` CSV column.
    #[serde(default)]
    pub bnb_usd_at_close: f64,
    /// Number of KOL sells we observed for this position. Live trader
    /// fires on the FIRST sell so this is always 1; the recompute backfill
    /// can rewrite it to the true multi-sell tranche count.
    #[serde(default)]
    pub kol_exit_count: u32,
    /// Mcap (USD) at the KOL's first observed exit. Same as last for the
    /// live trader; backfill overwrites with the true multi-sell sequence.
    #[serde(default)]
    pub kol_exit_mcap_first_usd: f64,
    /// Mcap (USD) at the KOL's last observed exit (= first for live trader).
    #[serde(default)]
    pub kol_exit_mcap_last_usd: f64,
    /// Token-weighted average mcap (USD) at our +1-block fills across all
    /// exit tranches. Single value for live trader (one exit).
    #[serde(default)]
    pub our_avg_exit_mcap_usd: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloseReason {
    /// Triggered by the KOL selling the same token.
    KolSell,
    /// Triggered by the configurable hold timeout (default 24h).
    Timeout,
    /// Manually forced (service shutdown, sim error, no-liquidity exit).
    ManualForce,
    /// Exit unquotable — both V2 and V3 returned zero or reverted.
    /// (Differentiated from ManualForce so it's filterable in PnL analysis.)
    NoLiquidity,
    /// We could not VALUE the exit (bonding-curve native-BNB sell, no
    /// readable price source on this pruned node). NOT a loss — booked
    /// flat (PnL 0) and excluded from win-rate so the ledger isn't
    /// poisoned by fabricated −100% rows.
    PriceUnavailable,
}

impl CloseReason {
    pub fn label(&self) -> &'static str {
        match self {
            CloseReason::KolSell => "kol_sell",
            CloseReason::Timeout => "timeout",
            CloseReason::ManualForce => "manual_force",
            CloseReason::NoLiquidity => "no_liquidity",
            CloseReason::PriceUnavailable => "price_unavailable",
        }
    }
}

/// Token reference. Self-contained on the BSC port (vs ETH's wrapper around
/// a separate TokenInfo struct).
#[derive(Debug, Clone)]
pub struct Token {
    pub address: Address,
    pub symbol: String,
    pub decimals: u8,
}

impl Token {
    pub fn new(address: Address, symbol: impl Into<String>, decimals: u8) -> Self {
        Self {
            address,
            symbol: symbol.into(),
            decimals,
        }
    }
}

/// Maximum holding period before a position auto-closes via timeout.
pub const DEFAULT_HOLD_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portfolio_mode_labels() {
        assert_eq!(PortfolioMode::FastTip.label(), "fast_tip");
        assert_eq!(PortfolioMode::NormalTip.label(), "normal_tip");
        // PortfolioMode::ALL was reduced to [FastTip] when per-KOL paper
        // budget tracking landed (2026-05-25) — see types.rs::ALL comment.
        // NormalTip variant kept for ledger backwards compat on old rows.
        assert_eq!(PortfolioMode::ALL.len(), 1);
    }

    #[test]
    fn close_reason_labels() {
        assert_eq!(CloseReason::KolSell.label(), "kol_sell");
        assert_eq!(CloseReason::Timeout.label(), "timeout");
        assert_eq!(CloseReason::ManualForce.label(), "manual_force");
        assert_eq!(CloseReason::NoLiquidity.label(), "no_liquidity");
    }

    #[test]
    fn position_key_round_trip() {
        let pos = OpenPosition {
            portfolio: PortfolioMode::FastTip,
            kol_name: "A".into(),
            token_address: Address::from([0xaa; 20]),
            token_symbol: "TKN".into(),
            token_decimals: 18,
            bnb_in: U256::from(1u64),
            tokens_held: U256::from(1u64),
            opened_at_block: 1,
            opened_at_unix_ns: 1,
            last_added_block: 1,
            buy_tx_hashes: vec![],
            d_mcap_usd: 0.0,
            our_entry_mcap_usd: 0.0,
        };
        let k = pos.key();
        assert_eq!(k.portfolio, PortfolioMode::FastTip);
        assert_eq!(k.kol_name, "A");
        assert_eq!(k.token, Address::from([0xaa; 20]));
    }
}
