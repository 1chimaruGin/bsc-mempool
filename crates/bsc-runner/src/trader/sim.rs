//! Paper-trading price simulator (BSC). Wraps `bsc-dex`'s V2 + V3 quoters
//! to compute the BNB ↔ token amount we'd receive at a specific historical
//! block. Pure view-call against the local bsc-geth — no signing, no fees.
//!
//! ## V2-first, V3-fallback
//!
//! PancakeSwap V2 carries the deep memecoin liquidity on BSC; V3 dominates
//! the high-cap pairs (WBNB-USDT, WBNB-BTCB). For a KOL paper trader the
//! V2 hit-rate is much higher, so we quote V2 first and only fall back to
//! V3 when V2 returns zero (no pair / drained liquidity).
//!
//! ## Limitations of this v1 simulator
//!
//! - Single-hop WBNB ↔ TARGET only. Multi-hop path-finding deferred.
//! - No stable-token routing (would help for stablecoin pairs but those
//!   aren't memecoin shapes the trader targets).
//! - No price-impact correction beyond what the quoters already model.

use alloy::primitives::{Address, U256};
use anyhow::Result;
use bsc_dex::{V2Quoter, V3Quoter, addresses::WBNB};

/// Trade venue used to fill the quote — recorded on the closed trade for
/// post-hoc analysis ("did V2 or V3 win this token?").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteVenue {
    PancakeV2,
    PancakeV3,
}

impl QuoteVenue {
    pub const fn label(self) -> &'static str {
        match self {
            QuoteVenue::PancakeV2 => "v2",
            QuoteVenue::PancakeV3 => "v3",
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuoteResult {
    pub amount_out: U256,
    pub venue: QuoteVenue,
    /// V3 fee tier (basis-points × 100) when `venue = PancakeV3`; `None` for V2.
    pub fee_tier: Option<u32>,
}

#[derive(Clone)]
pub struct Simulator {
    v2: V2Quoter,
    v3: V3Quoter,
}

impl Simulator {
    pub fn new(rpc_url: impl Into<String> + Clone) -> Self {
        let url: String = rpc_url.into();
        Self {
            v2: V2Quoter::new(url.clone()),
            v3: V3Quoter::new(url),
        }
    }

    /// Simulate buying `bnb_in_wei` worth of `token`. Returns the tokens
    /// we'd receive after pool slippage. `None` if neither V2 nor V3 has
    /// a usable pool.
    pub async fn simulate_buy(
        &self,
        bnb_in_wei: U256,
        token: Address,
        block: Option<u64>,
    ) -> Result<Option<QuoteResult>> {
        self.try_quote(WBNB, token, bnb_in_wei, block).await
    }

    /// Simulate selling `tokens_in` of `token` for BNB.
    pub async fn simulate_sell(
        &self,
        tokens_in: U256,
        token: Address,
        block: Option<u64>,
    ) -> Result<Option<QuoteResult>> {
        self.try_quote(token, WBNB, tokens_in, block).await
    }

    async fn try_quote(
        &self,
        src: Address,
        dst: Address,
        amount_in: U256,
        block: Option<u64>,
    ) -> Result<Option<QuoteResult>> {
        // V2 first — single-hop. Returns Ok(non-zero) on success, otherwise
        // we fall back to V3.
        match self.v2.quote_single_hop(amount_in, src, dst, block).await {
            Ok(amount_out) if !amount_out.is_zero() => {
                return Ok(Some(QuoteResult {
                    amount_out,
                    venue: QuoteVenue::PancakeV2,
                    fee_tier: None,
                }));
            }
            Ok(_) => {
                tracing::trace!(?src, ?dst, "V2 returned zero; trying V3");
            }
            Err(e) => {
                tracing::trace!(?src, ?dst, error = %e, "V2 quote errored; trying V3");
            }
        }

        // V3 fallback — try every fee tier, take the best.
        match self.v3.quote_best_tier(src, dst, amount_in, block).await {
            Ok(Some(r)) => Ok(Some(QuoteResult {
                amount_out: r.amount_out,
                venue: QuoteVenue::PancakeV3,
                fee_tier: Some(r.fee_tier),
            })),
            Ok(None) => Ok(None),
            Err(e) => {
                tracing::trace!(?src, ?dst, error = %e, "V3 quote errored");
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venue_labels() {
        assert_eq!(QuoteVenue::PancakeV2.label(), "v2");
        assert_eq!(QuoteVenue::PancakeV3.label(), "v3");
    }

    #[test]
    fn simulator_constructs() {
        // Smoke: builder doesn't panic; no network call.
        let _s = Simulator::new("http://127.0.0.1:8545");
    }
}
