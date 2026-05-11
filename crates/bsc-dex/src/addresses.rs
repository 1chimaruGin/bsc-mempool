//! BSC mainnet contract addresses. Sourced from official PancakeSwap and
//! BSC docs (May 2026). Verify on first startup with a sanity-check
//! `eth_chainId` against a fresh node.

use alloy::primitives::{Address, address};

/// Wrapped BNB. The native-token wrapper used by every router on BSC.
pub const WBNB: Address = address!("bb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c");

// ───── PancakeSwap V2 ─────

/// PancakeSwap V2 Factory.
pub const PANCAKE_V2_FACTORY: Address =
    address!("cA143Ce32Fe78f1f7019d7d551a6402fC5350c73");

/// PancakeSwap V2 Router02.
pub const PANCAKE_V2_ROUTER: Address =
    address!("10ED43C718714eb63d5aA57B78B54704E256024E");

// ───── PancakeSwap V3 ─────

/// PancakeSwap V3 Factory.
pub const PANCAKE_V3_FACTORY: Address =
    address!("0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865");

/// PancakeSwap V3 SmartRouter (handles V2 + V3 + StableSwap).
pub const PANCAKE_V3_ROUTER: Address =
    address!("13f4EA83D0bd40E75C8222255bc855a974568Dd4");

/// PancakeSwap V3 QuoterV2 — read-only quote computation.
pub const PANCAKE_V3_QUOTER_V2: Address =
    address!("B048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997");

/// V3 fee tiers in basis-points * 100 (i.e. 100 = 0.01%, 10000 = 1%).
pub const PANCAKE_V3_FEE_TIERS: &[u32] = &[10_000, 2_500, 500, 100];

// ───── Common stable tokens (BSC mainnet decimals) ─────

/// USDT-BSC (Tether). NOTE: **18 decimals on BSC**, not 6 like on Ethereum.
pub const USDT: Address = address!("55d398326f99059fF775485246999027B3197955");

/// USDC-BSC. 18 decimals.
pub const USDC: Address = address!("8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d");

/// BUSD-BSC (deprecated post-Paxos exit but still circulating; 18 dec).
pub const BUSD: Address = address!("e9e7CEA3DedcA5984780Bafc599bD69ADd087D56");

// ───── Multicall3 (canonical, same address on every EVM chain) ─────

pub const MULTICALL3: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_are_distinct() {
        // Trivial sanity-check that none of the constants accidentally collide.
        let all = [
            WBNB,
            PANCAKE_V2_FACTORY,
            PANCAKE_V2_ROUTER,
            PANCAKE_V3_FACTORY,
            PANCAKE_V3_ROUTER,
            PANCAKE_V3_QUOTER_V2,
            USDT,
            USDC,
            BUSD,
            MULTICALL3,
        ];
        let mut sorted = all.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), all.len(), "duplicate constant");
    }
}
