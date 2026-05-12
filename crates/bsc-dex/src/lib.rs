//! BSC DEX bindings — PancakeSwap V2 + V3 + Multicall3.
//!
//! Hand-rolled ABI calldata + decode (same pattern as the ETH stack's
//! Aave V3 bindings). No alloy macros, no abigen — keeps compile times
//! down and the surface deliberately small.
//!
//! Day-2A scope:
//! - PancakeSwap V2 single-hop quoter (`getAmountsOut`). V2 is dominant on
//!   BSC so it's the primary path; ~90% of trader hits will route here.
//! - Multicall3 batched `eth_call`.
//!
//! Day-2B (later): PancakeSwap V3 QuoterV2; multi-hop V2 paths.

pub mod addresses;
pub mod multicall;
pub mod v2;
pub mod v3;

pub use addresses::*;
pub use multicall::{Multicall3Call, Multicall3Result, aggregate3};
pub use v2::{V2Quoter, V2QuoteError};
pub use v3::{V3QuoteError, V3QuoteResult, V3Quoter};
