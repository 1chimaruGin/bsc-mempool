//! PancakeSwap V3 QuoterV2 — `quoteExactInputSingle((address,address,uint256,uint24,uint160))`.
//!
//! PancakeSwap V3 is a Uniswap V3 fork, so the ABI is byte-identical to
//! Uniswap V3's QuoterV2. The only meaningful difference for our purposes:
//!
//! ## Fee tiers
//!
//! | Pool tier | Uniswap V3 | PancakeSwap V3 |
//! |---:|---:|---:|
//! | tightest | 100 (0.01%) | 100 (0.01%) |
//! | low | 500 (0.05%) | 500 (0.05%) |
//! | mid | **3000 (0.30%)** | **2500 (0.25%)** |
//! | wide | 10000 (1.00%) | 10000 (1.00%) |
//!
//! BSC memecoin pools almost always sit at 10000 or 2500.
//!
//! ## When V3 is the right path on BSC
//!
//! Most KOL hits on BSC route through PancakeSwap V2 (deeper liquidity for
//! the long tail of memecoin pools). V3 takes the relatively-small share of
//! flow on high-cap pairs like WBNB-USDT, WBNB-BTCB, WBNB-USDC, where its
//! concentrated-liquidity advantage outweighs V2's reserve depth. The
//! trader should try V2 first (cheaper, deeper); fall back to V3 only when
//! V2 returns no pair (`getPair` → 0x0) or zero liquidity.

use crate::addresses::{PANCAKE_V3_FEE_TIERS, PANCAKE_V3_QUOTER_V2};
use alloy::primitives::{Address, U256};
use std::time::Duration;
use thiserror::Error;

/// Selector for `quoteExactInputSingle((address,address,uint256,uint24,uint160))`.
/// Same as Uniswap V3 — the function signature is identical even though the
/// implementation is on a different contract.
pub const QUOTE_EXACT_INPUT_SINGLE: [u8; 4] = [0xc6, 0xa5, 0x02, 0x6a];

#[derive(Debug, Error)]
pub enum V3QuoteError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc error: {0}")]
    Rpc(String),
}

#[derive(Debug, Clone)]
pub struct V3QuoteResult {
    pub amount_out: U256,
    pub fee_tier: u32,
}

#[derive(Clone)]
pub struct V3Quoter {
    rpc_url: String,
    client: reqwest::Client,
}

impl V3Quoter {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("build reqwest client"),
        }
    }

    /// Try every PancakeSwap V3 fee tier and return the BEST output. None if
    /// no tier has a pool (or every pool returns zero / reverts).
    ///
    /// `block` = None → `"latest"`; `Some(n)` → quote at that specific block.
    pub async fn quote_best_tier(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        block: Option<u64>,
    ) -> Result<Option<V3QuoteResult>, V3QuoteError> {
        let mut best: Option<V3QuoteResult> = None;
        for &fee in PANCAKE_V3_FEE_TIERS {
            match self
                .quote_one_tier(token_in, token_out, amount_in, fee, block)
                .await
            {
                Ok(amount_out) if !amount_out.is_zero() => {
                    if best.as_ref().is_none_or(|b| amount_out > b.amount_out) {
                        best = Some(V3QuoteResult {
                            amount_out,
                            fee_tier: fee,
                        });
                    }
                }
                _ => {} // empty / revert / error → try next tier
            }
        }
        Ok(best)
    }

    /// Quote a SPECIFIC fee tier. Returns `U256::ZERO` on revert / missing
    /// pool / decode failure; an `Err` only if the RPC transport itself fails.
    pub async fn quote_one_tier(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        fee: u32,
        block: Option<u64>,
    ) -> Result<U256, V3QuoteError> {
        let data = encode_quote_exact_input_single(token_in, token_out, amount_in, fee);
        let block_tag = match block {
            Some(n) => format!("0x{n:x}"),
            None => "latest".to_string(),
        };
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": format!("{PANCAKE_V3_QUOTER_V2:#x}"),
                    "data": format!("0x{}", hex_encode(&data))
                },
                block_tag
            ]
        });
        let resp = self.client.post(&self.rpc_url).json(&body).send().await?;
        let status = resp.status();
        let json: serde_json::Value = resp.json().await?;
        if !status.is_success() {
            return Err(V3QuoteError::Rpc(format!("HTTP {status}: {json}")));
        }
        // QuoterV2 reverts when there's no pool / insufficient liquidity / etc.
        // We treat revert as zero output, not as a transport error.
        if json.get("error").is_some() {
            return Ok(U256::ZERO);
        }
        let Some(result) = json.get("result").and_then(|v| v.as_str()) else {
            return Ok(U256::ZERO);
        };
        let h = result.strip_prefix("0x").unwrap_or(result);
        if h.len() < 64 {
            return Ok(U256::ZERO);
        }
        // amountOut is the first 32-byte word of the return tuple
        // (amountOut, sqrtPriceX96After, initializedTicksCrossed, gasEstimate).
        let amount_hex = &h[..64];
        let bytes = hex_decode(amount_hex).map_err(|e| V3QuoteError::Rpc(format!("hex: {e}")))?;
        if bytes.len() != 32 {
            return Ok(U256::ZERO);
        }
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&bytes);
        Ok(U256::from_be_bytes(buf))
    }
}

// ───── ABI encoding ─────

/// Pack the QuoterV2 args. Selector + 5 flat 32-byte words (the struct's
/// fields, no dynamic types so no offset/tail layout needed):
///   word 0: tokenIn  (address, left-padded)
///   word 1: tokenOut (address, left-padded)
///   word 2: amountIn (uint256)
///   word 3: fee      (uint24, right-padded into 32 bytes)
///   word 4: sqrtPriceLimitX96 (uint160) — we always pass 0 (no limit)
pub fn encode_quote_exact_input_single(
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    fee: u32,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 5 * 32);
    data.extend_from_slice(&QUOTE_EXACT_INPUT_SINGLE);
    data.extend_from_slice(&address_to_word(token_in));
    data.extend_from_slice(&address_to_word(token_out));
    data.extend_from_slice(&amount_in.to_be_bytes::<32>());
    data.extend_from_slice(&u32_to_word(fee));
    data.extend_from_slice(&[0u8; 32]); // sqrtPriceLimitX96 = 0
    data
}

fn address_to_word(a: Address) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[12..32].copy_from_slice(a.as_slice());
    out
}

fn u32_to_word(v: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[28..32].copy_from_slice(&v.to_be_bytes());
    out
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hex_decode(s: &str) -> Result<Vec<u8>, &'static str> {
    if s.len() % 2 != 0 {
        return Err("odd hex length");
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = (bytes[i] as char).to_digit(16).ok_or("bad hex")?;
        let lo = (bytes[i + 1] as char).to_digit(16).ok_or("bad hex")?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addresses::{USDT, WBNB};
    use std::str::FromStr;

    #[test]
    fn selector_matches_uniswap_v3() {
        // PancakeSwap V3 is a fork; selector is identical to Uniswap V3.
        // keccak256("quoteExactInputSingle((address,address,uint256,uint24,uint160))")[..4]
        assert_eq!(QUOTE_EXACT_INPUT_SINGLE, [0xc6, 0xa5, 0x02, 0x6a]);
    }

    #[test]
    fn encode_layout_matches_5_word_struct() {
        let amt = U256::from(1_000_000_000_000_000_000u128); // 1 WBNB
        let bytes = encode_quote_exact_input_single(WBNB, USDT, amt, 2_500);
        // selector(4) + 5 words(160) = 164
        assert_eq!(bytes.len(), 164);
        assert_eq!(&bytes[..4], &QUOTE_EXACT_INPUT_SINGLE);
        // word 0: token_in = WBNB, left-padded
        assert_eq!(&bytes[4..16], &[0u8; 12]);
        assert_eq!(&bytes[16..36], WBNB.as_slice());
        // word 1: token_out = USDT, left-padded
        assert_eq!(&bytes[36..48], &[0u8; 12]);
        assert_eq!(&bytes[48..68], USDT.as_slice());
        // word 2: amount_in (right-aligned uint256)
        assert_eq!(U256::from_be_slice(&bytes[68..100]), amt);
        // word 3: fee = 2500 = 0x09c4, right-aligned in 32 bytes
        assert_eq!(&bytes[100..128], &[0u8; 28]);
        assert_eq!(&bytes[128..132], &2500u32.to_be_bytes());
        // word 4: sqrtPriceLimitX96 = 0
        assert_eq!(&bytes[132..164], &[0u8; 32]);
    }

    #[test]
    fn address_word_is_left_padded() {
        let a = Address::from_str("0x10ED43C718714eb63d5aA57B78B54704E256024E").unwrap();
        let w = address_to_word(a);
        assert_eq!(&w[..12], &[0u8; 12]);
        assert_eq!(&w[12..], a.as_slice());
    }

    #[test]
    fn u32_word_right_pads() {
        let w = u32_to_word(10_000);
        // 10_000 = 0x2710 — sits in the last 4 bytes
        assert_eq!(&w[28..], &[0x00, 0x00, 0x27, 0x10]);
        assert_eq!(&w[..28], &[0u8; 28]);
    }

    #[test]
    fn pancake_fee_tiers_include_2500() {
        // Sanity-check the PancakeSwap-specific 0.25% tier is in the table.
        // This is the main thing distinguishing the BSC quoter from a
        // copy-paste of the Uniswap V3 one.
        assert!(PANCAKE_V3_FEE_TIERS.contains(&2_500));
        // And does NOT contain Uniswap's 0.3% tier.
        assert!(!PANCAKE_V3_FEE_TIERS.contains(&3_000));
    }

    #[test]
    fn hex_roundtrip() {
        let raw = vec![0x12, 0xab, 0xcd, 0xef, 0x00, 0xff];
        let s = hex_encode(&raw);
        assert_eq!(s, "12abcdef00ff");
        assert_eq!(hex_decode(&s).unwrap(), raw);
    }
}
