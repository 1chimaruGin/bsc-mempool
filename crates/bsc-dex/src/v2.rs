//! PancakeSwap V2 quoter — hand-rolled `getAmountsOut(uint256, address[])`.
//!
//! V2 is the dominant DEX on BSC by volume (April–May 2026 routinely shows
//! V2 carrying ~70-80% of PancakeSwap's swap throughput; V3 is growing but
//! V2's liquidity depth on memecoins is what makes BSC sniping viable for
//! a $50-ticket operator). So we wire V2 first; V3 is Day-2B.
//!
//! ## Selector
//!
//! `getAmountsOut(uint256,address[])` =
//!   `keccak256("getAmountsOut(uint256,address[])")[..4]` = `0xd06ca61f`.
//!
//! Returns `uint256[]` — one entry per node in the path. For a single-hop
//! quote (`[WBNB, TOKEN]`) the result has length 2; we return `amounts[1]`
//! as the destination-token output for `amount_in` of source token.
//!
//! For now we only support single-hop quotes (WBNB ↔ token) because:
//! 1. 90%+ of KOL paper-trade hits will be WBNB-routed swaps on BSC
//! 2. Multi-hop adds a path-finding step that benefits from the V3 SmartRouter
//!    instead. Defer until V3 is wired.

use crate::addresses::PANCAKE_V2_ROUTER;
use alloy::primitives::{Address, U256};
use std::time::Duration;
use thiserror::Error;

/// Selector for `getAmountsOut(uint256,address[])`.
pub const GET_AMOUNTS_OUT: [u8; 4] = [0xd0, 0x6c, 0xa6, 0x1f];

#[derive(Debug, Error)]
pub enum V2QuoteError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("short result ({0} bytes); expected >= 96")]
    ShortResult(usize),
    #[error("path too short ({0} hops); need at least 2")]
    BadPath(usize),
}

#[derive(Clone)]
pub struct V2Quoter {
    rpc_url: String,
    client: reqwest::Client,
}

impl V2Quoter {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("build reqwest client"),
        }
    }

    /// Single-hop quote: how much `dst` you get for `amount_in` of `src`.
    /// Calls PancakeSwap V2 Router's `getAmountsOut` with a 2-element path.
    pub async fn quote_single_hop(
        &self,
        amount_in: U256,
        src: Address,
        dst: Address,
        block: Option<u64>,
    ) -> Result<U256, V2QuoteError> {
        self.quote_path(amount_in, &[src, dst], block).await
    }

    /// Multi-hop quote. `path[0]` is the source token, `path[N-1]` is the
    /// destination, intermediate entries are the hop tokens. Returns the
    /// final output (`amounts[N-1]`).
    pub async fn quote_path(
        &self,
        amount_in: U256,
        path: &[Address],
        block: Option<u64>,
    ) -> Result<U256, V2QuoteError> {
        if path.len() < 2 {
            return Err(V2QuoteError::BadPath(path.len()));
        }
        let calldata = encode_get_amounts_out(amount_in, path);
        let raw = self.eth_call(PANCAKE_V2_ROUTER, &calldata, block).await?;
        decode_amounts_last(&raw, path.len())
    }

    async fn eth_call(
        &self,
        to: Address,
        data: &[u8],
        block: Option<u64>,
    ) -> Result<Vec<u8>, V2QuoteError> {
        let tag = match block {
            Some(n) => format!("0x{n:x}"),
            None => "latest".to_string(),
        };
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": format!("{to:#x}"),
                    "data": format!("0x{}", hex_encode(data))
                },
                tag
            ]
        });
        let resp = self.client.post(&self.rpc_url).json(&body).send().await?;
        let status = resp.status();
        let json: serde_json::Value = resp.json().await?;
        if !status.is_success() {
            return Err(V2QuoteError::Rpc(format!("HTTP {status}: {json}")));
        }
        if let Some(err) = json.get("error") {
            return Err(V2QuoteError::Rpc(err.to_string()));
        }
        let result = json
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| V2QuoteError::Rpc("missing result field".into()))?;
        let stripped = result.strip_prefix("0x").unwrap_or(result);
        hex_decode(stripped).map_err(|e| V2QuoteError::Rpc(format!("hex decode: {e}")))
    }
}

// ───── ABI encode / decode ─────

/// ABI-encode `getAmountsOut(uint256, address[])`. The array offset is fixed
/// at 0x40 (one word past the uint256 + the offset itself).
pub fn encode_get_amounts_out(amount_in: U256, path: &[Address]) -> Vec<u8> {
    let path_len_words: usize = 1 + path.len(); // length word + N address words
    let mut buf = Vec::with_capacity(4 + 32 + 32 + path_len_words * 32);
    buf.extend_from_slice(&GET_AMOUNTS_OUT);
    // word 0: amount_in
    buf.extend_from_slice(&amount_in.to_be_bytes::<32>());
    // word 1: offset to address[] data — 0x40 = 64 bytes from start of args
    let offset: U256 = U256::from(0x40);
    buf.extend_from_slice(&offset.to_be_bytes::<32>());
    // address[] data: length, then each address right-padded into 32 bytes
    let len: U256 = U256::from(path.len() as u64);
    buf.extend_from_slice(&len.to_be_bytes::<32>());
    for addr in path {
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(addr.as_slice());
        buf.extend_from_slice(&word);
    }
    buf
}

/// Decode the LAST element of the returned `uint256[]`. Result layout:
///   word 0:  offset (always 0x20)
///   word 1:  array length (== `expected_len`)
///   word 2:  amounts[0]
///   word 3:  amounts[1]
///   …
fn decode_amounts_last(raw: &[u8], expected_len: usize) -> Result<U256, V2QuoteError> {
    let min = 32 + 32 + expected_len * 32; // offset + length + N entries
    if raw.len() < min {
        return Err(V2QuoteError::ShortResult(raw.len()));
    }
    // Verify length word.
    let returned_len = U256::from_be_slice(&raw[32..64]);
    if returned_len != U256::from(expected_len as u64) {
        return Err(V2QuoteError::Rpc(format!(
            "unexpected returned-array length: got {returned_len}, expected {expected_len}"
        )));
    }
    let last_offset = 32 + 32 + (expected_len - 1) * 32;
    Ok(U256::from_be_slice(&raw[last_offset..last_offset + 32]))
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

    #[test]
    fn selector_is_correct() {
        // keccak256("getAmountsOut(uint256,address[])")[..4]
        assert_eq!(GET_AMOUNTS_OUT, [0xd0, 0x6c, 0xa6, 0x1f]);
    }

    #[test]
    fn encode_single_hop_layout() {
        let amt = U256::from(1_000_000_000_000_000_000u128); // 1 BNB
        let bytes = encode_get_amounts_out(amt, &[WBNB, USDT]);
        // selector(4) + amount(32) + offset(32) + length(32) + 2 addrs(64) = 164
        assert_eq!(bytes.len(), 164);
        assert_eq!(&bytes[..4], &GET_AMOUNTS_OUT);
        // amount_in is in word 0 of args (right-aligned)
        assert_eq!(U256::from_be_slice(&bytes[4..36]), amt);
        // offset word = 0x40
        assert_eq!(U256::from_be_slice(&bytes[36..68]), U256::from(0x40));
        // length = 2
        assert_eq!(U256::from_be_slice(&bytes[68..100]), U256::from(2));
        // first address right-padded
        assert_eq!(&bytes[100 + 12..100 + 32], WBNB.as_slice());
    }

    #[test]
    fn encode_multi_hop_layout_lengths() {
        let amt = U256::from(1u64);
        let path: &[Address] = &[WBNB, USDT, WBNB];
        let bytes = encode_get_amounts_out(amt, path);
        // selector(4) + amount(32) + offset(32) + length(32) + 3 addrs(96) = 196
        assert_eq!(bytes.len(), 196);
        assert_eq!(U256::from_be_slice(&bytes[68..100]), U256::from(3));
    }

    #[test]
    fn decode_extracts_last_amount_two_hop() {
        // Build a synthetic return: offset(0x20), length(2), amounts[0]=A, amounts[1]=B
        let a = U256::from(123u64);
        let b = U256::from(999u64);
        let mut raw = Vec::new();
        raw.extend_from_slice(&U256::from(0x20u64).to_be_bytes::<32>());
        raw.extend_from_slice(&U256::from(2u64).to_be_bytes::<32>());
        raw.extend_from_slice(&a.to_be_bytes::<32>());
        raw.extend_from_slice(&b.to_be_bytes::<32>());
        let got = decode_amounts_last(&raw, 2).unwrap();
        assert_eq!(got, b);
    }

    #[test]
    fn decode_three_hop() {
        let a = U256::from(1u64);
        let b = U256::from(2u64);
        let c = U256::from(42u64);
        let mut raw = Vec::new();
        raw.extend_from_slice(&U256::from(0x20u64).to_be_bytes::<32>());
        raw.extend_from_slice(&U256::from(3u64).to_be_bytes::<32>());
        raw.extend_from_slice(&a.to_be_bytes::<32>());
        raw.extend_from_slice(&b.to_be_bytes::<32>());
        raw.extend_from_slice(&c.to_be_bytes::<32>());
        let got = decode_amounts_last(&raw, 3).unwrap();
        assert_eq!(got, c);
    }

    #[test]
    fn decode_rejects_short_result() {
        let raw = vec![0u8; 16];
        let err = decode_amounts_last(&raw, 2);
        assert!(matches!(err, Err(V2QuoteError::ShortResult(_))));
    }

    #[test]
    fn quote_requires_path_len_ge_2() {
        // Synthetic Quoter; URL is never hit because BadPath fires first.
        let q = V2Quoter::new("http://127.0.0.1:0");
        let result = futures_blocking_quote(&q, &[WBNB]);
        assert!(matches!(result, Err(V2QuoteError::BadPath(1))));
    }

    fn futures_blocking_quote(q: &V2Quoter, path: &[Address]) -> Result<U256, V2QuoteError> {
        // Tiny tokio current-thread runtime just for one synchronous call in a
        // test. Avoids requiring `tokio::test` everywhere.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(q.quote_path(U256::from(1u64), path, None))
    }

    #[test]
    fn hex_roundtrip() {
        let raw = vec![0x12, 0xab, 0xcd, 0xef, 0x00, 0xff];
        let s = hex_encode(&raw);
        assert_eq!(s, "12abcdef00ff");
        assert_eq!(hex_decode(&s).unwrap(), raw);
    }
}
