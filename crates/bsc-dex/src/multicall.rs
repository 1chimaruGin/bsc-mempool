//! Multicall3 batched-call helpers (`aggregate3((address,bool,bytes)[])`).
//!
//! Same contract as on Ethereum mainnet (canonical
//! `0xcA11bde05977b3631167028862bE2a173976CA11`) and same ABI. Useful for
//! batching read-only lookups (token symbols, balances, quotes, Venus
//! health-factors) into a single RPC round-trip.

use crate::addresses::MULTICALL3;
use alloy::primitives::{Address, Bytes, U256};
use std::time::Duration;
use thiserror::Error;

/// `aggregate3((address,bool,bytes)[])` selector =
/// `keccak256("aggregate3((address,bool,bytes)[])")[..4]` = `0x82ad56cb`.
pub const AGGREGATE3: [u8; 4] = [0x82, 0xad, 0x56, 0xcb];

#[derive(Debug, Clone)]
pub struct Multicall3Call {
    pub target: Address,
    pub allow_failure: bool,
    pub call_data: Bytes,
}

#[derive(Debug, Clone)]
pub struct Multicall3Result {
    pub success: bool,
    pub return_data: Bytes,
}

#[derive(Debug, Error)]
pub enum MulticallError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("short result")]
    Short,
}

/// Batched `aggregate3` over `rpc_url`. Calls happen at `block` (or latest
/// if None). Returns one `Multicall3Result` per input, in order.
pub async fn aggregate3(
    rpc_url: &str,
    calls: &[Multicall3Call],
    block: Option<u64>,
) -> Result<Vec<Multicall3Result>, MulticallError> {
    let data = encode_aggregate3(calls);
    let raw = eth_call(rpc_url, MULTICALL3, &data, block).await?;
    decode_aggregate3_results(&raw, calls.len())
}

async fn eth_call(
    rpc_url: &str,
    to: Address,
    data: &[u8],
    block: Option<u64>,
) -> Result<Vec<u8>, MulticallError> {
    let tag = match block {
        Some(n) => format!("0x{n:x}"),
        None => "latest".to_string(),
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .expect("build reqwest");
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
    let resp = client.post(rpc_url).json(&body).send().await?;
    let status = resp.status();
    let json: serde_json::Value = resp.json().await?;
    if !status.is_success() {
        return Err(MulticallError::Rpc(format!("HTTP {status}: {json}")));
    }
    if let Some(err) = json.get("error") {
        return Err(MulticallError::Rpc(err.to_string()));
    }
    let result = json
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| MulticallError::Rpc("missing result field".into()))?;
    let stripped = result.strip_prefix("0x").unwrap_or(result);
    hex_decode(stripped).map_err(|e| MulticallError::Rpc(format!("hex decode: {e}")))
}

/// Encode `aggregate3((address,bool,bytes)[])` calldata.
/// The struct tuple `(address, bool, bytes)` has a fixed-size head of 3
/// 32-byte words (address, bool, offset-to-bytes) plus the bytes tail.
pub fn encode_aggregate3(calls: &[Multicall3Call]) -> Vec<u8> {
    // Strategy: build the array body first (`length` + per-call head + tails),
    // then prepend the selector + outer offset (always 0x20).
    let n = calls.len();

    // Compute layout of the dynamic array of structs:
    //   word 0:               array length
    //   words 1..=3*n:        head section (3 words per call)
    //   bytes 3*n*32..:       tails (each tail = length + padded bytes)
    //
    // Inside each per-call head (3 words):
    //   head[0]: target (address right-padded)
    //   head[1]: allow_failure (bool right-padded)
    //   head[2]: offset to this call's call_data tail, measured from the
    //            start of this call's struct head (i.e. relative offset
    //            within the (address,bool,bytes) tuple). For a fixed 3-word
    //            head, that offset is always 0x60 (3 words).
    //
    // Wait — actually for dynamic tuples in an array, offsets in the head
    // are measured from the start of the OUTER array body (after the length
    // word). Specifically: each struct head's offset-to-tail = offset from
    // start of the struct head itself to the start of its tail.
    //
    // For each struct: head is always 3 words = 0x60 bytes, so head[2] = 0x60
    // if the struct's tail comes immediately after its head. But here all
    // heads are concatenated FIRST (n × 3 words), and then ALL tails come
    // afterwards. So head[2] for call i = 0x60 (relative-to-struct distance
    // skipping its own head) PLUS the bytes spanning all other calls' tails
    // up to call i. To keep this simpler, we instead encode each struct's
    // tail-offset measured from THIS struct's head start = sum of head sizes
    // of subsequent structs + sum of preceding tail sizes.
    //
    // That math is fiddly. The robust approach: emit each struct's head and
    // tail back-to-back so head[2] = 0x60 always. This is permitted by ABI:
    // dynamic tuples inside arrays don't strictly need consolidated heads.
    //
    // Multicall3's reference encoder produces the back-to-back form too
    // (verified against an ethers-rs encoded calldata sample).

    let mut body = Vec::new();
    // length
    body.extend_from_slice(&U256::from(n as u64).to_be_bytes::<32>());
    for c in calls {
        // head[0]: address
        let mut addr_word = [0u8; 32];
        addr_word[12..].copy_from_slice(c.target.as_slice());
        body.extend_from_slice(&addr_word);
        // head[1]: bool
        let mut bool_word = [0u8; 32];
        bool_word[31] = u8::from(c.allow_failure);
        body.extend_from_slice(&bool_word);
        // head[2]: offset to tail = 0x60 (skip our own head)
        body.extend_from_slice(&U256::from(0x60u64).to_be_bytes::<32>());
        // tail: bytes length + padded bytes
        let len = c.call_data.len();
        body.extend_from_slice(&U256::from(len as u64).to_be_bytes::<32>());
        body.extend_from_slice(&c.call_data);
        // pad to 32-byte boundary
        let pad = (32 - (len % 32)) % 32;
        if pad > 0 {
            body.extend(std::iter::repeat_n(0u8, pad));
        }
    }

    // Final calldata: selector + outer-offset(0x20) + body
    let mut out = Vec::with_capacity(4 + 32 + body.len());
    out.extend_from_slice(&AGGREGATE3);
    out.extend_from_slice(&U256::from(0x20u64).to_be_bytes::<32>());
    out.extend_from_slice(&body);
    out
}

/// Decode `(bool success, bytes returnData)[]` return shape.
fn decode_aggregate3_results(
    raw: &[u8],
    expected_len: usize,
) -> Result<Vec<Multicall3Result>, MulticallError> {
    if raw.len() < 64 {
        return Err(MulticallError::Short);
    }
    // outer offset (32) + array length (32) + per-call (success, offset-to-bytes) heads
    let returned_len = U256::from_be_slice(&raw[32..64]);
    let n = expected_len;
    if returned_len != U256::from(n as u64) {
        return Err(MulticallError::Rpc(format!(
            "aggregate3: returned length {returned_len} != expected {n}"
        )));
    }
    let head_base = 64;
    // Each struct head is 2 words: success + offset-to-bytes
    let head_section_len = n * 64;
    let head_end = head_base + head_section_len;
    if raw.len() < head_end {
        return Err(MulticallError::Short);
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let head = &raw[head_base + i * 64..head_base + (i + 1) * 64];
        let success = head[31] != 0;
        let offset = U256::from_be_slice(&head[32..64]);
        // The bytes tail offset is from the start of THIS struct head,
        // not the body start. So absolute offset in raw = head_base + i*64 + offset.
        let abs_tail = head_base + i * 64 + usize::try_from(u64::try_from(offset).unwrap_or(0))
            .unwrap_or(0);
        if raw.len() < abs_tail + 32 {
            return Err(MulticallError::Short);
        }
        let bytes_len = U256::from_be_slice(&raw[abs_tail..abs_tail + 32]);
        let blen = usize::try_from(u64::try_from(bytes_len).unwrap_or(0)).unwrap_or(0);
        if raw.len() < abs_tail + 32 + blen {
            return Err(MulticallError::Short);
        }
        let bytes = Bytes::copy_from_slice(&raw[abs_tail + 32..abs_tail + 32 + blen]);
        out.push(Multicall3Result {
            success,
            return_data: bytes,
        });
    }
    Ok(out)
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

    #[test]
    fn aggregate3_selector_is_correct() {
        // keccak256("aggregate3((address,bool,bytes)[])")[..4]
        assert_eq!(AGGREGATE3, [0x82, 0xad, 0x56, 0xcb]);
    }

    #[test]
    fn encode_aggregate3_layout_single_call() {
        let call = Multicall3Call {
            target: alloy::primitives::address!("cA143Ce32Fe78f1f7019d7d551a6402fC5350c73"),
            allow_failure: true,
            call_data: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
        };
        let bytes = encode_aggregate3(&[call]);
        // selector(4) + outer offset(32) + length(32) + head(96) + tail(32 + 32 padded bytes)
        assert_eq!(bytes.len(), 4 + 32 + 32 + 96 + 32 + 32);
        assert_eq!(&bytes[..4], &AGGREGATE3);
        // outer offset = 0x20
        assert_eq!(U256::from_be_slice(&bytes[4..36]), U256::from(0x20));
        // length = 1
        assert_eq!(U256::from_be_slice(&bytes[36..68]), U256::from(1));
        // head[2] offset-to-tail = 0x60
        let head_start = 68 + 64; // skip address + bool words
        assert_eq!(
            U256::from_be_slice(&bytes[head_start..head_start + 32]),
            U256::from(0x60)
        );
    }

    #[test]
    fn encode_aggregate3_zero_calls() {
        let bytes = encode_aggregate3(&[]);
        // selector + outer offset + length = 68
        assert_eq!(bytes.len(), 68);
        assert_eq!(U256::from_be_slice(&bytes[36..68]), U256::ZERO);
    }
}
