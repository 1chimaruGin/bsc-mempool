//! Decoder for the GMGN universal router method `0x4d819a2a` on BSC.
//!
//! GMGN's router (`0x1de460f363AF910f51726DEf188F9004276Bf4bc`) multiplexes
//! flap-AMM and Four.Meme buys/sells through one selector with a non-fixed
//! ABI (the swap path is a variable-length array whose offset differs per
//! venue). Rather than model every sub-encoding we extract the traded token
//! with a structural heuristic that is stable across all observed variants:
//!
//!   A 32-byte word is a token address iff
//!     • bytes[0..12]  are all zero      (left-padded address slot), AND
//!     • bytes[12..16] are NOT all zero  (a real 20-byte address has its
//!       high bytes set; lengths/offsets/deadlines/amounts occupy only the
//!       low bytes and fail this test)
//!   excluding WBNB and the GMGN router itself. The FIRST such word is the
//!   traded token (path[0]=WBNB on buys, path[0]=token on sells — either way
//!   the first non-WBNB address is the meme token of interest).
//!
//! Buy vs sell is taken from msg.value: native BNB attached ⇒ BUY
//! (BNB→token), zero value ⇒ SELL (token→BNB). Verified empirically over
//! the live GOAT-wallet flow.

use alloy::primitives::{Address, U256, address};

/// GMGN universal router selector (BSC).
pub const GMGN_SELECTOR: [u8; 4] = [0x4d, 0x81, 0x9a, 0x2a];

/// WBNB — always the BNB leg of a GMGN path; never the token of interest.
const WBNB: Address = address!("bb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c");

/// GMGN router itself — can appear in calldata; never the token.
const GMGN_ROUTER: Address = address!("1de460f363af910f51726def188f9004276bf4bc");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GmgnSwap {
    pub token: Address,
    pub side: Side,
}

/// True if `input` is a GMGN `0x4d819a2a` call.
pub fn is_gmgn(input: &[u8]) -> bool {
    input.len() >= 4 && input[..4] == GMGN_SELECTOR
}

/// Decode the traded token + side from a GMGN router call.
///
/// `value` is the tx's msg.value (wei). Returns `None` if the selector does
/// not match or no token-looking word is present.
pub fn decode(input: &[u8], value: U256) -> Option<GmgnSwap> {
    if !is_gmgn(input) {
        return None;
    }
    let body = &input[4..];
    let mut token: Option<Address> = None;
    for word in body.chunks_exact(32) {
        // Left 12 bytes must be zero (address slot is right-aligned in 32B).
        if word[0..12].iter().any(|&b| b != 0) {
            continue;
        }
        // High 4 bytes of the 20-byte address must be non-zero — this is
        // what separates a real address from a small int (length, offset,
        // deadline, amount) that only occupies the low bytes.
        if word[12..16].iter().all(|&b| b == 0) {
            continue;
        }
        let addr = Address::from_slice(&word[12..32]);
        if addr == WBNB || addr == GMGN_ROUTER {
            continue;
        }
        token = Some(addr);
        break;
    }
    let token = token?;
    let side = if value > U256::ZERO {
        Side::Buy
    } else {
        Side::Sell
    };
    Some(GmgnSwap { token, side })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real BSC tx 0xf81fcf34… — GMGN→flap BUY, 1.5 BNB, token a0431c18….
    const BUY_FLAP: &str = "0x4d819a2a00000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014d1120d7b1600000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006a071f80000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000003000000000000000000000000bb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c000000000000000000000000a0431c1870070d3c6b751450db226a86b34388880000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000215600000000000000000000000000000000000000000000000000000000000000fa000000000000000000000000f1a9aa042454b8553be3896597ff11a0f011c1c10000000000000000000000000000000000000000000000000000000000000140000000000000000000000000a0ffb9c1ce1fe56963b0321b32e7a0302114058b0000000000000000000000000000000000000000000000000000000000fa00d50000000000000000000000000000000000000000000000000000000000000000";

    // Real BSC tx 0xe0299b00… — GMGN→Four.Meme BUY, 0.25 BNB, token 8d21ce49….
    const BUY_FOURMEME: &str = "0x4d819a2a00000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003782dace9d900000000000000000000000000000000000000000000000008b1ddaf44d3db04b1aa000000000000000000000000000000000000000000000000000000006a070bd200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000000000000000000000000000008d21ce4993d4aab34446c4716575e02af2e944440000000000000000000000008d21ce4993d4aab34446c4716575e02af2e9444400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000064000000000000000000000000b8159ba378904f803639d274cec79f788931c9c8";

    fn bytes(hex: &str) -> Vec<u8> {
        let h = hex.strip_prefix("0x").unwrap_or(hex);
        (0..h.len() / 2)
            .map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn decodes_flap_buy() {
        let g = decode(&bytes(BUY_FLAP), U256::from(1_500_000_000_000_000_000u64)).unwrap();
        assert_eq!(g.side, Side::Buy);
        assert_eq!(
            g.token,
            address!("a0431c1870070d3c6b751450db226a86b3438888")
        );
    }

    #[test]
    fn decodes_fourmeme_buy() {
        let g = decode(&bytes(BUY_FOURMEME), U256::from(250_000_000_000_000_000u64)).unwrap();
        assert_eq!(g.side, Side::Buy);
        assert_eq!(
            g.token,
            address!("8d21ce4993d4aab34446c4716575e02af2e94444")
        );
    }

    #[test]
    fn zero_value_is_sell() {
        // Same calldata, zero value ⇒ classified SELL (token→BNB).
        let g = decode(&bytes(BUY_FLAP), U256::ZERO).unwrap();
        assert_eq!(g.side, Side::Sell);
        assert_eq!(
            g.token,
            address!("a0431c1870070d3c6b751450db226a86b3438888")
        );
    }

    #[test]
    fn rejects_non_gmgn_selector() {
        assert!(decode(&[0x7f, 0xf3, 0x6a, 0xb5], U256::ZERO).is_none());
        assert!(decode(&[], U256::ZERO).is_none());
    }

    #[test]
    fn small_ints_are_not_addresses() {
        // selector + a word that is just the int 0x2156 (would be a false
        // positive without the high-4-bytes guard) then a real token.
        let mut inp = GMGN_SELECTOR.to_vec();
        let mut small = [0u8; 32];
        small[31] = 0x56;
        small[30] = 0x21;
        inp.extend_from_slice(&small); // 0x2156 — must be skipped
        let mut tok = [0u8; 32];
        let t = address!("1234567890abcdef1234567890abcdef12345678");
        tok[12..32].copy_from_slice(t.as_slice());
        inp.extend_from_slice(&tok);
        let g = decode(&inp, U256::from(1u64)).unwrap();
        assert_eq!(g.token, t);
        assert_eq!(g.side, Side::Buy);
    }
}
