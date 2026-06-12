//! Decide what action to take on a KOL hit (BSC).
//!
//! Differs from the ETH stack's strategy in one structural way: there's no
//! kol_confirm watcher yet, so we only see MEMPOOL-mode hits with the raw
//! tx envelope — no receipt, no decoded swap shape. To still classify
//! BUYs and extract the target token, we decode the calldata of the
//! common PancakeSwap V2 BNB-paying swap functions:
//!
//! - `swapExactETHForTokens(uint256,address[],address,uint256)` — selector 0x7ff36ab5
//! - `swapETHForExactTokens(uint256,address[],address,uint256)`  — selector 0xfb3bdb41
//! - `swapExactETHForTokensSupportingFeeOnTransferTokens(...)`   — selector 0xb6f9de95
//!
//! ("ETH" here is the original Uniswap V2 ABI; on BSC these route native
//! BNB through WBNB. The function names persisted across the fork.)
//!
//! Each carries an `address[] path` argument where:
//!   - `path[0]` = WBNB (the BNB-wrapper entry point)
//!   - `path[N-1]` = the actual target token
//!
//! For V3 SmartRouter / GMGN aggregator / 1inch / 0x — calldata decoding is
//! per-router and deferred. The trader will simply skip those hits in Day-3
//! and we'll widen coverage later as we observe real KOL routing patterns.

use crate::kol_watch::KolHit;
use crate::trader::types::{Decision, Side, SkipReason};
use alloy::primitives::{Address, B256, U256};
use bsc_dex::addresses::WBNB;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    /// Only act on these KOL names (matches `KolHit.kol_name`). Empty = all.
    /// User scope: just `["D"]`.
    pub kol_filter: Vec<String>,
    /// Minimum BUY size in **USD** to trigger entry. Computed as
    /// `value_bnb * bnb_usd`. Set to 0.0 to disable (BNB gate only).
    pub min_buy_usd: f64,
    /// Minimum BUY size in **BNB** to trigger entry. Set to 0.0 to disable
    /// (USD gate only). Use this when you want a price-stable size floor
    /// (KOLs trade in BNB, so BNB-denominated rules don't drift).
    pub min_buy_bnb: f64,
    /// Bot trade size as a fraction of KOL's input. 0.05 = 5%.
    pub size_fraction: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            kol_filter: vec!["D".to_string()],
            min_buy_usd: 400.0,
            min_buy_bnb: 0.0,
            size_fraction: 0.05,
        }
    }
}

pub struct Strategy {
    cfg: StrategyConfig,
}

impl Strategy {
    pub fn new(cfg: StrategyConfig) -> Self {
        Self { cfg }
    }

    /// Decide on a KOL hit. `bnb_usd` is the live BNB/USD price for the
    /// USD-denominated entry gate (fetched + cached by the consumer).
    ///
    /// Uses the GMGN-decoded `hit.side` / `hit.token` directly — no
    /// calldata re-decode. Produces:
    ///   - `Enter` on a filtered KOL's BUY ≥ `min_buy_usd`
    ///   - `Exit`  on a filtered KOL's SELL of any token (the executor
    ///     closes only if we actually hold it)
    pub fn evaluate(&self, hit: &KolHit, bnb_usd: f64) -> Decision {
        // KOL scope filter.
        if !self.cfg.kol_filter.is_empty()
            && !self.cfg.kol_filter.iter().any(|k| k == &hit.kol_name)
        {
            return Decision::Skip { reason: SkipReason::NotKolHit };
        }

        let tx_hash = match B256::from_str(&hit.tx_hash) {
            Ok(h) => h,
            Err(_) => return Decision::Skip { reason: SkipReason::UnknownToken },
        };

        // Need a GMGN-decoded token + side. Non-swap (approve/transfer) or
        // unrecognised router → no token/side → skip.
        let Some(token) = hit.token else {
            return Decision::Skip { reason: SkipReason::NotASwap };
        };

        match hit.side {
            Some("SELL") => {
                // Exit signal: the executor closes iff we hold this token.
                // `kol_block` is populated from the confirmed-block field
                // when this hit comes from kol_confirm — the paper trader
                // needs it to query balanceOf at sell_block-1 vs sell_block
                // to compute the KOL's actual sell fraction (proportional
                // exits, not all-or-nothing).
                let kol_addr = Address::from_str(&hit.from_addr).unwrap_or(Address::ZERO);
                Decision::Exit {
                    kol_name: hit.kol_name.clone(),
                    token,
                    kol_block: hit.confirmed_block.unwrap_or(0),
                    kol_tx: tx_hash,
                    kol_addr,
                }
            }
            Some("BUY") => {
                if hit.value_bnb <= 0.0 {
                    return Decision::Skip { reason: SkipReason::UnsupportedSide };
                }
                // Two-gate check; either can be disabled by setting to 0.
                if self.cfg.min_buy_bnb > 0.0 && hit.value_bnb < self.cfg.min_buy_bnb {
                    return Decision::Skip {
                        reason: SkipReason::BelowBuyThreshold,
                    };
                }
                if self.cfg.min_buy_usd > 0.0 {
                    let usd = hit.value_bnb * bnb_usd;
                    if usd < self.cfg.min_buy_usd {
                        return Decision::Skip {
                            reason: SkipReason::BelowBuyThreshold,
                        };
                    }
                }
                let kol_bnb = U256::from((hit.value_bnb * 1e18) as u128);
                let our_bnb = scale_size(kol_bnb, self.cfg.size_fraction);
                Decision::Enter {
                    kol_name: hit.kol_name.clone(),
                    token,
                    bnb_amount: our_bnb,
                    kol_bnb_input: kol_bnb,
                    kol_block: 0, // mempool mode — executor fills from head
                    kol_tx: tx_hash,
                }
            }
            _ => Decision::Skip {
                reason: SkipReason::UnsupportedSide,
            },
        }
    }
}

// =============================================================================
// Calldata path extraction (PancakeSwap V2 BNB-paying swaps)
// =============================================================================

/// Decode `path[last]` from PancakeSwap V2 calldata. Returns None for any
/// selector we don't recognise, or for malformed calldata.
///
/// Layout for `swapExactETHForTokens(uint256,address[],address,uint256)`:
///   bytes 0..4    : selector
///   bytes 4..36   : amountOutMin (uint256)
///   bytes 36..68  : offset to path (uint256, typically 0x80)
///   bytes 68..100 : to (address, right-padded)
///   bytes 100..132: deadline (uint256)
///   bytes 132..164: path.length (uint256)
///   bytes 164..164+32*N: path entries (each address right-padded)
pub fn extract_target_token(calldata: &[u8]) -> Option<Address> {
    if calldata.len() < 4 + 32 * 5 + 32 {
        return None;
    }
    let selector = &calldata[..4];

    // Whitelisted V2 BNB-paying selectors. The encoded path layout is the
    // same for all three.
    let supported = matches!(
        selector,
        [0x7f, 0xf3, 0x6a, 0xb5]      // swapExactETHForTokens
            | [0xfb, 0x3b, 0xdb, 0x41] // swapETHForExactTokens
            | [0xb6, 0xf9, 0xde, 0x95] // swapExactETHForTokensSupportingFeeOnTransferTokens
    );
    if !supported {
        return None;
    }

    // Read path length from word at position 132 (after selector + 4 head words).
    let path_len_word: [u8; 32] = calldata[132..164].try_into().ok()?;
    let path_len = U256::from_be_bytes(path_len_word);
    if path_len < U256::from(2u64) || path_len > U256::from(8u64) {
        return None; // implausible
    }
    let n: usize = u64::try_from(path_len).ok()?.try_into().ok()?;

    // path[N-1] sits at byte 164 + 32 * (N-1)
    let last_addr_start = 164 + 32 * (n - 1);
    let last_addr_end = last_addr_start + 32;
    if calldata.len() < last_addr_end {
        return None;
    }
    // Address is in the LAST 20 bytes of the 32-byte word.
    let word = &calldata[last_addr_start..last_addr_end];
    let mut buf = [0u8; 20];
    buf.copy_from_slice(&word[12..32]);
    let addr = Address::from(buf);

    // Sanity: don't return WBNB as the "target" — that means it was the
    // first hop, not the destination. If WBNB is the last entry, the
    // calldata is anomalous (token→BNB shouldn't use this selector).
    if addr == WBNB {
        return None;
    }
    Some(addr)
}

fn scale_size(kol_input: U256, fraction: f64) -> U256 {
    if fraction <= 0.0 {
        return U256::ZERO;
    }
    let permille = ((fraction * 1000.0).round() as u128).min(1000);
    kol_input.saturating_mul(U256::from(permille)) / U256::from(1000u128)
}

#[allow(dead_code)]
fn _classify_side_from_value(value_bnb: f64) -> Side {
    // For Day-3 BSC port: only BUYs are actionable from mempool data.
    // Kept as a forward-looking helper.
    if value_bnb > 0.0 {
        Side::Buy
    } else {
        Side::Sell
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kol_watch::KolHit;
    use alloy::primitives::Address;

    // GMGN-decoded hit (the real shape now): side + token populated.
    fn gmgn_hit(name: &str, bnb: f64, side: Option<&'static str>) -> KolHit {
        let tok: Address = "0x1234567890aBcdEF1234567890aBcdef12345678"
            .parse()
            .unwrap();
        KolHit {
            kol_name: name.into(),
            kol_emoji: None,
            kol_groups: vec!["GOAT".into()],
            tx_hash: format!("{:#x}", B256::ZERO),
            from_addr: format!("{:#x}", Address::ZERO),
            to_addr: None,
            to_label: Some("GMGN proxy"),
            method_id: "0x4d819a2a".into(),
            method_label: Some("GMGN router (flap/Four.Meme)"),
            value_bnb: bnb,
            gas_price_gwei: 1.0,
            gas_limit: 600_000,
            nonce: 1,
            source_seen: "(local_ipc,1)".into(),
            calldata: Vec::new(),
            decoded: None,
            token: side.map(|_| tok),
            side,
            confirmed_block: None,
            visibility: "public",
        }
    }

    // BNB ≈ $600 in these tests; min_buy_usd default = 400.
    const BNB_USD: f64 = 600.0;

    #[test]
    fn rejects_below_usd_threshold() {
        let strat = Strategy::new(StrategyConfig::default());
        // 0.1 BNB * $600 = $60 < $400
        assert!(matches!(
            strat.evaluate(&gmgn_hit("D", 0.1, Some("BUY")), BNB_USD),
            Decision::Skip { reason: SkipReason::BelowBuyThreshold }
        ));
    }

    #[test]
    fn accepts_above_usd_threshold() {
        let strat = Strategy::new(StrategyConfig::default());
        // 1.0 BNB * $600 = $600 ≥ $400
        match strat.evaluate(&gmgn_hit("D", 1.0, Some("BUY")), BNB_USD) {
            Decision::Enter {
                bnb_amount,
                kol_bnb_input,
                ..
            } => {
                assert_eq!(kol_bnb_input, U256::from(1_000_000_000_000_000_000u128));
                assert_eq!(bnb_amount, U256::from(50_000_000_000_000_000u128)); // 5%
            }
            other => panic!("expected Enter, got {other:?}"),
        }
    }

    #[test]
    fn other_kol_filtered_out() {
        let strat = Strategy::new(StrategyConfig::default()); // filter = ["D"]
        assert!(matches!(
            strat.evaluate(&gmgn_hit("A", 5.0, Some("BUY")), BNB_USD),
            Decision::Skip { reason: SkipReason::NotKolHit }
        ));
    }

    #[test]
    fn non_swap_skipped() {
        let strat = Strategy::new(StrategyConfig::default());
        // approve/transfer → gmgn decode gave no token/side
        assert!(matches!(
            strat.evaluate(&gmgn_hit("D", 1.0, None), BNB_USD),
            Decision::Skip { reason: SkipReason::NotASwap }
        ));
    }

    #[test]
    fn sell_emits_exit() {
        let strat = Strategy::new(StrategyConfig::default());
        match strat.evaluate(&gmgn_hit("D", 0.0, Some("SELL")), BNB_USD) {
            Decision::Exit { kol_name, .. } => assert_eq!(kol_name, "D"),
            other => panic!("expected Exit, got {other:?}"),
        }
    }

    #[test]
    fn buy_blocked_when_price_unavailable() {
        // Fail-closed: bnb_usd=0 → any BUY is below the USD gate.
        let strat = Strategy::new(StrategyConfig::default());
        assert!(matches!(
            strat.evaluate(&gmgn_hit("D", 100.0, Some("BUY")), 0.0),
            Decision::Skip { reason: SkipReason::BelowBuyThreshold }
        ));
    }

    #[test]
    fn extract_token_from_v2_buy_calldata() {
        // Build a synthetic swapExactETHForTokens calldata with path [WBNB, TARGET]
        let target: Address = "0x1234567890aBcdEF1234567890aBcdef12345678"
            .parse()
            .unwrap();
        let mut data = Vec::with_capacity(228);
        data.extend_from_slice(&[0x7f, 0xf3, 0x6a, 0xb5]); // selector
        // amountOutMin
        data.extend_from_slice(&U256::from(0u64).to_be_bytes::<32>());
        // offset to path array = 0x80 (after the 4 fixed-head words)
        data.extend_from_slice(&U256::from(0x80u64).to_be_bytes::<32>());
        // to (recipient)
        data.extend_from_slice(&[0u8; 12]);
        data.extend_from_slice(Address::ZERO.as_slice());
        // deadline
        data.extend_from_slice(&U256::from(0u64).to_be_bytes::<32>());
        // path.length = 2
        data.extend_from_slice(&U256::from(2u64).to_be_bytes::<32>());
        // path[0] = WBNB
        data.extend_from_slice(&[0u8; 12]);
        data.extend_from_slice(WBNB.as_slice());
        // path[1] = TARGET
        data.extend_from_slice(&[0u8; 12]);
        data.extend_from_slice(target.as_slice());

        let got = extract_target_token(&data);
        assert_eq!(got, Some(target));
    }

    #[test]
    fn extract_token_rejects_when_wbnb_at_last() {
        // Anomalous path [TARGET, WBNB] — this would be a SELL pattern, not
        // the BNB-paying BUY selector's normal usage. We refuse to interpret.
        let mut data = Vec::new();
        data.extend_from_slice(&[0x7f, 0xf3, 0x6a, 0xb5]);
        for _ in 0..4 {
            data.extend_from_slice(&[0u8; 32]);
        }
        data.extend_from_slice(&U256::from(2u64).to_be_bytes::<32>());
        let target: Address =
            "0x1111111111111111111111111111111111111111".parse().unwrap();
        data.extend_from_slice(&[0u8; 12]);
        data.extend_from_slice(target.as_slice());
        data.extend_from_slice(&[0u8; 12]);
        data.extend_from_slice(WBNB.as_slice());

        let got = extract_target_token(&data);
        assert_eq!(got, None);
    }

    #[test]
    fn extract_token_rejects_unknown_selector() {
        let mut data = vec![0xde, 0xad, 0xbe, 0xef];
        data.extend_from_slice(&vec![0u8; 200]);
        assert_eq!(extract_target_token(&data), None);
    }
}
