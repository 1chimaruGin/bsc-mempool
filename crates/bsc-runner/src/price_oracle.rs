//! Phase 3 — price / liquidity oracle for held tokens.
//!
//! Every `POLL` seconds, for each token in `HeldTokens`:
//!   1. resolve its PancakeSwap-V2 WBNB pair (`factory.getPair`)
//!   2. read `getReserves()`
//!   3. derive price (BNB per token) and pool BNB-side liquidity
//!   4. log it; if pool BNB liquidity drops ≥ `RUG_DROP` vs the previous
//!      poll → emit a loud LIQUIDITY-DROP (rug) warning
//!
//! V2 only: flap / Four.Meme tokens graduate to a V2 pair; pre-graduation
//! (no pair yet) we just note "no V2 pool". V3-only tokens are out of
//! scope for this first cut (slot0/tick math deferred).

use crate::held_tokens::HeldTokens;
use alloy::primitives::Address;
use bsc_dex::addresses::{PANCAKE_V2_FACTORY, WBNB};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const POLL: Duration = Duration::from_secs(15);
/// Fraction drop in pool BNB liquidity between polls that triggers a rug
/// warning (0.5 = lost ≥50% of BNB-side liquidity).
const RUG_DROP: f64 = 0.5;

const GET_PAIR: &str = "0xe6a43905"; // getPair(address,address)
const GET_RESERVES: &str = "0x0902f1ac"; // getReserves()
const TOKEN0: &str = "0x0dfe1681"; // token0()

pub fn start(held: Arc<HeldTokens>, rpc_url: String, shutdown: CancellationToken) {
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .expect("reqwest");
        // token -> previous pool BNB liquidity (wei, f64) for drop detection.
        let mut prev_liq: HashMap<Address, f64> = HashMap::new();
        tracing::info!(target: "priceoracle", "price/liquidity oracle up (Phase 3)");
        let mut tick = tokio::time::interval(POLL);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::info!(target: "priceoracle", "oracle shutdown");
                    return;
                }
                _ = tick.tick() => {
                    let toks = held.addresses();
                    if toks.is_empty() { continue; }
                    for t in toks {
                        poll_one(&client, &rpc_url, &held, t, &mut prev_liq).await;
                    }
                }
            }
        }
    });
}

async fn eth_call(c: &reqwest::Client, url: &str, to: &str, data: String) -> Option<String> {
    let body = serde_json::json!({
        "jsonrpc":"2.0","method":"eth_call",
        "params":[{"to":to,"data":data},"latest"],"id":1});
    let v: serde_json::Value =
        c.post(url).json(&body).send().await.ok()?.json().await.ok()?;
    v.get("result")?.as_str().map(|s| s.to_string())
}

fn addr_arg(a: Address) -> String {
    format!("{:0>64}", &format!("{a:x}"))
}

async fn poll_one(
    c: &reqwest::Client,
    url: &str,
    held: &HeldTokens,
    token: Address,
    prev_liq: &mut HashMap<Address, f64>,
) {
    let Some(meta) = held.get(&token) else { return };

    // factory.getPair(WBNB, token)
    let data = format!("{GET_PAIR}{}{}", addr_arg(WBNB), addr_arg(token));
    let pair = match eth_call(c, url, &format!("{PANCAKE_V2_FACTORY:#x}"), data).await {
        Some(h) if h.len() >= 66 => {
            let a = Address::from_slice(
                &hex_to_bytes(&h[h.len() - 40..]).unwrap_or_default(),
            );
            a
        }
        _ => return,
    };
    if pair == Address::ZERO {
        tracing::info!(
            target: "priceoracle",
            symbol = %meta.symbol, token = %format!("{token:#x}"),
            "no V2 pool yet (pre-graduation / V3-only)"
        );
        return;
    }

    // pair.token0() to orient reserves
    let t0 = match eth_call(c, url, &format!("{pair:#x}"), TOKEN0.into()).await {
        Some(h) if h.len() >= 66 => Address::from_slice(
            &hex_to_bytes(&h[h.len() - 40..]).unwrap_or_default(),
        ),
        _ => return,
    };
    // getReserves() -> (uint112 r0, uint112 r1, uint32 ts)
    let res = match eth_call(c, url, &format!("{pair:#x}"), GET_RESERVES.into()).await {
        Some(h) if h.len() >= 2 + 64 * 2 => h,
        _ => return,
    };
    let r0 = u128_from(&res[2..66]);
    let r1 = u128_from(&res[66..130]);
    let (wbnb_res, tok_res) = if t0 == WBNB { (r0, r1) } else { (r1, r0) };
    if wbnb_res == 0.0 || tok_res == 0.0 {
        return;
    }
    // price = BNB per token (raw reserve ratio; both 18-dec on BSC mainnet
    // for WBNB; token decimals vary but this is good for relative tracking)
    let price_bnb = wbnb_res / tok_res;

    let drop = prev_liq.get(&token).map(|&p| {
        if p > 0.0 { (p - wbnb_res) / p } else { 0.0 }
    });
    prev_liq.insert(token, wbnb_res);

    if let Some(d) = drop {
        if d >= RUG_DROP {
            tracing::warn!(
                target: "priceoracle",
                symbol = %meta.symbol, token = %format!("{token:#x}"),
                drop_pct = format!("{:.0}%", d * 100.0),
                pool_bnb = wbnb_res / 1e18,
                "🚨 LIQUIDITY DROP — possible rug / large exit; consider closing"
            );
        }
    }
    tracing::info!(
        target: "priceoracle",
        symbol = %meta.symbol,
        token = %format!("{token:#x}"),
        held_kol = %meta.kol_name,
        price_bnb = format!("{price_bnb:.3e}"),
        pool_bnb = format!("{:.2}", wbnb_res / 1e18),
        "price tick"
    );
}

fn hex_to_bytes(h: &str) -> Option<Vec<u8>> {
    let h = h.strip_prefix("0x").unwrap_or(h);
    if h.len() % 2 != 0 {
        return None;
    }
    (0..h.len() / 2)
        .map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16).ok())
        .collect()
}

fn u128_from(hex64: &str) -> f64 {
    u128::from_str_radix(hex64.trim_start_matches("0x"), 16)
        .map(|v| v as f64)
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addr_arg_is_left_padded_32() {
        let a = Address::repeat_byte(0xab);
        let s = addr_arg(a);
        assert_eq!(s.len(), 64);
        assert!(s.ends_with(&"ab".repeat(20)));
    }

    #[test]
    fn u128_from_parses() {
        assert_eq!(u128_from(&format!("{:064x}", 1500u128)), 1500.0);
    }

    #[test]
    fn rug_threshold_sane() {
        assert!(RUG_DROP > 0.0 && RUG_DROP < 1.0);
    }
}
