//! Token market-cap helper for the trade ledger.
//!
//! Returns `(price_bnb_per_token, total_supply_whole)` from the
//! PancakeSwap-V2 WBNB pair + `totalSupply()`. The executor turns this
//! into:
//!   - `d_mcap_usd`         — spot price × supply × BNB-USD at entry
//!     (the market cap D bought into; D's tx is usually still pending so
//!      current reserves ≈ pre-D state)
//!   - `our_entry_mcap_usd` — our effective fill price × supply × BNB-USD
//!     (what WE actually entered at, after D's next-block impact)
//!
//! The gap between the two = the copy-lag cost.

use alloy::primitives::Address;
use bsc_dex::addresses::{PANCAKE_V2_FACTORY, WBNB};

const GET_PAIR: &str = "0xe6a43905";
const GET_RESERVES: &str = "0x0902f1ac";
const TOKEN0: &str = "0x0dfe1681";
const TOTAL_SUPPLY: &str = "0x18160ddd";

async fn eth_call(
    c: &reqwest::Client,
    url: &str,
    to: &str,
    data: String,
) -> Option<String> {
    let body = serde_json::json!({
        "jsonrpc":"2.0","method":"eth_call",
        "params":[{"to":to,"data":data},"latest"],"id":1});
    let v: serde_json::Value =
        c.post(url).json(&body).send().await.ok()?.json().await.ok()?;
    v.get("result")?.as_str().map(|s| s.to_string())
}

fn u128_hex(h: &str) -> f64 {
    u128::from_str_radix(h.trim_start_matches("0x"), 16)
        .map(|v| v as f64)
        .unwrap_or(0.0)
}

fn addr_arg(a: Address) -> String {
    format!("{:0>64}", format!("{a:x}"))
}

/// Last 40 hex chars of an ABI word → Address.
fn addr_from_tail(h: &str) -> Option<Address> {
    let s = &h[h.len().checked_sub(40)?..];
    let mut b = [0u8; 20];
    for i in 0..20 {
        b[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(Address::from(b))
}

/// `(price_bnb_per_whole_token, total_supply_whole_tokens)` or `None` if
/// the token has no V2 WBNB pair yet (pre-graduation / V3-only).
pub async fn pool_spot_and_supply(
    client: &reqwest::Client,
    rpc_url: &str,
    token: Address,
    decimals: u8,
) -> Option<(f64, f64)> {
    let factory = format!("{PANCAKE_V2_FACTORY:#x}");
    let pair_hex = eth_call(
        client,
        rpc_url,
        &factory,
        format!("{GET_PAIR}{}{}", addr_arg(WBNB), addr_arg(token)),
    )
    .await?;
    if pair_hex.len() < 66 {
        return None;
    }
    let pair = addr_from_tail(&pair_hex)?;
    if pair == Address::ZERO {
        return None;
    }
    let pair_s = format!("{pair:#x}");

    let t0_hex = eth_call(client, rpc_url, &pair_s, TOKEN0.into()).await?;
    if t0_hex.len() < 66 {
        return None;
    }
    let t0 = addr_from_tail(&t0_hex)?;

    let res = eth_call(client, rpc_url, &pair_s, GET_RESERVES.into()).await?;
    if res.len() < 2 + 64 * 2 {
        return None;
    }
    let r0 = u128_hex(&res[2..66]);
    let r1 = u128_hex(&res[66..130]);
    let (wbnb_raw, tok_raw) = if t0 == WBNB { (r0, r1) } else { (r1, r0) };
    if wbnb_raw == 0.0 || tok_raw == 0.0 {
        return None;
    }

    let sup_hex = eth_call(client, rpc_url, &format!("{token:#x}"), TOTAL_SUPPLY.into()).await?;
    let supply_raw = u128_hex(sup_hex.trim());
    if supply_raw == 0.0 {
        return None;
    }

    // WBNB is 18-dec; token is `decimals`-dec.
    let wbnb = wbnb_raw / 1e18;
    let dscale = 10f64.powi(i32::from(decimals));
    let tok = tok_raw / dscale;
    let supply_whole = supply_raw / dscale;
    let price_bnb = wbnb / tok; // BNB per whole token
    Some((price_bnb, supply_whole))
}

/// Read `totalSupply()` and return it as whole-token units. Used for the
/// bonding-curve mcap path where there's no V2 pool yet — supply alone
/// suffices and the price comes from the KOL's tx receipt / chain-swap
/// median.
pub async fn total_supply_whole(
    client: &reqwest::Client,
    rpc_url: &str,
    token: Address,
    decimals: u8,
) -> Option<f64> {
    let h = eth_call(client, rpc_url, &format!("{token:#x}"), TOTAL_SUPPLY.into()).await?;
    let raw = u128_hex(h.trim());
    if raw == 0.0 {
        return None;
    }
    Some(raw / 10f64.powi(i32::from(decimals)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addr_arg_padded() {
        assert_eq!(addr_arg(Address::repeat_byte(0xab)).len(), 64);
    }

    #[test]
    fn u128_hex_parses() {
        assert_eq!(u128_hex(&format!("0x{:x}", 1_000_000u128)), 1_000_000.0);
    }
}
