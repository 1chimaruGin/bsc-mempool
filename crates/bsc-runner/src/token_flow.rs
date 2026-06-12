//! Phase 2 — per-token flow tape.
//!
//! Watches the mempool for *every* wallet's buys/sells of a token we
//! currently hold a paper position in (`HeldTokens`). This is the
//! whale/dump/momentum early-warning layer: while you're in a position,
//! you want to see all flow on that token — not just the KOL's — so a
//! large incoming sell or drying-up of buys is a manual exit cue on top
//! of the automatic KOL-sell exit-follow.
//!
//! Hot-path cheap: if no positions are open it does nothing. Per tx it is
//! a GMGN decode attempt + (fallback) a scan of 32-byte calldata words for
//! a held address.

use crate::gmgn;
use crate::held_tokens::HeldTokens;
use alloy::consensus::Transaction;
use alloy::primitives::Address;
use bsc_bus::Subscription;
use bsc_core::PendingTx;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Find a held token referenced by this tx. Tries the GMGN decoder first
/// (authoritative side), then a generic calldata address scan.
fn referenced_held(
    held: &HeldTokens,
    input: &[u8],
    value: alloy::primitives::U256,
) -> Option<(Address, Option<&'static str>)> {
    if let Some(g) = gmgn::decode(input, value) {
        if held.contains(&g.token) {
            return Some((g.token, Some(g.side.as_str())));
        }
    }
    // Generic: any 32-byte word that is a held address (covers Pancake
    // V2/V3 router swaps, direct pair calls, transfers, etc.). Skip the
    // 4-byte selector so chunks align with ABI word boundaries.
    let body = if input.len() > 4 { &input[4..] } else { &input[..] };
    for w in body.chunks_exact(32) {
        if w[0..12].iter().all(|&b| b == 0) && w[12..16].iter().any(|&b| b != 0) {
            let a = Address::from_slice(&w[12..32]);
            if held.contains(&a) {
                return Some((a, None));
            }
        }
    }
    None
}

fn wei_to_bnb(v: alloy::primitives::U256) -> f64 {
    v.to_string().parse::<f64>().unwrap_or(0.0) / 1e18
}

pub fn start(
    held: Arc<HeldTokens>,
    mut sub: Subscription,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        tracing::info!(target: "tokflow", "token-flow watcher up (Phase 2)");
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::info!(target: "tokflow", "token-flow watcher shutdown");
                    return;
                }
                res = sub.recv() => {
                    match res {
                        Ok(p) => process(&held, p.as_ref()),
                        Err(bsc_bus::RecvError::Lagged { skipped, .. }) => {
                            tracing::warn!(target: "tokflow", skipped, "flow watcher lagged");
                        }
                        Err(bsc_bus::RecvError::Closed) => return,
                    }
                }
            }
        }
    });
}

fn process(held: &HeldTokens, p: &PendingTx) {
    if held.is_empty() {
        return; // no open positions → nothing to monitor
    }
    let tx = p.tx.as_ref();
    let input = tx.input();
    let value = tx.value();
    let Some((token, side)) = referenced_held(held, input.as_ref(), value) else {
        return;
    };
    let Some(meta) = held.get(&token) else { return };
    let from = format!("{:#x}", p.from);
    let bnb = wei_to_bnb(value);
    let side_str = side.unwrap_or(if bnb > 0.0 { "BUY?" } else { "?" });

    metrics::counter!(
        "bsc_tokflow_total",
        "symbol" => meta.symbol.clone(),
        "side" => side.unwrap_or("?"),
    )
    .increment(1);

    tracing::info!(
        target: "tokflow",
        symbol = %meta.symbol,
        token = %format!("{token:#x}"),
        held_kol = %meta.kol_name,
        from = %from,
        side = side_str,
        value_bnb = bnb,
        tx_hash = %format!("{:#x}", p.hash),
        "FLOW on held token"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::held_tokens::HeldMeta;
    use alloy::primitives::U256;

    #[test]
    fn detects_held_addr_in_calldata() {
        let held = HeldTokens::new();
        let t = Address::repeat_byte(0xab);
        held.insert(
            t,
            HeldMeta {
                kol_name: "D".into(),
                symbol: "ABC".into(),
                entered_block: 1,
                entered_unix_ns: 1,
                bnb_in_wei: 0,
            },
        );
        // calldata: selector + a 32-byte word containing the held address
        let mut data = vec![0x12, 0x34, 0x56, 0x78];
        let mut w = [0u8; 32];
        w[12..32].copy_from_slice(t.as_slice());
        data.extend_from_slice(&w);
        let hit = referenced_held(&held, &data, U256::ZERO);
        assert_eq!(hit, Some((t, None)));
    }

    #[test]
    fn ignores_unheld() {
        let held = HeldTokens::new();
        let data = vec![0u8; 36];
        assert!(referenced_held(&held, &data, U256::ZERO).is_none());
    }
}
