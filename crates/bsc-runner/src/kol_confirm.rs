//! KOL confirm watcher — closes the loop on mempool-side KOL hits.
//!
//! `kol_watch` fires when a watched wallet's tx is seen *pending*. This
//! module tracks each such hash and, by subscribing to `newHeads`, detects
//! when it actually lands in a block — emitting a `CONFIRMED` line with the
//! measured **lead time** (wall-clock elapsed between first-seen-pending and
//! the block reaching us). That lead time is the actionable window: how long
//! you had to react before the trade was on-chain.
//!
//! A pending hit that never confirms within `TTL` (replaced / dropped from
//! the mempool) is swept and counted as `bsc_kol_unconfirmed_total`.
//!
//! ## Why a separate WS subscription
//! Reuses the same `newHeads` pattern as the block-coverage oracle but keyed
//! only on our small KOL set — O(block_txs) DashMap probes per block,
//! negligible. Runs on its own task; the block fetch is spawned so the WS
//! loop never blocks on `eth_getBlockByHash`.

use crate::gmgn;
use crate::kol_watch::{KolHit, KolIndex};
use alloy::consensus::Transaction as _;
use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use bsc_bus::current_block::unix_now_ns;
use dashmap::DashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// How long a pending KOL hit may wait for confirmation before it's swept
/// as dropped/replaced. BSC blocks are ~0.45 s; 300 s ≈ 650 blocks — well
/// past any realistic inclusion delay.
const TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct PendingMeta {
    pub kol_name: String,
    pub side: Option<&'static str>,
    pub token: Option<Address>,
    /// Unix-epoch ns when the mempool first saw the tx (`PendingTx.first_seen_ns`).
    pub first_seen_ns: u64,
    /// Wall-clock ns when we registered it (for TTL sweeping).
    pub registered_ns: u64,
    /// Chain head block number at first-sight (`BlockContext.block_number`).
    /// `None` if the head-state updater hadn't warmed yet — excluded from
    /// the block-delta stats so they stay correct.
    pub seen_block: Option<u64>,
    /// Wall-clock ms into the head block's slot when first seen
    /// (`BlockContext.ms_into_block`). 0..~450 on BSC.
    pub ms_into_block: u32,
}

/// Shared pending-hit table. `kol_watch` inserts; this watcher drains.
#[derive(Default)]
pub struct ConfirmRegistry {
    pending: DashMap<B256, PendingMeta>,
}

impl ConfirmRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: DashMap::with_capacity(512),
        })
    }

    /// Register a freshly-fired pending KOL hit. Cheap O(1) — called on the
    /// bus hot path. Idempotent on hash (same tx re-broadcast just refreshes).
    pub fn register(&self, hash: B256, meta: PendingMeta) {
        self.pending.insert(hash, meta);
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }
}

/// Spawn the confirm watcher. Returns immediately; work runs on tokio tasks.
pub fn start(
    registry: Arc<ConfirmRegistry>,
    ws_url: String,
    kol_file: String,
    kol_groups: Vec<String>,
    // PUBLIC paper trader sink. Receives confirmed SELLs (both public-
    // confirmed and private-confirmed) so it can EXIT positions even when
    // the KOL exited via private orderflow. Without this, the public
    // paper trader is blind to ~75% of KOL exits and the timeout sweep
    // closes positions at random LATEST prices days later (sometimes top,
    // sometimes zero). BUYs are NOT forwarded here — entries come from
    // the pending-mempool path which is the public-visibility channel.
    trader_tx: Option<mpsc::Sender<KolHit>>,
    // Separate private-confirmed paper trader sink. Receives a synthetic
    // `KolHit` for: (a) the KOL's PRIVATE confirmed BUYs (entries the
    // pending/public trader can never see), and (b) ALL the KOL's
    // confirmed SELLs (so it can exit-follow). `None` = feature off.
    private_tx: Option<mpsc::Sender<KolHit>>,
    // LIVE trader sink — receives only SELL events (both public-confirmed
    // and private-confirmed) so we can exit-follow on EVERY KOL sell, not
    // just the rare public-mempool ones the live consumer sees natively.
    // BUYs are deliberately NOT forwarded here: entries come from the
    // mempool path which has better latency and avoids double-firing.
    live_tx: Option<mpsc::Sender<KolHit>>,
    shutdown: CancellationToken,
) {
    // Own copy of the KOL index so we can detect KOL txs by `from` in a
    // block even when we NEVER saw them pending (private orderflow).
    let index = match KolIndex::load(Path::new(&kol_file), &kol_groups) {
        Ok(i) => Arc::new(i),
        Err(e) => {
            tracing::warn!(error = %e,
                "kol_confirm: KOL index load failed; private detection off");
            Arc::new(KolIndex::default())
        }
    };
    // newHeads loop with reconnect/backoff.
    {
        let registry = registry.clone();
        let index = index.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            let max = Duration::from_secs(10);
            loop {
                if shutdown.is_cancelled() {
                    return;
                }
                match run_once(
                    &ws_url,
                    registry.clone(),
                    index.clone(),
                    trader_tx.clone(),
                    private_tx.clone(),
                    live_tx.clone(),
                    shutdown.clone(),
                )
                .await
                {
                    Ok(()) => {
                        tracing::info!("kol_confirm WS ended; reconnecting");
                        backoff = Duration::from_millis(500);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "kol_confirm WS error; reconnecting");
                    }
                }
                let delay = backoff;
                backoff = (backoff * 2).min(max);
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        });
    }
    // TTL sweeper.
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    _ = tick.tick() => sweep_expired(&registry),
                }
            }
        });
    }
}

/// Try-send a KolHit to a sink, logging + counting drops when the sink's
/// channel is full or closed. Silent `let _ = sink.try_send(hit)` would
/// lose exit signals invisibly when downstream is overloaded — a real
/// money-loss vector since a missed sell on a held position lets the bag
/// drain. The metric is `bsc_kol_sink_dropped_total{sink,reason}`.
fn forward_or_log(
    sink: &Option<mpsc::Sender<KolHit>>,
    sink_name: &'static str,
    kol_name: &str,
    context: &'static str,
    hit: KolHit,
) {
    let Some(tx) = sink else { return };
    match tx.try_send(hit) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(
                target: "kol_confirm",
                sink = sink_name,
                kol = %kol_name,
                context,
                "KolHit DROPPED — sink channel full"
            );
            metrics::counter!(
                "bsc_kol_sink_dropped_total",
                "sink" => sink_name,
                "reason" => "full",
            ).increment(1);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!(
                target: "kol_confirm",
                sink = sink_name,
                kol = %kol_name,
                context,
                "KolHit DROPPED — sink channel closed (downstream shut down?)"
            );
            metrics::counter!(
                "bsc_kol_sink_dropped_total",
                "sink" => sink_name,
                "reason" => "closed",
            ).increment(1);
        }
    }
}

fn sweep_expired(registry: &ConfirmRegistry) {
    let now = unix_now_ns();
    let ttl_ns = TTL.as_nanos() as u64;
    let mut dropped = 0u64;
    registry.pending.retain(|_, m| {
        let keep = now.saturating_sub(m.registered_ns) < ttl_ns;
        if !keep {
            dropped += 1;
        }
        keep
    });
    if dropped > 0 {
        metrics::counter!("bsc_kol_unconfirmed_total").increment(dropped);
        tracing::debug!(dropped, "kol_confirm: swept unconfirmed (dropped/replaced)");
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_once(
    ws_url: &str,
    registry: Arc<ConfirmRegistry>,
    index: Arc<KolIndex>,
    trader_tx: Option<mpsc::Sender<KolHit>>,
    private_tx: Option<mpsc::Sender<KolHit>>,
    live_tx: Option<mpsc::Sender<KolHit>>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url.to_string()))
        .await?;
    let provider = Arc::new(provider);
    let mut sub = provider.subscribe_blocks().await?.into_stream();
    tracing::info!(url = ws_url, "kol_confirm subscribed to newHeads (public + private scan)");
    use futures::StreamExt;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            head = sub.next() => {
                let Some(header) = head else { return Ok(()) };
                let head_recv_ns = unix_now_ns();
                let block_hash = header.hash;
                let block_number = header.number;
                let registry = registry.clone();
                let index = index.clone();
                let provider = provider.clone();
                let trader_tx = trader_tx.clone();
                let private_tx = private_tx.clone();
                let live_tx = live_tx.clone();
                tokio::spawn(async move {
                    process_block(
                        provider, registry, index, block_hash, block_number,
                        head_recv_ns, trader_tx, private_tx, live_tx,
                    )
                    .await;
                });
            }
        }
    }
}

/// Build a synthetic `KolHit` from a confirmed block tx for the paper
/// traders. `from`/`hash`/`value`/`input` come off the block tx;
/// `side`/`token` from the gmgn decode; `confirmed_block` lets the paper
/// trader query `balanceOf` pre-/post-sell for proportional exits.
fn confirmed_hit(
    kol_name: &str,
    txn: &alloy::rpc::types::Transaction,
    side: &'static str,
    token: Address,
    block_number: u64,
    visibility: &'static str,
) -> KolHit {
    let v = txn
        .inner
        .value()
        .to_string()
        .parse::<f64>()
        .unwrap_or(0.0)
        / 1e18;
    KolHit {
        kol_name: kol_name.to_string(),
        kol_emoji: None,
        kol_groups: vec![],
        tx_hash: format!("{:#x}", txn.inner.tx_hash()),
        from_addr: format!("{:#x}", txn.inner.signer()),
        to_addr: None,
        to_label: None,
        method_id: String::new(),
        method_label: None,
        value_bnb: v,
        gas_price_gwei: 0.0,
        gas_limit: 0,
        nonce: 0,
        source_seen: "confirmed".into(),
        calldata: txn.inner.input().to_vec(),
        decoded: None,
        token: Some(token),
        side: Some(side),
        confirmed_block: Some(block_number),
        visibility,
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_block(
    provider: Arc<impl Provider + ?Sized + 'static>,
    registry: Arc<ConfirmRegistry>,
    index: Arc<KolIndex>,
    block_hash: B256,
    block_number: u64,
    head_recv_ns: u64,
    trader_tx: Option<mpsc::Sender<KolHit>>,
    private_tx: Option<mpsc::Sender<KolHit>>,
    live_tx: Option<mpsc::Sender<KolHit>>,
) {
    // Full block: we must inspect every tx's `from` to catch PRIVATE KOL
    // trades (submitted direct-to-builder, never in our public mempool).
    let block = match provider.get_block_by_hash(block_hash).full().await {
        Ok(Some(b)) => b,
        Ok(None) => return,
        Err(e) => {
            tracing::trace!(error = %e, ?block_hash, "kol_confirm: getBlockByHash failed");
            return;
        }
    };
    let recv_ns = unix_now_ns();
    // Internal detection latency: newHeads-received → block fetched+scanned.
    // Dominated by the get_block_by_hash().full() round-trip. Add the
    // (separately measured) ~80ms block-seal→newHeads propagation for the
    // total "block sealed → we know" figure.
    let detect_ms = recv_ns.saturating_sub(head_recv_ns) as f64 / 1e6;
    // Pipeline latency per block (newHeads → fetched+scanned), independent
    // of whether a KOL traded — the figure that gates +1-block copy.
    metrics::histogram!("bsc_kol_detect_seconds").record(detect_ms / 1e3);
    for txn in block.transactions.txns() {
        let hash = *txn.inner.tx_hash();
        let from = txn.inner.signer();
        if let Some((_, m)) = registry.pending.remove(&hash) {
            // Wall-clock latency (clock-noisy: includes WS delivery + our
            // task scheduling). Kept for context, NOT the gating metric.
            let lead_ms = recv_ns.saturating_sub(m.first_seen_ns) as f64 / 1e6;

            // DECISIVE metric for same-block/+1 feasibility, clock-independent:
            // both block numbers come from our own node.
            //   block_delta = mined_block − (chain head at first-sight)
            //     0  → already too late: its block was sealing when we saw it
            //     1  → mined in the block after head; you must land in that
            //          same block (only the slot remainder to act)
            //     ≥2 → 1+ full 450ms slots of reaction room
            let (block_delta, seen_block_str): (Option<i64>, String) =
                match m.seen_block {
                    Some(sb) => (
                        Some(block_number as i64 - sb as i64),
                        sb.to_string(),
                    ),
                    None => (None, "?".into()),
                };
            // Remaining slot time when we saw it (~450ms BSC slot). Rough
            // upper bound on the window to land in head+1.
            let slot_remaining_ms = 450i64.saturating_sub(m.ms_into_block as i64);

            let token = m
                .token
                .map(|t| format!("{t:#x}"))
                .unwrap_or_else(|| "?".into());

            metrics::counter!(
                "bsc_kol_confirmed_total",
                "name" => m.kol_name.clone(),
                "side" => m.side.unwrap_or("?"),
            )
            .increment(1);
            metrics::histogram!("bsc_kol_confirm_lead_seconds").record(lead_ms / 1e3);
            if let Some(d) = block_delta {
                metrics::histogram!("bsc_kol_confirm_block_delta").record(d as f64);
            }
            metrics::histogram!("bsc_kol_confirm_ms_into_block")
                .record(m.ms_into_block as f64);

            tracing::info!(
                target: "kol",
                kol_name = %m.kol_name,
                side = m.side.unwrap_or("?"),
                token = %token,
                tx_hash = %format!("{hash:#x}"),
                visibility = "public",
                seen_block = %seen_block_str,
                mined_block = block_number,
                block_delta = block_delta
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "?".into()),
                ms_into_block = m.ms_into_block,
                slot_remaining_ms = slot_remaining_ms,
                lead_ms = format!("{lead_ms:.0}"),
                detect_ms = format!("{detect_ms:.0}"),
                "KOL tx CONFIRMED"
            );
            // Public confirmed SELL → forward to all three sinks:
            //   (a) PUBLIC paper trader — for exit-follow on positions
            //       it entered via mempool BUY
            //   (b) PRIVATE paper trader — same, for its own positions
            //   (c) LIVE trader — same, for real-money positions
            // Visibility only gates ENTRIES (which channel saw the buy);
            // EXITS are observable to the operator regardless and must
            // close all books that hold the token.
            if let (Some("SELL"), Some(tok)) = (m.side, m.token) {
                let hit = confirmed_hit(&m.kol_name, txn, "SELL", tok, block_number, "public");
                forward_or_log(&trader_tx, "trader_tx", &m.kol_name, "public_confirmed_sell", hit.clone());
                forward_or_log(&private_tx, "private_tx", &m.kol_name, "public_confirmed_sell", hit.clone());
                forward_or_log(&live_tx, "live_tx", &m.kol_name, "public_confirmed_sell", hit);
            }
        } else if let Some(kol) = index.lookup(&from) {
            // PRIVATE: a KOL wallet's tx that we NEVER saw pending — it
            // bypassed the public mempool (direct-to-builder / private RPC).
            // No lead-time / block-delta exist (we had zero advance notice).
            let g = gmgn::decode(txn.inner.input(), txn.inner.value());
            let side = g.as_ref().map(|s| s.side.as_str()).unwrap_or("?");
            let token = g
                .as_ref()
                .map(|s| format!("{:#x}", s.token))
                .unwrap_or_else(|| "?".into());
            metrics::counter!(
                "bsc_kol_confirmed_total",
                "name" => kol.name.clone(),
                "side" => side,
                "visibility" => "private",
            )
            .increment(1);
            metrics::counter!("bsc_kol_private_total", "name" => kol.name.clone())
                .increment(1);
            tracing::info!(
                target: "kol",
                kol_name = %kol.name,
                side = side,
                token = %token,
                tx_hash = %format!("{hash:#x}"),
                visibility = "private",
                seen_block = "?",
                mined_block = block_number,
                block_delta = "?",
                ms_into_block = 0,
                slot_remaining_ms = 0,
                lead_ms = "?",
                detect_ms = format!("{detect_ms:.0}"),
                "KOL tx CONFIRMED"
            );
            // PRIVATE confirmed:
            //   • private_tx (paper) gets BOTH sides — this is its entry channel
            //   • trader_tx (PUBLIC paper) gets SELLs only — for exit-follow on
            //     positions it entered via mempool BUYs; BUYs are not entry
            //     signals for the public channel (visibility mismatch)
            //   • live_tx (real trader) gets BOTH sides — 2026-05-29 added
            //     PRIVATE BUYs as entry signals at half-size ($10 vs $20
            //     PUBLIC). We can't beat D into the block (his tx is mined
            //     by the time we observe), so we ride D's wave at N+1 with
            //     smaller capital. Paper proves D PRIVATE is +52% ROI.
            if let Some(g) = g.as_ref() {
                let hit = confirmed_hit(&kol.name, txn, g.side.as_str(), g.token, block_number, "private");
                forward_or_log(&private_tx, "private_tx", &kol.name, "private_confirmed_any", hit.clone());
                match g.side {
                    gmgn::Side::Sell => {
                        forward_or_log(&trader_tx, "trader_tx", &kol.name, "private_confirmed_sell", hit.clone());
                        forward_or_log(&live_tx, "live_tx", &kol.name, "private_confirmed_sell", hit);
                    }
                    gmgn::Side::Buy => {
                        // Live trader receives PRIVATE BUYs for half-size
                        // entries. Public paper (trader_tx) does NOT — that
                        // channel's "visibility" semantics are entry-only-
                        // from-public-mempool.
                        forward_or_log(&live_tx, "live_tx", &kol.name, "private_confirmed_buy", hit);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_then_sweep_expires() {
        let reg = ConfirmRegistry::new();
        let h = B256::repeat_byte(7);
        reg.register(
            h,
            PendingMeta {
                kol_name: "D".into(),
                side: Some("BUY"),
                token: None,
                first_seen_ns: 1,
                registered_ns: 1, // ancient ⇒ expires immediately
                seen_block: Some(100),
                ms_into_block: 0,
            },
        );
        assert_eq!(reg.len(), 1);
        sweep_expired(&reg);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn fresh_entry_survives_sweep() {
        let reg = ConfirmRegistry::new();
        reg.register(
            B256::repeat_byte(1),
            PendingMeta {
                kol_name: "A".into(),
                side: Some("SELL"),
                token: None,
                first_seen_ns: unix_now_ns(),
                registered_ns: unix_now_ns(),
                seen_block: None,
                ms_into_block: 0,
            },
        );
        sweep_expired(&reg);
        assert_eq!(reg.len(), 1);
    }
}
