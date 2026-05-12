//! KOL (key-opinion-leader) pending-tx watcher for BSC.
//!
//! Subscribes to the mempool bus, matches each tx's `from` against a
//! configured list (kols.toml), and on a hit:
//! 1. emits a structured `tracing::info!` line (authoritative record),
//! 2. increments `bsc_kol_hits_total{name,group}`,
//! 3. tries to push a `KolHit` event to the Telegram dispatcher via a
//!    bounded mpsc — non-blocking; full queue → drop with counter.
//!
//! ## Isolation
//! The Telegram dispatcher runs on its own dedicated OS thread with its own
//! single-thread Tokio runtime. HTTP outbound to `api.telegram.org` shares
//! zero scheduler resources with the main runtime — protects the
//! latency-sensitive trader from any Telegram slowness or hangs. The bus
//! consumer itself only does O(1) HashMap lookup + structured log +
//! try_send → microseconds.
//!
//! Day-2 scope: mempool-side hits only. Block-confirmation watcher
//! (kol_confirm) lands in Day 3 alongside the paper trader so we can
//! measure lead-time and missed flow.

use crate::config::{KolWatchConfig, TelegramConfig};
use alloy::consensus::Transaction;
use alloy::primitives::Address;
use anyhow::{Context, Result};
use bsc_bus::Subscription;
use bsc_core::PendingTx;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// =============================================================================
// KOL list (parsed from kols.toml)
// =============================================================================

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Kol {
    pub address: Address,
    pub name: String,
    #[serde(default)]
    pub emoji: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KolFile {
    #[serde(default)]
    kol: Vec<Kol>,
}

pub(crate) struct KolIndex {
    by_address: HashMap<Address, Kol>,
}

impl KolIndex {
    pub fn load(path: &Path, group_filter: &[String]) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("read kols file: {}", path.display()))?;
        let f: KolFile = toml::from_str(&s)
            .with_context(|| format!("parse kols file: {}", path.display()))?;
        let by_address: HashMap<Address, Kol> = f
            .kol
            .into_iter()
            .filter(|k| {
                group_filter.is_empty()
                    || k.groups.iter().any(|g| group_filter.contains(g))
            })
            .map(|k| (k.address, k))
            .collect();
        Ok(Self { by_address })
    }

    #[inline]
    pub fn lookup(&self, addr: &Address) -> Option<&Kol> {
        self.by_address.get(addr)
    }

    pub fn len(&self) -> usize {
        self.by_address.len()
    }
}

// =============================================================================
// Known method selectors / known BSC router addresses
// =============================================================================

/// Map a 4-byte method selector to a friendly name. Empty input = native
/// BNB transfer.
///
/// PancakeSwap V2 is a UniV2 fork, so selectors are identical to Uniswap V2's
/// — we just label them "PancakeV2" since that's where they hit on BSC.
fn known_method(input: &[u8]) -> Option<&'static str> {
    if input.is_empty() {
        return Some("BNB transfer");
    }
    if input.len() < 4 {
        return None;
    }
    match &input[..4] {
        // GMGN aggregator (same selector as ETH; GMGN exists on BSC too).
        [0xef, 0xfb, 0xec, 0x13] => Some("GMGN swap"),
        // PancakeSwap V2 (UniV2-fork selectors).
        [0x7f, 0xf3, 0x6a, 0xb5] => Some("PancakeV2 swapExactBNBForTokens"),
        [0x18, 0xcb, 0xaf, 0xe5] => Some("PancakeV2 swapExactTokensForBNB"),
        [0x38, 0xed, 0x17, 0x39] => Some("PancakeV2 swapExactTokensForTokens"),
        [0xfb, 0x3b, 0xdb, 0x41] => Some("PancakeV2 swapBNBForExactTokens"),
        [0xb6, 0xf9, 0xde, 0x95] => Some("PancakeV2 swapExactBNBForTokensFOT"),
        [0x5c, 0x11, 0xd7, 0x95] => Some("PancakeV2 swapExactTokensForTokensFOT"),
        // PancakeSwap V3 — same SwapRouter ABI as Uniswap V3.
        [0x04, 0xe4, 0x5a, 0xaf] => Some("PancakeV3 exactInputSingle"),
        [0xb8, 0x58, 0x18, 0x3f] => Some("PancakeV3 exactInput"),
        // SmartRouter multicall (V3 router umbrella).
        [0xac, 0x96, 0x50, 0xd8] => Some("PancakeSmart multicall"),
        // Generic ERC20.
        [0xa9, 0x05, 0x9c, 0xbb] => Some("BEP20 transfer"),
        [0x09, 0x5e, 0xa7, 0xb3] => Some("BEP20 approve"),
        [0x23, 0xb8, 0x72, 0xdd] => Some("BEP20 transferFrom"),
        _ => None,
    }
}

/// Map a known router/proxy address (BSC) to a friendly name.
fn known_address(addr: &Address) -> Option<&'static str> {
    let lc = format!("{:x}", addr);
    match lc.as_str() {
        // GMGN proxy on BSC (different from ETH's; observed in prior recon).
        "1de460f363af910f51726def188f9004276bf4bc" => Some("GMGN proxy"),
        // PancakeSwap V2 / V3 / SmartRouter.
        "10ed43c718714eb63d5aa57b78b54704e256024e" => Some("PancakeV2 Router"),
        "13f4ea83d0bd40e75c8222255bc855a974568dd4" => Some("PancakeV3 SmartRouter"),
        "1a0a18ac4becddbd6389559687d1a73d8927e416" => Some("PancakeV3 SwapRouter"),
        // Four.Meme launchpad.
        "5c952063c7fc8610ffdb798152d69f0b9550762b" => Some("Four.Meme"),
        // WBNB.
        "bb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c" => Some("WBNB"),
        // Aggregators.
        "1111111254eeb25477b68fb85ed929f73a960582" => Some("1inch v5"),
        "111111125421ca6dc452d289314280a0f8842a65" => Some("1inch v6"),
        "def1c0ded9bec7f1a1670819833240f027b25eff" => Some("0x ExchangeProxy"),
        "6131b5fae19ea4f9d964eac0408e4408b66337b5" => Some("KyberSwap"),
        "92b7807bf19b7dddf89b706143896d05228f3121" => Some("OpenOcean"),
        _ => None,
    }
}

// =============================================================================
// Hit event (passed via mpsc to the Telegram dispatcher)
// =============================================================================

#[derive(Debug, Clone)]
pub(crate) struct KolHit {
    pub kol_name: String,
    pub kol_emoji: Option<String>,
    pub kol_groups: Vec<String>,
    pub tx_hash: String,
    pub from_addr: String,
    pub to_addr: Option<String>,
    pub to_label: Option<&'static str>,
    pub method_id: String,
    pub method_label: Option<&'static str>,
    pub value_bnb: f64,
    pub gas_price_gwei: f64,
    pub gas_limit: u64,
    pub nonce: u64,
    pub source_seen: String,
    /// Raw tx calldata (input bytes). The trader's strategy decodes the
    /// PancakeSwap V2 swap path from this without re-fetching the tx.
    pub calldata: Vec<u8>,
    /// Human-readable decoded action when available. Filled by the trader's
    /// receipt decoder in Day 3; None on the raw mempool path.
    pub decoded: Option<String>,
}

impl KolHit {
    fn from_pending(kol: &Kol, p: &PendingTx) -> Self {
        let tx = p.tx.as_ref();
        let input = tx.input();
        let method_id = format_method_id(input.as_ref());
        let method_label = known_method(input.as_ref());
        let to = tx.to();
        let (to_addr, to_label) = match to {
            Some(a) => (Some(format!("{:#x}", a)), known_address(&a)),
            None => (None, None),
        };
        let value_wei = tx.value();
        let value_bnb = wei_to_bnb(value_wei);
        let gas_price_gwei = tx
            .gas_price()
            .map(wei_to_gwei)
            .unwrap_or_else(|| wei_to_gwei(u128::from(tx.max_fee_per_gas())));
        Self {
            kol_name: kol.name.clone(),
            kol_emoji: kol.emoji.clone(),
            kol_groups: kol.groups.clone(),
            tx_hash: format!("{:#x}", p.hash),
            from_addr: format!("{:#x}", p.from),
            to_addr,
            to_label,
            method_id,
            method_label,
            value_bnb,
            gas_price_gwei,
            gas_limit: tx.gas_limit(),
            nonce: tx.nonce(),
            source_seen: format!("{:?}", p.source_seen),
            calldata: input.to_vec(),
            decoded: None,
        }
    }
}

fn format_method_id(input: &[u8]) -> String {
    if input.len() >= 4 {
        format!(
            "0x{:02x}{:02x}{:02x}{:02x}",
            input[0], input[1], input[2], input[3]
        )
    } else {
        "0x".to_string()
    }
}

fn wei_to_bnb(wei: alloy::primitives::U256) -> f64 {
    // U256 → f64 via lossy string conversion; good enough for human display.
    let s = wei.to_string();
    let n: f64 = s.parse().unwrap_or(0.0);
    n / 1e18
}

fn wei_to_gwei(wei: u128) -> f64 {
    (wei as f64) / 1e9
}

// =============================================================================
// Public entry point
// =============================================================================

/// Hit sinks. Each hit gets cloned and try-sent to whichever sinks are `Some`.
/// Adding new sinks (trader, archiver, …) means adding a field here and a
/// corresponding try_send in `fire_hit`.
#[derive(Clone, Default)]
pub(crate) struct Sinks {
    pub telegram: Option<mpsc::Sender<KolHit>>,
    pub trader: Option<mpsc::Sender<KolHit>>,
}

/// Start the mempool-side KOL watcher. Spawns:
///   1. The bus consumer (on the caller's Tokio runtime).
///   2. The Telegram dispatcher on a dedicated OS thread with its own
///      single-thread Tokio runtime — HTTP I/O fully isolated.
///
/// Returns `None` if disabled or no KOLs were loaded.
pub fn start(
    cfg: KolWatchConfig,
    sub: Subscription,
    extra_sinks: Sinks,
    shutdown: CancellationToken,
) -> Option<Arc<KolIndex>> {
    if !cfg.enabled {
        tracing::info!("KOL watcher disabled in config; skipping");
        return None;
    }
    let kol_path = if cfg.file.is_empty() {
        "config/kols.toml".to_string()
    } else {
        cfg.file.clone()
    };
    let index = match KolIndex::load(Path::new(&kol_path), &cfg.groups) {
        Ok(i) if i.len() == 0 => {
            tracing::warn!(file = %kol_path, "KOL list loaded but empty; watcher idle");
            return None;
        }
        Ok(i) => {
            tracing::info!(
                file = %kol_path,
                count = i.len(),
                groups = ?cfg.groups,
                "KOL watcher loaded"
            );
            Arc::new(i)
        }
        Err(e) => {
            tracing::error!(error = %e, "KOL list load failed; watcher disabled");
            return None;
        }
    };

    // Telegram channel — owned here, fanned out via Sinks.
    let (tg_tx, tg_rx) = mpsc::channel::<KolHit>(256);
    let telegram_sink = if cfg.telegram.enabled {
        if cfg.telegram.bot_token.is_empty() || cfg.telegram.chat_id == 0 {
            tracing::warn!(
                "KOL Telegram enabled but bot_token/chat_id missing; alerts off \
                 (will still log + count)."
            );
            drop(tg_rx);
            None
        } else {
            spawn_telegram_dispatcher(cfg.telegram.clone(), tg_rx, shutdown.clone());
            Some(tg_tx)
        }
    } else {
        drop(tg_rx);
        None
    };

    let sinks = Arc::new(Sinks {
        telegram: telegram_sink,
        trader: extra_sinks.trader,
    });

    {
        let index = index.clone();
        let sinks = sinks.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_consumer(index, sub, sinks, shutdown).await;
        });
    }

    Some(index)
}

// =============================================================================
// Bus consumer
// =============================================================================

async fn run_consumer(
    index: Arc<KolIndex>,
    mut sub: Subscription,
    sinks: Arc<Sinks>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::info!("KOL watcher consumer: shutdown");
                return;
            }
            res = sub.recv() => {
                match res {
                    Ok(p) => process_one(&index, p.as_ref(), &sinks),
                    Err(bsc_bus::RecvError::Lagged { name, skipped }) => {
                        tracing::warn!(name, skipped, "KOL consumer lagged");
                    }
                    Err(bsc_bus::RecvError::Closed) => {
                        tracing::info!("KOL consumer: bus closed");
                        return;
                    }
                }
            }
        }
    }
}

fn process_one(index: &KolIndex, p: &PendingTx, sinks: &Sinks) {
    let Some(kol) = index.lookup(&p.from) else {
        return;
    };
    let hit = KolHit::from_pending(kol, p);
    fire_hit(hit, sinks);
}

/// Log + count + dispatch a hit to all configured sinks. Each sink uses
/// `try_send` so this never blocks the bus consumer.
pub(crate) fn fire_hit(hit: KolHit, sinks: &Sinks) {
    tracing::info!(
        target: "kol",
        kol_name = %hit.kol_name,
        kol_emoji = ?hit.kol_emoji,
        kol_groups = ?hit.kol_groups,
        tx_hash = %hit.tx_hash,
        from = %hit.from_addr,
        to = ?hit.to_addr,
        to_label = ?hit.to_label,
        method_id = %hit.method_id,
        method_label = ?hit.method_label,
        decoded = ?hit.decoded,
        value_bnb = hit.value_bnb,
        gas_price_gwei = hit.gas_price_gwei,
        gas_limit = hit.gas_limit,
        nonce = hit.nonce,
        source_seen = %hit.source_seen,
        "KOL tx observed"
    );

    metrics::counter!(
        "bsc_kol_hits_total",
        "name" => hit.kol_name.clone(),
        "group" => hit.kol_groups.first().cloned().unwrap_or_default(),
    )
    .increment(1);

    if let Some(tg) = &sinks.telegram {
        match tg.try_send(hit.clone()) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("bsc_kol_telegram_dropped_total").increment(1);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
    if let Some(tr) = &sinks.trader {
        match tr.try_send(hit) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("bsc_trader_dropped_total").increment(1);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

// =============================================================================
// Telegram dispatcher — dedicated OS thread, dedicated Tokio runtime
// =============================================================================

fn spawn_telegram_dispatcher(
    cfg: TelegramConfig,
    rx: mpsc::Receiver<KolHit>,
    shutdown: CancellationToken,
) {
    std::thread::Builder::new()
        .name("kol-telegram".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("kol-telegram-rt")
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!(error = %e, "failed to build telegram runtime");
                    return;
                }
            };
            rt.block_on(async move {
                run_telegram(cfg, rx, shutdown).await;
            });
        })
        .expect("spawn kol-telegram thread");
}

async fn run_telegram(
    cfg: TelegramConfig,
    mut rx: mpsc::Receiver<KolHit>,
    shutdown: CancellationToken,
) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to build telegram http client");
            return;
        }
    };
    let url = format!("https://api.telegram.org/bot{}/sendMessage", cfg.bot_token);
    tracing::info!(chat_id = cfg.chat_id, "KOL telegram dispatcher up");
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::info!("KOL telegram dispatcher: shutdown");
                return;
            }
            maybe = rx.recv() => {
                let Some(hit) = maybe else { return };
                send_one(&client, &url, cfg.chat_id, &hit).await;
            }
        }
    }
}

async fn send_one(client: &reqwest::Client, url: &str, chat_id: i64, hit: &KolHit) {
    let text = format_telegram_html(hit);
    let keyboard = build_inline_keyboard(hit);
    let payload = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
        "reply_markup": { "inline_keyboard": keyboard },
    });
    // Up to two attempts: a single retry on 429 with server-suggested delay.
    for attempt in 0..2 {
        match client.post(url).json(&payload).send().await {
            Ok(r) if r.status().is_success() => {
                metrics::counter!("bsc_kol_telegram_sent_total").increment(1);
                return;
            }
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                if status.as_u16() == 429 && attempt == 0 {
                    let retry_after = parse_retry_after(&body).unwrap_or(5);
                    metrics::counter!("bsc_kol_telegram_throttled_total").increment(1);
                    tracing::warn!(
                        retry_after,
                        "telegram 429 throttled; sleeping then retrying once"
                    );
                    tokio::time::sleep(Duration::from_secs(retry_after)).await;
                    continue;
                }
                metrics::counter!("bsc_kol_telegram_failed_total").increment(1);
                tracing::warn!(status = %status, body = %body, "telegram non-2xx");
                return;
            }
            Err(e) => {
                metrics::counter!("bsc_kol_telegram_failed_total").increment(1);
                tracing::warn!(error = %e, "telegram send error");
                return;
            }
        }
    }
}

fn parse_retry_after(body: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    v.get("parameters")
        .and_then(|p| p.get("retry_after"))
        .and_then(|n| n.as_u64())
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn action_emoji(method_label: Option<&'static str>) -> &'static str {
    match method_label {
        Some("BNB transfer") => "💸",
        Some("BEP20 transfer") | Some("BEP20 transferFrom") => "🪙",
        Some("BEP20 approve") => "✅",
        // Specific aggregators FIRST (the generic `contains("swap")` branch
        // below would otherwise swallow them).
        Some("GMGN swap") => "🐸",
        Some(m) if m.contains("swap") || m.contains("Swap") || m.contains("Pancake") => "💱",
        Some(_) => "🔧",
        None => "❓",
    }
}

/// Smart value formatting (BNB). Hides noise on contract calls (0 BNB).
fn format_value(bnb: f64) -> Option<String> {
    if bnb <= 0.0 {
        return None;
    }
    let s = if bnb >= 100.0 {
        format!("{:.0} BNB", bnb)
    } else if bnb >= 1.0 {
        format!("{:.2} BNB", bnb)
    } else if bnb >= 0.001 {
        format!("{:.4} BNB", bnb)
    } else {
        format!("{:.6} BNB", bnb)
    };
    Some(s)
}

fn format_gwei(g: f64) -> String {
    // BSC fees are quoted in gwei but median is ~1 gwei; the V3 fast tier
    // can spike to ~5-10 gwei during memecoin churn. Format accordingly.
    if g >= 100.0 {
        format!("{:.0} gwei", g)
    } else if g >= 10.0 {
        format!("{:.1} gwei", g)
    } else if g >= 1.0 {
        format!("{:.2} gwei", g)
    } else {
        format!("{:.3} gwei", g)
    }
}

fn now_hms_utc() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02} UTC", h, m, s)
}

fn format_telegram_html(h: &KolHit) -> String {
    let kol = html_escape(&h.kol_name);
    let kol_emoji = h.kol_emoji.as_deref().unwrap_or("🐐");
    let group = h
        .kol_groups
        .first()
        .map(|s| html_escape(s))
        .unwrap_or_default();
    let group_part = if group.is_empty() {
        String::new()
    } else {
        format!(" · <i>{}</i>", group)
    };

    let action = action_emoji(h.method_label);
    let method = html_escape(h.method_label.unwrap_or(&h.method_id));

    let value_part = format_value(h.value_bnb);
    let mut body = String::new();
    body.push_str(&format!("{} <b>{}</b>", action, method));
    if let Some(v) = value_part {
        body.push_str(&format!(" — {}", html_escape(&v)));
    }
    body.push_str(&format!(" · {}", html_escape(&format_gwei(h.gas_price_gwei))));

    let decoded = match &h.decoded {
        Some(d) => format!("\n💎 <b>{}</b>", html_escape(d)),
        None => String::new(),
    };

    let dest = match h.to_label {
        Some(l) => format!("\n📍 via <code>{}</code>", html_escape(l)),
        None => String::new(),
    };

    let meta = format!(
        "<i>nonce {} · {}</i>",
        h.nonce,
        html_escape(&now_hms_utc()),
    );

    format!(
        "🟢 <b>PENDING</b> · BSC\n\n{} <b>KOL {}</b>{}\n{}{}{}\n{}",
        kol_emoji, kol, group_part, body, decoded, dest, meta,
    )
}

fn build_inline_keyboard(h: &KolHit) -> serde_json::Value {
    let tx_btn = serde_json::json!({
        "text": "🔍 BscScan tx",
        "url": format!("https://bscscan.com/tx/{}", h.tx_hash),
    });
    let from_btn = serde_json::json!({
        "text": format!("👤 {}", h.kol_name),
        "url": format!("https://bscscan.com/address/{}", h.from_addr),
    });
    let mut row2 = vec![from_btn];
    if let Some(to) = &h.to_addr {
        let label = h
            .to_label
            .map(|s| s.to_string())
            .unwrap_or_else(|| short_addr(to));
        row2.push(serde_json::json!({
            "text": format!("🎯 {}", label),
            "url": format!("https://bscscan.com/address/{}", to),
        }));
    }
    serde_json::json!([[tx_btn], row2])
}

fn short_addr(addr: &str) -> String {
    if addr.len() < 14 {
        return addr.to_string();
    }
    format!("{}…{}", &addr[..8], &addr[addr.len() - 4..])
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn known_method_basics() {
        assert_eq!(known_method(&[]), Some("BNB transfer"));
        assert_eq!(
            known_method(&[0x7f, 0xf3, 0x6a, 0xb5]),
            Some("PancakeV2 swapExactBNBForTokens")
        );
        assert_eq!(
            known_method(&[0xef, 0xfb, 0xec, 0x13]),
            Some("GMGN swap")
        );
        assert_eq!(known_method(&[0xa9, 0x05, 0x9c, 0xbb]), Some("BEP20 transfer"));
        assert_eq!(known_method(&[0xde, 0xad, 0xbe, 0xef]), None);
        assert_eq!(known_method(&[0x12, 0x34]), None);
    }

    #[test]
    fn known_address_basics() {
        let pcs_v2: Address = "0x10ED43C718714eb63d5aA57B78B54704E256024E"
            .parse()
            .unwrap();
        assert_eq!(known_address(&pcs_v2), Some("PancakeV2 Router"));
        let gmgn: Address = "0x1de460f363AF910f51726DEf188F9004276Bf4bc"
            .parse()
            .unwrap();
        assert_eq!(known_address(&gmgn), Some("GMGN proxy"));
        let unknown: Address = "0x0000000000000000000000000000000000000001".parse().unwrap();
        assert_eq!(known_address(&unknown), None);
    }

    #[test]
    fn kol_index_loads_and_filters() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            tmp,
            r#"
[[kol]]
address = "0xbf004bff64725914ee36d03b87d6965b0ced4903"
name = "A"
groups = ["GOAT"]

[[kol]]
address = "0x0000000000000000000000000000000000000099"
name = "Z"
groups = ["other"]
"#
        )
        .unwrap();

        let idx = KolIndex::load(tmp.path(), &[]).unwrap();
        assert_eq!(idx.len(), 2);

        let idx = KolIndex::load(tmp.path(), &["GOAT".to_string()]).unwrap();
        assert_eq!(idx.len(), 1);
        let a: Address = "0xbf004bff64725914ee36d03b87d6965b0ced4903".parse().unwrap();
        assert!(idx.lookup(&a).is_some());
    }

    #[test]
    fn html_escape_basics() {
        assert_eq!(html_escape("a&b<c>d"), "a&amp;b&lt;c&gt;d");
        assert_eq!(html_escape("normal text"), "normal text");
    }

    #[test]
    fn action_emoji_picks_action() {
        assert_eq!(action_emoji(Some("BNB transfer")), "💸");
        assert_eq!(action_emoji(Some("BEP20 transfer")), "🪙");
        assert_eq!(action_emoji(Some("BEP20 approve")), "✅");
        assert_eq!(action_emoji(Some("PancakeV2 swapExactBNBForTokens")), "💱");
        assert_eq!(action_emoji(Some("PancakeV3 exactInputSingle")), "💱");
        assert_eq!(action_emoji(Some("GMGN swap")), "🐸");
        assert_eq!(action_emoji(None), "❓");
    }

    #[test]
    fn format_value_scales_precision() {
        assert_eq!(format_value(0.0), None);
        assert_eq!(format_value(0.0001).as_deref(), Some("0.000100 BNB"));
        assert_eq!(format_value(0.5).as_deref(), Some("0.5000 BNB"));
        assert_eq!(format_value(2.5).as_deref(), Some("2.50 BNB"));
        assert_eq!(format_value(150.0).as_deref(), Some("150 BNB"));
    }

    #[test]
    fn format_gwei_scales() {
        // BSC fees: median ~1 gwei, fast spike ~5-10
        assert_eq!(format_gwei(0.5), "0.500 gwei");
        assert_eq!(format_gwei(1.0), "1.00 gwei");
        assert_eq!(format_gwei(7.5), "7.50 gwei");
        assert_eq!(format_gwei(15.0), "15.0 gwei");
        assert_eq!(format_gwei(120.0), "120 gwei");
    }
}
