//! PaperExecutor — turns Strategy decisions into simulated trades on BSC.
//!
//! Day-3 BSC scope is leaner than the ETH version: we operate on raw
//! mempool hits (no kol_confirm watcher yet, no receipt decoder), so:
//!
//! ## Entries (mempool-mode buys)
//!   - decode the V2 path from calldata to identify the target token
//!   - resolve symbol + decimals via TokenResolver (cached eth_call)
//!   - simulate our buy at the CURRENT head block (no pre-block sim
//!     possible without a confirmed block number; we approximate same-block
//!     pricing by using `latest`)
//!   - insert into the position book in both portfolios (FastTip + NormalTip)
//!     using the same sim'd amount for now (Day-4 will differentiate via
//!     simulating at block-edge for FastTip vs end-of-block for NormalTip)
//!   - persist book to JSON
//!
//! ## Exits — DEFERRED to Day-4
//! Without a confirmed-receipt watcher we can't reliably detect KOL sells
//! from mempool data alone (path decoding for token→BNB calldata works but
//! we also need to know which token they're selling, which requires
//! knowing which tokens THEY own — not just from this tx). Day-3 closes
//! every position via the timeout sweep.
//!
//! ## Timeouts
//!   - sweep loop closes positions older than `hold_timeout_secs` by
//!     simulating a sell at the current head and writing a `Timeout`
//!     closed trade. If the sim returns zero (no liquidity), records as
//!     `NoLiquidity` instead.

use crate::token_resolver::TokenResolver;
use crate::trader::ledger::Ledger;
use crate::trader::position::PositionBook;
use crate::trader::sim::{QuoteResult, Simulator};
use crate::trader::strategy::extract_target_token;
use crate::trader::types::{
    CloseReason, ClosedTrade, Decision, OpenPosition, PortfolioMode, PositionKey,
};
use alloy::primitives::{Address, B256, U256};
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub struct PaperExecutor {
    pub rpc_url: String,
    pub resolver: Arc<TokenResolver>,
    pub sim: Simulator,
    pub book: Arc<Mutex<PositionBook>>,
    pub ledger: Arc<Ledger>,
    pub telegram: Option<Arc<TelegramAlerter>>,
}

impl PaperExecutor {
    pub fn new(
        rpc_url: String,
        resolver: Arc<TokenResolver>,
        ledger: Arc<Ledger>,
        telegram: Option<Arc<TelegramAlerter>>,
    ) -> Self {
        let book = ledger.load_book().unwrap_or_else(|e| {
            tracing::warn!(target: "trader", error = %e, "ledger load failed; starting empty");
            PositionBook::new()
        });
        let n = book.len();
        if n > 0 {
            tracing::info!(target: "trader", positions = n, "loaded open positions from JSON");
        }
        Self {
            rpc_url: rpc_url.clone(),
            resolver,
            sim: Simulator::new(rpc_url),
            book: Arc::new(Mutex::new(book)),
            ledger,
            telegram,
        }
    }

    /// Handle a strategy Decision. Only `Enter` is actionable in Day-3.
    pub async fn execute(&self, decision: Decision, calldata: &[u8]) -> Result<()> {
        match decision {
            Decision::Skip { .. } | Decision::Exit { .. } => Ok(()),
            Decision::Enter {
                kol_name,
                bnb_amount,
                kol_bnb_input,
                kol_tx,
                ..
            } => self.handle_enter(kol_name, calldata, bnb_amount, kol_bnb_input, kol_tx).await,
        }
    }

    async fn handle_enter(
        &self,
        kol_name: String,
        calldata: &[u8],
        bnb_amount: U256,
        kol_bnb_input: U256,
        kol_tx: B256,
    ) -> Result<()> {
        let Some(token_addr) = extract_target_token(calldata) else {
            tracing::debug!(
                target: "trader",
                kol = %kol_name,
                "could not decode target token from calldata; skipping"
            );
            return Ok(());
        };

        // Resolve symbol/decimals via the cached resolver.
        let info = match self.resolver.lookup(token_addr).await {
            Some(i) => i,
            None => {
                tracing::warn!(
                    target: "trader",
                    kol = %kol_name,
                    token = %format!("{token_addr:#x}"),
                    "token metadata lookup failed; skipping"
                );
                return Ok(());
            }
        };

        // Simulate our buy at the current head. Both portfolios get the same
        // sim'd output for Day-3; Day-4 will model FastTip (early-in-block
        // fill) vs NormalTip (late-in-block, after the cohort).
        let Some(quote) = self.sim.simulate_buy(bnb_amount, token_addr, None).await? else {
            tracing::warn!(
                target: "trader",
                kol = %kol_name,
                token = %info.symbol,
                "no V2/V3 liquidity for this token; skipping entry"
            );
            return Ok(());
        };

        let now_block = current_head_block(&self.rpc_url).await.unwrap_or(0);
        let mut book = self.book.lock().await;
        for mode in PortfolioMode::ALL {
            book.open_or_add(
                *mode,
                kol_name.clone(),
                token_addr,
                info.symbol.clone(),
                info.decimals,
                bnb_amount,
                quote.amount_out,
                now_block,
                kol_tx,
            );
        }
        let snapshot_count = book.len();
        drop(book);

        // Persist book.
        if let Err(e) = self.ledger.save_book(&*self.book.lock().await).await {
            tracing::warn!(target: "trader", error = %e, "save_book failed");
        }

        tracing::info!(
            target: "trader",
            kol = %kol_name,
            token = %info.symbol,
            token_addr = %format!("{token_addr:#x}"),
            bnb_amount_wei = %bnb_amount,
            tokens_bought = %quote.amount_out,
            venue = quote.venue.label(),
            fee_tier = ?quote.fee_tier,
            kol_bnb_in_wei = %kol_bnb_input,
            block = now_block,
            positions = snapshot_count,
            "paper trade ENTERED in both portfolios"
        );
        metrics::counter!(
            "bsc_trader_entries_total",
            "kol" => kol_name.clone(),
            "venue" => quote.venue.label().to_string(),
        )
        .increment(2);

        if let Some(tg) = &self.telegram {
            let body = format_enter_alert(
                &kol_name,
                &info.symbol,
                token_addr,
                bnb_amount,
                kol_bnb_input,
                &quote,
                kol_tx,
            );
            tg.send(body).await;
        }
        Ok(())
    }

    /// Walk every open position; if older than `hold_timeout`, simulate a
    /// sell at the current head and close.
    pub async fn sweep_timeouts(&self, hold_timeout: Duration) {
        let cutoff_ns = unix_ns().saturating_sub(hold_timeout.as_nanos() as u64);
        let keys = {
            let book = self.book.lock().await;
            book.opened_before(cutoff_ns)
        };
        if keys.is_empty() {
            return;
        }
        tracing::info!(target: "trader", n = keys.len(), "timeout sweep");
        for key in keys {
            if let Err(e) = self.close_one(&key, CloseReason::Timeout, None).await {
                tracing::warn!(
                    target: "trader",
                    error = %e,
                    kol = %key.kol_name,
                    token = %format!("{:#x}", key.token),
                    "close failed during sweep"
                );
            }
        }
    }

    async fn close_one(
        &self,
        key: &PositionKey,
        reason: CloseReason,
        trigger_sell_tx: Option<B256>,
    ) -> Result<()> {
        let position = {
            let book = self.book.lock().await;
            book.get(key).cloned()
        };
        let Some(p) = position else {
            return Ok(());
        };

        // Simulate selling all our held tokens for BNB.
        let now_block = current_head_block(&self.rpc_url).await.unwrap_or(0);
        let quote = self
            .sim
            .simulate_sell(p.tokens_held, p.token_address, None)
            .await
            .ok()
            .flatten();
        let (bnb_out, close_reason_final) = match quote {
            Some(q) if !q.amount_out.is_zero() => (q.amount_out, reason),
            _ => (U256::ZERO, CloseReason::NoLiquidity),
        };

        let pnl_wei: i128 = i128::try_from(u128::try_from(bnb_out).unwrap_or(u128::MAX))
            .unwrap_or(i128::MAX)
            .saturating_sub(
                i128::try_from(u128::try_from(p.bnb_in).unwrap_or(u128::MAX))
                    .unwrap_or(i128::MAX),
            );
        let pnl_pct = if p.bnb_in.is_zero() {
            0.0
        } else {
            // Use string-divide approach because U256 → f64 loses precision.
            let inp: f64 = p.bnb_in.to_string().parse().unwrap_or(1.0);
            let out: f64 = bnb_out.to_string().parse().unwrap_or(0.0);
            (out - inp) / inp
        };
        let closed_at_ns = unix_ns();
        let held_secs = closed_at_ns
            .saturating_sub(p.opened_at_unix_ns)
            .saturating_div(1_000_000_000);

        let trade = ClosedTrade {
            portfolio: p.portfolio,
            kol_name: p.kol_name.clone(),
            token_address: p.token_address,
            token_symbol: p.token_symbol.clone(),
            bnb_in_wei: p.bnb_in,
            bnb_out_wei: bnb_out,
            tokens_traded: p.tokens_held,
            pnl_wei,
            pnl_pct,
            opened_at_block: p.opened_at_block,
            opened_at_unix_ns: p.opened_at_unix_ns,
            closed_at_block: now_block,
            closed_at_unix_ns: closed_at_ns,
            held_secs,
            buy_tx_count: p.buy_tx_hashes.len(),
            close_reason: close_reason_final,
            trigger_sell_tx,
        };

        // Persist trade row, remove from book, persist book.
        if let Err(e) = self.ledger.append_trade(&trade).await {
            tracing::warn!(target: "trader", error = %e, "append_trade failed");
        }
        {
            let mut book = self.book.lock().await;
            book.remove(key);
        }
        if let Err(e) = self.ledger.save_book(&*self.book.lock().await).await {
            tracing::warn!(target: "trader", error = %e, "save_book failed after close");
        }

        tracing::info!(
            target: "trader",
            kol = %p.kol_name,
            token = %p.token_symbol,
            portfolio = %p.portfolio.label(),
            bnb_in_wei = %p.bnb_in,
            bnb_out_wei = %bnb_out,
            pnl_wei,
            pnl_pct,
            held_secs,
            close_reason = close_reason_final.label(),
            "paper trade CLOSED"
        );
        metrics::counter!(
            "bsc_trader_exits_total",
            "kol" => p.kol_name.clone(),
            "reason" => close_reason_final.label().to_string(),
        )
        .increment(1);

        if let Some(tg) = &self.telegram {
            let body = format_close_alert(&trade, &p);
            tg.send(body).await;
        }
        Ok(())
    }
}

// =============================================================================
// Telegram alerter — dedicated client, isolated from kol_watch
// =============================================================================

pub struct TelegramAlerter {
    pub bot_token: String,
    pub chat_id: i64,
    client: reqwest::Client,
}

impl TelegramAlerter {
    pub fn new(bot_token: String, chat_id: i64) -> Self {
        Self {
            bot_token,
            chat_id,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("build reqwest"),
        }
    }

    pub async fn send(&self, html_text: String) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        let payload = serde_json::json!({
            "chat_id": self.chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });
        for attempt in 0..2 {
            match self.client.post(&url).json(&payload).send().await {
                Ok(r) if r.status().is_success() => {
                    metrics::counter!("bsc_trader_telegram_sent_total").increment(1);
                    return;
                }
                Ok(r) => {
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    if status.as_u16() == 429 && attempt == 0 {
                        let retry_after = parse_retry_after(&body).unwrap_or(5);
                        tokio::time::sleep(Duration::from_secs(retry_after)).await;
                        continue;
                    }
                    metrics::counter!("bsc_trader_telegram_failed_total").increment(1);
                    tracing::warn!(target: "trader", status = %status, body = %body, "telegram non-2xx");
                    return;
                }
                Err(e) => {
                    metrics::counter!("bsc_trader_telegram_failed_total").increment(1);
                    tracing::warn!(target: "trader", error = %e, "telegram error");
                    return;
                }
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

// =============================================================================
// Format helpers + small RPC helpers
// =============================================================================

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn format_bnb(wei: U256) -> String {
    // Lossy U256 → f64 fine for human display.
    let s = wei.to_string();
    let n: f64 = s.parse().unwrap_or(0.0);
    let bnb = n / 1e18;
    if bnb >= 1.0 {
        format!("{bnb:.3} BNB")
    } else if bnb >= 0.001 {
        format!("{bnb:.4} BNB")
    } else {
        format!("{bnb:.6} BNB")
    }
}

fn format_enter_alert(
    kol_name: &str,
    token_symbol: &str,
    token_addr: Address,
    bnb_amount: U256,
    kol_bnb_input: U256,
    quote: &QuoteResult,
    kol_tx: B256,
) -> String {
    format!(
        "🟢 <b>PAPER ENTRY</b>\n\n\
         👤 KOL {kol}\n\
         💰 in:  {our_bnb}  ({size_pct:.1}% of KOL's {kol_bnb})\n\
         🎯 token:  <b>{sym}</b>  <code>{token:#x}</code>\n\
         💱 venue:  {venue}{fee}\n\
         🧾 KOL tx:  <a href=\"https://bscscan.com/tx/{tx:#x}\">view</a>",
        kol = html_escape(kol_name),
        our_bnb = format_bnb(bnb_amount),
        kol_bnb = format_bnb(kol_bnb_input),
        size_pct = {
            // (bnb_amount / kol_bnb_input) * 100, both U256
            let inp: f64 = kol_bnb_input.to_string().parse().unwrap_or(1.0);
            let our: f64 = bnb_amount.to_string().parse().unwrap_or(0.0);
            (our / inp) * 100.0
        },
        sym = html_escape(token_symbol),
        token = token_addr,
        venue = quote.venue.label(),
        fee = quote
            .fee_tier
            .map(|f| format!(" (fee {:.2}%)", f as f64 / 10_000.0))
            .unwrap_or_default(),
        tx = kol_tx,
    )
}

fn format_close_alert(trade: &ClosedTrade, _pos: &OpenPosition) -> String {
    let icon = if trade.pnl_wei >= 0 { "🟢" } else { "🔴" };
    let pnl_pct_str = format!("{:+.2}%", trade.pnl_pct * 100.0);
    format!(
        "{icon} <b>PAPER CLOSE</b> · {portfolio}\n\n\
         👤 KOL {kol}\n\
         🎯 <b>{sym}</b>  <code>{token:#x}</code>\n\
         💰 in {bnb_in}  →  out {bnb_out}\n\
         📊 PnL:  <b>{pnl_pct}</b>\n\
         ⏱  held {held}s\n\
         🛑 reason:  {reason}",
        portfolio = trade.portfolio.label(),
        kol = html_escape(&trade.kol_name),
        sym = html_escape(&trade.token_symbol),
        token = trade.token_address,
        bnb_in = format_bnb(trade.bnb_in_wei),
        bnb_out = format_bnb(trade.bnb_out_wei),
        pnl_pct = pnl_pct_str,
        held = trade.held_secs,
        reason = trade.close_reason.label(),
    )
}

async fn current_head_block(rpc_url: &str) -> Result<u64> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()?;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    });
    let resp: serde_json::Value = client.post(rpc_url).json(&body).send().await?.json().await?;
    let hex = resp
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("no result"))?;
    let s = hex.strip_prefix("0x").unwrap_or(hex);
    Ok(u64::from_str_radix(s, 16)?)
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_bnb_scales() {
        assert_eq!(format_bnb(U256::from(1_500_000_000_000_000_000u128)), "1.500 BNB");
        assert_eq!(format_bnb(U256::from(50_000_000_000_000_000u128)), "0.0500 BNB");
        // 0.001 BNB takes the `>= 0.001` branch → 4-decimal
        assert_eq!(format_bnb(U256::from(1_000_000_000_000_000u128)), "0.0010 BNB");
        // sub-millibnb → 6-decimal
        assert_eq!(format_bnb(U256::from(100_000_000_000_000u128)), "0.000100 BNB");
    }

    #[test]
    fn html_escape_works() {
        assert_eq!(html_escape("a & b < c > d"), "a &amp; b &lt; c &gt; d");
    }
}
