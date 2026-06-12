#![allow(dead_code)]  // paper-trader code retained for re-enable; many helpers unused while live trader is active
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
use crate::trader::kol_budget::KolBudgetBook;
use crate::trader::ledger::Ledger;
use crate::trader::position::PositionBook;
use crate::trader::sim::{QuoteResult, QuoteVenue, Simulator};
use crate::trader::strategy::extract_target_token;
use crate::trader::types::{
    CloseReason, ClosedTrade, Decision, OpenPosition, PortfolioMode, PositionKey,
};
use alloy::hex;
use alloy::primitives::{Address, B256, U256};
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

/// Realism haircuts — paper PnL without these is an upper bound. See
/// `TraderConfig::slippage_bps_*` and `gas_per_trade_usd` for the rationale.
#[derive(Debug, Clone, Copy)]
pub struct Costs {
    pub slip_v2_bps: u32,
    pub slip_bonding_bps: u32,
    pub gas_per_trade_usd: f64,
}

pub struct PaperExecutor {
    pub rpc_url: String,
    pub resolver: Arc<TokenResolver>,
    pub sim: Simulator,
    /// Archive-backed simulator (NodeReal). Used only as a fallback when
    /// the local pruned node can't serve historical state at kol_block+1
    /// (rare — PBSS window covers ~128 blocks ≈ 1 min, and we re-quote
    /// within seconds). Rate-capped via the global ARCHIVE_* counters.
    pub archive_sim: Option<Simulator>,
    pub book: Arc<Mutex<PositionBook>>,
    pub ledger: Arc<Ledger>,
    pub telegram: Option<Arc<TelegramAlerter>>,
    /// Shared held-token set — Phase 2/3 monitors read this.
    pub held: Arc<crate::held_tokens::HeldTokens>,
    /// BNB/USD for mcap + readable PnL.
    pub bnb_price: Arc<crate::bnb_price::BnbPrice>,
    /// HTTP client for mcap RPC calls.
    pub http: reqwest::Client,
    /// Optional archive RPC (NodeReal) — exit-pricing FALLBACK only,
    /// rate-capped. Empty = disabled.
    pub archive_rpc: String,
    /// Slippage + gas haircut parameters.
    pub costs: Costs,
    /// Per-KOL closed-loop budget book. Each KOL has an independent BNB
    /// pot; position size = `position_pct_bps` × current cash. Trades that
    /// would size below the dust floor (or for a fully-exhausted KOL) are
    /// skipped. `None` ⇒ no budgeting, falls back to strategy-supplied
    /// `bnb_amount`.
    pub kol_budgets: Option<Arc<KolBudgetBook>>,
}

impl PaperExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_url: String,
        resolver: Arc<TokenResolver>,
        ledger: Arc<Ledger>,
        telegram: Option<Arc<TelegramAlerter>>,
        held: Arc<crate::held_tokens::HeldTokens>,
        bnb_price: Arc<crate::bnb_price::BnbPrice>,
        archive_rpc: String,
        costs: Costs,
        kol_budgets: Option<Arc<KolBudgetBook>>,
    ) -> Self {
        let book = ledger.load_book().unwrap_or_else(|e| {
            tracing::warn!(target: "trader", error = %e, "ledger load failed; starting empty");
            PositionBook::new()
        });
        let n = book.len();
        if n > 0 {
            tracing::info!(target: "trader", positions = n, "loaded open positions from JSON");
        }
        // Rehydrate held-set from any positions restored off disk.
        for p in book.snapshot() {
            held.insert(
                p.token_address,
                crate::held_tokens::HeldMeta {
                    kol_name: p.kol_name.clone(),
                    symbol: p.token_symbol.clone(),
                    entered_block: p.opened_at_block,
                    entered_unix_ns: p.opened_at_unix_ns,
                    bnb_in_wei: u128::try_from(p.bnb_in).unwrap_or(u128::MAX),
                },
            );
        }
        let archive_sim = if archive_rpc.is_empty() {
            None
        } else {
            Some(Simulator::new(archive_rpc.clone()))
        };
        Self {
            rpc_url: rpc_url.clone(),
            resolver,
            sim: Simulator::new(rpc_url),
            archive_sim,
            book: Arc::new(Mutex::new(book)),
            ledger,
            telegram,
            held,
            bnb_price,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(4))
                .build()
                .expect("reqwest"),
            archive_rpc,
            costs,
            kol_budgets,
        }
    }

    /// Handle a strategy Decision.
    pub async fn execute(&self, decision: Decision, calldata: &[u8]) -> Result<()> {
        match decision {
            Decision::Skip { .. } => Ok(()),
            Decision::Exit {
                kol_name,
                token,
                kol_tx,
                kol_block,
                kol_addr,
            } => self.handle_exit(kol_name, token, kol_tx, kol_block, kol_addr).await,
            Decision::Enter {
                kol_name,
                token,
                bnb_amount,
                kol_bnb_input,
                kol_tx,
                ..
            } => {
                self.handle_enter(
                    kol_name, token, calldata, bnb_amount, kol_bnb_input, kol_tx,
                )
                .await
            }
        }
    }

    /// KOL (in scope) sold a token — close a PROPORTIONAL slice of every
    /// open position on that token across all portfolios, reason `KolSell`.
    ///
    /// "Proportional" = the same fraction the KOL just sold.
    /// `fraction = (balanceOf(kol,token,sell_block-1) - balanceOf(kol,token,sell_block)) / balanceOf(kol,token,sell_block-1)`.
    /// If we can't compute the fraction (sell_block unknown, RPC error,
    /// or KOL's pre-sell balance was zero / increased), fall back to a
    /// FULL close — matches the legacy behaviour.
    async fn handle_exit(
        &self,
        kol_name: String,
        token: Address,
        sell_tx: B256,
        kol_block: u64,
        kol_addr: Address,
    ) -> Result<()> {
        let keys: Vec<PositionKey> = {
            let book = self.book.lock().await;
            book.keys_for_kol_token(&kol_name, token)
        };
        if keys.is_empty() {
            return Ok(());
        }

        // Compute the KOL's sell fraction. Returns 1.0 (full close) on any
        // failure path so we don't silently DROP an exit signal.
        let fraction = self
            .kol_sell_fraction(kol_addr, token, kol_block)
            .await
            .unwrap_or(1.0);

        tracing::info!(
            target: "trader",
            kol = %kol_name,
            token = %format!("{token:#x}"),
            n = keys.len(),
            kol_block,
            fraction = format!("{:.4}", fraction),
            "KOL SELL on held token — closing [exit-follow, proportional]"
        );
        for key in keys {
            if let Err(e) = self
                .close_one(&key, CloseReason::KolSell, Some(sell_tx), fraction)
                .await
            {
                tracing::warn!(target: "trader", error = %e, "exit-follow close failed");
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_enter(
        &self,
        kol_name: String,
        token: Address,
        calldata: &[u8],
        bnb_amount: U256,
        kol_bnb_input: U256,
        kol_tx: B256,
    ) -> Result<()> {
        let token_addr = if token != Address::ZERO {
            token
        } else {
            match extract_target_token(calldata) {
                Some(t) => t,
                None => {
                    tracing::debug!(
                        target: "trader",
                        kol = %kol_name,
                        "no token in decision or calldata; skipping"
                    );
                    return Ok(());
                }
            }
        };

        // Blacklist gate — stables, BTCB, ETH, ASTER, etc. KOLs sometimes
        // route through these (USDT-bridged, BNB→majors arbitrage) but they
        // are NEVER meme signals. Skipping them keeps tokflow + ledger
        // clean and prevents bogus paper PnL from being attributed to the
        // KOL on a major-asset swap.
        if crate::trader::blacklist::is_blacklisted(token_addr) {
            tracing::debug!(
                target: "trader",
                kol = %kol_name,
                token = %format!("{token_addr:#x}"),
                "skip: blacklisted (stable / major / non-meme)"
            );
            return Ok(());
        }

        // Per-KOL closed-loop budget gate. If enabled, we override the
        // strategy-supplied `bnb_amount` with `position_pct × KOL_cash`.
        // Each entry reserves cash; matching credit happens in close_one.
        let bnb_amount = if let Some(book) = self.kol_budgets.as_ref() {
            match book.try_reserve(&kol_name).await {
                Some(amt) => {
                    if let Err(e) = book.save().await {
                        tracing::warn!(
                            target: "trader", error = %e, "kol_budgets save failed"
                        );
                    }
                    amt
                }
                None => {
                    tracing::info!(
                        target: "trader",
                        kol = %kol_name,
                        token = %format!("{token_addr:#x}"),
                        "skip: per-KOL budget exhausted"
                    );
                    return Ok(());
                }
            }
        } else {
            bnb_amount
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

        // Price our copy fill. Prefer a PancakeSwap V2/V3 quote (graduated
        // tokens). If there's no pool — the token is still on a Four.Meme
        // bonding curve / flap (the PRIMARY target) — fall back to filling
        // at the KOL's own executed price, read from his tx receipt
        // (venue-agnostic).
        //
        // After the venue is settled, we apply a per-venue ADVERSE-slippage
        // haircut (`Costs::slip_*`) because we land at +1 block AFTER D —
        // pool/curve has already moved. Without this the paper PnL is an
        // upper bound; with it, it is a conservative lower bound suitable
        // for go/no-go decisions on real money.
        let raw_quote = match self.sim.simulate_buy(bnb_amount, token_addr, None).await? {
            Some(q) => q,
            None => {
                match kol_exec_price(
                    &self.http,
                    &self.rpc_url,
                    &self.archive_rpc,
                    kol_tx,
                    token_addr,
                    info.decimals,
                    true,
                )
                .await
                {
                    Some(price) if price > 0.0 => {
                        let in_bnb =
                            bnb_amount.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;
                        let toks_whole = in_bnb / price; // fill at KOL's price
                        let raw = toks_whole * 10f64.powi(i32::from(info.decimals));
                        QuoteResult {
                            amount_out: U256::from(raw.max(0.0) as u128),
                            venue: QuoteVenue::Bonding,
                            fee_tier: None,
                        }
                    }
                    _ => {
                        tracing::warn!(
                            target: "trader",
                            kol = %kol_name,
                            token = %info.symbol,
                            "no V2/V3 pool and KOL receipt price unavailable; \
                             skipping entry"
                        );
                        return Ok(());
                    }
                }
            }
        };

        // Replace the optimistic pre-D quote with the REAL fill our +1-block
        // copy tx would have received, by re-simulating at the end of block
        // kol_block+1 (post-D, post-cohort). Only works for V2/V3 venues
        // because Four.Meme's bonding curve has no public quoter ABI — for
        // bonding we keep the static-haircut model.
        let (quote, fill_source) = if matches!(
            raw_quote.venue,
            QuoteVenue::PancakeV2 | QuoteVenue::PancakeV3
        ) {
            match measured_v2v3_fill_at_kol_plus1(
                &self.sim,
                self.archive_sim.as_ref(),
                &self.http,
                &self.rpc_url,
                true,
                bnb_amount,
                token_addr,
                kol_tx,
            )
            .await
            {
                Some(m) => (m, "measured"),
                None => {
                    // Local + archive both unavailable — fall back to haircut.
                    let after = bps_haircut_u256(raw_quote.amount_out, self.costs.slip_v2_bps);
                    (
                        QuoteResult {
                            amount_out: after,
                            venue: raw_quote.venue,
                            fee_tier: raw_quote.fee_tier,
                        },
                        "static_haircut",
                    )
                }
            }
        } else {
            // Bonding: no public quoter. Replace D's price with the REAL
            // median fill price of actual buyers in block kol_block+1
            // (chain-observed via NodeReal archive eth_getLogs). No estimate.
            match measured_chain_swap_price_at_kol_plus1(
                &self.http,
                &self.rpc_url,
                &self.archive_rpc,
                true,
                token_addr,
                info.decimals,
                kol_tx,
            )
            .await
            {
                Some(real_price) if real_price > 0.0 => {
                    let in_bnb = bnb_amount.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;
                    let toks_whole = in_bnb / real_price;
                    let raw = toks_whole * 10f64.powi(i32::from(info.decimals));
                    (
                        QuoteResult {
                            amount_out: U256::from(raw.max(0.0) as u128),
                            venue: raw_quote.venue,
                            fee_tier: raw_quote.fee_tier,
                        },
                        "measured_chain_swap",
                    )
                }
                _ => {
                    // No real swap observable in N+1..N+5 — fall back to
                    // D's exec price × static haircut as the absolute last
                    // resort.
                    let after =
                        bps_haircut_u256(raw_quote.amount_out, self.costs.slip_bonding_bps);
                    (
                        QuoteResult {
                            amount_out: after,
                            venue: raw_quote.venue,
                            fee_tier: raw_quote.fee_tier,
                        },
                        "static_haircut",
                    )
                }
            }
        };
        tracing::info!(
            target: "trader",
            kol = %kol_name,
            sym = %info.symbol,
            venue = quote.venue.label(),
            tokens_pre = %raw_quote.amount_out,
            tokens_fill = %quote.amount_out,
            fill_source,
            "entry fill resolved"
        );

        // ── market-cap context ────────────────────────────────────────
        // d_mcap   = KOL's entry mcap (price he paid × supply × bnb_usd)
        // our_mcap = our +1-block effective fill mcap
        //
        // V2/V3 venues: read pool reserves directly (most accurate).
        // Bonding venues: derive KOL price from his receipt + use our
        // chain-swap based fill price for ours.
        let bnb_usd = self.bnb_price.get().await.unwrap_or(0.0);
        let (d_mcap_usd, our_mcap_usd) = if matches!(
            quote.venue,
            QuoteVenue::PancakeV2 | QuoteVenue::PancakeV3
        ) {
            match crate::mcap::pool_spot_and_supply(
                &self.http,
                &self.rpc_url,
                token_addr,
                info.decimals,
            )
            .await
            {
                Some((spot_bnb, supply_whole)) => {
                    let d = spot_bnb * supply_whole * bnb_usd;
                    let in_bnb = bnb_amount.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;
                    let out_tok = quote.amount_out.to_string().parse::<f64>().unwrap_or(0.0)
                        / 10f64.powi(i32::from(info.decimals));
                    let our_price = if out_tok > 0.0 { in_bnb / out_tok } else { 0.0 };
                    (d, our_price * supply_whole * bnb_usd)
                }
                None => (0.0, 0.0),
            }
        } else {
            // Bonding-only path: chain-derived prices + raw supply lookup.
            let supply_whole = crate::mcap::total_supply_whole(
                &self.http,
                &self.rpc_url,
                token_addr,
                info.decimals,
            )
            .await
            .unwrap_or(0.0);
            if supply_whole > 0.0 {
                let kol_price = kol_exec_price(
                    &self.http,
                    &self.rpc_url,
                    &self.archive_rpc,
                    kol_tx,
                    token_addr,
                    info.decimals,
                    true,
                )
                .await
                .unwrap_or(0.0);
                let in_bnb = bnb_amount.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;
                let out_tok = quote.amount_out.to_string().parse::<f64>().unwrap_or(0.0)
                    / 10f64.powi(i32::from(info.decimals));
                let our_price = if out_tok > 0.0 { in_bnb / out_tok } else { 0.0 };
                (
                    kol_price * supply_whole * bnb_usd,
                    our_price * supply_whole * bnb_usd,
                )
            } else {
                (0.0, 0.0)
            }
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
            book.set_entry_mcaps(
                &PositionKey {
                    portfolio: *mode,
                    kol_name: kol_name.clone(),
                    token: token_addr,
                },
                d_mcap_usd,
                our_mcap_usd,
            );
        }
        let snapshot_count = book.len();
        drop(book);

        // Register in the shared held-set so Phase 2/3 monitors pick it up.
        self.held.insert(
            token_addr,
            crate::held_tokens::HeldMeta {
                kol_name: kol_name.clone(),
                symbol: info.symbol.clone(),
                entered_block: now_block,
                entered_unix_ns: unix_ns(),
                bnb_in_wei: u128::try_from(bnb_amount)
                    .unwrap_or(u128::MAX)
                    .saturating_mul(2), // both portfolios
            },
        );

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
            "paper trade ENTERED"
        );
        metrics::counter!(
            "bsc_trader_entries_total",
            "kol" => kol_name.clone(),
            "venue" => quote.venue.label().to_string(),
        )
        .increment(1);

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
            if let Err(e) = self.close_one(&key, CloseReason::Timeout, None, 1.0).await {
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
        fraction: f64,
    ) -> Result<()> {
        let position = {
            let book = self.book.lock().await;
            book.get(key).cloned()
        };
        let Some(p) = position else {
            return Ok(());
        };

        // PROPORTIONAL close: sell `fraction` of our held tokens and the
        // matching slice of cost basis. fraction >= 0.99 → full close (avoid
        // dust remnant positions that we'd never sell otherwise).
        let fraction = fraction.clamp(0.0, 1.0);
        let is_full_close = fraction >= 0.99;
        let sell_tokens = if is_full_close {
            p.tokens_held
        } else {
            scale_u256_by_fraction(p.tokens_held, fraction)
        };
        let sell_bnb_in = if is_full_close {
            p.bnb_in
        } else {
            scale_u256_by_fraction(p.bnb_in, fraction)
        };
        let remaining_tokens = if is_full_close {
            U256::ZERO
        } else {
            saturating_sub_u256(p.tokens_held, sell_tokens)
        };
        let remaining_bnb_in = if is_full_close {
            U256::ZERO
        } else {
            saturating_sub_u256(p.bnb_in, sell_bnb_in)
        };
        if sell_tokens.is_zero() {
            // Nothing to close (fraction effectively zero); leave position open.
            return Ok(());
        }

        // Simulate selling the PROPORTIONAL slice (not the full bag).
        // V2/V3: `measured_v2v3_fill_at_kol_plus1` already does impact-aware
        // simulation at sell_block+1 — passing the proportional amount gives
        // a fair quote for OUR actual exit size.
        // Bonding: chain-observed median price × proportional tokens; we
        // skip extra size-impact modelling because our copy size (~10% of
        // KOL's BNB → capped by per-KOL budget) is << the curve volume the
        // KOL's own sell just absorbed.
        let now_block = current_head_block(&self.rpc_url).await.unwrap_or(0);
        let measured_exit = if let Some(stx) = trigger_sell_tx {
            measured_v2v3_fill_at_kol_plus1(
                &self.sim,
                self.archive_sim.as_ref(),
                &self.http,
                &self.rpc_url,
                false,
                sell_tokens,
                p.token_address,
                stx,
            )
            .await
        } else {
            None
        };
        let exit_was_measured = measured_exit.is_some();
        let quote = match measured_exit {
            Some(q) => Some(q),
            None => self
                .sim
                .simulate_sell(sell_tokens, p.token_address, None)
                .await
                .ok()
                .flatten(),
        };
        let bnb_usd_at_close = self.bnb_price.get().await.unwrap_or(0.0);
        // 2× gas (entry+exit round-trip) in wei at current BNB/USD. Subtract
        // from bnb_out so the paper number includes the real cost of trading.
        let gas_wei: u128 = if bnb_usd_at_close > 0.0 {
            let bnb = (self.costs.gas_per_trade_usd * 2.0) / bnb_usd_at_close;
            (bnb * 1e18).max(0.0) as u128
        } else {
            0
        };

        let (bnb_out, close_reason_final) = match quote {
            Some(q) if !q.amount_out.is_zero() => {
                // V2/V3 sim succeeded. If MEASURED at sell_block+1 the haircut
                // is built-in (real post-D-sell state). Otherwise apply static
                // haircut over LATEST. Gas always deducted.
                let after_slip = if exit_was_measured {
                    q.amount_out
                } else {
                    bps_haircut_u256(q.amount_out, self.costs.slip_v2_bps)
                };
                let after_gas = saturating_sub_u256(after_slip, U256::from(gas_wei));
                tracing::info!(
                    target: "trader",
                    kol = %p.kol_name,
                    sym = %p.token_symbol,
                    fill_source = if exit_was_measured { "measured" } else { "static_haircut" },
                    venue = q.venue.label(),
                    bnb_out_wei = %after_gas,
                    "exit fill resolved"
                );
                (after_gas, reason)
            }
            _ => {
                // Bonding-curve token (no V2/V3 pool). Prefer the REAL
                // median sell price observed in actual on-chain sells in
                // block sell_block+1..+5 (chain-derived, exact). Falls
                // back to D's exec price × static haircut only if no real
                // sell observable in that window, and finally to FLAT.
                let bonding_priced = if let Some(stx) = trigger_sell_tx {
                    measured_chain_swap_price_at_kol_plus1(
                        &self.http,
                        &self.rpc_url,
                        &self.archive_rpc,
                        false,
                        p.token_address,
                        p.token_decimals,
                        stx,
                    )
                    .await
                } else {
                    None
                };
                let (priced, source): (Option<f64>, &'static str) = match bonding_priced {
                    Some(px) => (Some(px), "measured_chain_swap"),
                    None => {
                        let p2 = if let Some(stx) = trigger_sell_tx {
                            kol_exec_price(
                                &self.http,
                                &self.rpc_url,
                                &self.archive_rpc,
                                stx,
                                p.token_address,
                                p.token_decimals,
                                false,
                            )
                            .await
                        } else {
                            None
                        };
                        (p2, "static_haircut")
                    }
                };
                match priced {
                    Some(px) if px > 0.0 => {
                        // Price the PROPORTIONAL slice, not the full bag.
                        let toks_whole = sell_tokens.to_string().parse::<f64>().unwrap_or(0.0)
                            / 10f64.powi(i32::from(p.token_decimals));
                        let out_bnb = toks_whole * px;
                        let raw = out_bnb * 1e18;
                        let pre = U256::from(raw.max(0.0) as u128);
                        let after_slip = if source == "measured_chain_swap" {
                            pre // chain-derived: no synthetic haircut
                        } else {
                            bps_haircut_u256(pre, self.costs.slip_bonding_bps)
                        };
                        let after_gas = saturating_sub_u256(after_slip, U256::from(gas_wei));
                        tracing::info!(
                            target: "trader",
                            kol = %p.kol_name,
                            sym = %p.token_symbol,
                            fill_source = source,
                            venue = "bonding",
                            bnb_out_wei = %after_gas,
                            "exit fill resolved"
                        );
                        (after_gas, reason)
                    }
                    // Could NOT value the exit — no observable swap and
                    // no D-receipt price (or out of state window). Book
                    // FLAT on the sold slice (out = sell_bnb_in, PnL 0),
                    // tag PriceUnavailable so these rows are excluded
                    // from win-rate.
                    _ => (sell_bnb_in, CloseReason::PriceUnavailable),
                }
            }
        };

        // PnL is on the PROPORTIONAL slice we just sold (sell_bnb_in cost
        // basis → bnb_out proceeds). Remaining position's PnL is realized
        // on its own future close.
        let pnl_wei: i128 = i128::try_from(u128::try_from(bnb_out).unwrap_or(u128::MAX))
            .unwrap_or(i128::MAX)
            .saturating_sub(
                i128::try_from(u128::try_from(sell_bnb_in).unwrap_or(u128::MAX))
                    .unwrap_or(i128::MAX),
            );
        let pnl_pct = if sell_bnb_in.is_zero() {
            0.0
        } else {
            let inp: f64 = sell_bnb_in.to_string().parse().unwrap_or(1.0);
            let out: f64 = bnb_out.to_string().parse().unwrap_or(0.0);
            (out - inp) / inp
        };
        let closed_at_ns = unix_ns();
        let held_secs = closed_at_ns
            .saturating_sub(p.opened_at_unix_ns)
            .saturating_div(1_000_000_000);

        // ── exit market caps ──────────────────────────────────────────
        // KOL's exit price × supply (= what GMGN shows for "sold at $X mcap")
        // Our +1-block fill price × supply = the mcap our copy lands at
        let (kol_exit_mcap, our_exit_mcap) = if let Some(stx) = trigger_sell_tx {
            let supply_whole = crate::mcap::total_supply_whole(
                &self.http,
                &self.rpc_url,
                p.token_address,
                p.token_decimals,
            )
            .await
            .unwrap_or(0.0);
            if supply_whole > 0.0 {
                let kol_px = kol_exec_price(
                    &self.http,
                    &self.rpc_url,
                    &self.archive_rpc,
                    stx,
                    p.token_address,
                    p.token_decimals,
                    false,
                )
                .await
                .unwrap_or(0.0);
                let toks_whole = sell_tokens.to_string().parse::<f64>().unwrap_or(0.0)
                    / 10f64.powi(i32::from(p.token_decimals));
                let bnb_out_bnb = bnb_out.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;
                let our_px = if toks_whole > 0.0 { bnb_out_bnb / toks_whole } else { 0.0 };
                (
                    kol_px * supply_whole * bnb_usd_at_close,
                    our_px * supply_whole * bnb_usd_at_close,
                )
            } else {
                (0.0, 0.0)
            }
        } else {
            (0.0, 0.0)
        };

        let trade = ClosedTrade {
            portfolio: p.portfolio,
            kol_name: p.kol_name.clone(),
            token_address: p.token_address,
            token_symbol: p.token_symbol.clone(),
            bnb_in_wei: sell_bnb_in,
            bnb_out_wei: bnb_out,
            tokens_traded: sell_tokens,
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
            d_mcap_usd: p.d_mcap_usd,
            our_entry_mcap_usd: p.our_entry_mcap_usd,
            bnb_usd_at_close,
            kol_exit_count: if trigger_sell_tx.is_some() { 1 } else { 0 },
            kol_exit_mcap_first_usd: kol_exit_mcap,
            kol_exit_mcap_last_usd: kol_exit_mcap,
            our_avg_exit_mcap_usd: our_exit_mcap,
        };

        // Credit the SOLD slice back to the per-KOL budget (committed
        // shrinks by sell_bnb_in; cash grows by bnb_out). The remaining
        // open portion's bnb_in stays committed until a future close.
        if let Some(book) = self.kol_budgets.as_ref() {
            book.credit_close(&p.kol_name, sell_bnb_in, bnb_out).await;
            if let Err(e) = book.save().await {
                tracing::warn!(target: "trader", error = %e, "kol_budgets save failed");
            }
        }

        // Persist trade row, update book.
        if let Err(e) = self.ledger.append_trade(&trade).await {
            tracing::warn!(target: "trader", error = %e, "append_trade failed");
        }
        let token_still_held = {
            let mut book = self.book.lock().await;
            if is_full_close {
                book.remove(key);
            } else {
                book.shrink(key, remaining_tokens, remaining_bnb_in);
            }
            book.iter().any(|(k, _)| k.token == key.token)
        };
        // Drop from the shared held-set only when NO portfolio holds it.
        if !token_still_held {
            self.held.remove(&key.token);
        }
        if let Err(e) = self.ledger.save_book(&*self.book.lock().await).await {
            tracing::warn!(target: "trader", error = %e, "save_book failed after close");
        }

        tracing::info!(
            target: "trader",
            kol = %p.kol_name,
            token = %p.token_symbol,
            portfolio = %p.portfolio.label(),
            slice_bnb_in_wei = %sell_bnb_in,
            bnb_out_wei = %bnb_out,
            remaining_bnb_in_wei = %remaining_bnb_in,
            pnl_wei,
            pnl_pct,
            held_secs,
            full_close = is_full_close,
            fraction = format!("{:.4}", fraction),
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

    /// Compute the KOL's sell fraction by diffing balanceOf at sell_block-1
    /// and sell_block. Returns `None` if we can't (sell_block unknown, RPC
    /// failure, KOL's pre-sell balance was zero, or post >= pre — caller
    /// then falls back to FULL close so we never drop an exit signal).
    async fn kol_sell_fraction(
        &self,
        kol_addr: Address,
        token: Address,
        kol_block: u64,
    ) -> Option<f64> {
        if kol_addr == Address::ZERO || kol_block == 0 {
            return None;
        }
        let pre = self.balance_of_at(kol_addr, token, kol_block.saturating_sub(1)).await?;
        let post = self.balance_of_at(kol_addr, token, kol_block).await?;
        if pre.is_zero() || post >= pre {
            return None;
        }
        let sold = pre - post;
        let sold_f: f64 = sold.to_string().parse().ok()?;
        let pre_f: f64 = pre.to_string().parse().ok()?;
        Some((sold_f / pre_f).clamp(0.0, 1.0))
    }

    /// `balanceOf(who, token)` at a specific block. Tries local node first
    /// (sufficient if block is within ~128-block PBSS window), then archive
    /// RPC. Returns `None` on any failure.
    async fn balance_of_at(
        &self,
        who: Address,
        token: Address,
        block: u64,
    ) -> Option<U256> {
        let mut data = String::with_capacity(74);
        data.push_str("0x70a08231");
        data.push_str(&"0".repeat(24));
        data.push_str(&hex::encode(who.as_slice()));
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                { "to": format!("{:#x}", token), "data": data },
                format!("0x{:x}", block),
            ],
            "id": 1,
        });
        if let Some(r) = self.eth_call_u256(&self.rpc_url, &body).await {
            return Some(r);
        }
        if !self.archive_rpc.is_empty() {
            return self.eth_call_u256(&self.archive_rpc, &body).await;
        }
        None
    }

    async fn eth_call_u256(&self, url: &str, body: &serde_json::Value) -> Option<U256> {
        let v: serde_json::Value = self.http.post(url).json(body).send().await.ok()?
            .json().await.ok()?;
        let s = v.get("result").and_then(|x| x.as_str())?;
        let hex = s.strip_prefix("0x")?;
        if hex.is_empty() {
            return Some(U256::ZERO);
        }
        U256::from_str_radix(hex, 16).ok()
    }
}

/// Scale a U256 by a fractional weight in basis points. `f` is clamped to
/// [0, 1]. Precision floor: 1 bp (0.01%) — sufficient for position sizing.
fn scale_u256_by_fraction(x: U256, f: f64) -> U256 {
    let bps = (f.clamp(0.0, 1.0) * 10_000.0) as u64;
    if bps == 0 {
        return U256::ZERO;
    }
    if bps >= 10_000 {
        return x;
    }
    (x * U256::from(bps)) / U256::from(10_000u32)
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

const TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const WBNB_LC: &str = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c";

// ── NodeReal archive fallback rate limiter (free plan — be gentle) ─────────
// Only ever hit when the pruned local node can't serve historical state for
// an EXIT price (rare: bonding sells outside the ~128-block window). Hard
// daily cap + min gap; over budget ⇒ skip (caller flats the trade).
static ARCHIVE_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ARCHIVE_LAST_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
const ARCHIVE_DAILY_CAP: u64 = 800; // process-lifetime; resets on restart
const ARCHIVE_MIN_GAP_NS: u64 = 400_000_000; // ≤ ~2.5 req/s

/// Rate-capped archive `eth_getBalance`. `None` if disabled / over budget /
/// throttled / failed — caller must treat as "couldn't price".
async fn archive_balance(
    http: &reqwest::Client,
    archive_url: &str,
    addr: &str,
    block: u128,
) -> Option<f64> {
    use std::sync::atomic::Ordering::Relaxed;
    if archive_url.is_empty() {
        return None;
    }
    if ARCHIVE_CALLS.load(Relaxed) >= ARCHIVE_DAILY_CAP {
        return None; // free-plan safety: stop hitting NodeReal
    }
    let now = unix_ns();
    if now.saturating_sub(ARCHIVE_LAST_NS.load(Relaxed)) < ARCHIVE_MIN_GAP_NS {
        return None; // too soon — skip rather than burst the free plan
    }
    ARCHIVE_LAST_NS.store(now, Relaxed);
    ARCHIVE_CALLS.fetch_add(1, Relaxed);
    rpc(
        http,
        archive_url,
        "eth_getBalance",
        serde_json::json!([addr, format!("0x{block:x}")]),
    )
    .await
    .and_then(|v| v.as_str().map(u_hex))
}

async fn rpc(
    http: &reqwest::Client,
    url: &str,
    method: &str,
    params: serde_json::Value,
) -> Option<serde_json::Value> {
    let body = serde_json::json!({"jsonrpc":"2.0","method":method,"params":params,"id":1});
    http.post(url)
        .json(&body)
        .send()
        .await
        .ok()?
        .json::<serde_json::Value>()
        .await
        .ok()?
        .get("result")
        .cloned()
}

fn u_hex(h: &str) -> f64 {
    u128::from_str_radix(h.trim_start_matches("0x"), 16)
        .map(|v| v as f64)
        .unwrap_or(0.0)
}

/// For a bonding-curve token (no V2/V3 quoter), find the REAL fill price
/// at block kol_block+1 by aggregating actual same-token swap txs in that
/// block (and the next few if quiet). Returns BNB-per-whole-token at the
/// MEDIAN price of those real buyers/sellers — the price an N+1 copier
/// actually paid, observed from chain.
///
/// No estimate, no synthesis: the answer comes from receipts of other
/// people's real swaps on the same token in the same block window.
///
/// Uses NodeReal archive `eth_getLogs` (1 call) to enumerate Transfer
/// events of the token, then per-tx receipt fetches via the LOCAL node
/// (free, within PBSS window). Rate-capped against the global archive
/// counters.
async fn measured_chain_swap_price_at_kol_plus1(
    http: &reqwest::Client,
    rpc_url: &str,
    archive_url: &str,
    is_buy: bool,
    token: Address,
    decimals: u8,
    kol_tx: B256,
) -> Option<f64> {
    use std::sync::atomic::Ordering::Relaxed;

    if archive_url.is_empty() {
        return None;
    }

    // 1. Resolve kol_block from KOL's tx receipt.
    let txh = format!("{kol_tx:#x}");
    let mut kol_block: Option<u64> = None;
    for _ in 0..10 {
        if let Some(r) = rpc(http, rpc_url, "eth_getTransactionReceipt", serde_json::json!([txh]))
            .await
            .filter(|v| !v.is_null())
        {
            if r.get("status").and_then(|v| v.as_str()) != Some("0x1") {
                return None;
            }
            if let Some(b_hex) = r.get("blockNumber").and_then(|v| v.as_str()) {
                if let Ok(b) = u64::from_str_radix(b_hex.trim_start_matches("0x"), 16) {
                    kol_block = Some(b);
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let kol_block = kol_block?;

    // 2. Wait briefly for the target window to land.
    for _ in 0..10 {
        let head = current_head_block(rpc_url).await.unwrap_or(0);
        if head >= kol_block + 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 3. Pay one archive call to enumerate Transfer logs of `token` over the
    //    +1..+5 block window. (Local eth_getLogs is broken by pruneancient.)
    if ARCHIVE_CALLS.load(Relaxed) >= ARCHIVE_DAILY_CAP {
        return None;
    }
    let now = unix_ns();
    if now.saturating_sub(ARCHIVE_LAST_NS.load(Relaxed)) < ARCHIVE_MIN_GAP_NS {
        return None;
    }
    ARCHIVE_LAST_NS.store(now, Relaxed);
    ARCHIVE_CALLS.fetch_add(1, Relaxed);

    let filter = serde_json::json!([{
        "address": format!("{token:#x}"),
        "fromBlock": format!("0x{:x}", kol_block + 1),
        "toBlock":   format!("0x{:x}", kol_block + 5),
        "topics": [TRANSFER_TOPIC],
    }]);
    let logs = rpc(http, archive_url, "eth_getLogs", filter).await?;
    let logs = logs.as_array()?;

    // 4. Collect distinct tx_hashes (chronological by transactionIndex).
    let mut tx_hashes: Vec<String> = logs
        .iter()
        .filter_map(|l| {
            l.get("transactionHash")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .collect();
    tx_hashes.sort();
    tx_hashes.dedup();

    // 5. Price each tx by reading its receipt (local node — within PBSS
    //    window — cost-free). kol_exec_price handles native-BNB and WBNB
    //    legs alike, and returns None for txs that aren't buys/sells.
    let mut prices: Vec<f64> = Vec::new();
    for h_str in tx_hashes.iter().take(12) {
        let Ok(tx_hash) = h_str.parse::<B256>() else {
            continue;
        };
        if let Some(p) =
            kol_exec_price(http, rpc_url, archive_url, tx_hash, token, decimals, is_buy).await
        {
            if p > 0.0 {
                prices.push(p);
            }
        }
    }
    if prices.is_empty() {
        return None;
    }

    // 6. Median.
    prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = prices.len() / 2;
    Some(prices[mid])
}

/// Re-quote our fill at the END of block kol_block+1 — the realistic
/// +1-block landing point AFTER D's tx confirmed. This replaces the static
/// slippage haircut with the REAL post-D pool/curve state.
///
/// Why kol_block+1 (not kol_block):
///   - Block N closes with D's tx included. State at END(N) = post-D, the
///     start of N+1.
///   - State at END(N+1) = post-D + everyone-else-in-N+1 (the cohort of
///     racers). This is the conservative "we landed last in N+1" model.
///
/// Local pruned node serves the state if we re-quote within ~128 blocks
/// (PBSS window ≈ 1 min on BSC). We trigger within seconds, so local
/// almost always wins. NodeReal archive is the fallback (rate-capped).
///
/// Returns None on: tx still pending, D's tx reverted, no V2/V3 pool
/// (bonding-only token), or state truly unavailable.
async fn measured_v2v3_fill_at_kol_plus1(
    local_sim: &Simulator,
    archive_sim: Option<&Simulator>,
    http: &reqwest::Client,
    rpc_url: &str,
    is_buy: bool,
    amount_in: U256,
    token: Address,
    kol_tx: B256,
) -> Option<QuoteResult> {
    use std::sync::atomic::Ordering::Relaxed;

    let txh = format!("{kol_tx:#x}");
    let mut kol_block: Option<u64> = None;
    for _ in 0..10 {
        if let Some(r) = rpc(http, rpc_url, "eth_getTransactionReceipt", serde_json::json!([txh]))
            .await
            .filter(|v| !v.is_null())
        {
            // Reverted tx: D never moved the pool — measurement would be
            // pre-D, indistinguishable from no-haircut. Bail.
            if r.get("status").and_then(|v| v.as_str()) != Some("0x1") {
                return None;
            }
            if let Some(b_hex) = r.get("blockNumber").and_then(|v| v.as_str()) {
                if let Ok(b) = u64::from_str_radix(b_hex.trim_start_matches("0x"), 16) {
                    kol_block = Some(b);
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let kol_block = kol_block?;
    let target = kol_block + 1;

    // Wait for kol_block+1 to land locally so historical state exists.
    for _ in 0..10 {
        let head = current_head_block(rpc_url).await.unwrap_or(0);
        if head >= target {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Re-quote at block kol_block+1 against LOCAL first (free, in PBSS window).
    let local_q = if is_buy {
        local_sim.simulate_buy(amount_in, token, Some(target)).await
    } else {
        local_sim.simulate_sell(amount_in, token, Some(target)).await
    };
    if let Ok(Some(q)) = local_q {
        if !q.amount_out.is_zero() {
            return Some(q);
        }
    }

    // Archive fallback — only if NodeReal configured AND in budget.
    let archive = archive_sim?;
    if ARCHIVE_CALLS.load(Relaxed) >= ARCHIVE_DAILY_CAP {
        return None;
    }
    let now = unix_ns();
    if now.saturating_sub(ARCHIVE_LAST_NS.load(Relaxed)) < ARCHIVE_MIN_GAP_NS {
        return None;
    }
    ARCHIVE_LAST_NS.store(now, Relaxed);
    ARCHIVE_CALLS.fetch_add(1, Relaxed);

    let archive_q = if is_buy {
        archive.simulate_buy(amount_in, token, Some(target)).await
    } else {
        archive.simulate_sell(amount_in, token, Some(target)).await
    };
    match archive_q {
        Ok(Some(q)) if !q.amount_out.is_zero() => Some(q),
        _ => None,
    }
}

/// Multiply `x` by (10_000 - bps) / 10_000 in U256 space.
/// bps is clamped to <= 10_000. Used for adverse-slippage haircuts.
fn bps_haircut_u256(x: U256, bps: u32) -> U256 {
    let bps = bps.min(10_000) as u64;
    let num = U256::from(10_000u64 - bps);
    let den = U256::from(10_000u64);
    x.saturating_mul(num) / den
}

fn saturating_sub_u256(a: U256, b: U256) -> U256 {
    if a > b { a - b } else { U256::ZERO }
}

/// The KOL's *executed* BNB-per-whole-token price, read from his own tx
/// receipt. Venue-agnostic — works for Four.Meme bonding curve / flap /
/// PancakeV2 alike (it reads what actually moved, not a pool quote).
///
/// BUY  : BNB leg = tx.value (native) or WBNB Transfer; token = received.
/// SELL : token leg = sent; BNB = WBNB Transfer received.
/// Retries while the tx is still pending (we act on the mempool hit).
async fn kol_exec_price(
    http: &reqwest::Client,
    url: &str,
    archive_url: &str,
    tx: B256,
    token: Address,
    decimals: u8,
    is_buy: bool,
) -> Option<f64> {
    let txh = format!("{tx:#x}");
    let tok_lc = format!("{token:#x}").to_lowercase();
    let mut rc = None;
    for _ in 0..8 {
        if let Some(r) = rpc(http, url, "eth_getTransactionReceipt", serde_json::json!([txh]))
            .await
            .filter(|v| !v.is_null())
        {
            rc = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let rc = rc?;
    let logs = rc.get("logs")?.as_array()?;
    let mut tok_amt = 0f64;
    let mut wbnb_amt = 0f64;
    for lg in logs {
        let topics = lg.get("topics").and_then(|t| t.as_array());
        let t0 = topics
            .and_then(|t| t.first())
            .and_then(|x| x.as_str())
            .unwrap_or("");
        if !t0.eq_ignore_ascii_case(TRANSFER_TOPIC) {
            continue;
        }
        let amt = u_hex(lg.get("data").and_then(|d| d.as_str()).unwrap_or("0x0"));
        let addr = lg
            .get("address")
            .and_then(|a| a.as_str())
            .unwrap_or("")
            .to_lowercase();
        if addr == tok_lc {
            tok_amt = tok_amt.max(amt);
        } else if addr == WBNB_LC {
            wbnb_amt = wbnb_amt.max(amt);
        }
    }
    if tok_amt <= 0.0 {
        return None;
    }
    let tok_whole = tok_amt / 10f64.powi(i32::from(decimals));
    let bnb = if is_buy {
        // Native BNB spent (most GMGN/Four.Meme buys) → tx.value; else WBNB.
        let v = rpc(http, url, "eth_getTransactionByHash", serde_json::json!([txh]))
            .await
            .and_then(|t| {
                t.get("value")
                    .and_then(|x| x.as_str())
                    .map(|s| u_hex(s))
            })
            .unwrap_or(0.0);
        if v > 0.0 { v / 1e18 } else { wbnb_amt / 1e18 }
    } else if wbnb_amt > 0.0 {
        // SELL with a WBNB Transfer (V2/graduated, or router that wraps).
        wbnb_amt / 1e18
    } else {
        // SELL paying NATIVE BNB (Four.Meme / flap) → no WBNB log, node
        // can't debug_trace. Recover D's proceeds from his balance delta
        // around the sell block (works only while sellBlk−1 is still in
        // the PBSS state window ≈ last ~128 blocks — true for exit-follow,
        // which fires within ~1s of D's sell).
        let blk = u128::from_str_radix(
            rc.get("blockNumber")?.as_str()?.trim_start_matches("0x"),
            16,
        )
        .ok()?;
        let gas_used = u_hex(rc.get("gasUsed").and_then(|x| x.as_str()).unwrap_or("0x0"));
        let gas_price = u_hex(
            rc.get("effectiveGasPrice")
                .and_then(|x| x.as_str())
                .unwrap_or("0x0"),
        );
        let d = rpc(http, url, "eth_getTransactionByHash", serde_json::json!([txh]))
            .await
            .and_then(|t| t.get("from").and_then(|x| x.as_str()).map(str::to_string))?;
        let bal = |b: u128| {
            let d = d.clone();
            async move {
                // Local pruned node first (free, ~128-block window). If it
                // can't serve that height, fall back to NodeReal archive
                // (rate-capped, free-plan-safe).
                if let Some(x) = rpc(
                    http,
                    url,
                    "eth_getBalance",
                    serde_json::json!([d, format!("0x{b:x}")]),
                )
                .await
                .and_then(|v| v.as_str().map(|s| u_hex(s)))
                {
                    return Some(x);
                }
                archive_balance(http, archive_url, &d, b).await
            }
        };
        let after = bal(blk).await?;
        let before = bal(blk.checked_sub(1)?).await?;
        // proceeds = Δbalance + gas burned by the sell tx itself
        let proceeds = (after - before) + gas_used * gas_price;
        if proceeds <= 0.0 {
            return None; // noisy block / out of state window → caller flats
        }
        proceeds / 1e18
    };
    if bnb <= 0.0 || tok_whole <= 0.0 {
        return None;
    }
    Some(bnb / tok_whole) // BNB per whole token
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

    #[test]
    fn bps_haircut_scales_correctly() {
        // 100 BNB - 1.5% haircut = 98.5 BNB
        let x = U256::from(100_000_000_000_000_000_000u128);
        let y = bps_haircut_u256(x, 150);
        assert_eq!(y, U256::from(98_500_000_000_000_000_000u128));

        // 0 bps = no-op
        assert_eq!(bps_haircut_u256(x, 0), x);

        // > 10_000 bps clamps to 100% haircut
        assert_eq!(bps_haircut_u256(x, 20_000), U256::ZERO);
    }

    #[test]
    fn saturating_sub_does_not_underflow() {
        assert_eq!(
            saturating_sub_u256(U256::from(10u64), U256::from(3u64)),
            U256::from(7u64)
        );
        assert_eq!(
            saturating_sub_u256(U256::from(3u64), U256::from(10u64)),
            U256::ZERO
        );
    }
}
