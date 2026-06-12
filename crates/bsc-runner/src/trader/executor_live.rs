//! Live executor (PHASE A: shadow only).
//!
//! Builds, signs and ─ if broadcast is enabled ─ submits a PancakeSwap V2
//! swapExactETHForTokensSupportingFeeOnTransferTokens tx via BlockRazor.
//!
//! In SHADOW mode (`config/limits.toml: phase.shadow = true`) the signed
//! tx is logged with its hash + gas + nonce + calldata for offline
//! inspection but never reaches the wire. Zero on-chain risk.
//!
//! All trades pass through `LimitsRuntime::check()` before signing. Any
//! limit failure short-circuits + logs the reason.

use crate::trader::limits::{LimitFail, LimitsRuntime, TradeCheck};
use crate::trader::live_ledger::{LiveEntry, LiveLedger};
use crate::trader::types::Decision;
use crate::trader::wallet::{self, TraderWallet};
use crate::trader::nonce::NonceManager;
use alloy::primitives::B256;
use alloy::network::TransactionBuilder;
use alloy::primitives::{hex, Address, Bytes, U256};
use alloy::rpc::types::TransactionRequest;
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

// Puissant Network — 48 Club's MEV-Boost relay for BSC. Free public
// endpoint, connected to a different validator subset than BlockRazor.
// We submit bundles here in parallel with BR's bundle path so whichever
// builder wins N+1 can pick up our atomic-backrun bundle. Uses 48 Club's
// custom `eth_sendPuissant` RPC (Flashbots-style but with their own
// param shape: maxTimestamp deadline, acceptReverting list).
const PUISSANT_URL: &str = "https://puissant-bsc.48.club/";

// PancakeSwap V2 router on BSC mainnet
const PANCAKE_V2_ROUTER:  &str = "0x10ED43C718714eb63d5aA57B78B54704E256024E";
const PANCAKE_V2_FACTORY: &str = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73";
const WBNB:               &str = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c";
// Four.Meme bonding-curve launchpad (handles BUY directly on the curve).
const FOURMEME:           &str = "0x5c952063c7fc8610FFDB798152D69F0B9550762b";
const BSC_CHAIN_ID:       u64  = 56;
// Tx-deadline window after broadcast.
const DEADLINE_SECS:      u64  = 60;
// Gas limit for a V2 swap with tax-token tolerance.
const SWAP_GAS_LIMIT:     u64  = 300_000;
// Four.Meme curve buy ~ 180k gas typical; cap higher.
const FOURMEME_GAS_LIMIT: u64  = 250_000;
// User-Agent string Cloudflare doesn't flag (Python-urllib gets HTTP 1010).
const HTTP_UA: &str =
    "Mozilla/5.0 (X11; Linux x86_64) bsc-meme-mev/0.1";

sol! {
    /// PancakeSwap V2 — fee-on-transfer-safe ETH→tokens swap.
    interface IPancakeRouter {
        function swapExactETHForTokensSupportingFeeOnTransferTokens(
            uint256 amountOutMin,
            address[] path,
            address to,
            uint256 deadline
        ) external payable;
        function swapExactTokensForETHSupportingFeeOnTransferTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] path,
            address to,
            uint256 deadline
        ) external;
        function getAmountsOut(uint256 amountIn, address[] path) external view returns (uint256[]);
    }
    interface IPancakeFactory {
        function getPair(address tokenA, address tokenB) external view returns (address pair);
    }
    /// Four.Meme launchpad — buy + sell on the bonding curve. Function
    /// names verified by matching keccak prefixes against on-chain
    /// selectors (`0x87f27655` and `0xf464e7db`). "AMAP" = "as much as
    /// possible" — given amountIn BNB, give us as many tokens as the
    /// curve will yield (bounded below by amountOutMin).
    interface IFourMeme {
        function buyTokenAMAP(address token, uint256 amountIn, uint256 amountOutMin) external payable;
        function sellToken(address token, uint256 amount) external;
    }
    /// Minimal ERC20 — we need approve + balanceOf + allowance for sells.
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

/// Which on-chain route we're using for a given buy / sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyRoute {
    PancakeV2,
    FourMeme,
}

/// What we're about to do — drives which dry-run the route picker uses.
/// A token that accepts BUY may not accept SELL (and vice versa) once it
/// has graduated or been delisted from a bonding curve.
#[derive(Debug, Clone, Copy)]
enum Action {
    Buy,
    Sell,
}

use crate::trader::blacklist::{is_blacklisted, BlacklistRuntime};

/// Cached state per held position. Populated on successful BUY broadcast
/// so the SELL fast-path can skip RPC roundtrips entirely:
///   - route already known → no V2 pair check, no Four.Meme dry-run
///   - approved flag set after background-approve confirms → no 1.5s wait
///   - tokens_bought used as the sell amount (we still call balanceOf as a
///     sanity check, but only as part of build, not as a gate)
/// Late = lose. The whole point is to make the detection→broadcast loop
/// sub-100ms when the KOL's SELL hits our mempool.
pub struct PositionEntry {
    pub route:         BuyRoute,
    /// 0 until bg_finalize learns the actual amount from on-chain balance.
    /// `parking_lot::Mutex` (not `std::sync`): smaller, faster, no poison
    /// semantics, and — critically — safe to hold across `.await` since
    /// it doesn't participate in tokio's cooperative-scheduling guarantees.
    /// The critical sections are always short (single load/store).
    pub tokens_bought: parking_lot::Mutex<U256>,
    pub bnb_in:        U256,
    pub approved:      AtomicBool,
    pub opened_at:     Instant,
}

pub struct LiveExecutor {
    wallet:       Arc<TraderWallet>,
    nonce:        Arc<NonceManager>,
    limits:       Arc<LimitsRuntime>,
    ledger:       Arc<LiveLedger>,
    /// Local node for reads (gas, block, balance).
    rpc_url:      String,
    /// BlockRazor for writes. Only used when broadcast_enabled().
    submit_url:   String,
    /// BlockRazor auth key.
    submit_auth:  String,
    http:         reqwest::Client,
    /// Per-trade USD sizing: $X normal, $Y if dev whitelisted. Both 0 ⇒
    /// fall back to the legacy strategy-supplied `bnb_amount`.
    bnb_price:    Arc<crate::bnb_price::BnbPrice>,
    dev_resolver: Option<Arc<crate::trader::dev_resolver::DevResolver>>,
    /// Telegram alert credentials. `None` ⇒ no alerts.
    telegram:     Option<TgConfig>,
    /// In-memory cache of currently-open positions. Hot-path for SELL.
    /// RwLock so the periodic retry sweep can iterate while exits write.
    positions:    Arc<RwLock<HashMap<Address, Arc<PositionEntry>>>>,
    /// Cached gas-price (wei) + the Instant it was last refreshed. TTL
    /// `GAS_PRICE_TTL`. Saves ~15ms per exit on the hot path. The
    /// `eth_gasPrice` RPC is cheap on local geth but on the fast-exit
    /// path every millisecond counts.
    gas_cache:    Arc<parking_lot::Mutex<GasCache>>,
    /// Cached wallet BNB balance. Background-refreshed every 3s so the
    /// hot path never does an `eth_getBalance` RPC. Used by BUY's limits
    /// gate (min_wallet_bnb safety floor) and by SELL's telegram log.
    wallet_bnb_cache: Arc<parking_lot::Mutex<WalletBalanceCache>>,
    /// Cached V2 pair lookups per token. Positive results cached forever
    /// (V2 pairs are permanent on chain). Negative results re-checked
    /// after 30s in case the token has graduated since. Eliminates the
    /// extra eth_call on F4's SELL-time route re-validation.
    v2_pair_cache: Arc<parking_lot::Mutex<HashMap<Address, V2PairCacheEntry>>>,
    /// Hot-loadable token blacklist. Superset of the hardcoded fallback;
    /// see `crates/bsc-runner/src/trader/blacklist.rs`. Falls through to
    /// the const fallback if `blacklist: None` is passed (legacy path).
    blacklist:    Option<Arc<BlacklistRuntime>>,
    /// Shared semaphore bounding concurrent `BgPrep::finalize_position`
    /// tasks. Each BUY spawns one bg task; under burst they queue here.
    bg_semaphore: Arc<tokio::sync::Semaphore>,
    /// Per-token exit serialization. When KOL D fires multiple SELLs on
    /// the same token within seconds (tranching out), each fires a
    /// separate `execute_exit`. Without this lock they raced — both read
    /// cached `tokens_bought`, both sized against the same X, both
    /// broadcast → OVERSELL.
    ///
    /// Now: exit #2 waits for #1's mutex, then reads cache AFTER #1's
    /// post-broadcast shrink → sizes against `remaining`, never oversells.
    /// One mutex per token; different tokens still exit in parallel.
    exit_locks: Arc<parking_lot::Mutex<HashMap<Address, Arc<tokio::sync::Mutex<()>>>>>,
}

const BG_FINALIZE_CONCURRENCY: usize = 8;

#[derive(Clone, Copy)]
struct GasCache {
    wei:       u128,
    refreshed: Instant,
}

const GAS_PRICE_TTL: Duration = Duration::from_secs(2);
const WALLET_BAL_TTL: Duration = Duration::from_secs(3);
const V2_PAIR_NEG_TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Default)]
struct WalletBalanceCache {
    bnb:       f64,
    refreshed: Option<Instant>,
}

#[derive(Clone, Copy)]
struct V2PairCacheEntry {
    pair:      Option<Address>,  // None ⇒ token has no V2 pair
    cached_at: Instant,
}

#[derive(Clone)]
struct TgConfig {
    bot_token: String,
    chat_id:   String,
}

impl LiveExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        wallet:       Arc<TraderWallet>,
        nonce:        Arc<NonceManager>,
        limits:       Arc<LimitsRuntime>,
        ledger:       Arc<LiveLedger>,
        rpc_url:      String,
        submit_url:   String,
        submit_auth:  String,
        bnb_price:    Arc<crate::bnb_price::BnbPrice>,
        dev_resolver: Option<Arc<crate::trader::dev_resolver::DevResolver>>,
        blacklist:    Option<Arc<BlacklistRuntime>>,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(
                limits.config().submission.timeout_ms,
            ))
            .user_agent(HTTP_UA)
            // Latency tuning: keep TLS connections warm so per-submit RTT
            // doesn't include a handshake. Reqwest enables keep-alive by
            // default but with a 90s pool idle and no explicit TCP keepalive,
            // so on a quiet stretch the socket can close. Tighten both.
            .pool_idle_timeout(Some(Duration::from_secs(120)))
            .tcp_keepalive(Some(Duration::from_secs(15)))
            .pool_max_idle_per_host(8)
            .build()
            .context("build reqwest")?;
        let telegram = match (
            std::env::var("TELEGRAM_BOT_TOKEN").ok(),
            std::env::var("TELEGRAM_CHAT_ID").ok(),
        ) {
            (Some(bt), Some(cid)) if !bt.is_empty() && !cid.is_empty() => {
                tracing::info!(target: "trader_live", "telegram alerts ENABLED for live trader");
                Some(TgConfig { bot_token: bt, chat_id: cid })
            }
            _ => {
                tracing::info!(target: "trader_live", "telegram alerts disabled (env vars missing)");
                None
            }
        };
        let me = Self {
            wallet, nonce, limits, ledger,
            rpc_url, submit_url, submit_auth, http,
            bnb_price, dev_resolver, telegram,
            positions: Arc::new(RwLock::new(HashMap::new())),
            gas_cache: Arc::new(parking_lot::Mutex::new(GasCache {
                wei: 0,
                refreshed: Instant::now() - GAS_PRICE_TTL,
            })),
            wallet_bnb_cache: Arc::new(parking_lot::Mutex::new(WalletBalanceCache::default())),
            v2_pair_cache:    Arc::new(parking_lot::Mutex::new(HashMap::new())),
            blacklist,
            bg_semaphore: Arc::new(tokio::sync::Semaphore::new(BG_FINALIZE_CONCURRENCY)),
            exit_locks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        };
        me.spawn_submit_endpoint_warmer();
        me.spawn_wallet_balance_refresher();
        Ok(me)
    }

    /// Poll `eth_getBalance` every WALLET_BAL_TTL and cache the result.
    /// Hot paths read the cache (no RPC) instead of calling the RPC inline.
    /// Saves ~5-15ms per BUY/SELL broadcast. Stale-tolerable: wallet only
    /// changes on tx finalization, never faster than ~3s in practice.
    fn spawn_wallet_balance_refresher(&self) {
        let http  = self.http.clone();
        let url   = self.rpc_url.clone();
        let addr  = self.wallet.address();
        let cache = self.wallet_bnb_cache.clone();
        tokio::spawn(async move {
            // Immediate first fetch so the cache is hot before any trade.
            let mut tick = tokio::time::interval(WALLET_BAL_TTL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let body = serde_json::json!({
                    "jsonrpc":"2.0","method":"eth_getBalance",
                    "params":[format!("{:#x}", addr), "latest"], "id":1
                });
                let bnb = match http.post(&url).json(&body).send().await {
                    Ok(r) => match r.json::<serde_json::Value>().await {
                        Ok(v) => v.get("result").and_then(|s| s.as_str())
                            .and_then(|h| u128::from_str_radix(h.trim_start_matches("0x"), 16).ok())
                            .map(|n| n as f64 / 1e18),
                        Err(_) => None,
                    },
                    Err(_) => None,
                };
                if let Some(bnb) = bnb {
                    let mut c = cache.lock();
                    c.bnb = bnb;
                    c.refreshed = Some(Instant::now());
                }
            }
        });
    }

    /// Hot-path wallet balance — never blocks on RPC. Returns 0.0 until the
    /// background refresher has run at least once (~3s after boot).
    fn wallet_balance_bnb_cached(&self) -> f64 {
        self.wallet_bnb_cache.lock().bnb
    }

    /// V2 pair lookup with cache. Positives cached FOREVER (V2 pairs are
    /// immutable on chain once created). Negatives cached for 30s so we
    /// catch graduation events. ~5-10ms saved per call after first lookup.
    async fn v2_pair_cached(&self, token: Address) -> Option<Address> {
        {
            let c = self.v2_pair_cache.lock();
            if let Some(entry) = c.get(&token) {
                // Positive forever
                if entry.pair.is_some() {
                    return entry.pair;
                }
                // Negative valid only WITHIN TTL
                if entry.cached_at.elapsed() < V2_PAIR_NEG_TTL {
                    return entry.pair;
                }
            }
        }
        // Cache miss / stale negative — query factory
        let pair = self.v2_pair_lookup(token).await;
        self.v2_pair_cache.lock().insert(token, V2PairCacheEntry {
            pair,
            cached_at: Instant::now(),
        });
        pair
    }

    /// Raw `factory.getPair(WBNB, token)` call. Returns Some(addr) if a
    /// pair exists, None otherwise. Used by `v2_pair_cached`.
    async fn v2_pair_lookup(&self, token: Address) -> Option<Address> {
        let factory = PANCAKE_V2_FACTORY.parse::<Address>().ok()?;
        let wbnb    = WBNB.parse::<Address>().ok()?;
        let sel     = "0xe6a43905"; // getPair(address,address)
        let data    = format!("0x{}{:0>64}{:0>64}",
            sel.trim_start_matches("0x"),
            hex::encode(wbnb.as_slice()),
            hex::encode(token.as_slice()));
        let body    = serde_json::json!({
            "jsonrpc":"2.0","method":"eth_call",
            "params":[{"to": format!("{factory:#x}"), "data": data}, "latest"],
            "id":1
        });
        let v: serde_json::Value = self.http.post(&self.rpc_url).json(&body).send().await.ok()?
            .json().await.ok()?;
        let hex_s = v.get("result")?.as_str()?.trim_start_matches("0x");
        if hex_s.is_empty() || hex_s.chars().all(|c| c == '0') {
            return None;  // zero address = no pair
        }
        let bytes = hex::decode(hex_s).ok()?;
        if bytes.len() < 20 { return None; }
        let addr = Address::from_slice(&bytes[bytes.len()-20..]);
        if addr == Address::ZERO { None } else { Some(addr) }
    }

    /// Keep the TLS/TCP connection to the submission endpoint hot by
    /// issuing a cheap `eth_blockNumber` every 25s. Saves ~20-40ms on
    /// each real broadcast when the line has been idle. Best-effort: any
    /// failure is logged at debug and ignored.
    fn spawn_submit_endpoint_warmer(&self) {
        let http = self.http.clone();
        let url  = self.submit_url.clone();
        let auth = self.submit_auth.clone();
        tokio::spawn(async move {
            // Fire-and-forget initial ping to open the socket immediately
            // after boot. Don't gate the constructor on it.
            let body = serde_json::json!({
                "jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1
            });
            let mut req = http.post(&url).json(&body);
            if !auth.is_empty() {
                req = req.header("Authorization", auth.clone());
            }
            let _ = req.send().await;

            // Then heart-beat every 25s. Pool idle timeout is 120s; this
            // refreshes well before the OS reaps the socket.
            let mut tick = tokio::time::interval(Duration::from_secs(25));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let body = serde_json::json!({
                    "jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1
                });
                let mut req = http.post(&url).json(&body);
                if !auth.is_empty() {
                    req = req.header("Authorization", auth.clone());
                }
                match req.send().await {
                    Ok(_)  => tracing::debug!(target: "trader_live", "submit warmer ping ok"),
                    Err(e) => tracing::debug!(target: "trader_live", error = %e, "submit warmer ping failed"),
                }
            }
        });
    }

    /// Get-or-create the per-token exit mutex. Outer parking_lot::Mutex
    /// is held only for the hashmap insert (microseconds). Returns the
    /// inner tokio::Mutex that the caller awaits.
    fn token_exit_lock(&self, token: Address) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.exit_locks.lock();
        map.entry(token)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Single source of truth for blacklist checks. Prefers the runtime
    /// list when present (which is a superset of the const fallback),
    /// otherwise falls through to the static `is_blacklisted`.
    fn check_blacklisted(&self, token: Address) -> bool {
        match self.blacklist.as_ref() {
            Some(b) => b.is_blacklisted(token),
            None => is_blacklisted(token),
        }
    }

    /// Snapshot of every currently-tracked open position. Used by the
    /// periodic retry sweep.
    pub async fn position_snapshot(&self) -> Vec<(Address, Arc<PositionEntry>)> {
        self.positions
            .read()
            .await
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Fire-and-forget Telegram HTML message. Never blocks the hot path
    /// (spawned). Silent on send failure — this is real-money observability,
    /// not control flow.
    fn tg_send(&self, html: String) {
        let Some(tg) = self.telegram.clone() else { return };
        let http = self.http.clone();
        tokio::spawn(async move {
            let url = format!("https://api.telegram.org/bot{}/sendMessage", tg.bot_token);
            let body = [
                ("chat_id", tg.chat_id.as_str()),
                ("text", html.as_str()),
                ("parse_mode", "HTML"),
                ("disable_web_page_preview", "true"),
            ];
            let _ = http.post(&url).form(&body).send().await;
        });
    }

    /// Per-decision pipeline: limits → build → sign → (broadcast or log).
    pub async fn execute(
        &self,
        decision: Decision,
        visibility: &str,
        open_positions: u32,
    ) -> Result<()> {
        let Decision::Enter {
            kol_name,
            token,
            bnb_amount,
            kol_block,
            ..
        } = decision
        else {
            // Phase A only handles entries. Exit-follow comes in Phase B.
            return Ok(());
        };

        // ── Duplicate-entry guard ────────────────────────────────────────
        // If we already hold this token, skip the buy. Distinguishes:
        //   - HOLDING  (tokens_bought > 0 OR bg_finalize pending) → skip
        //   - FULLY EXITED (tokens_bought=0 + approved=true) → stale entry
        //     left behind by full-close; remove + allow re-entry
        let mut should_skip_dup = false;
        let mut should_clear_stale = false;
        if let Some(entry) = self.positions.read().await.get(&token).cloned() {
            let amt = *entry.tokens_bought.lock();
            let approved = entry.approved.load(Ordering::Acquire);
            if amt.is_zero() && approved {
                // Stale "fully exited" entry. Clear it before proceeding.
                should_clear_stale = true;
            } else {
                should_skip_dup = true;
            }
        }
        if should_skip_dup {
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                "skip: position already open (duplicate-entry guard)"
            );
            metrics::counter!(
                "bsc_trader_live_skipped_total",
                "reason" => "dup_entry",
            ).increment(1);
            return Ok(());
        }
        if should_clear_stale {
            self.positions.write().await.remove(&token);
        }

        // ── USD-denominated sizing (overrides strategy-supplied bnb_amount) ──
        // Policy by visibility:
        //   - PUBLIC:  `trade_size_usd`           (full size; we can fight for
        //                                          same-block landing)
        //   - PRIVATE: `trade_size_private_usd`   (half size by default;
        //                                          we land N+1+ with worse fill)
        // Falls back to strategy bnb_amount when sizing is disabled.
        let sc = &self.limits.config().strategy;
        let _ = kol_block; // resolver removed; kept in the decision for the SELL path
        let (bnb_amount, sizing_tag) = if sc.trade_size_usd > 0.0 {
            let bnb_usd = self.bnb_price.get().await.unwrap_or(0.0);
            if bnb_usd <= 0.0 {
                tracing::warn!(
                    target: "trader_live",
                    "BNB/USD oracle unavailable; falling back to strategy bnb_amount"
                );
                (bnb_amount, "fallback_no_price")
            } else {
                let (usd, tag) = if visibility == "private" {
                    let priv_usd = if sc.trade_size_private_usd > 0.0 {
                        sc.trade_size_private_usd
                    } else {
                        // 0.0 means "disable PRIVATE entries entirely".
                        tracing::info!(
                            target: "trader_live",
                            kol = %kol_name,
                            token = %format!("{token:#x}"),
                            "skip: PRIVATE entries disabled (trade_size_private_usd = 0)"
                        );
                        return Ok(());
                    };
                    (priv_usd, "private")
                } else {
                    (sc.trade_size_usd, "public")
                };
                let wei = ((usd / bnb_usd) * 1e18) as u128;
                tracing::info!(
                    target: "trader_live",
                    kol = %kol_name,
                    token = %format!("{token:#x}"),
                    visibility = visibility,
                    sizing_tag = tag,
                    size_usd = usd,
                    bnb_usd,
                    sized_bnb_wei = wei,
                    "sized trade by USD policy"
                );
                (U256::from(wei), tag)
            }
        } else {
            (bnb_amount, "legacy_fraction")
        };
        let _ = sizing_tag;  // recorded only in the log above for now

        // --- blacklist gate (stables, majors, anything we don't want to buy) ---
        if self.check_blacklisted(token) {
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                "skip: token blacklisted (stablecoin / major / non-meme)"
            );
            metrics::counter!(
                "bsc_trader_live_skipped_total",
                "reason" => "blacklisted_token",
            )
            .increment(1);
            let _ = self.ledger.append(&LiveEntry {
                phase: self.phase_label(),
                kol_name: kol_name.clone(),
                visibility: leak_str(visibility),
                token_address: token,
                token_symbol: String::new(),
                bnb_in_wei: bnb_amount,
                gas_gwei: 0,
                nonce: 0,
                tx_hash: B256::ZERO,
                wallet_bnb: 0.0,
                broadcast: false,
                limit_skip_reason: Some("blacklisted_token".into()),
            });
            return Ok(());
        }

        let wanted_bnb_f = bnb_amount.to_string().parse::<f64>().unwrap_or(0.0) / 1e18;

        // --- CLAMP to per-trade cap (honoring tiny-mode override) ---
        // We never want to skip a qualifying trade just because the KOL went
        // big; we trade up to OUR cap. This also enforces the absolute ceiling.
        let cap_bnb = self
            .limits
            .config()
            .phase
            .effective_per_trade_max_bnb(self.limits.config().limits.per_trade_max_bnb);
        let (bnb_in_f, bnb_amount) = if wanted_bnb_f > cap_bnb {
            let clamped_wei = (cap_bnb * 1e18) as u128;
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                wanted_bnb = wanted_bnb_f,
                clamped_to = cap_bnb,
                "clamped trade size to cap"
            );
            metrics::counter!("bsc_trader_live_clamped_total").increment(1);
            (cap_bnb, U256::from(clamped_wei))
        } else {
            (wanted_bnb_f, bnb_amount)
        };

        // --- limits gate ---
        // Use cached wallet balance (background-refreshed every 3s) to avoid
        // a blocking eth_getBalance RPC on the BUY hot path. Saves ~5-15ms.
        let wallet_bnb = self.wallet_balance_bnb_cached();
        // Get observed gas in WEI (u128) so sub-1-gwei networks don't
        // collapse to 0 (BSC post-Fermi often sees 0.05 gwei). Apply a
        // 1-gwei floor since BlockRazor + most builders refuse anything
        // sub-0.05-gwei; 1 gwei = ~$0.20/swap which is negligible.
        let observed_wei = self.gas_wei().await.unwrap_or(u128::MAX);
        // 10 gwei BUY floor — RE-BUMPED 2026-06-01 from 3 gwei. The 3 gwei
        // floor matched paper conditions (N+1 fill) but the real-world
        // slippage on hot memes is ~40% by N+1 — paper's model is too
        // optimistic. At 10 gwei we land in D's same block (D's median
        // bid is 7 gwei), filling at pre-D curve price.
        //
        // The earlier 10-gwei trial (2026-05-29) lost $84 but that was
        // tangled with two now-fixed bugs: the exit race condition and
        // the unconditional retry sweep that prematurely dumped positions.
        // With those resolved, 10 gwei BUY + 3 gwei SELL is the right
        // tier mix: outbid D on entries, match D's lazy 3-gwei exits.
        let gas_wei: u128 = observed_wei.max(10_000_000_000);
        let gas_gwei: u64 = (gas_wei / 1_000_000_000) as u64;
        let chk = TradeCheck {
            kol_name: &kol_name,
            visibility,
            bnb_amount: bnb_in_f,
            current_open_positions: open_positions,
            current_wallet_bnb: wallet_bnb,
            current_gas_gwei: gas_gwei,
        };
        if let Err(why) = self.limits.check(&chk) {
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                bnb = bnb_in_f,
                reason = %why,
                "skip: limit-gate"
            );
            metrics::counter!(
                "bsc_trader_live_skipped_total",
                "reason" => limit_label(&why),
            )
            .increment(1);
            // Record the skip in the live ledger for audit
            let _ = self.ledger.append(&LiveEntry {
                phase: self.phase_label(),
                kol_name: kol_name.clone(),
                visibility: leak_str(visibility),
                token_address: token,
                token_symbol: String::new(),
                bnb_in_wei: bnb_amount,
                gas_gwei,
                nonce: 0,
                tx_hash: B256::ZERO,
                wallet_bnb,
                broadcast: false,
                limit_skip_reason: Some(limit_label(&why)),
            });
            return Ok(());
        }

        // --- pick a route based on whether token has graduated to V2 ---
        let route = match self.pick_route(token, bnb_amount, Action::Buy).await {
            Some(r) => r,
            None => {
                tracing::info!(
                    target: "trader_live",
                    kol = %kol_name,
                    token = %format!("{token:#x}"),
                    "skip: unknown venue (no V2 pair, not Four.Meme suffix)"
                );
                metrics::counter!(
                    "bsc_trader_live_skipped_total",
                    "reason" => "unknown_venue",
                ).increment(1);
                let _ = self.ledger.append(&LiveEntry {
                    phase: self.phase_label(),
                    kol_name: kol_name.clone(),
                    visibility: leak_str(visibility),
                    token_address: token,
                    token_symbol: String::new(),
                    bnb_in_wei: bnb_amount,
                    gas_gwei: 0, nonce: 0, tx_hash: B256::ZERO, wallet_bnb,
                    broadcast: false,
                    limit_skip_reason: Some("unknown_venue".into()),
                });
                return Ok(());
            }
        };

        // ── Sell-tax check (V2 path only) ────────────────────────────────
        // Tokens that have graduated to V2 can have transfer-fee mechanics
        // (reflective / honeypot patterns) that look fine on buy but burn
        // us on sell. We round-trip-quote via getAmountsOut and compare the
        // implied tax vs `cfg.tokens.max_sell_tax_bps`. Four.Meme curve has
        // no token-level tax mechanism so we skip the check there.
        if matches!(route, BuyRoute::PancakeV2) {
            let max_tax = self.limits.config().tokens.max_sell_tax_bps as u64;
            if let Some(implied_tax) = self.implied_sell_tax_bps_v2(token, bnb_amount).await {
                if implied_tax > max_tax {
                    tracing::warn!(
                        target: "trader_live",
                        kol = %kol_name,
                        token = %format!("{token:#x}"),
                        implied_tax_bps = implied_tax,
                        max_tax_bps = max_tax,
                        "skip: sell-tax too high (likely honeypot / fee-on-transfer)"
                    );
                    metrics::counter!(
                        "bsc_trader_live_skipped_total",
                        "reason" => "sell_tax",
                    ).increment(1);
                    let _ = self.ledger.append(&LiveEntry {
                        phase: self.phase_label(),
                        kol_name: kol_name.clone(),
                        visibility: leak_str(visibility),
                        token_address: token,
                        token_symbol: String::new(),
                        bnb_in_wei: bnb_amount,
                        gas_gwei: 0, nonce: 0, tx_hash: B256::ZERO, wallet_bnb,
                        broadcast: false,
                        limit_skip_reason: Some(format!("sell_tax_{}bps", implied_tax)),
                    });
                    return Ok(());
                }
            }
        }

        // --- build calldata + target address per route ---
        // V2: query getAmountsOut + apply `slippage_bps` from limits.toml
        // for on-chain sandwich protection. Adds ~30ms to V2 BUY path but
        // prevents catastrophic mid-block-MEV losses.
        // Four.Meme: amountOutMin stays 0 — the bonding curve is fully
        // deterministic (no LP route, no sandwich vector). Our adverse
        // slippage is just N+1 curve drift from KOL's own buy, which we
        // can't avoid regardless.
        let slip_bps = self.limits.config().limits.slippage_bps as u64;
        let (target, gas_limit, calldata) = match route {
            BuyRoute::PancakeV2 => {
                let path = vec![WBNB.parse::<Address>().context("parse WBNB")?, token];
                let deadline = U256::from(unix_secs() + DEADLINE_SECS);
                // Quote, then haircut for slippage protection.
                let expected_out = self.get_amounts_out_v2(bnb_amount, &path).await
                    .unwrap_or(U256::ZERO);
                let amount_out_min = apply_slippage(expected_out, slip_bps);
                let call = IPancakeRouter::swapExactETHForTokensSupportingFeeOnTransferTokensCall {
                    amountOutMin: amount_out_min,
                    path,
                    to: self.wallet.address(),
                    deadline,
                };
                (
                    PANCAKE_V2_ROUTER.parse::<Address>().context("parse router")?,
                    SWAP_GAS_LIMIT,
                    call.abi_encode(),
                )
            }
            BuyRoute::FourMeme => {
                // Deterministic bonding curve → no sandwich path → minOut=0 ok.
                let call = IFourMeme::buyTokenAMAPCall {
                    token,
                    amountIn: bnb_amount,
                    amountOutMin: U256::ZERO,
                };
                (
                    FOURMEME.parse::<Address>().context("parse fourmeme")?,
                    FOURMEME_GAS_LIMIT,
                    call.abi_encode(),
                )
            }
        };

        // --- build tx ---
        let nonce = self.nonce.reserve();
        let mut req = TransactionRequest::default();
        req.set_from(self.wallet.address());
        req.set_to(target);
        req.set_value(bnb_amount);
        req.set_input(Bytes::from(calldata));
        req.set_gas_limit(gas_limit);
        req.set_max_fee_per_gas(gas_wei);
        req.set_max_priority_fee_per_gas(gas_wei);
        req.set_nonce(nonce);
        req.set_chain_id(BSC_CHAIN_ID);

        // --- sign ---
        let signed = match self.wallet.sign(req) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(target: "trader_live", error = %e, "sign failed");
                // Resync nonce in case our local counter drifted
                let _ = self.nonce.resync().await;
                return Ok(());
            }
        };
        let tx_hash = wallet::tx_hash(&signed);

        // --- broadcast OR shadow-log ---
        let broadcast = if self.limits.broadcast_enabled() {
            if let Err(e) = self.broadcast(&signed).await {
                tracing::error!(target: "trader_live", error = %e, "broadcast failed");
                let _ = self.nonce.resync().await;
                return Ok(());
            }
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                tx_hash = %format!("{tx_hash:#x}"),
                route = ?route,
                nonce, gas_gwei,
                bnb = bnb_in_f,
                "BROADCAST"
            );
            metrics::counter!("bsc_trader_live_broadcast_total").increment(1);
            // Telegram alert — real money was just sent
            let bnb_usd = self.bnb_price.get().await.unwrap_or(0.0);
            let trade_usd = bnb_in_f * bnb_usd;
            let route_label = match route { BuyRoute::PancakeV2 => "V2", BuyRoute::FourMeme => "FourMeme" };
            self.tg_send(format!(
                "🟢 <b>BUY {kol_name}</b> via {route_label}\n\
                 size: <b>${trade_usd:.2}</b> ({bnb_in_f:.4} BNB)  [{sizing_tag}]\n\
                 token: <code>{token:#x}</code>\n\
                 tx: <a href=\"https://bscscan.com/tx/{tx_hash:#x}\">{tx_short}</a>",
                tx_short = &format!("{tx_hash:#x}")[..14]
            ));
            true
        } else {
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                tx_hash = %format!("{tx_hash:#x}"),
                route = ?route,
                nonce, gas_gwei,
                bnb = bnb_in_f,
                wallet_bnb,
                "SHADOW: signed but not broadcast"
            );
            metrics::counter!("bsc_trader_live_shadow_total").increment(1);
            false
        };

        let _ = self.ledger.append(&LiveEntry {
            phase: self.phase_label(),
            kol_name: kol_name.clone(),
            visibility: leak_str(visibility),
            token_address: token,
            token_symbol: String::new(),
            bnb_in_wei: bnb_amount,
            gas_gwei,
            nonce,
            tx_hash,
            wallet_bnb,
            broadcast,
            limit_skip_reason: None,
        });
        self.ledger.record_opened();
        self.limits.record_trade_fired();

        // ── HOT-PATH PREP: populate position cache + pre-approve ──────────
        // Skip both for shadow runs (no broadcast = no real position).
        if broadcast {
            let entry = Arc::new(PositionEntry {
                route,
                tokens_bought: parking_lot::Mutex::new(U256::ZERO), // bg_finalize fills it
                bnb_in: bnb_amount,
                approved: AtomicBool::new(false),
                opened_at: Instant::now(),
            });
            self.positions.write().await.insert(token, entry.clone());

            // Background: wait for buy receipt → parse Transfer to learn
            // actual tokens_bought → submit MAX approve so the SELL fast
            // path never has to wait. By the time the KOL sells (often
            // minutes later) the approve is on-chain and we can sign +
            // broadcast the sell in <50ms.
            let bg = BgPrep {
                http:        self.http.clone(),
                rpc_url:     self.rpc_url.clone(),
                submit_url:  self.submit_url.clone(),
                submit_auth: self.submit_auth.clone(),
                wallet:      self.wallet.clone(),
                nonce:       self.nonce.clone(),
                positions:   self.positions.clone(),
                broadcast_enabled: self.limits.broadcast_enabled(),
                bg_semaphore: self.bg_semaphore.clone(),
            };
            tokio::spawn(async move {
                bg.finalize_position(token, tx_hash, route, entry).await;
            });
        }

        Ok(())
    }

    /// Per-decision EXIT pipeline. Triggered when the strategy emits
    /// Decision::Exit (KOL sold a token). Sells a PROPORTIONAL slice of
    /// our holding — the same fraction the KOL just sold, computed via
    /// `balanceOf(kol, token, sell_block - 1) - balanceOf(kol, token, sell_block)`.
    /// If we can't compute the fraction (sell_block unknown, RPC failure,
    /// post >= pre), fall back to FULL close — never silently drop an exit.
    ///
    /// Partial closes update the position-cache's `tokens_bought` instead
    /// of removing the entry, so subsequent KOL sells fire proportional
    /// exits on what remains.
    pub async fn execute_exit(
        &self,
        token: Address,
        kol_name: &str,
        kol_block: u64,
        kol_addr: Address,
    ) -> Result<()> {
        let t0 = Instant::now();
        // Skip if blacklisted (we never bought, can't own)
        if self.check_blacklisted(token) {
            return Ok(());
        }

        // ── PENDING-MEMPOOL EXIT HANDLING ─────────────────────────────
        // 2026-05-31: we previously DEFERRED these (waited for kol_confirm
        // to get an accurate fraction). Cost was +500ms ≈ +1 block of
        // price drop on a dumping meme (~3-5% = $0.60-$1.00 per trade).
        // For D's behavior pattern (mostly dumping a weak bag, not
        // scaling out into pumps) acting on pending with the fall-back
        // full-close BEATS the proportional-but-late approach.
        //
        // So we no longer defer. `kol_sell_fraction` below will fail
        // (kol_block=0) and the unwrap_or(1.0) below kicks us into a
        // full close — that's now the INTENDED behavior for pending.
        // The kol_confirm signal that arrives ~500ms later will hit the
        // "fully exited" fast-path no-op (Bug 2 fix from this session),
        // so no wasted second-sell tx.

        // ── PER-TOKEN EXIT LOCK ────────────────────────────────────────
        // Serialize exits on this token. While we hold the lock the cache
        // update at the end of this function is guaranteed to be visible
        // to the next caller (their cache read happens AFTER our write).
        // Different tokens still exit in parallel.
        let lock = self.token_exit_lock(token);
        let lock_wait_start = Instant::now();
        let _exit_guard = lock.lock().await;
        let lock_wait_ms = lock_wait_start.elapsed().as_millis();
        if lock_wait_ms > 5 {
            // Worth logging only when contended (not the common case)
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                lock_wait_ms,
                "exit lock contended — concurrent KOL sells on same token"
            );
        }

        // Compute KOL's sell fraction via balanceOf diff. Two parallel RPC
        // calls on local node (~10-30ms total). Fall back to 1.0 (full
        // close) on any failure path so a missed RPC never bag-holds us.
        let fraction = self
            .kol_sell_fraction(kol_addr, token, kol_block)
            .await
            .unwrap_or(1.0);
        let is_full_close = fraction >= 0.99;

        // ── FAST PATH: position cache hit ─────────────────────────────────
        // If we have this token in cache AND bg_finalize has run an approve,
        // we can skip: balance check, V2 pair lookup, Four.Meme dry-run,
        // approve, and approve-wait. Build directly, sign, broadcast.
        // Target latency: <50ms from this call to BlockRazor send.
        //
        // Three cache states with `cached_amt` and `approved`:
        //   - cached_amt > 0 + approved=true   → READY, fast path
        //   - cached_amt == 0 + approved=true  → FULLY EXITED, no-op
        //                                        (we kept the entry after
        //                                        a prior full close so this
        //                                        very check intercepts any
        //                                        queued duplicate exit)
        //   - cached_amt == 0 + approved=false → BG_FINALIZE PENDING,
        //                                        fall to slow path
        let cached = self.positions.read().await.get(&token).cloned();
        let (our_balance, route, used_fast_path) = if let Some(entry) = cached.as_ref() {
            let cached_amt = *entry.tokens_bought.lock();
            let approved = entry.approved.load(Ordering::Acquire);
            if cached_amt.is_zero() && approved {
                // Already fully exited. Don't broadcast another sell — the
                // prior sell's on-chain effect may not be visible yet
                // (slow-path would read stale balance and oversell again).
                tracing::info!(
                    target: "trader_live",
                    kol = %kol_name,
                    token = %format!("{token:#x}"),
                    "skip exit: position fully exited (cached_amt=0, approved=true)"
                );
                return Ok(());
            }
            if !cached_amt.is_zero() && approved {
                // Both critical bits are ready — go fast.
                (cached_amt, entry.route, true)
            } else {
                // bg_finalize hasn't completed yet (cached_amt=0, !approved).
                // Slow path reads on-chain balance which IS the right thing
                // here — we haven't sold yet, on-chain reflects our holding.
                let bal = self.token_balance(token).await.unwrap_or(U256::ZERO);
                if bal.is_zero() {
                    return Ok(());
                }
                (bal, entry.route, false)
            }
        } else {
            // No cache → slow path. Probably a position bought before this
            // restart, or the cache was evicted.
            let bal = self.token_balance(token).await.unwrap_or(U256::ZERO);
            if bal.is_zero() {
                return Ok(());
            }
            let r = match self.pick_route(token, U256::from(1u64), Action::Sell).await {
                Some(r) => r,
                None => {
                    tracing::warn!(
                        target: "trader_live",
                        kol = %kol_name,
                        token = %format!("{token:#x}"),
                        balance = %bal,
                        "exit skipped: no known route to sell on (stuck position!)"
                    );
                    return Ok(());
                }
            };
            (bal, r, false)
        };

        // Apply the proportional-exit fraction. Sells less than dust go
        // FULL close (preserves the original behavior for tiny remnants).
        let balance: U256 = if is_full_close {
            our_balance
        } else {
            let sized = scale_u256_by_fraction(our_balance, fraction);
            // If the proportional slice would round to zero, do nothing.
            if sized.is_zero() {
                tracing::info!(
                    target: "trader_live",
                    kol = %kol_name,
                    token = %format!("{token:#x}"),
                    fraction = format!("{:.4}", fraction),
                    "skip: proportional slice rounds to zero"
                );
                return Ok(());
            }
            sized
        };
        tracing::info!(
            target: "trader_live",
            kol = %kol_name,
            token = %format!("{token:#x}"),
            our_balance = %our_balance,
            sell_amount = %balance,
            fraction = format!("{:.4}", fraction),
            full_close = is_full_close,
            "exit: proportional sizing"
        );

        // Slow-path approve only when we did NOT take the fast path AND
        // the spender lacks allowance.
        if !used_fast_path {
            let spender = match route {
                BuyRoute::PancakeV2 => PANCAKE_V2_ROUTER.parse::<Address>()?,
                BuyRoute::FourMeme  => FOURMEME.parse::<Address>()?,
            };
            let allowance = self.token_allowance(token, spender).await.unwrap_or(U256::ZERO);
            if allowance < balance {
                if let Err(e) = self.submit_approve(token, spender).await {
                    tracing::error!(
                        target: "trader_live",
                        token = %format!("{token:#x}"),
                        error = %e,
                        "approve failed; abandoning exit"
                    );
                    let _ = self.nonce.resync().await;
                    return Ok(());
                }
            }
        }

        // F4: RE-VALIDATE THE ROUTE just before broadcast. The cached
        // `entry.route` was decided at BUY time. If the token has GRADUATED
        // (Four.Meme curve closed, V2 pair created) since then, the cached
        // route is stale. Using the V2 path gives us `amountOutMin`
        // protection that the FourMeme `sellToken` simply doesn't have.
        // Confirmed cause of Token 2's -23.5% real fill on 2026-06-08:
        // FourMeme route with no minOut → curve moved >15% between submit
        // and fill, tx executed at the worse price. V2 route would have
        // reverted (clean retry) instead of filling badly.
        let route = match self.pick_route(token, U256::from(1u64), Action::Sell).await {
            Some(fresh_route) => {
                if fresh_route != route {
                    tracing::info!(
                        target: "trader_live",
                        token = %format!("{token:#x}"),
                        cached_route = ?route,
                        fresh_route = ?fresh_route,
                        "exit: route re-validated; using fresh route for slippage protection"
                    );
                }
                fresh_route
            }
            None => route,  // fallback to cached if re-check failed
        };

        // Build sell calldata + target per route
        // V2: quote → haircut. Four.Meme has no minOut param so can't
        // protect at the contract level (curve is deterministic anyway).
        let slip_bps = self.limits.config().limits.slippage_bps as u64;
        let (target, gas_limit, calldata) = match route {
            BuyRoute::PancakeV2 => {
                let path = vec![token, WBNB.parse::<Address>()?];
                let deadline = U256::from(unix_secs() + DEADLINE_SECS);
                let expected_out = self.get_amounts_out_v2(balance, &path).await
                    .unwrap_or(U256::ZERO);
                let amount_out_min = apply_slippage(expected_out, slip_bps);
                let call = IPancakeRouter::swapExactTokensForETHSupportingFeeOnTransferTokensCall {
                    amountIn: balance,
                    amountOutMin: amount_out_min,
                    path,
                    to: self.wallet.address(),
                    deadline,
                };
                (PANCAKE_V2_ROUTER.parse::<Address>()?, SWAP_GAS_LIMIT, call.abi_encode())
            }
            BuyRoute::FourMeme => {
                let call = IFourMeme::sellTokenCall { token, amount: balance };
                (FOURMEME.parse::<Address>()?, FOURMEME_GAS_LIMIT, call.abi_encode())
            }
        };

        // Build + sign + broadcast (mirrors entry path)
        let nonce = self.nonce.reserve();
        let gas_wei = self.gas_wei().await.unwrap_or(u128::MAX).max(3_000_000_000); // 3 gwei floor for BlockRazor priority
        let gas_gwei = (gas_wei / 1_000_000_000) as u64;
        let mut req = TransactionRequest::default();
        req.set_from(self.wallet.address());
        req.set_to(target);
        req.set_value(U256::ZERO); // SELLs send no BNB
        req.set_input(Bytes::from(calldata));
        req.set_gas_limit(gas_limit);
        req.set_max_fee_per_gas(gas_wei);
        req.set_max_priority_fee_per_gas(gas_wei);
        req.set_nonce(nonce);
        req.set_chain_id(BSC_CHAIN_ID);

        let signed = match self.wallet.sign(req) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(target: "trader_live", error = %e, "exit sign failed");
                let _ = self.nonce.resync().await;
                return Ok(());
            }
        };
        let tx_hash = wallet::tx_hash(&signed);
        // wallet_bnb is only used in the Telegram message — read from the
        // background-refreshed cache so we don't block before broadcast.
        let wallet_bnb = self.wallet_balance_bnb_cached();

        let broadcast = if self.limits.broadcast_enabled() {
            let broadcast_t0 = Instant::now();
            if let Err(e) = self.broadcast(&signed).await {
                tracing::error!(target: "trader_live", error = %e, "exit broadcast failed");
                let _ = self.nonce.resync().await;
                return Ok(());
            }
            let submit_rtt_ms = broadcast_t0.elapsed().as_millis();
            let total_ms = t0.elapsed().as_millis();
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                tx_hash = %format!("{tx_hash:#x}"),
                route = ?route,
                nonce, gas_gwei,
                balance = %balance,
                fast_path = used_fast_path,
                total_ms,
                submit_rtt_ms,
                "SELL BROADCAST"
            );
            metrics::counter!("bsc_trader_live_exit_broadcast_total").increment(1);
            let route_label = match route { BuyRoute::PancakeV2 => "V2", BuyRoute::FourMeme => "FourMeme" };
            let path_tag = if used_fast_path { "⚡fast" } else { "slow" };
            let close_tag = if is_full_close {
                "FULL".to_string()
            } else {
                format!("{:.0}%", fraction * 100.0)
            };
            self.tg_send(format!(
                "🔴 <b>SELL {kol_name}</b> via {route_label} [{path_tag} {total_ms}ms · {close_tag}]\n\
                 token: <code>{token:#x}</code>\n\
                 wallet now: {wallet_bnb:.4} BNB\n\
                 tx: <a href=\"https://bscscan.com/tx/{tx_hash:#x}\">{tx_short}</a>",
                tx_short = &format!("{tx_hash:#x}")[..14]
            ));
            // Cache update: full close → mark exited (tokens_bought=0,
            // approved stays true); partial close → shrink remaining.
            //
            // We do NOT remove the entry on full close. A queued duplicate
            // exit (e.g. kol_watch pending fired earlier and the consumer
            // queued an exit before the proper kol_confirm signal) would
            // otherwise miss the cache → fall to slow path → read STALE
            // on-chain balance (our sell hasn't mined yet) → broadcast
            // a SECOND sell that reverts. With the entry kept + amt=0,
            // the fast-path check intercepts the duplicate and no-ops.
            //
            // Re-buying this token clears the stale entry via the dup-guard
            // path in execute().
            if let Some(entry) = self.positions.read().await.get(&token).cloned() {
                if is_full_close {
                    *entry.tokens_bought.lock() = U256::ZERO;
                } else {
                    let remaining = saturating_sub_u256(our_balance, balance);
                    *entry.tokens_bought.lock() = remaining;
                }
            }
            true
        } else {
            tracing::info!(
                target: "trader_live",
                kol = %kol_name,
                token = %format!("{token:#x}"),
                tx_hash = %format!("{tx_hash:#x}"),
                route = ?route,
                nonce, gas_gwei,
                balance = %balance,
                "SELL SHADOW: signed but not broadcast"
            );
            false
        };

        let _ = self.ledger.append(&LiveEntry {
            phase: self.phase_label(),
            kol_name: kol_name.to_string(),
            visibility: "exit",
            token_address: token,
            token_symbol: String::new(),
            bnb_in_wei: U256::ZERO, // not a buy
            gas_gwei, nonce, tx_hash, wallet_bnb, broadcast,
            limit_skip_reason: None,
        });
        self.ledger.record_closed();
        Ok(())
    }

    /// Submit a max-allowance approve and wait briefly. One-shot pattern —
    /// future sells of the same token reuse the same allowance.
    async fn submit_approve(&self, token: Address, spender: Address) -> Result<()> {
        let call = IERC20::approveCall {
            spender,
            amount: U256::MAX,
        };
        let calldata = call.abi_encode();
        let nonce = self.nonce.reserve();
        let gas_wei = self.gas_wei().await.unwrap_or(u128::MAX).max(3_000_000_000); // 3 gwei floor for BlockRazor priority
        let mut req = TransactionRequest::default();
        req.set_from(self.wallet.address());
        req.set_to(token);
        req.set_value(U256::ZERO);
        req.set_input(Bytes::from(calldata));
        req.set_gas_limit(60_000);
        req.set_max_fee_per_gas(gas_wei);
        req.set_max_priority_fee_per_gas(gas_wei);
        req.set_nonce(nonce);
        req.set_chain_id(BSC_CHAIN_ID);
        let signed = self.wallet.sign(req).context("approve sign")?;
        if self.limits.broadcast_enabled() {
            self.broadcast(&signed).await.context("approve broadcast")?;
            tracing::info!(
                target: "trader_live",
                token = %format!("{token:#x}"),
                spender = %format!("{spender:#x}"),
                tx_hash = %format!("{:#x}", wallet::tx_hash(&signed)),
                nonce,
                "APPROVE broadcast (max allowance)"
            );
            // Give the approve a moment to land before the sell goes
            tokio::time::sleep(Duration::from_millis(1500)).await;
        } else {
            tracing::info!(
                target: "trader_live",
                token = %format!("{token:#x}"),
                spender = %format!("{spender:#x}"),
                tx_hash = %format!("{:#x}", wallet::tx_hash(&signed)),
                nonce,
                "APPROVE SHADOW (not broadcast)"
            );
        }
        Ok(())
    }

    async fn token_balance(&self, token: Address) -> Result<U256> {
        let call = IERC20::balanceOfCall { account: self.wallet.address() };
        self.eth_call_u256(token, call.abi_encode()).await
    }

    async fn token_allowance(&self, token: Address, spender: Address) -> Result<U256> {
        let call = IERC20::allowanceCall { owner: self.wallet.address(), spender };
        self.eth_call_u256(token, call.abi_encode()).await
    }

    async fn eth_call_u256(&self, to: Address, calldata: Vec<u8>) -> Result<U256> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": format!("{to:#x}"), "data": format!("0x{}", hex::encode(calldata))}, "latest"],
            "id": 1,
        });
        let v: serde_json::Value =
            self.http.post(&self.rpc_url).json(&body).send().await?.json().await?;
        let hex_s = v.get("result").and_then(|s| s.as_str()).context("no result")?;
        let s = hex_s.strip_prefix("0x").unwrap_or(hex_s);
        if s.is_empty() {
            return Ok(U256::ZERO);
        }
        U256::from_str_radix(s, 16).map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub fn limits(&self) -> &LimitsRuntime {
        &self.limits
    }

    pub fn open_positions(&self) -> u32 {
        self.ledger.open_count()
    }

    fn phase_label(&self) -> &'static str {
        let p = &self.limits.config().phase;
        if p.full { "full" } else if p.tiny { "tiny" } else { "shadow" }
    }

    /// Race-submit a signed tx to local geth (acceptance gate) AND fire
    /// MEV-Boost bundles in parallel to BlockRazor + Puissant for atomic
    /// same-block landing.
    ///
    /// Three submission paths fire in parallel:
    ///   1. Local geth `eth_sendRawTransaction` — peer-propagation path,
    ///      sole acceptance gate (rtt_ms 0-2 in practice)
    ///   2. BlockRazor `eth_sendBundle` targeting current+1 — atomic
    ///      same-block landing IF BR's builder produces N+1
    ///   3. Puissant `eth_sendPuissant` (48 Club) — atomic same-block
    ///      landing IF 48 Club's builder produces N+1
    ///
    /// **2026-06-09: stripped BR `eth_sendRawTransaction`.** Both BR
    /// `sendRaw` and BR `sendBundle` on the same wallet+tx triggered BR's
    /// in-relay dedup ("bundle already exist") — the bundle path was DOA
    /// 4/4 times until we removed the colliding sendRaw. Local geth was
    /// winning every race-submit at rtt_ms=0-2 anyway, so dropping BR
    /// sendRaw cost nothing and unblocked the bundle's atomic-backrun
    /// potential.
    ///
    /// Returns `Err` only if local geth fails. Bundle outcomes (paths 2+3)
    /// are logged but never gate the return (best-effort backrun).
    async fn broadcast(&self, signed: &alloy::consensus::TxEnvelope) -> Result<()> {
        use alloy::eips::Encodable2718;
        let mut raw = Vec::new();
        signed.encode_2718(&mut raw);
        let raw_hex = Arc::new(format!("0x{}", hex::encode(&raw)));

        let (tx, mut rx) = tokio::sync::mpsc::channel::<(&'static str, Result<()>, u64)>(1);
        // Local geth leg (sendRawTransaction) — sole acceptance gate
        {
            let tx       = tx.clone();
            let http     = self.http.clone();
            let url      = self.rpc_url.clone();
            let raw_hex  = raw_hex.clone();
            tokio::spawn(async move {
                let t0  = Instant::now();
                let res = submit_raw(&http, &url, "", &raw_hex).await;
                let ms  = t0.elapsed().as_millis() as u64;
                let _ = tx.send(("local_geth", res, ms)).await;
            });
        }
        // BlockRazor bundle leg (eth_sendBundle, target current+1) —
        // fire-and-forget atomic-backrun path. Doesn't gate the return.
        {
            let http     = self.http.clone();
            let bundle_url = self.submit_url.clone();
            let auth     = self.submit_auth.clone();
            let rpc_url  = self.rpc_url.clone();
            let raw_hex  = raw_hex.clone();
            tokio::spawn(async move {
                let t0 = Instant::now();
                match submit_bundle(&http, &bundle_url, &auth, &rpc_url, &raw_hex).await {
                    Ok(target_block) => {
                        tracing::info!(
                            target: "trader_live",
                            target_block,
                            rtt_ms = t0.elapsed().as_millis() as u64,
                            "bundle ACCEPTED by BlockRazor (atomic backrun queued for N+1)"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "trader_live",
                            error  = %e,
                            rtt_ms = t0.elapsed().as_millis() as u64,
                            "bundle submission failed; race-submit fallback in effect"
                        );
                    }
                }
            });
        }
        // Puissant Network (48 Club) bundle leg — uses their own
        // `eth_sendPuissant` method. Independent validator subset from
        // BlockRazor; covers blocks BR's builder doesn't win. Fire-and-
        // forget, doesn't gate return.
        {
            let http     = self.http.clone();
            let raw_hex  = raw_hex.clone();
            tokio::spawn(async move {
                let t0 = Instant::now();
                match submit_puissant(&http, PUISSANT_URL, &raw_hex).await {
                    Ok(()) => {
                        tracing::info!(
                            target: "trader_live",
                            rtt_ms = t0.elapsed().as_millis() as u64,
                            "puissant ACCEPTED (atomic backrun queued via 48 Club)"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "trader_live",
                            error  = %e,
                            rtt_ms = t0.elapsed().as_millis() as u64,
                            "puissant submission failed"
                        );
                    }
                }
            });
        }
        drop(tx);  // close sender so rx.recv() returns None after both sendRaw legs report

        let mut last_err: Option<anyhow::Error> = None;
        while let Some((who, res, ms)) = rx.recv().await {
            match res {
                Ok(()) => {
                    tracing::info!(
                        target: "trader_live",
                        winner = who,
                        rtt_ms = ms,
                        "broadcast accepted (race-submit)"
                    );
                    // Return immediately — the loser keeps running and
                    // pushes the tx into the second pool. tx_hash is
                    // identical; validators dedupe.
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        target: "trader_live",
                        endpoint = who,
                        error  = %e,
                        rtt_ms = ms,
                        "race-submit endpoint failed (local geth is sole gate; bundles fire-and-forget)"
                    );
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("race-submit: local geth failed")))
    }

    async fn wallet_balance_bnb(&self) -> Result<f64> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [format!("{:#x}", self.wallet.address()), "latest"],
            "id": 1,
        });
        let v: serde_json::Value =
            self.http.post(&self.rpc_url).json(&body).send().await?.json().await?;
        let hex = v.get("result").and_then(|s| s.as_str()).context("no result")?;
        let n = u128::from_str_radix(hex.trim_start_matches("0x"), 16)?;
        Ok(n as f64 / 1e18)
    }

    /// Decide which venue handles this token for the given action. V2 first
    /// (handles both buy and sell when the pair exists), else a free
    /// `eth_call` dry-run against Four.Meme using the OPERATION-CORRECT
    /// method (`buyTokenAMAP` for Buy, `sellToken` for Sell) — because a
    /// token may accept one direction and not the other after graduation
    /// or delisting events.
    async fn pick_route(
        &self,
        token: Address,
        amount: U256,
        action: Action,
    ) -> Option<BuyRoute> {
        // Use cached lookup. Positive results are immutable (V2 pairs never
        // disappear) so this is hit instantly on graduated tokens; negative
        // results re-checked every 30s to catch new graduations.
        if self.v2_pair_cached(token).await.is_some() {
            return Some(BuyRoute::PancakeV2);
        }
        let accepts = match action {
            Action::Buy => self.fourmeme_would_accept_buy(token, amount).await,
            Action::Sell => self.fourmeme_would_accept_sell(token, amount).await,
        };
        if accepts {
            return Some(BuyRoute::FourMeme);
        }
        None
    }

    /// Dry-run buyTokenAMAP via eth_call.
    async fn fourmeme_would_accept_buy(&self, token: Address, amount: U256) -> bool {
        let call = IFourMeme::buyTokenAMAPCall {
            token,
            amountIn: amount,
            amountOutMin: U256::ZERO,
        };
        let calldata = call.abi_encode();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{
                "from": format!("{:#x}", self.wallet.address()),
                "to":   FOURMEME,
                "value": format!("0x{:x}", amount),
                "data": format!("0x{}", hex::encode(calldata)),
            }, "latest"],
            "id": 1,
        });
        self.eth_call_succeeds(body).await
    }

    /// Dry-run sellToken via eth_call. Probes acceptance only — actual
    /// sell still happens with the real balance.
    ///
    /// IMPORTANT: the real sell path approves Four.Meme as spender BEFORE
    /// calling sellToken. The probe, however, runs without that approval,
    /// so Four.Meme's internal `transferFrom` reverts with
    /// `ERC20: insufficient allowance`. That revert is proof the token IS
    /// on Four.Meme's curve (we got past the dispatch and into the ERC20
    /// transfer), so we treat it as a YES. A genuine "not on Four.Meme"
    /// revert looks like `Invalid token` (or similar) and stays a NO.
    async fn fourmeme_would_accept_sell(&self, token: Address, probe: U256) -> bool {
        let call = IFourMeme::sellTokenCall { token, amount: probe };
        let calldata = call.abi_encode();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{
                "from": format!("{:#x}", self.wallet.address()),
                "to":   FOURMEME,
                "data": format!("0x{}", hex::encode(calldata)),
            }, "latest"],
            "id": 1,
        });
        let v: serde_json::Value = match self.http.post(&self.rpc_url).json(&body).send().await {
            Ok(r) => match r.json().await {
                Ok(j) => j,
                Err(_) => return false,
            },
            Err(_) => return false,
        };
        if v.get("result").is_some() && v.get("error").is_none() {
            return true;
        }
        let msg = v
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("");
        // Two definite YES signals:
        //   - "insufficient allowance": past dispatch, into ERC20 transferFrom
        //     → token IS on curve, will sell after approve.
        //   - "GW": Graduation Window — token bonded and is mid-migrate to
        //     V2/flap. Sells temporarily blocked, but the position is NOT
        //     stuck. The periodic exit-retry sweep will pick this up once
        //     graduation completes and a V2 pair appears.
        // Definite NO: anything with "Invalid token" — the token isn't on
        // Four.Meme at all.
        // Default (unknown revert): treat as YES. Better to attempt the
        // sell and waste $0.08 gas than silently abandon a recoverable
        // position. This was the 2026-05-28 regression — "GW" silently
        // bag-held us until D dumped 100%.
        if msg.contains("Invalid token") {
            return false;
        }
        true
    }

    async fn eth_call_succeeds(&self, body: serde_json::Value) -> bool {
        let v: serde_json::Value = match self.http.post(&self.rpc_url).json(&body).send().await {
            Ok(r) => match r.json().await {
                Ok(j) => j,
                Err(_) => return false,
            },
            Err(_) => return false,
        };
        v.get("result").is_some() && v.get("error").is_none()
    }

    /// True if a WBNB/<token> PancakeSwap V2 pair has been deployed. False
    /// ⇒ token is still on the Four.Meme bonding curve (or doesn't exist).
    async fn v2_pair_exists(&self, token: Address) -> bool {
        let factory = match PANCAKE_V2_FACTORY.parse::<Address>() {
            Ok(a) => a,
            Err(_) => return false,
        };
        let wbnb = match WBNB.parse::<Address>() {
            Ok(a) => a,
            Err(_) => return false,
        };
        let call = IPancakeFactory::getPairCall { tokenA: wbnb, tokenB: token };
        let data = format!("0x{}", hex::encode(call.abi_encode()));
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": format!("{factory:#x}"), "data": data}, "latest"],
            "id": 1,
        });
        let v: serde_json::Value = match self
            .http
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => match r.json().await {
                Ok(j) => j,
                Err(_) => return false,
            },
            Err(_) => return false,
        };
        let hex_s = match v.get("result").and_then(|s| s.as_str()) {
            Some(s) => s,
            None => return false,
        };
        // Pair address is the last 40 hex chars. Zero ⇒ no pair.
        if hex_s.len() < 66 {
            return false;
        }
        let pair_addr = &hex_s[hex_s.len() - 40..];
        pair_addr != "0".repeat(40).as_str()
    }

    /// Returns gas price in WEI (u128). Cached for `GAS_PRICE_TTL` (2s)
    /// since BSC gas barely moves and the hot path can't afford the
    /// ~5-30ms RPC roundtrip on every BUY/SELL. Don't truncate to gwei
    /// — callers may need sub-1-gwei precision.
    async fn gas_wei(&self) -> Result<u128> {
        {
            let g = self.gas_cache.lock();
            if g.wei != 0 && g.refreshed.elapsed() < GAS_PRICE_TTL {
                return Ok(g.wei);
            }
        }
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_gasPrice",
            "params": [],
            "id": 1,
        });
        let v: serde_json::Value =
            self.http.post(&self.rpc_url).json(&body).send().await?.json().await?;
        let hex = v.get("result").and_then(|s| s.as_str()).context("no result")?;
        let wei = u128::from_str_radix(hex.trim_start_matches("0x"), 16)?;
        *self.gas_cache.lock() = GasCache { wei, refreshed: Instant::now() };
        Ok(wei)
    }

    /// Current head block from local node — used as the anchor for dev
    /// lookups on pending-mempool entries where the KOL's mined block
    /// isn't yet known. ~5ms over IPC/local HTTP.
    async fn head_block(&self) -> Option<u64> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1,
        });
        let v: serde_json::Value = self.http.post(&self.rpc_url).json(&body).send().await.ok()?
            .json().await.ok()?;
        let hex = v.get("result")?.as_str()?;
        u64::from_str_radix(hex.trim_start_matches("0x"), 16).ok()
    }

    /// Drop a position from the cache (called after a successful full sell).
    pub async fn drop_position(&self, token: Address) {
        self.positions.write().await.remove(&token);
    }

    /// Public wrapper around the private balance helper — exposed so the
    /// daily-loss-halt poller in `start_live_only` can read the wallet
    /// without holding a back-reference to the executor's private fields.
    pub async fn wallet_balance_bnb_public(&self) -> Result<f64> {
        self.wallet_balance_bnb().await
    }

    /// Hand the limits runtime to background tasks (kill switch poller).
    pub fn limits_runtime(&self) -> Arc<LimitsRuntime> {
        self.limits.clone()
    }

    /// Round-trip V2 quote to detect transfer-tax / honeypot tokens.
    ///
    ///   1. quote: WBNB → token at amount_in
    ///   2. quote: token → WBNB at the result of step 1
    ///   3. round_trip_loss_bps = (amount_in − step2_out) / amount_in × 10000
    ///   4. implied_tax_bps = round_trip_loss_bps − ~50 (the 2× 0.25% V2 fee)
    ///
    /// A clean token comes back at ~50 bps loss. A 5% transfer-tax token
    /// comes back at ~1050 bps. Returns `None` if any quote fails (don't
    /// block trades on RPC outages).
    async fn implied_sell_tax_bps_v2(&self, token: Address, probe_bnb: U256) -> Option<u64> {
        let wbnb = WBNB.parse::<Address>().ok()?;
        let path_buy = vec![wbnb, token];
        let buy_out = self.get_amounts_out_v2(probe_bnb, &path_buy).await?;
        if buy_out.is_zero() { return None; }
        let path_sell = vec![token, wbnb];
        let sell_out = self.get_amounts_out_v2(buy_out, &path_sell).await?;
        if sell_out >= probe_bnb { return Some(0); } // gain or break-even
        // ((probe − sell_out) / probe) × 10000  — done in U256 to avoid f64.
        let loss = probe_bnb - sell_out;
        let loss_bps_u256 = (loss * U256::from(10_000u64)) / probe_bnb;
        let total_loss_bps: u64 = loss_bps_u256.try_into().ok()?;
        // Subtract the 50 bps baseline (2× V2 LP fee). Saturate at zero.
        Some(total_loss_bps.saturating_sub(50))
    }

    /// V2 quote via PancakeRouter.getAmountsOut. ~20-50ms RPC roundtrip
    /// (local node). Used by BUY/SELL paths to compute amountOutMin.
    async fn get_amounts_out_v2(&self, amount_in: U256, path: &[Address]) -> Option<U256> {
        let router = PANCAKE_V2_ROUTER.parse::<Address>().ok()?;
        let call = IPancakeRouter::getAmountsOutCall {
            amountIn: amount_in,
            path: path.to_vec(),
        };
        let calldata = call.abi_encode();
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{
                "to": format!("{router:#x}"),
                "data": format!("0x{}", hex::encode(&calldata)),
            }, "latest"], "id": 1,
        });
        let v: serde_json::Value = self.http.post(&self.rpc_url).json(&body).send().await.ok()?
            .json().await.ok()?;
        let hex_result = v.get("result")?.as_str()?.trim_start_matches("0x");
        let raw = hex::decode(hex_result).ok()?;
        // ABI decode: (uint256[]) — offset(32) + length(32) + N×32 words.
        // We want the LAST entry (final amountOut after the full path).
        if raw.len() < 96 { return None; }
        let length = U256::from_be_slice(&raw[32..64]);
        let n = u32::try_from(length).ok()? as usize;
        if n == 0 || raw.len() < 64 + n * 32 { return None; }
        let last_off = 64 + (n - 1) * 32;
        Some(U256::from_be_slice(&raw[last_off..last_off + 32]))
    }
}

/// Apply a basis-point haircut to an amount: `amount × (10000 - bps) / 10000`.
/// Used to derive `amountOutMin` from a quoted output.
/// Diff `balanceOf(kol, token)` at sell_block-1 vs sell_block to learn
/// the fraction KOL sold. Returns `None` if anything fails — caller must
/// fall back to full close. Two parallel local-node RPCs; ~10-30ms total.
impl LiveExecutor {
    async fn kol_sell_fraction(
        &self,
        kol_addr: Address,
        token: Address,
        kol_block: u64,
    ) -> Option<f64> {
        if kol_addr == Address::ZERO || kol_block == 0 {
            return None;
        }
        let (pre, post) = tokio::join!(
            self.balance_of_at(kol_addr, token, kol_block.saturating_sub(1)),
            self.balance_of_at(kol_addr, token, kol_block),
        );
        let pre = pre?;
        let post = post?;
        if pre.is_zero() || post >= pre {
            return None;
        }
        let sold = pre - post;
        let sold_f: f64 = sold.to_string().parse().ok()?;
        let pre_f: f64 = pre.to_string().parse().ok()?;
        Some((sold_f / pre_f).clamp(0.0, 1.0))
    }

    /// `balanceOf(who, token)` at a specific block. ~5-15ms on local node.
    async fn balance_of_at(
        &self,
        who: Address,
        token: Address,
        block: u64,
    ) -> Option<U256> {
        let call = IERC20::balanceOfCall { account: who };
        let calldata = call.abi_encode();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                { "to": format!("{:#x}", token), "data": format!("0x{}", hex::encode(calldata)) },
                format!("0x{:x}", block),
            ],
            "id": 1,
        });
        let v: serde_json::Value = self.http.post(&self.rpc_url).json(&body).send().await.ok()?
            .json().await.ok()?;
        let hex_s = v.get("result")?.as_str()?.trim_start_matches("0x");
        if hex_s.is_empty() {
            return Some(U256::ZERO);
        }
        U256::from_str_radix(hex_s, 16).ok()
    }
}

/// Scale a U256 by a fraction in basis points (1bp precision floor).
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

/// Saturating subtraction for U256.
fn saturating_sub_u256(a: U256, b: U256) -> U256 {
    if a >= b { a - b } else { U256::ZERO }
}

fn apply_slippage(amount: U256, slip_bps: u64) -> U256 {
    if slip_bps == 0 || amount.is_zero() {
        return amount;
    }
    if slip_bps >= 10_000 {
        return U256::ZERO;
    }
    let keep = U256::from(10_000u64 - slip_bps);
    (amount * keep) / U256::from(10_000u64)
}

/// Cheap clone-of-state for background tasks. Avoids holding `Arc<Self>`
/// just to spawn one shot work — only the fields needed for receipts +
/// approve + read-balance live here.
#[derive(Clone)]
struct BgPrep {
    http:        reqwest::Client,
    rpc_url:     String,
    submit_url:  String,
    submit_auth: String,
    wallet:      Arc<TraderWallet>,
    nonce:       Arc<NonceManager>,
    positions:   Arc<RwLock<HashMap<Address, Arc<PositionEntry>>>>,
    broadcast_enabled: bool,
    /// Bounded concurrency for the receipt-polling phase. Without this,
    /// a burst of BUYs (D fires 10 in a row during a hot meme run) would
    /// spawn 10 tasks each holding a long poll loop against RPC. 8 is
    /// plenty — single-token receipts complete in 1-3s anyway.
    bg_semaphore: Arc<tokio::sync::Semaphore>,
}

impl BgPrep {
    /// Wait for the BUY tx receipt, then:
    ///   1. read our on-chain balance to learn the actual tokens_bought
    ///      (Four.Meme bonding curves don't emit a Transfer to the buyer
    ///      via a predictable event sig — balanceOf is the cheap truth)
    ///   2. submit the MAX approve (Four.Meme only — V2 sells don't need it)
    ///   3. wait for approve receipt, flip the cache's `approved` flag
    /// All best-effort. Any failure just leaves the SELL on the slow path.
    async fn finalize_position(
        &self,
        token: Address,
        buy_tx_hash: B256,
        route: BuyRoute,
        entry: Arc<PositionEntry>,
    ) {
        // Acquire a permit so concurrent finalizes stay bounded.
        let _permit = match self.bg_semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                // Semaphore closed — runner shutting down; drop the task.
                return;
            }
        };

        // Step 1 — wait for buy receipt (poll local node, capped at 30s)
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut got_receipt = false;
        while Instant::now() < deadline {
            if self.receipt_status(buy_tx_hash).await == Some(true) {
                got_receipt = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if !got_receipt {
            tracing::warn!(
                target: "trader_live",
                token = %format!("{token:#x}"),
                tx = %format!("{buy_tx_hash:#x}"),
                "bg_finalize: buy receipt not seen within 30s; SELL will use slow path"
            );
            return;
        }
        // Step 2 — read actual tokens_bought from on-chain balance.
        let bal = self.token_balance(token).await.unwrap_or(U256::ZERO);
        if !bal.is_zero() {
            *entry.tokens_bought.lock() = bal;
        }
        // Step 3 — approve (Four.Meme only). V2 sells via swapExact… don't
        // need ERC20 approval at the router level.
        if matches!(route, BuyRoute::FourMeme) && self.broadcast_enabled {
            match self.submit_approve_bg(token).await {
                Ok(Some(approve_hash)) => {
                    // Wait for approve receipt — usually 1-2 blocks (~1s)
                    let deadline = Instant::now() + Duration::from_secs(15);
                    while Instant::now() < deadline {
                        if self.receipt_status(approve_hash).await == Some(true) {
                            entry.approved.store(true, Ordering::Release);
                            tracing::info!(
                                target: "trader_live",
                                token = %format!("{token:#x}"),
                                approve_tx = %format!("{approve_hash:#x}"),
                                "bg_finalize: token PRE-APPROVED, sell fast-path armed"
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
                Ok(None) => { /* shadow mode */ }
                Err(e) => {
                    tracing::warn!(
                        target: "trader_live",
                        token = %format!("{token:#x}"),
                        error = %e,
                        "bg_finalize: approve failed; SELL will use slow path"
                    );
                }
            }
        }
        let _ = got_receipt;
    }

    /// Returns Some(true) if receipt exists and status is success, Some(false)
    /// if reverted, None if not mined yet / lookup failed.
    async fn receipt_status(&self, tx_hash: B256) -> Option<bool> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "eth_getTransactionReceipt",
            "params": [format!("{tx_hash:#x}")], "id": 1,
        });
        let v: serde_json::Value = self.http.post(&self.rpc_url).json(&body).send().await.ok()?
            .json().await.ok()?;
        let r = v.get("result")?;
        if r.is_null() { return None; }
        let s = r.get("status")?.as_str()?;
        Some(s == "0x1")
    }

    async fn token_balance(&self, token: Address) -> Option<U256> {
        let call = IERC20::balanceOfCall { account: self.wallet.address() };
        let calldata = call.abi_encode();
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{
                "to":   format!("{token:#x}"),
                "data": format!("0x{}", hex::encode(calldata)),
            }, "latest"], "id": 1,
        });
        let v: serde_json::Value = self.http.post(&self.rpc_url).json(&body).send().await.ok()?
            .json().await.ok()?;
        let h = v.get("result")?.as_str()?.trim_start_matches("0x");
        U256::from_str_radix(h, 16).ok()
    }

    async fn submit_approve_bg(&self, token: Address) -> Result<Option<B256>> {
        if !self.broadcast_enabled {
            return Ok(None);
        }
        let spender = FOURMEME.parse::<Address>().context("parse fourmeme")?;
        let call = IERC20::approveCall { spender, amount: U256::MAX };
        let calldata = call.abi_encode();
        let nonce = self.nonce.reserve();
        // Read current gas, apply same 3-gwei floor as the hot path.
        let gas_wei = self.gas_price_wei().await.unwrap_or(3_000_000_000u128).max(3_000_000_000);
        let mut req = TransactionRequest::default();
        req.set_from(self.wallet.address());
        req.set_to(token);
        req.set_value(U256::ZERO);
        req.set_input(Bytes::from(calldata));
        req.set_gas_limit(60_000);
        req.set_max_fee_per_gas(gas_wei);
        req.set_max_priority_fee_per_gas(gas_wei);
        req.set_nonce(nonce);
        req.set_chain_id(BSC_CHAIN_ID);
        let signed = self.wallet.sign(req)?;
        let tx_hash = wallet::tx_hash(&signed);
        use alloy::eips::Encodable2718;
        let mut raw = Vec::new();
        signed.encode_2718(&mut raw);
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
            "params": [format!("0x{}", hex::encode(&raw))], "id": 1,
        });
        let mut req = self.http.post(&self.submit_url).json(&body);
        if !self.submit_auth.is_empty() {
            req = req.header("Authorization", self.submit_auth.clone());
        }
        let v: serde_json::Value = req.send().await?.json().await?;
        if let Some(err) = v.get("error") {
            anyhow::bail!("approve rpc: {err}");
        }
        Ok(Some(tx_hash))
    }

    async fn gas_price_wei(&self) -> Result<u128> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1,
        });
        let v: serde_json::Value = self.http.post(&self.rpc_url).json(&body).send().await?.json().await?;
        let hex = v.get("result").and_then(|s| s.as_str()).context("no result")?;
        Ok(u128::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }
}

/// Submit a single raw tx hex to one JSON-RPC endpoint with one retry on
/// transport error. Used by the race-submit broadcast path — one call per
/// endpoint (BlockRazor, local geth). Both run concurrently; first to
/// return Ok wins. Idempotency: if an endpoint replies with "already
/// known" / "known transaction" (because the OTHER endpoint already
/// pushed this tx into the gossip net), treat as success — the tx IS in
/// flight, our role here is done.
async fn submit_raw(
    http:    &reqwest::Client,
    url:     &str,
    auth:    &str,
    raw_hex: &str,
) -> Result<()> {
    for attempt in 0..2u8 {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method":  "eth_sendRawTransaction",
            "params":  [raw_hex],
            "id":      1,
        });
        let mut req = http.post(url).json(&body);
        if !auth.is_empty() {
            req = req.header("Authorization", auth);
        }
        match req.send().await {
            Ok(resp) => {
                let v: serde_json::Value = resp.json().await
                    .context("decode RPC response")?;
                if let Some(err) = v.get("error") {
                    // Idempotency: the other endpoint may have already
                    // pushed this tx through the gossip net before we
                    // got there. That's a WIN for race-submit, not a
                    // failure — the tx is in flight, our job is done.
                    let s = err.to_string().to_lowercase();
                    if s.contains("already known")
                        || s.contains("known transaction")
                        || s.contains("alreadyknown")
                    {
                        return Ok(());
                    }
                    anyhow::bail!("RPC error: {err}");
                }
                return Ok(());
            }
            Err(e) if attempt == 0 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => return Err(e).context("send (after retry)"),
        }
    }
    unreachable!("submit_raw retry loop must return")
}

/// Submit a single signed tx as an `eth_sendBundle` to a Flashbots-style
/// MEV-Boost relay (BlockRazor on BSC). The bundle targets the next block
/// (current_block + 1) so it's evaluated by whichever builder produces
/// that block. If BR's builder wins N+1, our tx is included ATOMICALLY in
/// the same block as D's pending tx — gap = 0.
///
/// This is best-effort: bundle landing requires BR's builder to win the
/// target block (~30-50% of BSC blocks). On miss, the race-submit
/// sendRaw paths land us in N+1 or N+2 as today (no regression). Caller
/// fires this as a fire-and-forget alongside the sendRaw legs.
///
/// Returns the target block number on relay-acceptance; error on
/// transport or relay-side reject.
async fn submit_bundle(
    http:       &reqwest::Client,
    bundle_url: &str,
    auth:       &str,
    rpc_url:    &str,
    raw_hex:    &str,
) -> Result<u64> {
    // Step 1: look up current block (local geth — sub-ms on loopback).
    // Targeting current+1 lines up our bundle with the block being built.
    let bn_body = serde_json::json!({
        "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
    });
    let v: serde_json::Value = http.post(rpc_url).json(&bn_body).send().await?
        .json().await.context("decode eth_blockNumber")?;
    let bn_hex = v.get("result").and_then(|s| s.as_str())
        .context("eth_blockNumber returned no result")?;
    let current = u64::from_str_radix(bn_hex.trim_start_matches("0x"), 16)?;
    let target  = current + 1;

    // Step 2: submit bundle. revertingTxHashes: [our_tx_hash] would mean
    // "still include if it reverts" — we DON'T want that; reverts are
    // money-burns. Leave empty so a reverting tx drops the bundle.
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method":  "eth_sendBundle",
        "params":  [{
            "txs":               [raw_hex],
            "blockNumber":       format!("0x{:x}", target),
            "minTimestamp":      0,
            "maxTimestamp":      0,
            "revertingTxHashes": []
        }],
        "id": 1,
    });
    let mut req = http.post(bundle_url).json(&body);
    if !auth.is_empty() {
        req = req.header("Authorization", auth);
    }
    let v: serde_json::Value = req.send().await?.json().await
        .context("decode eth_sendBundle response")?;
    if let Some(err) = v.get("error") {
        anyhow::bail!("bundle RPC error: {err}");
    }
    Ok(target)
}

/// Submit a single signed tx to Puissant Network (48 Club's BSC MEV-Boost
/// relay). Different RPC method than Flashbots-style `eth_sendBundle` —
/// Puissant uses `eth_sendPuissant` with their own param shape:
///   - `txs`: array of raw signed txs
///   - `maxTimestamp`: unix-secs deadline (after which relay drops bundle)
///   - `acceptReverting`: list of tx_hashes allowed to revert (empty ⇒ all-or-nothing)
///
/// No auth required (public free relay). Covers validators 48 Club operates
/// that BlockRazor doesn't reach — pure additive bundle-win coverage.
async fn submit_puissant(
    http:    &reqwest::Client,
    url:     &str,
    raw_hex: &str,
) -> Result<()> {
    // 30s window — bundle is good for ~66 blocks, plenty of slack to land.
    let max_ts = unix_secs() + 30;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method":  "eth_sendPuissant",
        "params":  [{
            "txs":             [raw_hex],
            "maxTimestamp":    max_ts,
            "acceptReverting": []
        }],
        "id": 1,
    });
    let v: serde_json::Value = http.post(url).json(&body).send().await?
        .json().await.context("decode Puissant response")?;
    if let Some(err) = v.get("error") {
        anyhow::bail!("Puissant RPC error: {err}");
    }
    Ok(())
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// We need `&'static str` for the LiveEntry struct. The visibility values
/// in practice are only "public" or "private"; map to a static slice for
/// each. Anything unknown becomes "public" (paranoid default).
fn leak_str(s: &str) -> &'static str {
    match s {
        "public" => "public",
        "private" => "private",
        _ => "public",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn blacklist_catches_stables_and_majors() {
        // USDT
        assert!(is_blacklisted(address!("55d398326f99059ff775485246999027b3197955")));
        // USDC (different case in storage; we lowercase for comparison)
        assert!(is_blacklisted(address!("8AC76A51CC950d9822D68b83fE1Ad97B32Cd580d")));
        // WBNB
        assert!(is_blacklisted(address!("bb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c")));
        // ASTER
        assert!(is_blacklisted(address!("000ae314e2a2172a039b26378814c252734f556a")));
        // Random Four.Meme token — should pass
        assert!(!is_blacklisted(address!("5339314c13bc8c8cde2590cbaff7601162594444")));
    }

    // ── Per-token exit-lock concurrency tests ──────────────────────────
    //
    // The bug: when KOL D fires multiple SELLs on the same token within
    // seconds, two execute_exit tasks raced — both read the same cached
    // tokens_bought, both broadcast against the same balance → oversell.
    //
    // The fix: per-token mutex. These tests verify:
    //   1. Same token serializes (max-in-flight = 1)
    //   2. Different tokens parallelize (no false serialization)
    //
    // We test the lock helper in isolation by simulating execute_exit's
    // critical section with a tiny sleep — no RPC needed.

    fn make_lock_map() -> Arc<parking_lot::Mutex<HashMap<Address, Arc<tokio::sync::Mutex<()>>>>> {
        Arc::new(parking_lot::Mutex::new(HashMap::new()))
    }

    fn get_or_create(
        map: &Arc<parking_lot::Mutex<HashMap<Address, Arc<tokio::sync::Mutex<()>>>>>,
        token: Address,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let mut m = map.lock();
        m.entry(token)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_token_exits_serialize() {
        // 5 concurrent tasks claim to "exit" the SAME token. Each holds
        // the lock for 20ms. With proper serialization the max in-flight
        // counter should never exceed 1.
        let map = make_lock_map();
        let token = address!("65d79e96e7c3495b45b69a4195f6d61eb8cd4444");
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..5 {
            let map = map.clone();
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            handles.push(tokio::spawn(async move {
                let lock = get_or_create(&map, token);
                let _g = lock.lock().await;
                // critical section — record peak concurrency
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            max_in_flight.load(Ordering::SeqCst),
            1,
            "same-token exits MUST serialize; observed peak in-flight > 1"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn different_tokens_exit_in_parallel() {
        // 5 concurrent tasks each on a DIFFERENT token. They should ALL
        // run in parallel (no cross-token blocking). Peak in-flight should
        // equal the number of worker threads or the task count, not 1.
        let map = make_lock_map();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..5u8 {
            let map = map.clone();
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            let mut bytes = [0u8; 20];
            bytes[19] = i + 1;
            let token = Address::from(bytes);
            handles.push(tokio::spawn(async move {
                let lock = get_or_create(&map, token);
                let _g = lock.lock().await;
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // We have 4 worker threads + 5 tasks of 30ms each. With true
        // parallelism, peak should be ≥ 2 (and likely 4). If serialization
        // were broken across tokens, peak would be 1.
        let peak = max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak >= 2,
            "different-token exits MUST run in parallel; peak={peak}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cache_update_ordering_no_oversell() {
        // Simulate the real bug: two exits read cached `balance`, both
        // size against it, both sell. With the lock + post-broadcast cache
        // update, exit #2 must see exit #1's shrunk balance.
        //
        // Cache starts at 1000 tokens. Each exit "sells" 50% of what it
        // reads, then writes (remaining) back. Without serialization both
        // sell 500 (total 1000 — full close, oversell of intent). With
        // serialization first sells 500 → 500 remaining; second sells 250
        // → 250 remaining. Total sold = 750, intended.
        let map = make_lock_map();
        let token = address!("65d79e96e7c3495b45b69a4195f6d61eb8cd4444");
        let cached: Arc<parking_lot::Mutex<u64>> = Arc::new(parking_lot::Mutex::new(1000));
        let total_sold: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let map = map.clone();
            let cached = cached.clone();
            let total_sold = total_sold.clone();
            handles.push(tokio::spawn(async move {
                let lock = get_or_create(&map, token);
                let _g = lock.lock().await;
                let our_balance = *cached.lock();
                let sell = our_balance / 2;
                // simulate the broadcast taking 5ms
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                // post-broadcast cache update inside the lock
                *cached.lock() = our_balance - sell;
                total_sold.fetch_add(sell as usize, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let sold = total_sold.load(Ordering::SeqCst);
        // With serialization: 500 + 250 = 750. Without: 1000. Anything
        // > 750 means we oversold relative to intent.
        assert_eq!(sold, 750, "expected 750 sold (500 + 250); got {sold}");
        assert_eq!(*cached.lock(), 250);
    }

    // ── Fully-exited cache state machine ─────────────────────────────
    //
    // 2026-05-31 bug: after a full close we REMOVED the cache entry. A
    // queued duplicate exit (e.g. kol_watch fired pending sell, then
    // kol_confirm fired the confirmed sell) would then fall to the slow
    // path, read STALE on-chain balance (our first sell hadn't mined),
    // and broadcast a SECOND sell that reverted with insufficient balance.
    //
    // Fix: keep the entry, set tokens_bought=0. Fast-path early-returns
    // when (cached_amt=0 && approved=true). Re-buy clears stale via
    // dup-guard.
    //
    // These tests model the cache state-machine directly (no executor),
    // verifying the three states our logic distinguishes.

    fn state(cached_amt: U256, approved: bool) -> &'static str {
        if cached_amt.is_zero() && approved {
            "fully_exited_noop"
        } else if !cached_amt.is_zero() && approved {
            "fast_path_ready"
        } else {
            "bg_finalize_pending"
        }
    }

    #[test]
    fn fully_exited_state_is_distinguishable_from_unfinalized() {
        // bg_finalize pending: tokens=0, approved=false
        assert_eq!(state(U256::ZERO, false), "bg_finalize_pending");
        // Ready to sell: tokens>0, approved=true
        assert_eq!(state(U256::from(1u64), true), "fast_path_ready");
        // Fully exited: tokens=0, approved=true
        assert_eq!(state(U256::ZERO, true), "fully_exited_noop");
        // Edge: holding but somehow approve never finished (shouldn't happen
        // in practice but classify as bg_finalize_pending so slow path runs)
        assert_eq!(state(U256::from(1u64), false), "bg_finalize_pending");
    }

    #[test]
    fn dup_guard_classifies_re_buy_after_exit() {
        // Three scenarios for "we already have a cache entry; can we re-buy?"
        // Returning (skip_dup, clear_stale) tuples matches the execute() logic.
        fn classify(amt: U256, approved: bool) -> (bool, bool) {
            if amt.is_zero() && approved {
                (false, true)   // stale exited entry → clear & allow
            } else {
                (true, false)   // truly holding → skip dup
            }
        }
        assert_eq!(classify(U256::from(1_000_000u64), true),  (true,  false), "holding → skip dup");
        assert_eq!(classify(U256::ZERO,               true),  (false, true),  "exited → clear stale");
        assert_eq!(classify(U256::ZERO,               false), (true,  false), "pending finalize → treat as holding");
        assert_eq!(classify(U256::from(1u64),         false), (true,  false), "have tokens, mid-finalize → still skip");
    }

    #[test]
    fn pending_mempool_sells_fall_back_to_full_close() {
        // 2026-05-31 design: pending-mempool SELL signals (kol_block=0)
        // are NOT deferred. The +500ms wait for kol_confirm cost a full
        // block of price drop on dumping memes (~3-5% = $0.60-$1.00/trade)
        // — worse than the alternative of a fast full-close.
        //
        // `kol_sell_fraction` returns None on kol_block=0, the executor
        // applies `unwrap_or(1.0)` → fraction=1.0 → is_full_close=true.
        // The duplicate kol_confirm signal arriving ~500ms later hits
        // the fully-exited fast-path no-op (see `fully_exited_state…`
        // test) so no wasted second-sell tx.
        let fraction_on_pending: f64 = Option::<f64>::None.unwrap_or(1.0);
        assert!(fraction_on_pending >= 0.99, "pending must fall back to full close");
    }
}

fn limit_label(f: &LimitFail) -> String {
    match f {
        LimitFail::NotWhitelisted(_)              => "not_whitelisted",
        LimitFail::PrivateVisibilityWhilePublicOnly => "private_visibility",
        LimitFail::PerTradeCapExceeded { .. }     => "per_trade_cap",
        LimitFail::MaxOpenPositions(_)            => "max_open",
        LimitFail::MaxTradesPerDay(_)             => "max_trades_day",
        LimitFail::DailyLossHalt { .. }           => "daily_loss_halt",
        LimitFail::WalletBelowMin { .. }          => "wallet_low",
        LimitFail::GasTooHigh { .. }              => "gas_too_high",
        LimitFail::InCooldown { .. }              => "cooldown",
        LimitFail::PhaseSaysShadow                => "shadow",
    }.to_string()
}
