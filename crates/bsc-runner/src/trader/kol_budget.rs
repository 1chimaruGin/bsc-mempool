//! Per-KOL closed-loop paper-trading budgets.
//!
//! Each KOL gets an independent BNB pot at startup. Position size is a
//! configured fraction of CURRENT cash (compounds with wins, shrinks with
//! losses). When cash drops below the dust floor, that KOL stops trading.
//!
//! State is persisted to JSON next to the ledger so per-KOL portfolios
//! survive restarts.

use alloy::primitives::U256;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// One KOL's paper portfolio. All amounts in wei.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KolBudget {
    /// Starting capital (constant after init).
    pub initial_wei: u128,
    /// Available cash (not committed to open positions).
    pub cash_wei: u128,
    /// Reserved by currently-open positions.
    pub committed_wei: u128,
    /// Lifetime sum of (bnb_out - bnb_in) across closed trades.
    pub realized_pnl_wei: i128,
    pub trades_taken: u32,
    pub trades_skipped_budget: u32,
    pub last_updated_ns: u64,
}

impl KolBudget {
    fn new(initial_wei: u128) -> Self {
        Self {
            initial_wei,
            cash_wei: initial_wei,
            committed_wei: 0,
            realized_pnl_wei: 0,
            trades_taken: 0,
            trades_skipped_budget: 0,
            last_updated_ns: 0,
        }
    }

    /// Total equity = cash + committed (mark-to-market of open positions
    /// is NOT included — we report at-cost on the open side).
    pub fn equity_wei(&self) -> u128 {
        self.cash_wei.saturating_add(self.committed_wei)
    }
}

/// Per-KOL paper-portfolio book. Thread-safe; persists to JSON.
pub struct KolBudgetBook {
    inner: Mutex<HashMap<String, KolBudget>>,
    initial_wei: u128,
    /// Position size as a fraction of CURRENT cash, in basis points.
    /// e.g. 1000 = 10%.
    pub position_pct_bps: u32,
    /// Trades smaller than this in wei are skipped (dust floor).
    pub dust_floor_wei: u128,
    path: PathBuf,
}

impl KolBudgetBook {
    /// Load from disk, or create empty.
    pub fn load_or_new(
        path: PathBuf,
        initial_wei: u128,
        position_pct_bps: u32,
        dust_floor_wei: u128,
    ) -> Arc<Self> {
        let inner = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<HashMap<String, KolBudget>>(&s).ok())
            .unwrap_or_default();
        Arc::new(Self {
            inner: Mutex::new(inner),
            initial_wei,
            position_pct_bps,
            dust_floor_wei,
            path,
        })
    }

    /// Reserve a position-sized chunk from `kol_name`'s cash. Returns the
    /// reserved BNB wei, or `None` if the KOL is over-budget (cash too low
    /// after the position-pct math).
    pub async fn try_reserve(&self, kol_name: &str) -> Option<U256> {
        let mut map = self.inner.lock().await;
        let entry = map
            .entry(kol_name.to_string())
            .or_insert_with(|| KolBudget::new(self.initial_wei));
        let want = (u128::from(self.position_pct_bps)
            .saturating_mul(entry.cash_wei))
            / 10_000u128;
        if want < self.dust_floor_wei {
            entry.trades_skipped_budget += 1;
            entry.last_updated_ns = now_ns();
            return None;
        }
        entry.cash_wei = entry.cash_wei.saturating_sub(want);
        entry.committed_wei = entry.committed_wei.saturating_add(want);
        entry.trades_taken += 1;
        entry.last_updated_ns = now_ns();
        Some(U256::from(want))
    }

    /// Credit close proceeds back to the KOL's cash; track realized PnL.
    /// `bnb_in` is what we committed at entry; `bnb_out` is what came back.
    pub async fn credit_close(&self, kol_name: &str, bnb_in: U256, bnb_out: U256) {
        let mut map = self.inner.lock().await;
        let Some(entry) = map.get_mut(kol_name) else {
            tracing::warn!(
                target: "trader",
                kol = %kol_name,
                "credit_close: KOL not in budget book (lost row?)"
            );
            return;
        };
        let inp = u128::try_from(bnb_in).unwrap_or(u128::MAX);
        let outp = u128::try_from(bnb_out).unwrap_or(u128::MAX);
        entry.committed_wei = entry.committed_wei.saturating_sub(inp);
        entry.cash_wei = entry.cash_wei.saturating_add(outp);
        // Signed PnL: outp - inp.
        let delta = i128::try_from(outp).unwrap_or(i128::MAX)
            .saturating_sub(i128::try_from(inp).unwrap_or(i128::MAX));
        entry.realized_pnl_wei = entry.realized_pnl_wei.saturating_add(delta);
        entry.last_updated_ns = now_ns();
    }

    /// Snapshot for reporting.
    pub async fn snapshot(&self) -> HashMap<String, KolBudget> {
        self.inner.lock().await.clone()
    }

    /// Persist to disk. Caller decides cadence (every close, periodic, ...).
    pub async fn save(&self) -> Result<()> {
        let snap = self.inner.lock().await.clone();
        let tmp = self.path.with_extension("json.tmp");
        let body = serde_json::to_vec_pretty(&snap)?;
        tokio::fs::write(&tmp, &body).await?;
        tokio::fs::rename(&tmp, &self.path).await?;
        Ok(())
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
