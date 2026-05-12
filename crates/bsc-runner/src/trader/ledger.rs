//! Persistence for the paper trader.
//!
//! - `closed_trades.csv` — append-only, one row per closed trade.
//! - `open_positions.json` — point-in-time snapshot of open positions.
//!   Atomic write (tmp + rename) so a crash mid-write can't corrupt it.
//!
//! Both files live under `<ledger_dir>` (default `/data/bsc-meme-mev/trader/`).

use crate::trader::position::PositionBook;
use crate::trader::types::{ClosedTrade, OpenPosition};
use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

const CSV_HEADER: &str = "ts_unix_ns,portfolio,kol_name,token_address,token_symbol,\
bnb_in_wei,bnb_out_wei,pnl_wei,pnl_pct,opened_at_block,closed_at_block,\
held_secs,buy_tx_count,close_reason,trigger_sell_tx\n";

pub struct Ledger {
    csv_path: PathBuf,
    json_path: PathBuf,
    csv_lock: Mutex<()>,
    json_lock: Mutex<()>,
}

impl Ledger {
    pub fn new(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
        let csv_path = dir.join("closed_trades.csv");
        let json_path = dir.join("open_positions.json");

        if !csv_path.exists() {
            let mut f = File::create(&csv_path)
                .with_context(|| format!("create {}", csv_path.display()))?;
            f.write_all(CSV_HEADER.as_bytes())?;
        }

        Ok(Self {
            csv_path,
            json_path,
            csv_lock: Mutex::new(()),
            json_lock: Mutex::new(()),
        })
    }

    /// Append one closed trade row. Synchronous-style (acquires async lock,
    /// blocks within the lock for the file write — fine for a low-volume
    /// paper trader, no need for a separate writer task).
    pub async fn append_trade(&self, trade: &ClosedTrade) -> Result<()> {
        let _g = self.csv_lock.lock().await;
        let mut f = OpenOptions::new()
            .append(true)
            .open(&self.csv_path)
            .with_context(|| format!("open {}", self.csv_path.display()))?;
        let row = format_csv_row(trade);
        f.write_all(row.as_bytes())?;
        f.flush()?;
        Ok(())
    }

    /// Snapshot the position book to JSON. Atomic via tmp + rename.
    pub async fn save_book(&self, book: &PositionBook) -> Result<()> {
        let _g = self.json_lock.lock().await;
        let positions: Vec<OpenPosition> = book.snapshot();
        let tmp = self.json_path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&positions)?;
        std::fs::write(&tmp, &bytes).with_context(|| format!("write {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.json_path)
            .with_context(|| format!("rename to {}", self.json_path.display()))?;
        Ok(())
    }

    /// Load the position book on startup. Returns an empty book if the file
    /// doesn't exist yet (first run).
    pub fn load_book(&self) -> Result<PositionBook> {
        if !self.json_path.exists() {
            return Ok(PositionBook::new());
        }
        let bytes = std::fs::read(&self.json_path)
            .with_context(|| format!("read {}", self.json_path.display()))?;
        if bytes.is_empty() {
            return Ok(PositionBook::new());
        }
        let positions: Vec<OpenPosition> =
            serde_json::from_slice(&bytes).context("parse open_positions.json")?;
        let mut book = PositionBook::new();
        for p in positions {
            // Re-insert via open_or_add — for a single restore-from-snapshot
            // we just plant the aggregated position back. We lose the
            // creation-ns-precision (it gets re-stamped to "now"), which
            // matters slightly for the 24h timeout — restored positions get
            // a fresh 24h clock. Acceptable trade-off for the simplicity.
            book.open_or_add(
                p.portfolio,
                p.kol_name,
                p.token_address,
                p.token_symbol,
                p.token_decimals,
                p.bnb_in,
                p.tokens_held,
                p.opened_at_block,
                *p.buy_tx_hashes
                    .first()
                    .unwrap_or(&alloy::primitives::B256::ZERO),
            );
        }
        Ok(book)
    }

    pub fn csv_path(&self) -> &Path {
        &self.csv_path
    }
    pub fn json_path(&self) -> &Path {
        &self.json_path
    }
}

fn format_csv_row(t: &ClosedTrade) -> String {
    let trigger = t
        .trigger_sell_tx
        .map(|h| format!("{h:#x}"))
        .unwrap_or_default();
    format!(
        "{ts},{portfolio},{kol},{addr:#x},{sym},{bnb_in},{bnb_out},{pnl_wei},{pnl_pct:.6},\
         {open_block},{close_block},{held},{buys},{reason},{trigger}\n",
        ts = t.opened_at_unix_ns,
        portfolio = t.portfolio.label(),
        kol = csv_escape(&t.kol_name),
        addr = t.token_address,
        sym = csv_escape(&t.token_symbol),
        bnb_in = t.bnb_in_wei,
        bnb_out = t.bnb_out_wei,
        pnl_wei = t.pnl_wei,
        pnl_pct = t.pnl_pct,
        open_block = t.opened_at_block,
        close_block = t.closed_at_block,
        held = t.held_secs,
        buys = t.buy_tx_count,
        reason = t.close_reason.label(),
        trigger = trigger,
    )
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trader::types::{CloseReason, PortfolioMode};
    use alloy::primitives::{Address, U256};

    #[test]
    fn header_starts_with_expected_columns() {
        assert!(CSV_HEADER.starts_with("ts_unix_ns,portfolio,kol_name,"));
        assert!(CSV_HEADER.contains("bnb_in_wei,bnb_out_wei"));
    }

    #[test]
    fn format_row_no_special_chars() {
        let t = ClosedTrade {
            portfolio: PortfolioMode::NormalTip,
            kol_name: "D".into(),
            token_address: Address::ZERO,
            token_symbol: "PEPE".into(),
            bnb_in_wei: U256::from(500_000_000_000_000_000u128),  // 0.5 BNB
            bnb_out_wei: U256::from(750_000_000_000_000_000u128), // 0.75 BNB
            tokens_traded: U256::from(1_000_000u128),
            pnl_wei: 250_000_000_000_000_000i128,
            pnl_pct: 0.5,
            opened_at_block: 100,
            opened_at_unix_ns: 1_700_000_000_000_000_000,
            closed_at_block: 105,
            closed_at_unix_ns: 1_700_000_060_000_000_000,
            held_secs: 60,
            buy_tx_count: 1,
            close_reason: CloseReason::KolSell,
            trigger_sell_tx: None,
        };
        let row = format_csv_row(&t);
        assert!(row.contains("normal_tip"));
        assert!(row.contains("D,"));
        assert!(row.contains("PEPE,"));
        assert!(row.contains("kol_sell"));
        assert!(row.ends_with('\n'));
    }

    #[test]
    fn csv_escape_handles_commas() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
        assert_eq!(csv_escape("he said \"hi\""), "\"he said \"\"hi\"\"\"");
    }

    #[test]
    fn no_liquidity_close_reason_in_csv() {
        let t = ClosedTrade {
            portfolio: PortfolioMode::FastTip,
            kol_name: "X".into(),
            token_address: Address::ZERO,
            token_symbol: "RUG".into(),
            bnb_in_wei: U256::from(1_000_000_000_000_000_000u128),
            bnb_out_wei: U256::ZERO,
            tokens_traded: U256::from(1u128),
            pnl_wei: -1_000_000_000_000_000_000i128,
            pnl_pct: -1.0,
            opened_at_block: 1,
            opened_at_unix_ns: 1,
            closed_at_block: 2,
            closed_at_unix_ns: 86400,
            held_secs: 86400,
            buy_tx_count: 1,
            close_reason: CloseReason::NoLiquidity,
            trigger_sell_tx: None,
        };
        let row = format_csv_row(&t);
        assert!(row.contains("no_liquidity"));
    }
}
