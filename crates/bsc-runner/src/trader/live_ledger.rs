//! Live-trader ledger — append-only CSV of every shadow-signed (and
//! broadcast, when enabled) tx + open-position state. Same column shape
//! as the paper trader's CSV where possible so the same reporting
//! scripts can read both.
//!
//! In SHADOW mode rows record txs that would have been broadcast.
//! In TINY/FULL mode rows reflect real on-chain submissions.

use alloy::primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CSV_HEADER: &str = "ts_unix_ns,phase,kol_name,visibility,token_address,\
token_symbol,bnb_in_wei,gas_gwei,nonce,tx_hash,wallet_bnb,broadcast,\
limit_skip_reason\n";

pub struct LiveLedger {
    path: PathBuf,
    write_lock: Mutex<()>,
    open_count: Mutex<u32>,
}

#[derive(Debug, Clone)]
pub struct LiveEntry {
    pub phase: &'static str,        // "shadow" | "tiny" | "full"
    pub kol_name: String,
    pub visibility: &'static str,   // "public" | "private"
    pub token_address: Address,
    pub token_symbol: String,
    pub bnb_in_wei: U256,
    pub gas_gwei: u64,
    pub nonce: u64,
    pub tx_hash: B256,
    pub wallet_bnb: f64,
    pub broadcast: bool,            // true if it actually hit the wire
    pub limit_skip_reason: Option<String>,  // populated only on skip rows
}

impl LiveLedger {
    pub fn new(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
        let path = dir.join("live_log.csv");
        // Header invariant: file MUST start with CSV_HEADER. If missing
        // (e.g. file was truncated or schema-bumped) we prepend it without
        // dropping existing rows.
        let needs_header = if path.exists() {
            std::fs::read_to_string(&path)
                .map(|s| !s.starts_with(CSV_HEADER))
                .unwrap_or(true)
        } else {
            true
        };
        if needs_header {
            let existing = if path.exists() {
                std::fs::read_to_string(&path).unwrap_or_default()
            } else {
                String::new()
            };
            let mut f = File::create(&path)
                .with_context(|| format!("create {}", path.display()))?;
            f.write_all(CSV_HEADER.as_bytes())?;
            if !existing.is_empty() && !existing.starts_with(CSV_HEADER) {
                f.write_all(existing.as_bytes())?;
            }
        }
        Ok(Self {
            path,
            write_lock: Mutex::new(()),
            open_count: Mutex::new(0),
        })
    }

    pub fn append(&self, e: &LiveEntry) -> Result<()> {
        let _g = self.write_lock.lock();
        let mut f = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .with_context(|| format!("open {}", self.path.display()))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let row = format!(
            "{ts},{phase},{kol},{vis},{addr:#x},{sym},{bnb_in},{gas},{nonce},\
             {hash:#x},{wallet:.6},{bcast},{skip}\n",
            ts = now,
            phase = e.phase,
            kol = csv_escape(&e.kol_name),
            vis = e.visibility,
            addr = e.token_address,
            sym = csv_escape(&e.token_symbol),
            bnb_in = e.bnb_in_wei,
            gas = e.gas_gwei,
            nonce = e.nonce,
            hash = e.tx_hash,
            wallet = e.wallet_bnb,
            bcast = e.broadcast,
            skip = e.limit_skip_reason.as_deref().unwrap_or(""),
        );
        f.write_all(row.as_bytes())?;
        f.flush()?;
        Ok(())
    }

    /// Track an open position locally. The CSV is append-only; this in-memory
    /// counter is enough for the `max_open_positions` limit gate.
    pub fn record_opened(&self) {
        *self.open_count.lock() += 1;
    }
    pub fn record_closed(&self) {
        let mut g = self.open_count.lock();
        *g = g.saturating_sub(1);
    }
    pub fn open_count(&self) -> u32 {
        *self.open_count.lock()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
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

    #[test]
    fn header_has_expected_columns() {
        assert!(CSV_HEADER.starts_with("ts_unix_ns,phase,kol_name,"));
        assert!(CSV_HEADER.contains("tx_hash,wallet_bnb,broadcast"));
    }

    #[test]
    fn opens_dir_and_writes_header() {
        let tmp = tempfile::tempdir().unwrap();
        let l = LiveLedger::new(tmp.path()).unwrap();
        let s = std::fs::read_to_string(l.path()).unwrap();
        assert!(s.starts_with("ts_unix_ns,"));
    }

    #[test]
    fn open_count_tracks() {
        let tmp = tempfile::tempdir().unwrap();
        let l = LiveLedger::new(tmp.path()).unwrap();
        assert_eq!(l.open_count(), 0);
        l.record_opened();
        l.record_opened();
        assert_eq!(l.open_count(), 2);
        l.record_closed();
        assert_eq!(l.open_count(), 1);
    }
}
