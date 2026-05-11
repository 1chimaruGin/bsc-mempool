//! Capture sink: subscribe to the bus and write zstd-compressed records to
//! `/data/bsc-meme-mev/captures/`. Hourly rotation; janitor evicts oldest
//! files when total size exceeds `retention_bytes` or any file is older than
//! `max_age`.
//!
//! Wire format: length-prefixed bincode records inside a zstd frame. Replay
//! reads it back with `replay::ReplayReader`.

use bincode::config::Configuration;
use bsc_bus::Subscription;
use bsc_core::PendingTx;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::metric_names as N;

#[derive(Debug, Clone)]
pub struct CaptureConfig {
    pub dir: PathBuf,
    pub rotate_secs: u64,
    pub retention_bytes: u64,
    pub max_age: Duration,
    pub zstd_level: i32,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/data/bsc-meme-mev/captures"),
            rotate_secs: 3_600,
            // 100 GiB — BSC has higher tx rate than ETH; smaller default than
            // the 200 GiB ETH used so disk pressure doesn't blow up at 0.45 s
            // blocks. Bump in config if you want longer retention.
            retention_bytes: 100 * 1024 * 1024 * 1024,
            max_age: Duration::from_secs(48 * 3_600),
            zstd_level: 3,
        }
    }
}

/// Wire format. Compact, deterministic, decode-friendly without alloy.
/// `from` and hash are raw bytes so replay doesn't depend on alloy at all
/// (we lift them back to `Address`/`B256` at consume time).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureRecord {
    pub first_seen_ns: u64,
    /// `(SourceId.0, recv_ns)` pairs from `PendingTx::source_seen`.
    pub source_seen: Vec<(u8, u64)>,
    pub hash: [u8; 32],
    pub from: [u8; 20],
    pub rlp: Option<Vec<u8>>,
    /// BSC has no slots/epochs — block context fields only.
    pub block_number: u64,
    pub parent_block_hash: [u8; 32],
    pub ms_into_block: u32,
}

impl From<&PendingTx> for CaptureRecord {
    fn from(p: &PendingTx) -> Self {
        let (block_number, parent_block_hash, ms_into_block) = match &p.block_context {
            Some(c) => (c.block_number, c.parent_block_hash.0, c.ms_into_block),
            None => (0, [0u8; 32], 0),
        };
        Self {
            first_seen_ns: p.first_seen_ns,
            source_seen: p.source_seen.iter().map(|(s, t)| (s.0, *t)).collect(),
            hash: p.hash.0,
            from: p.from.0.0,
            rlp: p.raw.as_ref().map(|b| b.to_vec()),
            block_number,
            parent_block_hash,
            ms_into_block,
        }
    }
}

#[derive(Debug, Error)]
pub enum CaptureError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("encode (serde): {0}")]
    EncodeSerde(String),
}

const BINCODE_CFG: Configuration = bincode::config::standard();

pub struct CaptureWriter {
    cfg: CaptureConfig,
    state: parking_lot::Mutex<WriterState>,
}

struct WriterState {
    encoder: zstd::stream::Encoder<'static, std::fs::File>,
    bytes_written: u64,
    rotate_at_unix: u64,
}

impl CaptureWriter {
    /// Open a new capture writer. Creates the directory if missing.
    pub fn open(cfg: CaptureConfig) -> Result<Arc<Self>, CaptureError> {
        std::fs::create_dir_all(&cfg.dir)?;
        let state = open_segment(&cfg.dir, cfg.rotate_secs, cfg.zstd_level)?;
        Ok(Arc::new(Self {
            cfg,
            state: parking_lot::Mutex::new(state),
        }))
    }

    /// Subscribe to the bus and write each tx as a capture record. Returns
    /// when shutdown is signaled or the bus is closed.
    pub async fn run(
        self: Arc<Self>,
        mut sub: Subscription,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                msg = sub.recv() => {
                    match msg {
                        Ok(tx) => {
                            if let Err(e) = self.write_record(&CaptureRecord::from(&*tx)) {
                                tracing::error!(error = ?e, "capture write failed");
                            }
                        }
                        Err(bsc_bus::RecvError::Lagged { name, skipped }) => {
                            tracing::warn!(name, skipped, "capture subscription lagged");
                        }
                        Err(bsc_bus::RecvError::Closed) => break,
                    }
                }
            }
        }
        // Best-effort flush + close on shutdown.
        if let Err(e) = self.finalize() {
            tracing::error!(error = ?e, "capture finalize failed");
        }
        Ok(())
    }

    /// Synchronously serialize and append one record. Caller is the bus task,
    /// so back-pressure here directly slows the dedupe stage — keep cheap.
    pub fn write_record(&self, rec: &CaptureRecord) -> Result<(), CaptureError> {
        let payload = bincode::serde::encode_to_vec(rec, BINCODE_CFG)
            .map_err(|e| CaptureError::EncodeSerde(e.to_string()))?;
        let len_bytes = u32::try_from(payload.len()).unwrap_or(u32::MAX).to_le_bytes();

        let mut state = self.state.lock();
        if unix_now() >= state.rotate_at_unix {
            let new = open_segment(&self.cfg.dir, self.cfg.rotate_secs, self.cfg.zstd_level)?;
            let old = std::mem::replace(&mut *state, new);
            old.encoder.finish()?;
            metrics::counter!(N::CAPTURE_FILES_ROTATED).increment(1);
        }

        state.encoder.write_all(&len_bytes)?;
        state.encoder.write_all(&payload)?;
        let added = u64::try_from(len_bytes.len() + payload.len()).unwrap_or(0);
        state.bytes_written = state.bytes_written.saturating_add(added);

        metrics::counter!(N::CAPTURE_BYTES_WRITTEN).increment(added);
        metrics::counter!(N::CAPTURE_RECORDS_WRITTEN).increment(1);
        Ok(())
    }

    /// Flush + close the active segment. Idempotent enough for shutdown.
    pub fn finalize(&self) -> Result<(), CaptureError> {
        let mut state = self.state.lock();
        let placeholder = open_segment(&self.cfg.dir, self.cfg.rotate_secs, self.cfg.zstd_level)?;
        let old = std::mem::replace(&mut *state, placeholder);
        old.encoder.finish()?;
        Ok(())
    }
}

/// Background janitor: drop files older than `max_age` or evict oldest until
/// total size is under `retention_bytes`.
pub async fn run_capture_janitor(
    cfg: CaptureConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                if let Err(e) = sweep_directory(&cfg.dir, cfg.retention_bytes, cfg.max_age).await {
                    tracing::warn!(error = ?e, "capture janitor sweep failed");
                }
            }
        }
    }
    Ok(())
}

async fn sweep_directory(
    dir: &Path,
    retention_bytes: u64,
    max_age: Duration,
) -> Result<(), CaptureError> {
    let mut entries: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
    let mut rd = fs::read_dir(dir).await?;
    while let Some(ent) = rd.next_entry().await? {
        let p = ent.path();
        if p.extension().and_then(|s| s.to_str()) != Some("zst") {
            continue;
        }
        let md = ent.metadata().await?;
        entries.push((p, md.len(), md.modified()?));
    }
    let now = SystemTime::now();
    let mut total: u64 = 0;
    for (p, len, mtime) in &entries {
        if now.duration_since(*mtime).is_ok_and(|d| d > max_age) {
            let _ = fs::remove_file(p).await;
        } else {
            total = total.saturating_add(*len);
        }
    }
    if total > retention_bytes {
        let mut surviving: Vec<_> = entries
            .iter()
            .filter(|(p, _, _)| std::path::Path::new(p).exists())
            .cloned()
            .collect();
        surviving.sort_by_key(|(_, _, m)| *m); // oldest first
        for (p, len, _) in surviving {
            if total <= retention_bytes {
                break;
            }
            let _ = fs::remove_file(&p).await;
            total = total.saturating_sub(len);
        }
    }
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn open_segment(dir: &Path, rotate_secs: u64, level: i32) -> Result<WriterState, CaptureError> {
    let now = unix_now();
    let path = segment_path(dir, now);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    let encoder = zstd::stream::Encoder::new(file, level)?;
    Ok(WriterState {
        encoder,
        bytes_written: 0,
        rotate_at_unix: now + rotate_secs,
    })
}

fn segment_path(dir: &Path, unix_secs: u64) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    dir.join(format!("bsc-mempool-{unix_secs}-{n:06}.bincode.zst"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsc_core::SourceId;

    #[test]
    fn record_roundtrips_through_bincode() {
        let source_seen: Vec<(u8, u64)> = vec![
            (SourceId::LOCAL_IPC.0, 1_000),
            (SourceId::WSS_PROVIDER.0, 1_500),
        ];
        let r = CaptureRecord {
            first_seen_ns: 1_000,
            source_seen,
            hash: [7u8; 32],
            from: [9u8; 20],
            rlp: Some(vec![0xde, 0xad, 0xbe, 0xef]),
            block_number: 50_000_000,
            parent_block_hash: [3u8; 32],
            ms_into_block: 213,
        };
        let bytes = bincode::serde::encode_to_vec(&r, BINCODE_CFG).unwrap();
        let (back, _): (CaptureRecord, _) =
            bincode::serde::decode_from_slice(&bytes, BINCODE_CFG).unwrap();
        assert_eq!(back, r);
    }
}
