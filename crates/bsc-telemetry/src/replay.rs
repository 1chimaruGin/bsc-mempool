//! Replay reader: inverse of `capture` — opens a `*.bincode.zst` file and
//! yields `ReplayedRecord`s in stored order. Used by `bsc-runner replay`
//! and as a harness for future modules to develop against without burning
//! provider quotas.

use bincode::config::Configuration;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::capture::CaptureRecord;

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub path: PathBuf,
    /// Replay speed multiplier. 1.0 = real time. 0.0 = as fast as possible.
    pub speed: f64,
}

/// What the replay reader yields. The runner can choose to wrap this back
/// into `PendingTx` (with `recover_signer` etc.) or just drive subscribers
/// with a no-op event for plumbing tests.
#[derive(Debug, Clone)]
pub struct ReplayedRecord {
    pub record: CaptureRecord,
    /// 0-based index in the file (handy for assertions in tests).
    pub index: u64,
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode (serde): {0}")]
    DecodeSerde(String),
    #[error("truncated file: missing length-prefix at offset {0}")]
    TruncatedHeader(u64),
    #[error("truncated file: short payload (wanted {wanted}, got {got}) at index {index}")]
    TruncatedPayload {
        wanted: u32,
        got: usize,
        index: u64,
    },
}

const BINCODE_CFG: Configuration = bincode::config::standard();

pub struct ReplayReader {
    inner: BufReader<zstd::stream::Decoder<'static, BufReader<File>>>,
    next_index: u64,
}

impl ReplayReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        let f = File::open(path)?;
        let inner = BufReader::new(zstd::stream::Decoder::new(f)?);
        Ok(Self {
            inner,
            next_index: 0,
        })
    }

    /// Read the next record. Returns `Ok(None)` at clean EOF.
    pub fn next_record(&mut self) -> Result<Option<ReplayedRecord>, ReplayError> {
        let mut len_bytes = [0u8; 4];
        match self.inner.read_exact(&mut len_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes);
        let mut buf = vec![0u8; usize::try_from(len).unwrap_or(0)];
        self.inner.read_exact(&mut buf).map_err(|_| {
            ReplayError::TruncatedPayload {
                wanted: len,
                got: 0,
                index: self.next_index,
            }
        })?;
        let (record, _): (CaptureRecord, _) = bincode::serde::decode_from_slice(&buf, BINCODE_CFG)
            .map_err(|e| ReplayError::DecodeSerde(e.to_string()))?;
        let out = ReplayedRecord {
            record,
            index: self.next_index,
        };
        self.next_index = self.next_index.saturating_add(1);
        Ok(Some(out))
    }

    /// Drain the reader into a Vec. Mostly useful in tests.
    pub fn read_all(mut self) -> Result<Vec<ReplayedRecord>, ReplayError> {
        let mut out = Vec::new();
        while let Some(r) = self.next_record()? {
            out.push(r);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::{CaptureConfig, CaptureRecord, CaptureWriter};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn record(i: u64) -> CaptureRecord {
        CaptureRecord {
            first_seen_ns: 1_000_000_000 + i,
            source_seen: vec![(1, 1_000_000_000 + i)],
            hash: [u8::try_from(i % 256).unwrap_or(0); 32],
            from: [u8::try_from(i % 256).unwrap_or(0); 20],
            rlp: Some(vec![0xab; 8]),
            block_number: i,
            parent_block_hash: [u8::try_from(i % 256).unwrap_or(0); 32],
            ms_into_block: u32::try_from(i % 450).unwrap_or(0),
        }
    }

    #[test]
    fn write_then_replay_roundtrips_record_count() {
        let dir = TempDir::with_prefix("bsc-replay-").unwrap();
        let cfg = CaptureConfig {
            dir: dir.path().to_path_buf(),
            rotate_secs: 3_600,
            retention_bytes: 1024 * 1024,
            max_age: std::time::Duration::from_secs(3_600),
            zstd_level: 1,
        };
        let writer: Arc<CaptureWriter> = CaptureWriter::open(cfg).unwrap();
        for i in 0..50 {
            writer.write_record(&record(i)).unwrap();
        }
        writer.finalize().unwrap();

        let mut files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s == "zst")
            })
            .collect();
        files.sort_by_key(std::fs::DirEntry::path);
        assert!(!files.is_empty());

        let mut total = 0;
        for f in files {
            let reader = ReplayReader::open(f.path()).unwrap();
            let records = reader.read_all().unwrap();
            total += records.len();
        }
        assert_eq!(total, 50);
    }

    #[test]
    fn replay_record_fields_match() {
        let dir = TempDir::with_prefix("bsc-replay-").unwrap();
        let cfg = CaptureConfig {
            dir: dir.path().to_path_buf(),
            rotate_secs: 3_600,
            retention_bytes: 1024 * 1024,
            max_age: std::time::Duration::from_secs(3_600),
            zstd_level: 1,
        };
        let writer = CaptureWriter::open(cfg).unwrap();
        let r = record(42);
        writer.write_record(&r).unwrap();
        writer.finalize().unwrap();

        let mut all_records = Vec::new();
        for f in std::fs::read_dir(dir.path()).unwrap().flatten() {
            if f.path().extension().and_then(|s| s.to_str()) == Some("zst") {
                if let Ok(reader) = ReplayReader::open(f.path()) {
                    if let Ok(rr) = reader.read_all() {
                        all_records.extend(rr);
                    }
                }
            }
        }
        let found = all_records.iter().find(|rr| rr.record.block_number == 42);
        assert!(found.is_some());
        assert_eq!(found.unwrap().record, r);
    }
}
