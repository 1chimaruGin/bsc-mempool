//! Prometheus metrics exporter, capture sink (zstd), replay reader.
//!
//! NOTE: the ETH stack's `block_oracle.rs` (CL-driven coverage/lead-time
//! comparator) is intentionally NOT ported here — BSC has no separate
//! Beacon CL, and a from-scratch EL-driven block oracle is a Day-2+ piece.
//! See `docs/port-plan.md`.

pub mod capture;
pub mod metric_names;
pub mod metrics_exporter;
pub mod replay;

pub use capture::{CaptureConfig, CaptureRecord, CaptureWriter};
pub use metrics_exporter::{ExporterError, init_metrics};
pub use replay::{ReplayConfig, ReplayReader, ReplayedRecord};
