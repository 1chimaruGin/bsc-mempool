//! Prometheus metrics exporter, capture sink (zstd), replay reader, and
//! EL-driven block-coverage oracle.

pub mod block_oracle;
pub mod capture;
pub mod metric_names;
pub mod metrics_exporter;
pub mod replay;

pub use block_oracle::{BlockOracle, BlockOracleConfig};
pub use capture::{CaptureConfig, CaptureRecord, CaptureWriter};
pub use metrics_exporter::{ExporterError, init_metrics};
pub use replay::{ReplayConfig, ReplayReader, ReplayedRecord};
