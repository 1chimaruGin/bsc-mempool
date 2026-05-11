//! Prometheus exporter — installs the recorder and starts an HTTP server on
//! the configured address. Histogram bucket sets tuned for sub-second BSC
//! mempool latencies post-Fermi (450 ms blocks).

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use std::net::SocketAddr;
use thiserror::Error;

use crate::metric_names as N;

#[derive(Debug, Error)]
pub enum ExporterError {
    #[error("prometheus install failed: {0}")]
    Install(#[from] metrics_exporter_prometheus::BuildError),
}

/// Install the global metrics recorder AND start the Prometheus HTTP exporter
/// listening on `addr`. The HTTP server runs on a background tokio task
/// owned by the runtime.
pub fn init_metrics(addr: SocketAddr) -> Result<(), ExporterError> {
    // Latency-sensitive buckets (seconds). Tightened low-end vs ETH because
    // BSC block time is 0.45 s and we care about sub-100 ms first-seen latency.
    let latency_buckets: &[f64] = &[
        0.000_001, 0.000_01, 0.000_1, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
        0.2, 0.3, 0.45, 0.6, 1.0, 2.5, 5.0,
    ];
    // Lead-time tail (txs sitting in mempool for many blocks).
    let lead_buckets: &[f64] = &[
        0.001, 0.01, 0.05, 0.1, 0.2, 0.45, 0.9, 1.5, 3.0, 6.0, 12.0, 30.0, 60.0,
    ];

    let builder = PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets_for_metric(Matcher::Suffix("seconds".into()), latency_buckets)?
        .set_buckets_for_metric(Matcher::Full(N::BLOCK_LEAD_TIME.into()), lead_buckets)?
        .set_buckets_for_metric(
            Matcher::Full(N::BLOCK_LEAD_TIME_BY_PHASE.into()),
            lead_buckets,
        )?
        .set_buckets_for_metric(
            Matcher::Full(N::REORG_DEPTH.into()),
            &[1.0, 2.0, 4.0, 8.0, 16.0],
        )?;

    builder.install()?;
    Ok(())
}
