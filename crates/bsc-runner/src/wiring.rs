//! Compose the live runtime: sources → pipeline → (Day 2+ subscribers).
//!
//! Day-1 scope: pipeline + capture sink + EL block oracle (newHeads
//! subscription). Trader / KOL watcher / liquidator land in Day 2-3.

use crate::config::{self, Config};
use alloy::primitives::B256;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use anyhow::{Context, Result};
use bsc_bus::{CurrentBlockState, build_pipeline};
use bsc_core::{RawTx, SourceId};
use bsc_sources::{
    Source,
    ipc::{IpcConfig, IpcSource, auto_detect as ipc_auto_detect},
    wss::{WssConfig, WssSource},
};
use bsc_telemetry::{CaptureWriter, init_metrics};
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use url::Url;

pub async fn run(config_path: &Path) -> Result<()> {
    let cfg = config::load(config_path)?;
    let metrics_addr = cfg
        .metrics
        .listen_addr
        .parse()
        .with_context(|| format!("parse metrics.listen_addr `{}`", cfg.metrics.listen_addr))?;
    init_metrics(metrics_addr)?;
    tracing::info!(
        chain = cfg.chain.id,
        chain_name = %cfg.chain.name,
        metrics = %metrics_addr,
        "bsc-runner up"
    );

    let block_state = CurrentBlockState::new();
    let shutdown = CancellationToken::new();

    // Pipeline (decoder pool + dedupe + fanout + janitor).
    let pipeline_cfg = (&cfg.pipeline).into();
    let (pipeline, _handles) = build_pipeline(&pipeline_cfg, block_state.clone());

    // Head-state updater: WS subscription to newHeads on bsc-geth. Updates
    // the shared CurrentBlockState so each PendingTx is stamped with the
    // correct (block_number, parent_hash, ms_into_block). This is on the
    // mempool hot path — keep it cheap.
    {
        let ws_url = cfg.block_oracle.el_ws_url.clone();
        let block_state = block_state.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = run_head_state_updater(ws_url, block_state, shutdown).await {
                tracing::error!(error = %e, "head-state updater terminated");
            }
        });
    }

    // Block-coverage oracle: separate WS subscription that for every new
    // block computes how many of its txs we saw in our mempool prior to
    // inclusion. Pure observability — runs in parallel to the head-state
    // updater. Emits `mempool_block_coverage_ratio` + per-tx lead-time
    // histograms.
    {
        let oracle = bsc_telemetry::BlockOracle::new(bsc_telemetry::BlockOracleConfig {
            el_ws_url: Some(cfg.block_oracle.el_ws_url.clone()),
            aggregate_interval: Duration::from_secs(10),
        })
        .with_dedupe(pipeline.dedupe.clone());
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = oracle.run(shutdown).await {
                tracing::error!(error = %e, "block-coverage oracle terminated");
            }
        });
    }

    // Capture sink (optional).
    if cfg.capture.enabled {
        let cap_cfg = bsc_telemetry::CaptureConfig {
            dir: cfg.capture.dir.clone(),
            rotate_secs: cfg.capture.rotate_secs,
            retention_bytes: cfg.capture.retention_bytes,
            max_age: Duration::from_secs(cfg.capture.max_age_secs),
            zstd_level: cfg.capture.zstd_level,
        };
        let writer = CaptureWriter::open(cap_cfg.clone())?;
        let sub = pipeline.bus.subscribe("capture");
        let shutdown_writer = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = writer.run(sub, shutdown_writer).await {
                tracing::error!(error = %e, "capture writer terminated");
            }
        });
        let shutdown_jan = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = bsc_telemetry::capture::run_capture_janitor(cap_cfg, shutdown_jan).await {
                tracing::error!(error = %e, "capture janitor terminated");
            }
        });
        tracing::info!("capture sink enabled");
    }

    // KOL watcher (Day 2). Subscribes to the bus, looks up `from` against
    // kols.toml, fires Telegram alerts on hits. Trader sink stays None
    // until Day 3.
    if cfg.kol_watch.enabled {
        let sub = pipeline.bus.subscribe("kol_watch");
        let _ = crate::kol_watch::start(
            cfg.kol_watch.clone(),
            sub,
            crate::kol_watch::Sinks::default(),
            shutdown.clone(),
        );
    }

    // Spawn EL sources.
    spawn_sources(&cfg, &pipeline.raw_tx_in, &shutdown);

    tracing::info!("bsc-runner pipeline live; press Ctrl-C to stop");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");
    shutdown.cancel();
    pipeline.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}

pub async fn replay(path: &Path, _speed: f64) -> Result<()> {
    use bsc_telemetry::ReplayReader;
    let path = path.to_path_buf();
    let count = tokio::task::spawn_blocking(move || -> Result<usize> {
        let reader = ReplayReader::open(&path)?;
        let records = reader.read_all()?;
        Ok(records.len())
    })
    .await??;
    println!("replayed records: {count}");
    Ok(())
}

/// Subscribe to bsc-geth's `newHeads` over WS and feed `CurrentBlockState`.
/// Reconnects on disconnect with exponential backoff.
async fn run_head_state_updater(
    ws_url: String,
    block_state: Arc<CurrentBlockState>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut backoff = Duration::from_millis(500);
    let max = Duration::from_secs(10);
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let res = run_head_state_updater_once(&ws_url, block_state.clone(), shutdown.clone()).await;
        match res {
            Ok(()) => {
                tracing::info!("block oracle WS ended; reconnecting");
                backoff = Duration::from_millis(500);
            }
            Err(e) => {
                tracing::warn!(error = %e, "block oracle error; reconnecting");
            }
        }
        let delay = backoff;
        backoff = (backoff * 2).min(max);
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn run_head_state_updater_once(
    ws_url: &str,
    block_state: Arc<CurrentBlockState>,
    shutdown: CancellationToken,
) -> Result<()> {
    let ws = WsConnect::new(ws_url.to_string());
    let provider = ProviderBuilder::new().connect_ws(ws).await?;
    let mut sub = provider.subscribe_blocks().await?.into_stream();
    tracing::info!(url = ws_url, "block oracle WS subscribed (newHeads)");
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            next = sub.next() => {
                let Some(header) = next else { return Ok(()); };
                let unix_ns = bsc_bus::current_block::unix_now_ns();
                let block_number = header.number;
                let parent: B256 = header.parent_hash;
                let block_hash: B256 = header.hash;
                block_state.on_new_head(block_number, block_hash, parent, unix_ns);
                metrics::gauge!("mempool_block_oracle_head").set(block_number as f64);
            }
        }
    }
}

fn spawn_sources(cfg: &Config, raw_tx_in: &mpsc::Sender<RawTx>, shutdown: &CancellationToken) {
    for entry in &cfg.sources.wss {
        if let Err(e) = Url::parse(&entry.url) {
            tracing::warn!(name = %entry.name, error = %e, "skipping WSS source: bad URL");
            continue;
        }
        let mut wss_cfg = WssConfig::new(SourceId(entry.source_id), entry.url.clone());
        if let Some(c) = entry.backfill_concurrency {
            wss_cfg.backfill_concurrency = c;
        }
        let source = WssSource::new(wss_cfg);
        let out = raw_tx_in.clone();
        let shutdown = shutdown.clone();
        let name = entry.name.clone();
        tokio::spawn(async move {
            tracing::info!(source = %name, "WSS source starting");
            if let Err(e) = source.run(out, shutdown).await {
                tracing::error!(error = %e, source = %name, "WSS source terminated");
            }
        });
    }

    if let Some(ipc_cfg) = &cfg.sources.ipc {
        let path = ipc_cfg.path.clone().or_else(ipc_auto_detect);
        if let Some(path) = path {
            let source = IpcSource::new(IpcConfig::new(path.clone()));
            let out = raw_tx_in.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                tracing::info!(?path, "IPC source starting");
                if let Err(e) = source.run(out, shutdown).await {
                    tracing::error!(error = %e, "IPC source terminated");
                }
            });
        } else {
            tracing::info!("IPC enabled but no socket detected; skipping");
        }
    }
}
