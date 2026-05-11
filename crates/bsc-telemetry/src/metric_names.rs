//! Centralized metric names so spelling stays consistent across crates.
//! Labels passed at recording time, names declared here.
//!
//! Names retain the `mempool_` prefix so dashboards from the ETH stack are
//! mostly reusable. Slot/epoch concepts dropped; block-number concepts
//! substituted where relevant.

pub const SOURCE_FIRST_SEEN: &str = "mempool_source_first_seen_seconds";
pub const DEDUPE_LAG: &str = "mempool_dedupe_lag_seconds";
pub const BLOCK_COVERAGE_RATIO: &str = "mempool_block_coverage_ratio";
pub const BLOCK_LEAD_TIME: &str = "mempool_block_lead_time_seconds";
pub const BLOCK_LEAD_TIME_BY_PHASE: &str = "mempool_block_lead_time_by_block_phase_seconds";
pub const SOURCE_BACKPRESSURE_DROPS: &str = "mempool_source_backpressure_drops_total";
pub const SUBSCRIBER_LAG: &str = "mempool_subscriber_lag_total";
pub const PENDING_TX_RATE: &str = "mempool_pending_tx_rate";
pub const REORG_DEPTH: &str = "mempool_reorg_depth";
pub const BLOCK_ORACLE_TRIGGER_LAG: &str = "mempool_block_oracle_trigger_lag_seconds";
pub const TX_SEEN_AGAIN: &str = "mempool_tx_seen_again_total";
pub const DEDUPE_MAP_SIZE: &str = "mempool_dedupe_map_size";
pub const CAPTURE_BYTES_WRITTEN: &str = "mempool_capture_bytes_written_total";
pub const CAPTURE_RECORDS_WRITTEN: &str = "mempool_capture_records_written_total";
pub const CAPTURE_FILES_ROTATED: &str = "mempool_capture_files_rotated_total";

/// Block-phase bucket label for `BLOCK_LEAD_TIME_BY_PHASE`.
/// BSC post-Fermi blocks are 450 ms. Buckets split into quartiles +
/// "past-deadline" tail for txs seen after the next block landed.
pub const fn block_phase_label(ms_into_block: u32) -> &'static str {
    match ms_into_block {
        0..=112 => "0_113ms",     // first quartile
        113..=224 => "113_225ms", // second quartile
        225..=336 => "225_337ms", // third quartile
        337..=449 => "337_450ms", // fourth quartile (block-edge)
        _ => "past_450ms",        // tail: tx still pending when next block landed
    }
}
