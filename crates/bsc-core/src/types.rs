use alloy::consensus::TxEnvelope;
use alloy::primitives::{Address, B256, Bytes};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;

/// Identifier for a pending-transaction source on BSC.
///
/// `u8` so it fits in a byte for capture files and label cardinality stays
/// small. Constants are BSC-relevant; ETH-only sources (e.g. Alchemy ETH)
/// have been dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub u8);

impl SourceId {
    /// Local bsc-geth IPC subscription (preferred — zero RPC fees, lowest latency).
    pub const LOCAL_IPC: Self = Self(10);
    /// Generic WSS provider (Chainstack, QuickNode-BSC, Ankr-BSC, …).
    pub const WSS_PROVIDER: Self = Self(20);
    /// bloXroute / Eden style premium mempool gateways.
    pub const BLOXROUTE: Self = Self(30);
    pub const EDEN: Self = Self(31);
    /// Replay/synthetic source used by tests and benchmarks.
    pub const REPLAY: Self = Self(99);

    pub const fn label(self) -> &'static str {
        match self.0 {
            10 => "local_ipc",
            20 => "wss_provider",
            30 => "bloxroute",
            31 => "eden",
            99 => "replay",
            _ => "unknown",
        }
    }
}

/// Per-source first-sighting record. Inline storage for the common case of
/// ≤4 sources observing the same tx.
pub type SourceSeen = SmallVec<[(SourceId, u64); 4]>;

/// Block context attached to each `PendingTx` by the bus.
///
/// BSC's PoSA consensus is single-stage (no separate Beacon CL), so there
/// is no slot/epoch concept — just block number and parent hash. The
/// `ms_into_block` field measures wall-clock ms since the most recent
/// `newHeads` arrival, useful for measuring lead time over block boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockContext {
    pub block_number: u64,
    pub parent_block_hash: B256,
    pub ms_into_block: u32,
}

/// Pre-decode payload from a source. Sources produce `Decoded` whenever they
/// can (IPC, well-behaved WSS providers); only raw RLP bytes (e.g. devp2p)
/// use `RlpBytes` and the decoder stage runs `alloy::rlp::decode`.
///
/// `TxEnvelope` is boxed so the enum stays compact when most variants in
/// flight are `RlpBytes` (a single allocation per decoded tx is acceptable;
/// it's reused as `Arc<TxEnvelope>` downstream).
#[derive(Debug)]
pub enum RawPayload {
    Decoded {
        tx: Box<TxEnvelope>,
        raw: Option<Bytes>,
    },
    RlpBytes {
        rlp: Bytes,
    },
}

/// What a `Source` pushes into the source→decoder mpsc channel.
///
/// `hash` is hoisted out of `RawPayload` because the dedupe stage needs it
/// even before payload decode (early reject on repeats from the same
/// source).
#[derive(Debug)]
pub struct RawTx {
    pub source: SourceId,
    /// Monotonic ns when the source first saw the tx (`CLOCK_MONOTONIC`).
    pub recv_ns: u64,
    pub hash: B256,
    pub payload: RawPayload,
}

/// Canonical pending-transaction event. `Arc<PendingTx>` flows through the
/// fanout — refcount bumps, no clones.
///
/// `from` is recovered once at decode time and cached on the struct so
/// downstream subscribers don't repeat ECDSA work.
#[derive(Debug, Clone)]
pub struct PendingTx {
    pub hash: B256,
    pub from: Address,
    pub tx: Arc<TxEnvelope>,
    pub first_seen_ns: u64,
    pub source_seen: SourceSeen,
    pub raw: Option<Bytes>,
    pub block_context: Option<BlockContext>,
}

/// Dedupe table entry. Owned by the dedupe stage; not exposed downstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirstSeen {
    pub first_ns: u64,
    pub sources: SourceSeen,
    pub expires_at_ns: u64,
}
