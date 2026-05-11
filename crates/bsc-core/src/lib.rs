//! Core types and traits shared by all `bsc-*` crates.
//!
//! Kept dependency-light so downstream modules (KOL filter, paper trader,
//! liquidator, Four.Meme sniper) can depend on it without pulling in
//! networking or telemetry. Direct port of the ETH stack's `mempool-core`
//! with chain-specific changes:
//!   - `SourceId` constants relabeled for BSC providers and bsc-geth IPC
//!   - `SlotContext` → `BlockContext` (BSC PoSA has no slot/epoch concept;
//!     block number + parent hash is the canonical handle)

pub mod decode;
pub mod types;

pub use decode::{DecodeError, decode_envelope, recover_signer};
pub use types::{
    BlockContext, FirstSeen, PendingTx, RawPayload, RawTx, SourceId, SourceSeen,
};
