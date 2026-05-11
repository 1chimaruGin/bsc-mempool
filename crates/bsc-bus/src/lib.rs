//! Decode → dedupe → fanout pipeline for pending transactions on BSC.
//!
//! The runner composes these pieces:
//! 1. Sources push `RawTx` into `raw_tx_in` (mpsc, bounded).
//! 2. Decoder pool decodes RLP (when needed) and recovers signers.
//! 3. Dedupe stage owns the `DashMap<TxHash, FirstSeen>` and emits exactly
//!    one `Arc<PendingTx>` per unique tx, stamped with the current
//!    `BlockContext` (block_number + parent_hash + ms_into_block).
//! 4. Fanout broadcasts to all subscribers.
//!
//! Differs from the ETH stack's `mempool-bus` in one place only: there is no
//! Beacon CL on BSC, so `CurrentBlockState` is fed by EL `newHeads` events
//! rather than CL SSE.
//!
//! Backpressure invariant: drop oldest, never block ingestion.

pub mod current_block;
pub mod decoder;
pub mod dedupe;
pub mod fanout;
pub mod pipeline;
pub mod subscription;

pub use current_block::CurrentBlockState;
pub use decoder::{DecodedTx, decode_to_decoded};
pub use dedupe::{Dedupe, DedupeOutcome};
pub use fanout::Bus;
pub use pipeline::{Pipeline, PipelineConfig, PipelineHandles, build_pipeline};
pub use subscription::{MempoolModule, RecvError, Subscription};
