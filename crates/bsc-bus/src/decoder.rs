//! Decode stage: convert `RawTx` (potentially RLP bytes) into a fully
//! decoded `DecodedTx` with signer recovered.
//!
//! ECDSA recovery dominates this stage at ~10–50µs per call; pinning the
//! decoder pool to dedicated cores keeps the per-tx tail bounded.

use alloy::consensus::TxEnvelope;
use alloy::primitives::{Address, B256, Bytes};
use bsc_core::{RawPayload, RawTx, SourceId, decode_envelope, recover_signer};
use thiserror::Error;

#[derive(Debug)]
pub struct DecodedTx {
    pub hash: B256,
    pub from: Address,
    pub tx: TxEnvelope,
    pub source: SourceId,
    pub recv_ns: u64,
    pub raw: Option<Bytes>,
}

#[derive(Debug, Error)]
pub enum DecodeStageError {
    #[error("rlp decode failed: {0}")]
    Rlp(#[from] bsc_core::DecodeError),
}

/// Synchronously turn a `RawTx` into `DecodedTx`. Called by the decoder pool
/// tasks; we intentionally do not async-yield mid-decode (the work is
/// CPU-bound).
pub fn decode_to_decoded(raw: RawTx) -> Result<DecodedTx, DecodeStageError> {
    let RawTx {
        source,
        recv_ns,
        hash,
        payload,
    } = raw;

    let (tx, raw_bytes) = match payload {
        RawPayload::Decoded { tx, raw } => (*tx, raw),
        RawPayload::RlpBytes { rlp } => (decode_envelope(&rlp)?, Some(rlp)),
    };

    let from = recover_signer(&tx)?;

    Ok(DecodedTx {
        hash,
        from,
        tx,
        source,
        recv_ns,
        raw: raw_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use bsc_core::{RawPayload, RawTx, SourceId};

    #[test]
    fn decode_garbage_rlp_errors_cleanly() {
        let raw = RawTx {
            source: SourceId::WSS_PROVIDER,
            recv_ns: 1,
            hash: B256::default(),
            payload: RawPayload::RlpBytes {
                rlp: alloy::primitives::Bytes::from(vec![0xffu8; 4]),
            },
        };
        let result = decode_to_decoded(raw);
        assert!(result.is_err());
    }
}
