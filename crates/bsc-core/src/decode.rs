use alloy::consensus::TxEnvelope;
use alloy::consensus::crypto::RecoveryError;
use alloy::consensus::transaction::SignerRecoverable;
use alloy::primitives::Address;
use alloy::rlp::Decodable;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("rlp decode failed: {0}")]
    Rlp(alloy::rlp::Error),
    #[error("signer recovery failed: {0}")]
    Recover(RecoveryError),
}

impl From<alloy::rlp::Error> for DecodeError {
    fn from(e: alloy::rlp::Error) -> Self {
        Self::Rlp(e)
    }
}

impl From<RecoveryError> for DecodeError {
    fn from(e: RecoveryError) -> Self {
        Self::Recover(e)
    }
}

/// Decode a network-format signed transaction (the bytes a peer/RPC delivers).
///
/// BSC supports legacy + EIP-155 + EIP-2718-typed envelopes via the same
/// `TxEnvelope` dispatch alloy uses for Ethereum mainnet — chain ID is
/// embedded in the signature for EIP-155 txs and the envelope decoder
/// handles the dispatch internally.
pub fn decode_envelope(mut rlp: &[u8]) -> Result<TxEnvelope, DecodeError> {
    Ok(TxEnvelope::decode(&mut rlp)?)
}

/// Recover the signer address. ECDSA recovery is ~10–50µs per call; cache
/// the result on `PendingTx::from` rather than calling this on the hot path.
pub fn recover_signer(tx: &TxEnvelope) -> Result<Address, DecodeError> {
    Ok(tx.recover_signer()?)
}
