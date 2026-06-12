//! Live-trader wallet — loads the private key from `.env`, derives the
//! address, signs transactions. The key never leaves this process; it's
//! held in memory as a `PrivateKeySigner` and used only to sign txs.
//!
//! Safety invariants:
//!   - Constructor verifies `WALLET_ADDRESS` in .env matches the address
//!     derived from `WALLET_PRIVATE_KEY`. Mismatch ⇒ refuse to start.
//!   - The signer struct does not expose the raw key bytes via Display /
//!     Debug; only the address is loggable.
//!   - All txs are signed offline (no RPC round-trip during signing).

use alloy::consensus::{SignableTransaction, TxEnvelope};
use alloy::network::TxSignerSync;
use alloy::primitives::{Address, B256};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{anyhow, bail, Context, Result};
use std::fmt;
use std::str::FromStr;

pub struct TraderWallet {
    signer: PrivateKeySigner,
    address: Address,
}

impl TraderWallet {
    /// Load from the standard env-var names. Returns an error if either is
    /// missing, the key is malformed, or the derived address doesn't match
    /// the declared one.
    pub fn from_env(env_key: &str, env_addr: &str) -> Result<Self> {
        let pk_hex = std::env::var(env_key)
            .with_context(|| format!("missing env: {env_key}"))?;
        let declared = std::env::var(env_addr)
            .with_context(|| format!("missing env: {env_addr}"))?;
        Self::from_parts(&pk_hex, &declared)
    }

    pub fn from_parts(pk_hex: &str, declared_addr: &str) -> Result<Self> {
        let pk = pk_hex.trim().trim_start_matches("0x");
        if pk.len() != 64 {
            bail!("WALLET_PRIVATE_KEY must be 32 bytes / 64 hex chars (got {})", pk.len());
        }
        let signer = PrivateKeySigner::from_str(pk)
            .map_err(|e| anyhow!("PrivateKeySigner::from_str failed: {e}"))?;
        let derived = signer.address();
        let declared = Address::from_str(declared_addr.trim())
            .with_context(|| format!("WALLET_ADDRESS not valid: {declared_addr:?}"))?;
        if derived != declared {
            bail!(
                "WALLET_ADDRESS mismatch — declared {declared:#x} but key derives {derived:#x}; \
                 refusing to start (likely wrong key/addr pair in .env)"
            );
        }
        Ok(Self { signer, address: derived })
    }

    pub const fn address(&self) -> Address {
        self.address
    }

    /// Sign a fully-populated TransactionRequest (must already have nonce,
    /// chain_id, gas, to, value, input set by the caller). Returns the
    /// fully-encoded `TxEnvelope` ready for `eth_sendRawTransaction`.
    pub fn sign(&self, req: TransactionRequest) -> Result<TxEnvelope> {
        // Build the typed transaction (EIP-1559 if max-fees are set, else
        // legacy). alloy chooses based on the populated fields.
        let mut typed = req
            .build_typed_tx()
            .map_err(|e| anyhow!("build_typed_tx: {e:?}"))?;
        let sig = self
            .signer
            .sign_transaction_sync(&mut typed)
            .map_err(|e| anyhow!("sign_transaction_sync: {e}"))?;
        Ok(typed.into_signed(sig).into())
    }
}

impl fmt::Debug for TraderWallet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never expose the key. Only the address.
        f.debug_struct("TraderWallet")
            .field("address", &format_args!("{:#x}", self.address))
            .finish()
    }
}

/// Convenience: tx hash (= keccak256(rlp(tx))) from a signed envelope.
pub fn tx_hash(env: &TxEnvelope) -> B256 {
    *env.tx_hash()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Anvil/Hardhat test account #0 — well-known throwaway key for tests.
    // (Public on every Ethereum tutorial; safe to embed.)
    const TEST_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cfFFb92266";

    #[test]
    fn loads_and_verifies_address() {
        let w = TraderWallet::from_parts(TEST_KEY, TEST_ADDR).unwrap();
        assert_eq!(format!("{:#x}", w.address()).to_lowercase(),
                   TEST_ADDR.to_lowercase());
    }

    #[test]
    fn rejects_mismatched_address() {
        let bad = "0x0000000000000000000000000000000000000001";
        let err = TraderWallet::from_parts(TEST_KEY, bad).unwrap_err();
        assert!(err.to_string().contains("mismatch"));
    }

    #[test]
    fn rejects_malformed_key() {
        assert!(TraderWallet::from_parts("abc", TEST_ADDR).is_err());
        assert!(TraderWallet::from_parts("not-hex", TEST_ADDR).is_err());
    }

    #[test]
    fn debug_never_leaks_key() {
        let w = TraderWallet::from_parts(TEST_KEY, TEST_ADDR).unwrap();
        let s = format!("{:?}", w);
        assert!(!s.contains(TEST_KEY), "Debug leaked the private key!");
        assert!(s.to_lowercase().contains(&TEST_ADDR.to_lowercase()[2..10]),
                "Debug should show the address");
    }
}
