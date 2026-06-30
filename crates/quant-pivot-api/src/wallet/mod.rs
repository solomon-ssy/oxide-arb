//! Wallet topology resolution.
//!
//! Maps [`ExecutionWalletKind`] to the Polymarket venue signature type and the
//! money-holding funder address, validating that the configured funder is
//! actually controlled by the signer EOA.
//!
//! Polymarket holds collateral/positions in one of three places:
//!
//! - **EOA** — the signer account itself (funder == signer), signature type 0.
//! - **Proxy** — an EIP-1167 minimal proxy deterministically derived from the
//!   signer (Magic / email users), signature type 1.
//! - **Gnosis Safe** — a 1-of-1 Safe deterministically derived from the signer
//!   (browser-wallet users), signature type 2.
//!
//! The same topology drives both order signing (CLOB `funder`/`signature_type`)
//! and on-chain settlement routing (direct EOA redeem vs. gasless relayer).

use alloy::primitives::Address;
use polymarket_client_sdk_v2::{
    clob::types::SignatureType, derive_proxy_wallet, derive_safe_wallet,
};
use quant_pivot_models::enums::quant::ExecutionWalletKind;
use std::str::FromStr;

/// Resolved, validated wallet identity for money-moving venue operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletTopology {
    /// Configured wallet shape.
    pub kind: ExecutionWalletKind,
    /// Signer EOA address (derived from the private key).
    pub signer: Address,
    /// Money-holding funder address (the EOA itself, or the proxy/Safe wallet).
    pub funder: Address,
    /// Venue signature type matching `kind`.
    pub signature_type: SignatureType,
}

/// Reasons a wallet topology cannot be resolved (fail-closed at startup).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WalletTopologyError {
    /// The configured funder address could not be parsed.
    #[error("invalid funder address '{0}'")]
    InvalidFunder(String),
    /// EOA topology requires the signer to equal the funder.
    #[error("eoa wallet requires signer {signer} to equal quant.account.funder {funder}")]
    EoaMismatch { signer: String, funder: String },
    /// Proxy/Safe CREATE2 derivation is not available on this chain.
    #[error("{kind} wallet derivation is not supported on chain {chain_id}")]
    DerivationUnsupported { kind: &'static str, chain_id: u64 },
    /// The configured funder is not the wallet controlled by the signer EOA.
    #[error(
        "{kind} funder {configured} is not the wallet controlled by signer {signer} \
         (derived {derived}); check quant.account.funder"
    )]
    DerivedMismatch {
        kind: &'static str,
        configured: String,
        derived: String,
        signer: String,
    },
}

impl WalletTopology {
    /// Resolve and validate a topology from config inputs.
    ///
    /// `funder` is the configured `quant.account.funder`. For EOA it must equal
    /// the signer; for Proxy/Safe it must equal the CREATE2-derived wallet for
    /// the signer on `chain_id`.
    pub fn resolve(
        kind: ExecutionWalletKind,
        signer: Address,
        funder: &str,
        chain_id: u64,
    ) -> Result<Self, WalletTopologyError> {
        let funder = Address::from_str(funder.trim())
            .map_err(|_| WalletTopologyError::InvalidFunder(funder.to_owned()))?;
        let signature_type = signature_type_for(kind);
        match kind {
            ExecutionWalletKind::Eoa => {
                if signer != funder {
                    return Err(WalletTopologyError::EoaMismatch {
                        signer: signer.to_checksum(None),
                        funder: funder.to_checksum(None),
                    });
                }
            }
            ExecutionWalletKind::Proxy => {
                Self::check_derived(
                    "proxy",
                    funder,
                    derive_proxy_wallet(signer, chain_id),
                    signer,
                    chain_id,
                )?;
            }
            ExecutionWalletKind::GnosisSafe => {
                Self::check_derived(
                    "gnosis_safe",
                    funder,
                    derive_safe_wallet(signer, chain_id),
                    signer,
                    chain_id,
                )?;
            }
        }
        Ok(Self {
            kind,
            signer,
            funder,
            signature_type,
        })
    }

    /// Construct an EOA topology directly (signer is also the funder). Used by
    /// tests and contexts where the venue is reached as a bare EOA.
    #[must_use]
    pub const fn eoa(signer: Address) -> Self {
        Self {
            kind: ExecutionWalletKind::Eoa,
            signer,
            funder: signer,
            signature_type: SignatureType::Eoa,
        }
    }

    /// Whether this topology signs and pays gas directly (no relayer).
    #[must_use]
    pub const fn is_eoa(&self) -> bool {
        matches!(self.kind, ExecutionWalletKind::Eoa)
    }

    fn check_derived(
        kind: &'static str,
        funder: Address,
        derived: Option<Address>,
        signer: Address,
        chain_id: u64,
    ) -> Result<(), WalletTopologyError> {
        let derived =
            derived.ok_or(WalletTopologyError::DerivationUnsupported { kind, chain_id })?;
        if derived != funder {
            return Err(WalletTopologyError::DerivedMismatch {
                kind,
                configured: funder.to_checksum(None),
                derived: derived.to_checksum(None),
                signer: signer.to_checksum(None),
            });
        }
        Ok(())
    }
}

/// Map a wallet kind to its Polymarket venue signature type.
#[must_use]
pub const fn signature_type_for(kind: ExecutionWalletKind) -> SignatureType {
    match kind {
        ExecutionWalletKind::Eoa => SignatureType::Eoa,
        ExecutionWalletKind::Proxy => SignatureType::Proxy,
        ExecutionWalletKind::GnosisSafe => SignatureType::GnosisSafe,
    }
}
