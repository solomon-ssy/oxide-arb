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

mod ownership;

pub use ownership::{WalletOwnershipClient, WalletOwnershipVerifier};

use std::str::FromStr;

use alloy::primitives::Address;
use polymarket_client_sdk_v2::{
    clob::types::SignatureType, derive_proxy_wallet, derive_safe_wallet,
};
use quant_pivot_models::enums::quant::ExecutionWalletKind;

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
    /// The on-chain ownership fallback could not complete (RPC/decode failure).
    #[error("on-chain ownership check for {kind} funder failed: {detail}")]
    OwnershipCheckFailed { kind: &'static str, detail: String },
    /// On-chain state proves the funder is not controlled by the signer.
    #[error(
        "{kind} funder {funder} is not controlled by signer {signer} \
         (on-chain ownership check returned false); check quant.account.funder"
    )]
    FunderNotControlled {
        kind: &'static str,
        funder: String,
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

    /// Resolve a topology, falling back to on-chain ownership when CREATE2
    /// derivation cannot reproduce the configured funder.
    ///
    /// Fast path: [`Self::resolve`] (pure, no network) accepts EOA identity and
    /// any Proxy / Gnosis Safe whose funder matches the SDK's CREATE2 derivation.
    /// When derivation *misses* (a Polymarket wallet from a factory generation the
    /// pinned SDK does not know), the deterministic check is not authoritative, so
    /// this consults the chain: if `verifier` proves the funder is controlled by
    /// `signer`, the configured funder is trusted; otherwise it fails closed.
    ///
    /// EOA mismatches never fall back — an EOA funder must *be* the signer.
    pub async fn resolve_verified<V>(
        kind: ExecutionWalletKind,
        signer: Address,
        funder: &str,
        chain_id: u64,
        verifier: &V,
    ) -> Result<Self, WalletTopologyError>
    where
        V: WalletOwnershipVerifier + ?Sized,
    {
        match Self::resolve(kind, signer, funder, chain_id) {
            Ok(topology) => Ok(topology),
            Err(WalletTopologyError::DerivedMismatch { .. }) => {
                let funder = Address::from_str(funder.trim())
                    .map_err(|_| WalletTopologyError::InvalidFunder(funder.to_owned()))?;
                let controlled = verifier
                    .is_controlled_by(kind, funder, signer)
                    .await
                    .map_err(|error| WalletTopologyError::OwnershipCheckFailed {
                        kind: kind.as_str(),
                        detail: error.to_string(),
                    })?;
                if controlled {
                    Ok(Self::attested(kind, signer, funder))
                } else {
                    Err(WalletTopologyError::FunderNotControlled {
                        kind: kind.as_str(),
                        funder: funder.to_checksum(None),
                        signer: signer.to_checksum(None),
                    })
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Construct a topology for a funder whose control by `signer` was verified
    /// out-of-band (on-chain ownership attestation), bypassing CREATE2 derivation.
    #[must_use]
    pub const fn attested(kind: ExecutionWalletKind, signer: Address, funder: Address) -> Self {
        Self {
            kind,
            signer,
            funder,
            signature_type: signature_type_for(kind),
        }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use async_trait::async_trait;
    use quant_pivot_error::rpc::RpcError;

    // Foundry/Anvil test key EOA — matches the SDK's CREATE2 derivation vectors.
    const SIGNER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
    // derive_safe_wallet(SIGNER, 137) per the pinned SDK.
    const DERIVED_SAFE: &str = "0xd93b25Cb943D14d0d34FBAf01fc93a0F8b5f6e47";
    // A real Polymarket proxy that no CREATE2 constant reproduces for SIGNER.
    const NON_DERIVABLE_FUNDER: &str = "0x07cBF6f926bFc91534bE58a8511cD5414E7CA310";

    struct StubVerifier {
        controlled: bool,
        called: std::sync::atomic::AtomicBool,
    }

    impl StubVerifier {
        fn new(controlled: bool) -> Self {
            Self {
                controlled,
                called: std::sync::atomic::AtomicBool::new(false),
            }
        }
        fn was_called(&self) -> bool {
            self.called.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl WalletOwnershipVerifier for StubVerifier {
        async fn is_controlled_by(
            &self,
            _kind: ExecutionWalletKind,
            _funder: Address,
            _signer: Address,
        ) -> Result<bool, RpcError> {
            self.called.store(true, Ordering::SeqCst);
            Ok(self.controlled)
        }
    }

    fn signer() -> Address {
        Address::from_str(SIGNER).unwrap()
    }

    #[tokio::test]
    async fn derivable_funder_skips_on_chain_check() {
        let verifier = StubVerifier::new(false);
        let topology = WalletTopology::resolve_verified(
            ExecutionWalletKind::GnosisSafe,
            signer(),
            DERIVED_SAFE,
            137,
            &verifier,
        )
        .await
        .expect("CREATE2 match resolves without RPC");
        assert_eq!(topology.signature_type, SignatureType::GnosisSafe);
        assert!(!verifier.was_called(), "fast path must not touch the chain");
    }

    #[tokio::test]
    async fn non_derivable_funder_accepted_when_owned() {
        let verifier = StubVerifier::new(true);
        let topology = WalletTopology::resolve_verified(
            ExecutionWalletKind::Proxy,
            signer(),
            NON_DERIVABLE_FUNDER,
            137,
            &verifier,
        )
        .await
        .expect("on-chain ownership proof accepts the configured funder");
        assert!(verifier.was_called());
        assert_eq!(topology.signature_type, SignatureType::Proxy);
        assert_eq!(
            topology.funder,
            Address::from_str(NON_DERIVABLE_FUNDER).unwrap()
        );
    }

    #[tokio::test]
    async fn non_derivable_funder_rejected_when_not_owned() {
        let verifier = StubVerifier::new(false);
        let error = WalletTopology::resolve_verified(
            ExecutionWalletKind::Proxy,
            signer(),
            NON_DERIVABLE_FUNDER,
            137,
            &verifier,
        )
        .await
        .expect_err("unowned funder must fail closed");
        assert!(matches!(
            error,
            WalletTopologyError::FunderNotControlled { .. }
        ));
    }

    #[tokio::test]
    async fn eoa_mismatch_never_falls_back() {
        let verifier = StubVerifier::new(true);
        let error = WalletTopology::resolve_verified(
            ExecutionWalletKind::Eoa,
            signer(),
            NON_DERIVABLE_FUNDER,
            137,
            &verifier,
        )
        .await
        .expect_err("eoa funder must equal signer");
        assert!(matches!(error, WalletTopologyError::EoaMismatch { .. }));
        assert!(!verifier.was_called(), "eoa mismatch is not attestable");
    }
}
