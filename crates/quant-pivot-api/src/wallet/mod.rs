//! Wallet topology resolution.
//!
//! Maps [`ExecutionWalletKind`] to the Polymarket venue signature type and the
//! money-holding funder address, validating that the configured funder is
//! actually controlled by the signer EOA.
//!
//! Polymarket holds collateral/positions in one of four places:
//!
//! - **EOA** — the signer account itself (funder == signer), signature type 0.
//! - **Proxy** — an EIP-1167 minimal proxy deterministically derived from the
//!   signer (Magic / email users), signature type 1.
//! - **Gnosis Safe** — a 1-of-1 Safe deterministically derived from the signer
//!   (browser-wallet users), signature type 2.
//! - **Deposit Wallet** — the current deterministic `BeaconProxy` wallet,
//!   signature type 3 (`Poly1271`).
//!
//! The same topology drives both order signing (CLOB `funder`/`signature_type`)
//! and on-chain settlement routing (direct EOA redeem vs. gasless relayer).

mod ownership;

use std::str::FromStr;

use alloy::primitives::{Address, address, hex, keccak256};
pub use ownership::{WalletControlEvidence, WalletOwnershipClient, WalletOwnershipVerifier};
use polymarket_client_sdk_v2::{
    clob::types::SignatureType, derive_proxy_wallet, derive_safe_wallet,
};
use quant_pivot_models::enums::quant::ExecutionWalletKind;

const POLYGON_CHAIN_ID: u64 = 137;
const DEPOSIT_WALLET_FACTORY: Address = address!("00000000000Fb5C9ADea0298D729A0CB3823Cc07");
const DEPOSIT_WALLET_BEACON: Address = address!("7A18EDfe055488A3128f01F563e5B479D92ffc3a");
const DEPOSIT_WALLET_FACTORY_IMPLEMENTATION: Address =
    address!("528CC05Efac2b0d255E423272187Efd41248aBd7");
const DEPOSIT_WALLET_BEACON_IMPLEMENTATION: Address =
    address!("f7F27c29e60Fe6325beF8dA7F93250353D2e3294");
const UUPS_PROXY_CODE_HASH: &str =
    "0xaaa52c8cc8a0e3fd27ce756cc6b4e70c51423e9b597b11f32d3e49f8b1fc890d";
const DEPOSIT_WALLET_FACTORY_IMPLEMENTATION_CODE_HASH: &str =
    "0xe6424f1008e46b4b657efacf9500ea7747cbbf3055d9d76459253ac2884793d2";
const DEPOSIT_WALLET_BEACON_CODE_HASH: &str =
    "0xf87b06a1302051471df08ff79a938757509569e16b7a7efa55a3ea7b29b0b9d1";
const DEPOSIT_WALLET_BEACON_IMPLEMENTATION_CODE_HASH: &str =
    "0xf5c1072460e64902af84d35f5bb1d0a15d80a88c5827b831a977fbc5a0684b96";
const ERC1967_BEACON_PREFIX: u128 = 0x6100_523d_8160_233d_3973;
const ERC1967_BEACON_CONST1: [u8; 32] =
    hex!("b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3");
const ERC1967_BEACON_CONST2: [u8; 32] =
    hex!("1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c");
const ERC1967_BEACON_CONST3: [u8; 23] = hex!("60195155f3363d3d373d3d363d602036600436635c60da");
const ERC1967_BEACON_RUNTIME: [u8; 82] = hex!(
    "363d3d373d3d363d602036600436635c60da1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3"
);

/// Contract identity persisted with an immutable execution-account snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletContractIdentity {
    pub factory: Address,
    pub implementation_code_hash: &'static str,
}

/// Exact current Polygon code and binding catalog for a derived Deposit Wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DepositWalletVerificationCatalog {
    pub factory: Address,
    pub factory_implementation: Address,
    pub beacon: Address,
    pub wallet_implementation: Address,
    pub factory_code_hash: &'static str,
    pub factory_code_size: u64,
    pub factory_implementation_code_hash: &'static str,
    pub factory_implementation_code_size: u64,
    pub beacon_code_hash: &'static str,
    pub beacon_code_size: u64,
    pub wallet_implementation_code_hash: &'static str,
    pub wallet_implementation_code_size: u64,
}

/// Resolved, validated wallet identity for money-moving venue operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletTopology {
    /// Configured wallet shape.
    pub kind: ExecutionWalletKind,
    /// Signer EOA address (derived from the private key).
    pub signer: Address,
    /// Wallet owner at topology verification time. For a Deposit Wallet this
    /// can differ from `signer` when an approved session signer is used.
    pub owner: Address,
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
    /// A Deposit Wallet must equal one of the two official factory-derived
    /// generations; arbitrary owner-controlled contracts are not accepted.
    #[error("deposit_wallet funder {configured} is not derived for signer {signer} ")]
    DepositWalletDerivedMismatch { configured: String, signer: String },
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
            ExecutionWalletKind::DepositWallet => {
                verify_deposit_wallet_derivation(signer, funder, chain_id)?;
            }
        }
        Ok(Self {
            kind,
            signer,
            owner: signer,
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
            Ok(topology) if kind == ExecutionWalletKind::DepositWallet => {
                Self::verify_control(topology, verifier).await
            }
            Ok(topology) => Ok(topology),
            Err(
                WalletTopologyError::DerivedMismatch { .. }
                | WalletTopologyError::DepositWalletDerivedMismatch { .. },
            ) => {
                let funder = Address::from_str(funder.trim())
                    .map_err(|_| WalletTopologyError::InvalidFunder(funder.to_owned()))?;
                let evidence = verifier
                    .control_evidence(kind, funder, signer)
                    .await
                    .map_err(|error| WalletTopologyError::OwnershipCheckFailed {
                        kind: kind.as_str(),
                        detail: error.to_string(),
                    })?;
                if kind == ExecutionWalletKind::DepositWallet {
                    verify_deposit_wallet_derivation(evidence.owner, funder, chain_id)?;
                }
                let topology = Self::attested(kind, signer, evidence.owner, funder);
                if evidence.controller_authorized {
                    Ok(topology)
                } else {
                    Err(Self::not_controlled(topology))
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Construct a topology for a funder whose control by `signer` was verified
    /// out-of-band (on-chain ownership attestation), bypassing CREATE2 derivation.
    #[must_use]
    pub const fn attested(
        kind: ExecutionWalletKind,
        signer: Address,
        owner: Address,
        funder: Address,
    ) -> Self {
        Self {
            kind,
            signer,
            owner,
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
            owner: signer,
            funder: signer,
            signature_type: SignatureType::Eoa,
        }
    }

    /// Whether this topology signs and pays gas directly (no relayer).
    #[must_use]
    pub const fn is_eoa(&self) -> bool {
        matches!(self.kind, ExecutionWalletKind::Eoa)
    }

    /// Return the pinned smart-wallet identity used by immutable account
    /// lineage. Deposit Wallet identity is derived from the observed owner;
    /// session signers therefore cannot change the account address lineage.
    pub fn contract_identity(&self) -> Result<Option<WalletContractIdentity>, WalletTopologyError> {
        if self.kind != ExecutionWalletKind::DepositWallet {
            return Ok(None);
        }
        verify_deposit_wallet_derivation(self.owner, self.funder, POLYGON_CHAIN_ID)?;
        Ok(Some(WalletContractIdentity {
            factory: DEPOSIT_WALLET_FACTORY,
            implementation_code_hash: DEPOSIT_WALLET_BEACON_IMPLEMENTATION_CODE_HASH,
        }))
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

    async fn verify_control<V>(topology: Self, verifier: &V) -> Result<Self, WalletTopologyError>
    where
        V: WalletOwnershipVerifier + ?Sized,
    {
        let evidence = verifier
            .control_evidence(topology.kind, topology.funder, topology.signer)
            .await
            .map_err(|error| WalletTopologyError::OwnershipCheckFailed {
                kind: topology.kind.as_str(),
                detail: error.to_string(),
            })?;
        if evidence.controller_authorized {
            Ok(Self {
                owner: evidence.owner,
                ..topology
            })
        } else {
            Err(Self::not_controlled(topology))
        }
    }

    fn not_controlled(topology: Self) -> WalletTopologyError {
        WalletTopologyError::FunderNotControlled {
            kind: topology.kind.as_str(),
            funder: topology.funder.to_checksum(None),
            signer: topology.signer.to_checksum(None),
        }
    }
}

/// Verify that a configured Deposit Wallet is derived by the current official
/// Polygon factory. This pure identity check is not a money-moving capability;
/// callers must additionally verify deployed code, beacon, implementation, and
/// owner on-chain.
fn verify_deposit_wallet_derivation(
    owner: Address,
    funder: Address,
    chain_id: u64,
) -> Result<(), WalletTopologyError> {
    if chain_id != POLYGON_CHAIN_ID {
        return Err(WalletTopologyError::DerivationUnsupported {
            kind: ExecutionWalletKind::DepositWallet.as_str(),
            chain_id,
        });
    }
    if funder == derive_deposit_wallet(owner) {
        return Ok(());
    }
    Err(WalletTopologyError::DepositWalletDerivedMismatch {
        configured: funder.to_checksum(None),
        signer: owner.to_checksum(None),
    })
}

pub(crate) fn derive_deposit_wallet_address(owner: Address) -> Address {
    derive_deposit_wallet(owner)
}

pub(crate) fn deposit_wallet_runtime_code(owner: Address) -> Vec<u8> {
    let mut runtime = Vec::with_capacity(146);
    runtime.extend_from_slice(&ERC1967_BEACON_RUNTIME);
    runtime.extend_from_slice(&deposit_wallet_args(owner, DEPOSIT_WALLET_FACTORY));
    runtime
}

pub(crate) const fn deposit_wallet_verification_catalog() -> DepositWalletVerificationCatalog {
    DepositWalletVerificationCatalog {
        factory: DEPOSIT_WALLET_FACTORY,
        factory_implementation: DEPOSIT_WALLET_FACTORY_IMPLEMENTATION,
        beacon: DEPOSIT_WALLET_BEACON,
        wallet_implementation: DEPOSIT_WALLET_BEACON_IMPLEMENTATION,
        factory_code_hash: UUPS_PROXY_CODE_HASH,
        factory_code_size: 61,
        factory_implementation_code_hash: DEPOSIT_WALLET_FACTORY_IMPLEMENTATION_CODE_HASH,
        factory_implementation_code_size: 9_250,
        beacon_code_hash: DEPOSIT_WALLET_BEACON_CODE_HASH,
        beacon_code_size: 1_540,
        wallet_implementation_code_hash: DEPOSIT_WALLET_BEACON_IMPLEMENTATION_CODE_HASH,
        wallet_implementation_code_size: 20_858,
    }
}

fn derive_deposit_wallet(owner: Address) -> Address {
    let factory = DEPOSIT_WALLET_FACTORY;
    let args = deposit_wallet_args(owner, factory);
    let salt = keccak256(args);
    let mut init_code = Vec::with_capacity(142);
    let prefix = ERC1967_BEACON_PREFIX + ((args.len() as u128) << 56);
    init_code.extend_from_slice(&prefix.to_be_bytes()[6..]);
    init_code.extend_from_slice(DEPOSIT_WALLET_BEACON.as_slice());
    init_code.extend_from_slice(&ERC1967_BEACON_CONST3);
    init_code.extend_from_slice(&ERC1967_BEACON_CONST2);
    init_code.extend_from_slice(&ERC1967_BEACON_CONST1);
    init_code.extend_from_slice(&args);
    factory.create2(salt, keccak256(init_code))
}

fn deposit_wallet_args(owner: Address, factory: Address) -> [u8; 64] {
    let mut args = [0_u8; 64];
    args[12..32].copy_from_slice(factory.as_slice());
    args[44..64].copy_from_slice(owner.as_slice());
    args
}

/// Map a wallet kind to its Polymarket venue signature type.
#[must_use]
pub const fn signature_type_for(kind: ExecutionWalletKind) -> SignatureType {
    match kind {
        ExecutionWalletKind::Eoa => SignatureType::Eoa,
        ExecutionWalletKind::Proxy => SignatureType::Proxy,
        ExecutionWalletKind::GnosisSafe => SignatureType::GnosisSafe,
        ExecutionWalletKind::DepositWallet => SignatureType::Poly1271,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use quant_pivot_error::rpc::RpcError;

    use super::*;

    // Foundry/Anvil test key EOA — matches the SDK's CREATE2 derivation vectors.
    const SIGNER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
    // derive_safe_wallet(SIGNER, 137) per the pinned SDK.
    const DERIVED_SAFE: &str = "0xd93b25Cb943D14d0d34FBAf01fc93a0F8b5f6e47";
    // A real Polymarket proxy that no CREATE2 constant reproduces for SIGNER.
    const NON_DERIVABLE_FUNDER: &str = "0x07cBF6f926bFc91534bE58a8511cD5414E7CA310";
    const BEACON_DEPOSIT_OWNER: &str = "0x26dB8f3Ab13163eBb41Bab241c0EeDE026a8fc8A";
    const BEACON_DEPOSIT_WALLET: &str = "0x4875924FBcac87A14b429B0af06E1C595ED3757c";

    struct StubVerifier {
        controlled: bool,
        called: AtomicBool,
    }

    impl StubVerifier {
        fn new(controlled: bool) -> Self {
            Self {
                controlled,
                called: AtomicBool::new(false),
            }
        }
        fn was_called(&self) -> bool {
            self.called.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl WalletOwnershipVerifier for StubVerifier {
        async fn control_evidence(
            &self,
            _kind: ExecutionWalletKind,
            _funder: Address,
            signer: Address,
        ) -> Result<WalletControlEvidence, RpcError> {
            self.called.store(true, Ordering::SeqCst);
            Ok(WalletControlEvidence {
                owner: signer,
                controller_authorized: self.controlled,
            })
        }
    }

    fn signer() -> Address {
        Address::from_str(SIGNER).unwrap()
    }

    #[test]
    fn deposit_wallet_derivation_matches_current_beacon_chain_vector() {
        let owner = Address::from_str(BEACON_DEPOSIT_OWNER).expect("fixture owner");
        let wallet = Address::from_str(BEACON_DEPOSIT_WALLET).expect("fixture Deposit Wallet");
        assert_eq!(derive_deposit_wallet_address(owner), wallet);
        verify_deposit_wallet_derivation(owner, wallet, 137)
            .expect("current official factory derivation resolves");
    }

    #[tokio::test]
    async fn deposit_wallet_always_requires_on_chain_control_attestation() {
        let verifier = StubVerifier::new(true);
        let topology = WalletTopology::resolve_verified(
            ExecutionWalletKind::DepositWallet,
            Address::from_str(BEACON_DEPOSIT_OWNER).expect("fixture owner"),
            BEACON_DEPOSIT_WALLET,
            137,
            &verifier,
        )
        .await
        .expect("derived and attested Deposit Wallet");
        assert!(verifier.was_called());
        assert_eq!(topology.signature_type, SignatureType::Poly1271);
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
