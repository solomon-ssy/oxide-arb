//! Read-only verification of Polymarket V2 settlement deployments.
//!
//! A raw address can never cross the submission boundary. Only this verifier
//! can mint [`VerifiedSettlementDeployment`] after the authoritative Contracts
//! catalog and every on-chain binding, code, pause, token, wallet, credential,
//! and freshness check succeeds at one observed Polygon block. ERC-1155
//! approval belongs to the narrower redeem capability, so this deployment
//! capability can safely construct the first approval transaction.

use std::{str::FromStr, time::Duration};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::{Address, B256, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::client::RpcClient,
    sol,
    transports::http::Http,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{hashing::CanonicalDigestError, rpc::RpcError};
use quant_pivot_models::{
    config::OnchainConfig,
    domain::quant::settlement_readiness::{
        SettlementArtifactReproducibilityEvidence, SettlementContractBinding,
        SettlementCredentialKind, SettlementDeploymentEvidence, SettlementDeploymentSource,
        SettlementReadiness, SettlementReadinessReason, SettlementTokenKind,
    },
    enums::{quant::ExecutionWalletKind, settlement::SettlementRoute},
    hashing::CanonicalDigest,
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCodeHash, EvmUint256, SemanticTextError,
        SettlementEvidenceVersion,
    },
};
use reqwest::{Client, Url};

use crate::wallet::{
    DepositWalletVerificationCatalog, WalletTopology, deposit_wallet_runtime_code,
    deposit_wallet_verification_catalog, derive_deposit_wallet_address,
};

const POLYGON_CHAIN_ID: u64 = 137;
const TOKEN_DECIMALS: u8 = 6;
const MAX_CHAIN_OBSERVATION_AGE_SECONDS: i64 = 120;
const CONTRACTS_DOCUMENTATION_URL: &str = "https://docs.polymarket.com/resources/contracts";
const CLOB_V2_CHANGELOG_URL: &str = "https://docs.polymarket.com/changelog#jul-14-2026";
const CTF_EXCHANGE_V2_README_URL: &str =
    "https://github.com/Polymarket/ctf-exchange-v2/blob/main/README.md";
const CTF_EXCHANGE_V2_REVISION: &str = "ccc0596074f4dfd62c944fbca4de252893b82b4b";
const OFFICIAL_SOURCES_RETRIEVED_AT: &str = "2026-07-22T15:08:33Z";
const FINGERPRINT_OBSERVED_BLOCK: u64 = 90_685_098;
const DEPLOYMENT_EVIDENCE_VERSION: &str = "polymarket-v2-2026-07-22.1";
const POLYGONSCAN_SOURCE_RETRIEVED_AT: &str = "2026-07-22T20:15:00Z";
const SOLC_VERSION: &str = "0.8.34+commit.80d5c536.Emscripten.clang";
const SOLC_OPTIMIZER_RUNS: u32 = 1_000_000;
const SOLC_EVM_VERSION: &str = "prague";
const COLLATERAL_TOKEN_IMPLEMENTATION: &str = "0x6bbcef9f7ef3b6c592c99e0f206a0de94ad0925f";
const COLLATERAL_TOKEN_PROXY_CODE_HASH: &str =
    "0xaaa52c8cc8a0e3fd27ce756cc6b4e70c51423e9b597b11f32d3e49f8b1fc890d";
const COLLATERAL_TOKEN_IMPLEMENTATION_CODE_HASH: &str =
    "0x932c9369433b333d6d97d99b7731885751862aa3502122786d24174a9fd8e58e";
const COLLATERAL_TOKEN_PROXY_CODE_SIZE: u64 = 61;
const COLLATERAL_TOKEN_IMPLEMENTATION_CODE_SIZE: u64 = 6_953;
const USDC: &str = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359";
const COLLATERAL_VAULT: &str = "0xc417fd8e9661c0d2120b64a04bb3278c17e99db1";
const WRAPPER_ROLE: u64 = 2;
const ERC1967_IMPLEMENTATION_SLOT: U256 = U256::from_be_bytes([
    0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d, 0xb9, 0x8d,
    0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50, 0x5d, 0x38, 0x2b, 0xbc,
]);
const ERC1967_BEACON_SLOT: U256 = U256::from_be_bytes([
    0xa3, 0xf0, 0xad, 0x74, 0xe5, 0x42, 0x3a, 0xeb, 0xfd, 0x80, 0xd3, 0xef, 0x43, 0x46, 0x57, 0x83,
    0x35, 0xa9, 0xa7, 0x2a, 0xea, 0xee, 0x59, 0xff, 0x6c, 0xb3, 0x58, 0x2b, 0x35, 0x13, 0x3d, 0x50,
]);

sol! {
    #[sol(rpc)]
    interface CtfCollateralAdapterView {
        function owner() external view returns (address);
        function CONDITIONAL_TOKENS() external view returns (address);
        function COLLATERAL_TOKEN() external view returns (address);
        function USDCE() external view returns (address);
        function paused(address asset) external view returns (bool);
    }
}

sol! {
    #[sol(rpc)]
    interface DepositWalletView {
        function owner() external view returns (address);
        function factory() external view returns (address);
        function id() external view returns (bytes32);
        function sessionSignerAuthorizedUntil(address signer) external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface UpgradeableBeaconView {
        function implementation() external view returns (address);
    }
}

sol! {
    #[sol(rpc)]
    interface CollateralTokenView {
        function owner() external view returns (address);
        function USDC() external view returns (address);
        function USDCE() external view returns (address);
        function VAULT() external view returns (address);
        function hasAnyRole(address user, uint256 roles) external view returns (bool);
    }
}

sol! {
    #[sol(rpc)]
    interface NegRiskCtfCollateralAdapterView {
        function NEG_RISK_ADAPTER() external view returns (address);
        function WRAPPED_COLLATERAL() external view returns (address);
    }
}

sol! {
    #[sol(rpc)]
    interface Erc20MetadataView {
        function decimals() external view returns (uint8);
    }
}

sol! {
    #[sol(rpc)]
    interface ConditionalTokensApprovalView {
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
}

sol! {
    #[sol(rpc)]
    interface LegacyNegRiskAdapterView {
        function wcol() external view returns (address);
    }
}

/// One official deployment publication captured with provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementDeploymentProvenance {
    pub source: SettlementDeploymentSource,
    pub source_url: &'static str,
    pub revision: Option<&'static str>,
    pub retrieved_at: &'static str,
}

/// One address-bearing deployment publication captured with provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementDeploymentSourceRecord {
    pub provenance: SettlementDeploymentProvenance,
    pub standard_adapter: EvmAddress,
    pub neg_risk_adapter: EvmAddress,
}

/// Frozen source set and expected chain fingerprint used by the verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementDeploymentCatalog {
    pub evidence_version: SettlementEvidenceVersion,
    pub chain_id: u64,
    pub authority: SettlementDeploymentSourceRecord,
    pub corroboration: SettlementDeploymentProvenance,
    pub repository_reference: SettlementDeploymentProvenance,
    pub owner: EvmAddress,
    pub conditional_tokens: EvmAddress,
    pub collateral_token: EvmAddress,
    pub collateral_token_implementation: EvmAddress,
    pub collateral_token_proxy_code_hash: EvmCodeHash,
    pub collateral_token_implementation_code_hash: EvmCodeHash,
    pub collateral_token_proxy_code_size: u64,
    pub collateral_token_implementation_code_size: u64,
    pub usdc: EvmAddress,
    pub usdce: EvmAddress,
    pub collateral_vault: EvmAddress,
    pub legacy_neg_risk_adapter: EvmAddress,
    pub neg_risk_wrapped_collateral: EvmAddress,
    pub standard_adapter_code_hash: Option<EvmCodeHash>,
    pub neg_risk_adapter_code_hash: Option<EvmCodeHash>,
    pub standard_adapter_code_size: Option<u64>,
    pub neg_risk_adapter_code_size: Option<u64>,
    pub fingerprint_observed_block: u64,
    pub fingerprint_observed_block_hash: EvmBlockHash,
    pub standard_artifact_evidence: SettlementArtifactReproducibilityEvidence,
    pub neg_risk_artifact_evidence: SettlementArtifactReproducibilityEvidence,
}

/// Invalid built-in catalog constant. This is a software/configuration defect,
/// never a reason to weaken readiness.
#[derive(Debug, thiserror::Error)]
pub enum SettlementCatalogError {
    #[error("invalid settlement catalog value for {field}: {source}")]
    Semantic {
        field: &'static str,
        #[source]
        source: SemanticTextError,
    },
    #[error("invalid settlement catalog digest for {field}: {source}")]
    Digest {
        field: &'static str,
        #[source]
        source: CanonicalDigestError,
    },
}

impl SettlementDeploymentCatalog {
    /// Authoritative Contracts addresses, corroborating changelog, reproducible
    /// fixed-block fingerprints, and non-authoritative repository reference.
    pub fn official_current() -> Result<Self, SettlementCatalogError> {
        Ok(Self {
            evidence_version: catalog_evidence_version(
                "evidence_version",
                DEPLOYMENT_EVIDENCE_VERSION,
            )?,
            chain_id: POLYGON_CHAIN_ID,
            authority: SettlementDeploymentSourceRecord {
                provenance: SettlementDeploymentProvenance {
                    source: SettlementDeploymentSource::ContractsDocumentation,
                    source_url: CONTRACTS_DOCUMENTATION_URL,
                    revision: None,
                    retrieved_at: OFFICIAL_SOURCES_RETRIEVED_AT,
                },
                standard_adapter: catalog_address(
                    "documentation.standard_adapter",
                    "0xada100db00ca00073811820692005400218fce1f",
                )?,
                neg_risk_adapter: catalog_address(
                    "documentation.neg_risk_adapter",
                    "0xada2005600dec949baf300f4c6120000bdb6eaab",
                )?,
            },
            corroboration: SettlementDeploymentProvenance {
                source: SettlementDeploymentSource::Changelog,
                source_url: CLOB_V2_CHANGELOG_URL,
                revision: Some("2026-07-14"),
                retrieved_at: OFFICIAL_SOURCES_RETRIEVED_AT,
            },
            repository_reference: SettlementDeploymentProvenance {
                source: SettlementDeploymentSource::CtfExchangeV2Readme,
                source_url: CTF_EXCHANGE_V2_README_URL,
                revision: Some(CTF_EXCHANGE_V2_REVISION),
                retrieved_at: OFFICIAL_SOURCES_RETRIEVED_AT,
            },
            owner: catalog_address("owner", "0x47ebfac3353314c788b96cdcbf41daadfe03629c")?,
            conditional_tokens: catalog_address(
                "conditional_tokens",
                "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
            )?,
            collateral_token: catalog_address(
                "collateral_token",
                "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb",
            )?,
            collateral_token_implementation: catalog_address(
                "collateral_token_implementation",
                COLLATERAL_TOKEN_IMPLEMENTATION,
            )?,
            collateral_token_proxy_code_hash: catalog_code_hash(
                "collateral_token_proxy_code_hash",
                COLLATERAL_TOKEN_PROXY_CODE_HASH,
            )?,
            collateral_token_implementation_code_hash: catalog_code_hash(
                "collateral_token_implementation_code_hash",
                COLLATERAL_TOKEN_IMPLEMENTATION_CODE_HASH,
            )?,
            collateral_token_proxy_code_size: COLLATERAL_TOKEN_PROXY_CODE_SIZE,
            collateral_token_implementation_code_size: COLLATERAL_TOKEN_IMPLEMENTATION_CODE_SIZE,
            usdc: catalog_address("usdc", USDC)?,
            usdce: catalog_address("usdce", "0x2791bca1f2de4661ed88a30c99a7a9449aa84174")?,
            collateral_vault: catalog_address("collateral_vault", COLLATERAL_VAULT)?,
            legacy_neg_risk_adapter: catalog_address(
                "legacy_neg_risk_adapter",
                "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296",
            )?,
            neg_risk_wrapped_collateral: catalog_address(
                "neg_risk_wrapped_collateral",
                "0x3a3bd7bb9528e159577f7c2e685cc81a765002e2",
            )?,
            standard_adapter_code_hash: Some(catalog_code_hash(
                "standard_adapter_code_hash",
                "0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f",
            )?),
            neg_risk_adapter_code_hash: Some(catalog_code_hash(
                "neg_risk_adapter_code_hash",
                "0x3b892c7c2f80e7af69f28faf72a51c2d793f6b79b96011bdf0a1996319fcbe5b",
            )?),
            standard_adapter_code_size: Some(11_096),
            neg_risk_adapter_code_size: Some(13_890),
            fingerprint_observed_block: FINGERPRINT_OBSERVED_BLOCK,
            fingerprint_observed_block_hash: catalog_block_hash(
                "fingerprint_observed_block_hash",
                "0xb8a2e2119b1fbcfb609e62fae53cabcdb016a288ad4de13b228193643c94ec3a",
            )?,
            standard_artifact_evidence: artifact_evidence(
                "https://polygonscan.com/address/0xAdA100Db00Ca00073811820692005400218FcE1f#code",
                "blake3:7fec858805cec9d142878ce81f6186b814490b80f4958174aeefc27ad15e93ac",
                "0x4bff12e199aa051c60183baf33bca7e414825ca32c95568b2877cb79fa6e0788",
                "0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f",
            )?,
            neg_risk_artifact_evidence: artifact_evidence(
                "https://polygonscan.com/address/0xadA2005600Dec949baf300f4C6120000bDB6eAab#code",
                "blake3:c3d72d094cbdebf4336349a9fb69fc6f319928ff21aa77bb2c729802fb73258e",
                "0x977f99a466cee2ef81ec6078186f561796242efcab34c917200f9e718f02460c",
                "0x3b892c7c2f80e7af69f28faf72a51c2d793f6b79b96011bdf0a1996319fcbe5b",
            )?,
        })
    }

    fn inspection_request(
        &self,
        route: SettlementRoute,
    ) -> (
        SettlementInspectionRequest,
        Vec<SettlementDeploymentEvidence>,
    ) {
        let authoritative_address = route_address(&self.authority, route);
        let advisories = vec![SettlementDeploymentEvidence::RepositoryDocumentationDrift {
            route,
            source: self.repository_reference.source,
            revision: self.repository_reference.revision.map(str::to_owned),
        }];
        (
            SettlementInspectionRequest {
                evidence_version: self.evidence_version.clone(),
                authority_source_url: self.authority.provenance.source_url,
                authority_retrieved_at: self.authority.provenance.retrieved_at,
                route,
                chain_id: self.chain_id,
                adapter: authoritative_address,
                expected_adapter_code_hash: match route {
                    SettlementRoute::StandardV2 => self.standard_adapter_code_hash.clone(),
                    SettlementRoute::NegRiskV2 => self.neg_risk_adapter_code_hash.clone(),
                },
                expected_adapter_code_size: match route {
                    SettlementRoute::StandardV2 => self.standard_adapter_code_size,
                    SettlementRoute::NegRiskV2 => self.neg_risk_adapter_code_size,
                },
                owner: self.owner.clone(),
                conditional_tokens: self.conditional_tokens.clone(),
                collateral_token: self.collateral_token.clone(),
                collateral_token_implementation: self.collateral_token_implementation.clone(),
                expected_collateral_token_proxy_code_hash: self
                    .collateral_token_proxy_code_hash
                    .clone(),
                expected_collateral_token_implementation_code_hash: self
                    .collateral_token_implementation_code_hash
                    .clone(),
                expected_collateral_token_proxy_code_size: self.collateral_token_proxy_code_size,
                expected_collateral_token_implementation_code_size: self
                    .collateral_token_implementation_code_size,
                usdc: self.usdc.clone(),
                usdce: self.usdce.clone(),
                collateral_vault: self.collateral_vault.clone(),
                legacy_neg_risk_adapter: (route == SettlementRoute::NegRiskV2)
                    .then(|| self.legacy_neg_risk_adapter.clone()),
                expected_wrapped_collateral: (route == SettlementRoute::NegRiskV2)
                    .then(|| self.neg_risk_wrapped_collateral.clone()),
                fingerprint_observed_block: self.fingerprint_observed_block,
                fingerprint_observed_block_hash: self.fingerprint_observed_block_hash.clone(),
            },
            advisories,
        )
    }

    /// Current authoritative target for a route. This is evidence metadata;
    /// callers still cannot construct a submission without verifier capability.
    #[must_use]
    pub fn current_target(&self, route: SettlementRoute) -> EvmAddress {
        route_address(&self.authority, route)
    }

    /// Pinned runtime code hash for the authoritative current target.
    #[must_use]
    pub fn current_code_hash(&self, route: SettlementRoute) -> Option<EvmCodeHash> {
        match route {
            SettlementRoute::StandardV2 => self.standard_adapter_code_hash.clone(),
            SettlementRoute::NegRiskV2 => self.neg_risk_adapter_code_hash.clone(),
        }
    }

    /// Authoritative publication provenance rendered by operator tooling.
    #[must_use]
    pub const fn authority_provenance(&self) -> &SettlementDeploymentProvenance {
        &self.authority.provenance
    }

    /// Independent `NegRisk` corroboration publication.
    #[must_use]
    pub const fn corroboration_provenance(&self) -> &SettlementDeploymentProvenance {
        &self.corroboration
    }

    /// Canonical evidence catalog version pinned by this catalog.
    #[must_use]
    pub const fn evidence_version(&self) -> &SettlementEvidenceVersion {
        &self.evidence_version
    }

    /// Reproducible `PolygonScan` Standard JSON/compiler evidence whose patched
    /// runtime is byte-for-byte equal to the authoritative deployment.
    #[must_use]
    pub const fn artifact_evidence(
        &self,
        route: SettlementRoute,
    ) -> &SettlementArtifactReproducibilityEvidence {
        match route {
            SettlementRoute::StandardV2 => &self.standard_artifact_evidence,
            SettlementRoute::NegRiskV2 => &self.neg_risk_artifact_evidence,
        }
    }

    /// Non-blocking documentation drift attached to the route.
    #[must_use]
    pub fn advisories(&self, route: SettlementRoute) -> Vec<SettlementDeploymentEvidence> {
        self.inspection_request(route).1
    }
}

fn catalog_address(field: &'static str, value: &str) -> Result<EvmAddress, SettlementCatalogError> {
    EvmAddress::parse(value).map_err(|source| SettlementCatalogError::Semantic { field, source })
}

fn catalog_code_hash(
    field: &'static str,
    value: &str,
) -> Result<EvmCodeHash, SettlementCatalogError> {
    EvmCodeHash::parse(value).map_err(|source| SettlementCatalogError::Semantic { field, source })
}

fn catalog_block_hash(
    field: &'static str,
    value: &str,
) -> Result<EvmBlockHash, SettlementCatalogError> {
    EvmBlockHash::parse(value).map_err(|source| SettlementCatalogError::Semantic { field, source })
}

fn catalog_evidence_version(
    field: &'static str,
    value: &str,
) -> Result<SettlementEvidenceVersion, SettlementCatalogError> {
    SettlementEvidenceVersion::parse(value)
        .map_err(|source| SettlementCatalogError::Semantic { field, source })
}

fn artifact_evidence(
    source_url: &str,
    source_bundle_hash: &str,
    runtime_template_hash: &str,
    deployed_runtime_hash: &str,
) -> Result<SettlementArtifactReproducibilityEvidence, SettlementCatalogError> {
    Ok(SettlementArtifactReproducibilityEvidence {
        source_url: source_url.to_owned(),
        retrieved_at: POLYGONSCAN_SOURCE_RETRIEVED_AT.to_owned(),
        source_bundle_hash: ContentHash::parse(source_bundle_hash).map_err(|source| {
            SettlementCatalogError::Digest {
                field: "artifact.source_bundle_hash",
                source,
            }
        })?,
        compiler_version: SOLC_VERSION.to_owned(),
        optimizer_runs: SOLC_OPTIMIZER_RUNS,
        evm_version: SOLC_EVM_VERSION.to_owned(),
        via_ir: false,
        runtime_template_hash: catalog_code_hash(
            "artifact.runtime_template_hash",
            runtime_template_hash,
        )?,
        deployed_runtime_hash: catalog_code_hash(
            "artifact.deployed_runtime_hash",
            deployed_runtime_hash,
        )?,
        immutable_patch_exact_match: true,
    })
}

fn route_address(source: &SettlementDeploymentSourceRecord, route: SettlementRoute) -> EvmAddress {
    match route {
        SettlementRoute::StandardV2 => source.standard_adapter.clone(),
        SettlementRoute::NegRiskV2 => source.neg_risk_adapter.clone(),
    }
}

/// Immutable input to one read-only chain inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementInspectionRequest {
    pub evidence_version: SettlementEvidenceVersion,
    pub authority_source_url: &'static str,
    pub authority_retrieved_at: &'static str,
    pub route: SettlementRoute,
    pub chain_id: u64,
    pub adapter: EvmAddress,
    pub expected_adapter_code_hash: Option<EvmCodeHash>,
    pub expected_adapter_code_size: Option<u64>,
    pub owner: EvmAddress,
    pub conditional_tokens: EvmAddress,
    pub collateral_token: EvmAddress,
    pub collateral_token_implementation: EvmAddress,
    pub expected_collateral_token_proxy_code_hash: EvmCodeHash,
    pub expected_collateral_token_implementation_code_hash: EvmCodeHash,
    pub expected_collateral_token_proxy_code_size: u64,
    pub expected_collateral_token_implementation_code_size: u64,
    pub usdc: EvmAddress,
    pub usdce: EvmAddress,
    pub collateral_vault: EvmAddress,
    pub legacy_neg_risk_adapter: Option<EvmAddress>,
    pub expected_wrapped_collateral: Option<EvmAddress>,
    pub fingerprint_observed_block: u64,
    pub fingerprint_observed_block_hash: EvmBlockHash,
}

/// Adapter getters captured at the same readiness observation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementAdapterBindings {
    pub owner: EvmAddress,
    pub conditional_tokens: EvmAddress,
    pub collateral_token: EvmAddress,
    pub collateral_token_implementation: EvmAddress,
    pub collateral_token_owner: EvmAddress,
    pub usdc: EvmAddress,
    pub usdce: EvmAddress,
    pub collateral_token_usdce: EvmAddress,
    pub collateral_vault: EvmAddress,
    pub adapter_has_wrapper_role: bool,
    pub neg_risk_adapter: Option<EvmAddress>,
    pub wrapped_collateral: Option<EvmAddress>,
    pub legacy_wrapped_collateral: Option<EvmAddress>,
}

/// Deposit Wallet identity and code observed at the same canonical block as
/// adapter readiness. This is absent for EOA, Proxy, and Safe accounts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementDepositWalletSnapshot {
    pub owner: EvmAddress,
    pub session_signer_valid_until: EvmUint256,
    pub factory: EvmAddress,
    pub wallet_id: B256,
    pub beacon: EvmAddress,
    pub wallet_implementation: EvmAddress,
    pub factory_implementation: EvmAddress,
    pub wallet_code: SettlementCodeFingerprint,
    pub factory_code: SettlementCodeFingerprint,
    pub factory_implementation_code: SettlementCodeFingerprint,
    pub beacon_code: Option<SettlementCodeFingerprint>,
    pub wallet_implementation_code: SettlementCodeFingerprint,
}

/// Exact runtime fingerprint observed from `eth_getCode` at a canonical block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementCodeFingerprint {
    pub hash: EvmCodeHash,
    pub size: u64,
}

/// Complete read-only Polygon observation consumed by the verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementChainSnapshot {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub block_timestamp: DateTime<Utc>,
    pub adapter_code: Vec<u8>,
    pub conditional_tokens_code: Vec<u8>,
    pub collateral_token_code: Vec<u8>,
    pub collateral_token_implementation_code: Vec<u8>,
    pub usdc_code: Vec<u8>,
    pub usdce_code: Vec<u8>,
    pub collateral_vault_code: Vec<u8>,
    pub legacy_neg_risk_adapter_code: Option<Vec<u8>>,
    pub funder_code: Vec<u8>,
    pub deposit_wallet: Option<SettlementDepositWalletSnapshot>,
    pub bindings: SettlementAdapterBindings,
    pub adapter_paused: bool,
    pub operator_approved: bool,
    pub pusd_decimals: u8,
    pub usdce_decimals: u8,
}

/// Credential evidence supplied by bootstrap/governance, separate from wallet
/// topology and ERC-1155 operator approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementCredentialAvailability {
    DirectEoaSigner,
    Relayer,
    Missing,
}

/// Whether verification requires live signing credentials.
///
/// Money paths must use [`Self::Required`]. External observation only needs a
/// signer-free deployment fingerprint and therefore uses [`Self::ObservationOnly`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementCredentialGate {
    Required(SettlementCredentialAvailability),
    ObservationOnly,
}

/// Read-only chain boundary for deterministic verifier tests and Polygon RPC.
#[async_trait]
pub trait SettlementChainReader: Send + Sync {
    async fn inspect(
        &self,
        request: &SettlementInspectionRequest,
        topology: &WalletTopology,
    ) -> Result<SettlementChainSnapshot, SettlementChainReadError>;

    async fn canonical_block_hash(
        &self,
        block_number: u64,
    ) -> Result<Option<EvmBlockHash>, SettlementChainReadError>;

    async fn finalized_head(&self) -> Result<SettlementFinalizedHead, SettlementChainReadError>;
}

/// Current Polygon finalized checkpoint used to prove canary finality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementFinalizedHead {
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
}

/// Typed failures at the chain-read boundary. A wrong network is a readiness
/// fact, not an opaque transport failure.
#[derive(Debug, thiserror::Error)]
pub enum SettlementChainReadError {
    #[error("configured settlement RPC is on chain {actual}, expected {expected}")]
    WrongChain { expected: u64, actual: u64 },
    #[error("Polygon block {block_number} changed during settlement verification")]
    CanonicalBlockChanged {
        block_number: u64,
        observed_hash: EvmBlockHash,
        current_hash: Option<EvmBlockHash>,
    },
    #[error(transparent)]
    Rpc(#[from] RpcError),
}

/// Non-serializable current-deployment capability required by every
/// approval/revocation gateway and by redeem-capability minting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSettlementDeployment {
    route: SettlementRoute,
    target: EvmAddress,
    target_code_hash: EvmCodeHash,
    conditional_tokens: EvmAddress,
    collateral_token: EvmAddress,
    usdce: EvmAddress,
    funder: EvmAddress,
    wallet_kind: ExecutionWalletKind,
    operator_approved: bool,
    verified_block: u64,
    verified_block_hash: EvmBlockHash,
    deployment_digest: ContentHash,
    advisories: Vec<SettlementDeploymentEvidence>,
    authority: SettlementDeploymentProvenance,
    corroboration: Option<SettlementDeploymentProvenance>,
    evidence_version: SettlementEvidenceVersion,
}

impl VerifiedSettlementDeployment {
    #[must_use]
    pub const fn route(&self) -> SettlementRoute {
        self.route
    }

    #[must_use]
    pub const fn target(&self) -> &EvmAddress {
        &self.target
    }

    #[must_use]
    pub const fn target_code_hash(&self) -> &EvmCodeHash {
        &self.target_code_hash
    }

    #[must_use]
    pub const fn conditional_tokens(&self) -> &EvmAddress {
        &self.conditional_tokens
    }

    #[must_use]
    pub const fn collateral_token(&self) -> &EvmAddress {
        &self.collateral_token
    }

    #[must_use]
    pub const fn usdce(&self) -> &EvmAddress {
        &self.usdce
    }

    #[must_use]
    pub const fn funder(&self) -> &EvmAddress {
        &self.funder
    }

    #[must_use]
    pub const fn wallet_kind(&self) -> ExecutionWalletKind {
        self.wallet_kind
    }

    #[must_use]
    pub const fn operator_approved(&self) -> bool {
        self.operator_approved
    }

    #[must_use]
    pub const fn verified_block(&self) -> u64 {
        self.verified_block
    }

    #[must_use]
    pub const fn verified_block_hash(&self) -> &EvmBlockHash {
        &self.verified_block_hash
    }

    #[must_use]
    pub const fn deployment_digest(&self) -> ContentHash {
        self.deployment_digest
    }

    #[must_use]
    pub fn advisories(&self) -> &[SettlementDeploymentEvidence] {
        &self.advisories
    }

    #[must_use]
    pub const fn authority(&self) -> &SettlementDeploymentProvenance {
        &self.authority
    }

    #[must_use]
    pub const fn corroboration(&self) -> Option<&SettlementDeploymentProvenance> {
        self.corroboration.as_ref()
    }

    #[must_use]
    pub const fn evidence_version(&self) -> &SettlementEvidenceVersion {
        &self.evidence_version
    }
}

#[cfg(test)]
pub(crate) fn verified_deployment_fixture(route: SettlementRoute) -> VerifiedSettlementDeployment {
    let catalog = SettlementDeploymentCatalog::official_current()
        .expect("built-in settlement catalog must remain canonical");
    let target = route_address(&catalog.authority, route);
    verified_deployment_fixture_at(
        route,
        target,
        EvmAddress::parse("0x1111111111111111111111111111111111111111").expect("fixture funder"),
        catalog.fingerprint_observed_block,
        catalog.fingerprint_observed_block_hash,
    )
}

#[cfg(test)]
pub(crate) fn verified_deployment_fixture_at(
    route: SettlementRoute,
    target: EvmAddress,
    funder: EvmAddress,
    verified_block: u64,
    verified_block_hash: EvmBlockHash,
) -> VerifiedSettlementDeployment {
    let catalog = SettlementDeploymentCatalog::official_current()
        .expect("built-in settlement catalog must remain canonical");
    let target_code_hash = match route {
        SettlementRoute::StandardV2 => catalog.standard_adapter_code_hash,
        SettlementRoute::NegRiskV2 => catalog.neg_risk_adapter_code_hash,
    }
    .expect("official fixture code hash");
    VerifiedSettlementDeployment {
        route,
        target,
        target_code_hash,
        conditional_tokens: catalog.conditional_tokens,
        collateral_token: catalog.collateral_token,
        usdce: catalog.usdce,
        funder,
        wallet_kind: ExecutionWalletKind::Eoa,
        operator_approved: true,
        verified_block,
        verified_block_hash,
        deployment_digest: ContentHash::from_bytes([0x71; 32]),
        advisories: Vec::new(),
        authority: catalog.authority.provenance,
        corroboration: (route == SettlementRoute::NegRiskV2).then_some(catalog.corroboration),
        evidence_version: catalog.evidence_version,
    }
}

/// Sole authority that can turn deployment evidence into a submission
/// capability.
pub struct ContractDeploymentVerifier<R> {
    catalog: SettlementDeploymentCatalog,
    reader: R,
}

impl<R> ContractDeploymentVerifier<R>
where
    R: SettlementChainReader,
{
    #[must_use]
    pub const fn new(catalog: SettlementDeploymentCatalog, reader: R) -> Self {
        Self { catalog, reader }
    }

    /// Verify one `route × wallet` money capability. Signing credentials are
    /// required; every failure returns typed blocked readiness.
    pub async fn verify(
        &self,
        route: SettlementRoute,
        topology: &WalletTopology,
        credentials: SettlementCredentialAvailability,
        checked_at: DateTime<Utc>,
    ) -> Result<VerifiedSettlementDeployment, SettlementReadiness> {
        self.verify_with_credential_gate(
            route,
            topology,
            SettlementCredentialGate::Required(credentials),
            checked_at,
        )
        .await
    }

    /// Signer-free fingerprint verification for external redemption observation.
    ///
    /// Missing relayer/EOA credentials must not block `ManualOnly` external
    /// observation of redemptions that already happened on-chain.
    pub async fn verify_for_observation(
        &self,
        route: SettlementRoute,
        topology: &WalletTopology,
        checked_at: DateTime<Utc>,
    ) -> Result<VerifiedSettlementDeployment, SettlementReadiness> {
        self.verify_with_credential_gate(
            route,
            topology,
            SettlementCredentialGate::ObservationOnly,
            checked_at,
        )
        .await
    }

    async fn verify_with_credential_gate(
        &self,
        route: SettlementRoute,
        topology: &WalletTopology,
        credential_gate: SettlementCredentialGate,
        checked_at: DateTime<Utc>,
    ) -> Result<VerifiedSettlementDeployment, SettlementReadiness> {
        let (request, advisories) = self.catalog.inspection_request(route);
        let snapshot = match self.reader.inspect(&request, topology).await {
            Ok(snapshot) => snapshot,
            Err(SettlementChainReadError::WrongChain { expected, actual }) => {
                return Err(SettlementReadiness::blocked(
                    route,
                    topology.kind,
                    vec![SettlementReadinessReason::WrongChain { expected, actual }],
                    None,
                    None,
                    checked_at,
                ));
            }
            Err(SettlementChainReadError::CanonicalBlockChanged {
                block_number,
                observed_hash,
                current_hash,
            }) => {
                return Err(SettlementReadiness::blocked(
                    route,
                    topology.kind,
                    vec![SettlementReadinessReason::CanonicalBlockChanged {
                        block_number,
                        observed_hash,
                        current_hash,
                    }],
                    Some(block_number),
                    None,
                    checked_at,
                ));
            }
            Err(SettlementChainReadError::Rpc(error)) => {
                return Err(SettlementReadiness::blocked(
                    route,
                    topology.kind,
                    vec![SettlementReadinessReason::RpcUnavailable {
                        operation: "settlement_deployment_inspection".to_owned(),
                        detail: error.to_string(),
                    }],
                    None,
                    None,
                    checked_at,
                ));
            }
        };
        let funder = typed_address(topology.funder).map_err(|reason| {
            SettlementReadiness::blocked(
                route,
                topology.kind,
                vec![reason],
                Some(snapshot.block_number),
                Some(snapshot.block_hash.clone()),
                checked_at,
            )
        })?;
        let reasons = verify_snapshot(
            &request,
            &snapshot,
            topology,
            credential_gate,
            &funder,
            checked_at,
        );
        if !reasons.is_empty() {
            return Err(SettlementReadiness::blocked(
                route,
                topology.kind,
                reasons,
                Some(snapshot.block_number),
                Some(snapshot.block_hash),
                checked_at,
            ));
        }
        let deployment_digest = deployment_digest(&request, &snapshot);
        let target_code_hash = request.expected_adapter_code_hash.clone().ok_or_else(|| {
            SettlementReadiness::blocked(
                route,
                topology.kind,
                vec![SettlementReadinessReason::CatalogIncomplete {
                    route,
                    field: "expected_adapter_code_hash".to_owned(),
                }],
                Some(snapshot.block_number),
                Some(snapshot.block_hash.clone()),
                checked_at,
            )
        })?;
        Ok(VerifiedSettlementDeployment {
            route,
            target: request.adapter,
            target_code_hash,
            conditional_tokens: request.conditional_tokens,
            collateral_token: request.collateral_token,
            usdce: request.usdce,
            funder,
            wallet_kind: topology.kind,
            operator_approved: snapshot.operator_approved,
            verified_block: snapshot.block_number,
            verified_block_hash: snapshot.block_hash,
            deployment_digest,
            advisories,
            authority: self.catalog.authority.provenance.clone(),
            corroboration: (route == SettlementRoute::NegRiskV2)
                .then(|| self.catalog.corroboration.clone()),
            evidence_version: self.catalog.evidence_version.clone(),
        })
    }
}

fn verify_snapshot(
    request: &SettlementInspectionRequest,
    snapshot: &SettlementChainSnapshot,
    topology: &WalletTopology,
    credential_gate: SettlementCredentialGate,
    funder: &EvmAddress,
    checked_at: DateTime<Utc>,
) -> Vec<SettlementReadinessReason> {
    let mut reasons = verify_deployment_snapshot(request, snapshot, checked_at);
    check_wallet_topology(&mut reasons, topology, funder, &snapshot.funder_code);
    check_deposit_wallet(&mut reasons, request, snapshot, topology, funder);
    if let SettlementCredentialGate::Required(credentials) = credential_gate {
        check_credentials(&mut reasons, topology.kind, credentials);
    }
    reasons
}

fn verify_deployment_snapshot(
    request: &SettlementInspectionRequest,
    snapshot: &SettlementChainSnapshot,
    checked_at: DateTime<Utc>,
) -> Vec<SettlementReadinessReason> {
    let mut reasons = Vec::new();
    if snapshot.chain_id != request.chain_id {
        reasons.push(SettlementReadinessReason::WrongChain {
            expected: request.chain_id,
            actual: snapshot.chain_id,
        });
    }
    check_chain_freshness(&mut reasons, snapshot, checked_at);
    check_code(
        &mut reasons,
        "adapter",
        &request.adapter,
        &snapshot.adapter_code,
    );
    check_code(
        &mut reasons,
        "conditional_tokens",
        &request.conditional_tokens,
        &snapshot.conditional_tokens_code,
    );
    check_code(
        &mut reasons,
        "collateral_token",
        &request.collateral_token,
        &snapshot.collateral_token_code,
    );
    check_code(
        &mut reasons,
        "collateral_token_implementation",
        &request.collateral_token_implementation,
        &snapshot.collateral_token_implementation_code,
    );
    check_code(&mut reasons, "usdc", &request.usdc, &snapshot.usdc_code);
    check_code(&mut reasons, "usdce", &request.usdce, &snapshot.usdce_code);
    check_code(
        &mut reasons,
        "collateral_vault",
        &request.collateral_vault,
        &snapshot.collateral_vault_code,
    );
    if let Some(address) = request.legacy_neg_risk_adapter.as_ref() {
        check_code(
            &mut reasons,
            "legacy_neg_risk_adapter",
            address,
            snapshot
                .legacy_neg_risk_adapter_code
                .as_deref()
                .unwrap_or_default(),
        );
    }
    check_code_hash(&mut reasons, request, &snapshot.adapter_code);
    check_code_size(&mut reasons, request, &snapshot.adapter_code);
    check_exact_code(
        &mut reasons,
        "collateral_token",
        &request.collateral_token,
        &request.expected_collateral_token_proxy_code_hash,
        request.expected_collateral_token_proxy_code_size,
        &snapshot.collateral_token_code,
    );
    check_exact_code(
        &mut reasons,
        "collateral_token_implementation",
        &request.collateral_token_implementation,
        &request.expected_collateral_token_implementation_code_hash,
        request.expected_collateral_token_implementation_code_size,
        &snapshot.collateral_token_implementation_code,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::Owner,
        &request.owner,
        &snapshot.bindings.owner,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::ConditionalTokens,
        &request.conditional_tokens,
        &snapshot.bindings.conditional_tokens,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::CollateralToken,
        &request.collateral_token,
        &snapshot.bindings.collateral_token,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::CollateralTokenImplementation,
        &request.collateral_token_implementation,
        &snapshot.bindings.collateral_token_implementation,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::CollateralTokenOwner,
        &request.owner,
        &snapshot.bindings.collateral_token_owner,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::Usdc,
        &request.usdc,
        &snapshot.bindings.usdc,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::Usdce,
        &request.usdce,
        &snapshot.bindings.usdce,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::CollateralTokenUsdce,
        &request.usdce,
        &snapshot.bindings.collateral_token_usdce,
    );
    check_binding(
        &mut reasons,
        SettlementContractBinding::CollateralVault,
        &request.collateral_vault,
        &snapshot.bindings.collateral_vault,
    );
    if !snapshot.bindings.adapter_has_wrapper_role {
        reasons.push(SettlementReadinessReason::CollateralWrapperRoleMissing {
            collateral_token: request.collateral_token.clone(),
            adapter: request.adapter.clone(),
        });
    }
    check_neg_risk_bindings(&mut reasons, request, &snapshot.bindings);
    if snapshot.adapter_paused {
        reasons.push(SettlementReadinessReason::AdapterPaused {
            adapter: request.adapter.clone(),
            asset: request.usdce.clone(),
        });
    }
    check_decimals(
        &mut reasons,
        SettlementTokenKind::Pusd,
        snapshot.pusd_decimals,
    );
    check_decimals(
        &mut reasons,
        SettlementTokenKind::Usdce,
        snapshot.usdce_decimals,
    );
    reasons
}

fn check_deposit_wallet(
    reasons: &mut Vec<SettlementReadinessReason>,
    request: &SettlementInspectionRequest,
    snapshot: &SettlementChainSnapshot,
    topology: &WalletTopology,
    funder: &EvmAddress,
) {
    if topology.kind != ExecutionWalletKind::DepositWallet {
        return;
    }
    let Some(observed) = snapshot.deposit_wallet.as_ref() else {
        reasons.push(SettlementReadinessReason::WalletTopologyMismatch {
            wallet_kind: topology.kind,
            funder: funder.clone(),
            detail: "Deposit Wallet identity evidence is absent".to_owned(),
        });
        return;
    };
    let expected = deposit_wallet_verification_catalog();
    let Ok(owner) = Address::from_str(observed.owner.as_str()) else {
        reasons.push(SettlementReadinessReason::WalletTopologyMismatch {
            wallet_kind: topology.kind,
            funder: funder.clone(),
            detail: "Deposit Wallet owner is not a canonical address".to_owned(),
        });
        return;
    };
    check_deposit_wallet_identity(
        reasons, snapshot, topology, funder, observed, owner, &expected,
    );
    check_deposit_wallet_code(reasons, request, funder, observed, owner, &expected);
}

fn check_deposit_wallet_identity(
    reasons: &mut Vec<SettlementReadinessReason>,
    snapshot: &SettlementChainSnapshot,
    topology: &WalletTopology,
    funder: &EvmAddress,
    observed: &SettlementDepositWalletSnapshot,
    owner: Address,
    expected: &DepositWalletVerificationCatalog,
) {
    match code_fingerprint(&snapshot.funder_code) {
        Ok(funder_code) => check_observed_fingerprint(
            reasons,
            "deposit_wallet_snapshot",
            funder,
            &funder_code,
            &observed.wallet_code,
        ),
        Err(error) => reasons.push(SettlementReadinessReason::RpcUnavailable {
            operation: "deposit_wallet_funder_code_hash".to_owned(),
            detail: error.to_string(),
        }),
    }
    if derive_deposit_wallet_address(owner) != topology.funder {
        reasons.push(SettlementReadinessReason::WalletTopologyMismatch {
            wallet_kind: topology.kind,
            funder: funder.clone(),
            detail: "funder does not match the official Deposit Wallet factory derivation"
                .to_owned(),
        });
    }
    if owner != topology.signer {
        let authorized_until = U256::from_str(observed.session_signer_valid_until.as_str());
        let observed_at = u64::try_from(snapshot.block_timestamp.timestamp())
            .ok()
            .map(U256::from);
        if authorized_until
            .ok()
            .zip(observed_at)
            .is_none_or(|(until, at)| until < at)
        {
            reasons.push(SettlementReadinessReason::WalletTopologyMismatch {
                wallet_kind: topology.kind,
                funder: funder.clone(),
                detail: "signer is neither the owner nor an unexpired session signer".to_owned(),
            });
        }
    }
    let mut expected_wallet_id = [0_u8; 32];
    expected_wallet_id[12..].copy_from_slice(owner.as_slice());
    if observed.wallet_id != B256::from(expected_wallet_id) {
        reasons.push(SettlementReadinessReason::WalletTopologyMismatch {
            wallet_kind: topology.kind,
            funder: funder.clone(),
            detail: "Deposit Wallet id is not bytes32(owner)".to_owned(),
        });
    }
    check_address_binding(
        reasons,
        SettlementContractBinding::DepositWalletOwner,
        topology.owner,
        &observed.owner,
    );
    check_address_binding(
        reasons,
        SettlementContractBinding::DepositWalletFactory,
        expected.factory,
        &observed.factory,
    );
    check_address_binding(
        reasons,
        SettlementContractBinding::DepositWalletBeacon,
        expected.beacon,
        &observed.beacon,
    );
    check_address_binding(
        reasons,
        SettlementContractBinding::DepositWalletImplementation,
        expected.wallet_implementation,
        &observed.wallet_implementation,
    );
    check_address_binding(
        reasons,
        SettlementContractBinding::DepositWalletImplementation,
        expected.factory_implementation,
        &observed.factory_implementation,
    );
}

fn check_deposit_wallet_code(
    reasons: &mut Vec<SettlementReadinessReason>,
    request: &SettlementInspectionRequest,
    funder: &EvmAddress,
    observed: &SettlementDepositWalletSnapshot,
    owner: Address,
    expected: &DepositWalletVerificationCatalog,
) {
    match code_fingerprint(&deposit_wallet_runtime_code(owner)) {
        Ok(expected_wallet_code) => check_observed_fingerprint(
            reasons,
            "deposit_wallet",
            funder,
            &expected_wallet_code,
            &observed.wallet_code,
        ),
        Err(error) => reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: format!("deposit_wallet.runtime_code: {error}"),
        }),
    }
    let Ok(factory) = typed_address(expected.factory) else {
        reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: "deposit_wallet.factory".to_owned(),
        });
        return;
    };
    check_catalog_code(
        reasons,
        request,
        "deposit_wallet_factory",
        &factory,
        expected.factory_code_hash,
        expected.factory_code_size,
        &observed.factory_code,
    );
    let Ok(factory_implementation) = typed_address(expected.factory_implementation) else {
        reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: "deposit_wallet.factory_implementation".to_owned(),
        });
        return;
    };
    check_catalog_code(
        reasons,
        request,
        "deposit_wallet_factory_implementation",
        &factory_implementation,
        expected.factory_implementation_code_hash,
        expected.factory_implementation_code_size,
        &observed.factory_implementation_code,
    );
    let Ok(beacon) = typed_address(expected.beacon) else {
        reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: "deposit_wallet.beacon".to_owned(),
        });
        return;
    };
    let Some(beacon_code) = observed.beacon_code.as_ref() else {
        reasons.push(SettlementReadinessReason::CodeMissing {
            contract: "deposit_wallet_beacon".to_owned(),
            address: beacon,
        });
        return;
    };
    check_catalog_code(
        reasons,
        request,
        "deposit_wallet_beacon",
        &beacon,
        expected.beacon_code_hash,
        expected.beacon_code_size,
        beacon_code,
    );
    let Ok(wallet_implementation) = typed_address(expected.wallet_implementation) else {
        reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: "deposit_wallet.wallet_implementation".to_owned(),
        });
        return;
    };
    check_catalog_code(
        reasons,
        request,
        "deposit_wallet_implementation",
        &wallet_implementation,
        expected.wallet_implementation_code_hash,
        expected.wallet_implementation_code_size,
        &observed.wallet_implementation_code,
    );
}

fn check_address_binding(
    reasons: &mut Vec<SettlementReadinessReason>,
    binding: SettlementContractBinding,
    expected: Address,
    actual: &EvmAddress,
) {
    match typed_address(expected) {
        Ok(expected) => check_binding(reasons, binding, &expected, actual),
        Err(reason) => reasons.push(reason),
    }
}

fn check_catalog_code(
    reasons: &mut Vec<SettlementReadinessReason>,
    request: &SettlementInspectionRequest,
    contract: &str,
    address: &EvmAddress,
    expected_hash: &str,
    expected_size: u64,
    code: &SettlementCodeFingerprint,
) {
    match EvmCodeHash::parse(expected_hash) {
        Ok(expected_hash) => {
            if code.size == 0 {
                reasons.push(SettlementReadinessReason::CodeMissing {
                    contract: contract.to_owned(),
                    address: address.clone(),
                });
            }
            if code.size != expected_size {
                reasons.push(SettlementReadinessReason::CodeSizeMismatch {
                    contract: contract.to_owned(),
                    address: address.clone(),
                    expected: expected_size,
                    actual: code.size,
                });
            }
            if code.hash != expected_hash {
                reasons.push(SettlementReadinessReason::CodeHashMismatch {
                    contract: contract.to_owned(),
                    address: address.clone(),
                    expected: expected_hash,
                    actual: code.hash.clone(),
                });
            }
        }
        Err(_) => reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: format!("{contract}.code_hash"),
        }),
    }
}

fn check_observed_fingerprint(
    reasons: &mut Vec<SettlementReadinessReason>,
    contract: &str,
    address: &EvmAddress,
    expected: &SettlementCodeFingerprint,
    actual: &SettlementCodeFingerprint,
) {
    if actual.size == 0 {
        reasons.push(SettlementReadinessReason::CodeMissing {
            contract: contract.to_owned(),
            address: address.clone(),
        });
    }
    if actual.size != expected.size {
        reasons.push(SettlementReadinessReason::CodeSizeMismatch {
            contract: contract.to_owned(),
            address: address.clone(),
            expected: expected.size,
            actual: actual.size,
        });
    }
    if actual.hash != expected.hash {
        reasons.push(SettlementReadinessReason::CodeHashMismatch {
            contract: contract.to_owned(),
            address: address.clone(),
            expected: expected.hash.clone(),
            actual: actual.hash.clone(),
        });
    }
}

fn check_code(
    reasons: &mut Vec<SettlementReadinessReason>,
    contract: &str,
    address: &EvmAddress,
    code: &[u8],
) {
    if code.is_empty() {
        reasons.push(SettlementReadinessReason::CodeMissing {
            contract: contract.to_owned(),
            address: address.clone(),
        });
    }
}

fn check_code_hash(
    reasons: &mut Vec<SettlementReadinessReason>,
    request: &SettlementInspectionRequest,
    code: &[u8],
) {
    let Some(expected) = request.expected_adapter_code_hash.as_ref() else {
        reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: "expected_adapter_code_hash".to_owned(),
        });
        return;
    };
    let actual = EvmCodeHash::parse(format!("{:#x}", keccak256(code)));
    match actual {
        Ok(actual) if &actual != expected => {
            reasons.push(SettlementReadinessReason::CodeHashMismatch {
                contract: "adapter".to_owned(),
                address: request.adapter.clone(),
                expected: expected.clone(),
                actual,
            });
        }
        Ok(_) => {}
        Err(error) => reasons.push(SettlementReadinessReason::RpcUnavailable {
            operation: "adapter_code_hash".to_owned(),
            detail: error.to_string(),
        }),
    }
}

fn check_code_size(
    reasons: &mut Vec<SettlementReadinessReason>,
    request: &SettlementInspectionRequest,
    code: &[u8],
) {
    let Some(expected) = request.expected_adapter_code_size else {
        reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: "expected_adapter_code_size".to_owned(),
        });
        return;
    };
    let actual = u64::try_from(code.len()).unwrap_or(u64::MAX);
    if actual != expected {
        reasons.push(SettlementReadinessReason::CodeSizeMismatch {
            contract: "adapter".to_owned(),
            address: request.adapter.clone(),
            expected,
            actual,
        });
    }
}

fn check_exact_code(
    reasons: &mut Vec<SettlementReadinessReason>,
    contract: &str,
    address: &EvmAddress,
    expected_hash: &EvmCodeHash,
    expected_size: u64,
    code: &[u8],
) {
    let actual_size = u64::try_from(code.len()).unwrap_or(u64::MAX);
    if actual_size != expected_size {
        reasons.push(SettlementReadinessReason::CodeSizeMismatch {
            contract: contract.to_owned(),
            address: address.clone(),
            expected: expected_size,
            actual: actual_size,
        });
    }
    match EvmCodeHash::parse(format!("{:#x}", keccak256(code))) {
        Ok(actual) if &actual != expected_hash => {
            reasons.push(SettlementReadinessReason::CodeHashMismatch {
                contract: contract.to_owned(),
                address: address.clone(),
                expected: expected_hash.clone(),
                actual,
            });
        }
        Ok(_) => {}
        Err(error) => reasons.push(SettlementReadinessReason::RpcUnavailable {
            operation: format!("{contract}_code_hash"),
            detail: error.to_string(),
        }),
    }
}

fn check_chain_freshness(
    reasons: &mut Vec<SettlementReadinessReason>,
    snapshot: &SettlementChainSnapshot,
    checked_at: DateTime<Utc>,
) {
    let age_seconds = checked_at
        .signed_duration_since(snapshot.block_timestamp)
        .num_seconds();
    if !(0..=MAX_CHAIN_OBSERVATION_AGE_SECONDS).contains(&age_seconds) {
        reasons.push(SettlementReadinessReason::ChainObservationNotFresh {
            block_number: snapshot.block_number,
            block_timestamp: snapshot.block_timestamp,
            checked_at,
            max_age_seconds: MAX_CHAIN_OBSERVATION_AGE_SECONDS,
        });
    }
}

fn check_binding(
    reasons: &mut Vec<SettlementReadinessReason>,
    binding: SettlementContractBinding,
    expected: &EvmAddress,
    actual: &EvmAddress,
) {
    if expected != actual {
        reasons.push(SettlementReadinessReason::ContractBindingMismatch {
            binding,
            expected: expected.clone(),
            actual: actual.clone(),
        });
    }
}

fn check_neg_risk_bindings(
    reasons: &mut Vec<SettlementReadinessReason>,
    request: &SettlementInspectionRequest,
    bindings: &SettlementAdapterBindings,
) {
    let Some(expected_adapter) = request.legacy_neg_risk_adapter.as_ref() else {
        return;
    };
    match bindings.neg_risk_adapter.as_ref() {
        Some(actual) => check_binding(
            reasons,
            SettlementContractBinding::NegRiskAdapter,
            expected_adapter,
            actual,
        ),
        None => reasons.push(SettlementReadinessReason::ContractBindingUnavailable {
            binding: SettlementContractBinding::NegRiskAdapter,
        }),
    }
    let Some(expected_wrapped) = request.expected_wrapped_collateral.as_ref() else {
        reasons.push(SettlementReadinessReason::CatalogIncomplete {
            route: request.route,
            field: "expected_wrapped_collateral".to_owned(),
        });
        return;
    };
    match bindings.wrapped_collateral.as_ref() {
        Some(actual) => check_binding(
            reasons,
            SettlementContractBinding::WrappedCollateral,
            expected_wrapped,
            actual,
        ),
        None => reasons.push(SettlementReadinessReason::ContractBindingUnavailable {
            binding: SettlementContractBinding::WrappedCollateral,
        }),
    }
    match bindings.legacy_wrapped_collateral.as_ref() {
        Some(actual) => check_binding(
            reasons,
            SettlementContractBinding::LegacyWrappedCollateral,
            expected_wrapped,
            actual,
        ),
        None => reasons.push(SettlementReadinessReason::ContractBindingUnavailable {
            binding: SettlementContractBinding::LegacyWrappedCollateral,
        }),
    }
}

fn check_decimals(
    reasons: &mut Vec<SettlementReadinessReason>,
    token: SettlementTokenKind,
    actual: u8,
) {
    if actual != TOKEN_DECIMALS {
        reasons.push(SettlementReadinessReason::TokenDecimalsMismatch {
            token,
            expected: TOKEN_DECIMALS,
            actual,
        });
    }
}

fn check_wallet_topology(
    reasons: &mut Vec<SettlementReadinessReason>,
    topology: &WalletTopology,
    funder: &EvmAddress,
    funder_code: &[u8],
) {
    let invalid = match topology.kind {
        ExecutionWalletKind::Eoa => !funder_code.is_empty(),
        ExecutionWalletKind::Proxy
        | ExecutionWalletKind::GnosisSafe
        | ExecutionWalletKind::DepositWallet => funder_code.is_empty(),
    };
    if invalid {
        reasons.push(SettlementReadinessReason::WalletTopologyMismatch {
            wallet_kind: topology.kind,
            funder: funder.clone(),
            detail: "configured wallet kind disagrees with funder bytecode presence".to_owned(),
        });
    }
}

fn check_credentials(
    reasons: &mut Vec<SettlementReadinessReason>,
    wallet_kind: ExecutionWalletKind,
    credentials: SettlementCredentialAvailability,
) {
    let required = match wallet_kind {
        ExecutionWalletKind::Eoa => SettlementCredentialKind::DirectEoaSigner,
        ExecutionWalletKind::Proxy
        | ExecutionWalletKind::GnosisSafe
        | ExecutionWalletKind::DepositWallet => SettlementCredentialKind::Relayer,
    };
    let available = matches!(
        (required, credentials),
        (
            SettlementCredentialKind::DirectEoaSigner,
            SettlementCredentialAvailability::DirectEoaSigner
        ) | (
            SettlementCredentialKind::Relayer,
            SettlementCredentialAvailability::Relayer
        )
    );
    if !available {
        reasons.push(SettlementReadinessReason::CredentialMissing {
            wallet_kind,
            required,
        });
    }
}

fn deployment_digest(
    request: &SettlementInspectionRequest,
    snapshot: &SettlementChainSnapshot,
) -> ContentHash {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"quant-pivot.settlement-deployment.v3\0");
    bytes.extend_from_slice(&request.chain_id.to_be_bytes());
    bytes.extend_from_slice(request.evidence_version.as_str().as_bytes());
    bytes.extend_from_slice(request.authority_source_url.as_bytes());
    bytes.extend_from_slice(request.authority_retrieved_at.as_bytes());
    bytes.extend_from_slice(&request.fingerprint_observed_block.to_be_bytes());
    bytes.extend_from_slice(request.fingerprint_observed_block_hash.as_str().as_bytes());
    bytes.extend_from_slice(format!("{:?}", request.route).as_bytes());
    bytes.extend_from_slice(request.adapter.as_str().as_bytes());
    bytes.extend_from_slice(keccak256(&snapshot.adapter_code).as_slice());
    bytes.extend_from_slice(snapshot.bindings.owner.as_str().as_bytes());
    bytes.extend_from_slice(snapshot.bindings.conditional_tokens.as_str().as_bytes());
    bytes.extend_from_slice(snapshot.bindings.collateral_token.as_str().as_bytes());
    bytes.extend_from_slice(
        snapshot
            .bindings
            .collateral_token_implementation
            .as_str()
            .as_bytes(),
    );
    bytes.extend_from_slice(keccak256(&snapshot.collateral_token_code).as_slice());
    bytes.extend_from_slice(keccak256(&snapshot.collateral_token_implementation_code).as_slice());
    bytes.extend_from_slice(snapshot.bindings.collateral_token_owner.as_str().as_bytes());
    bytes.extend_from_slice(snapshot.bindings.usdc.as_str().as_bytes());
    bytes.extend_from_slice(snapshot.bindings.usdce.as_str().as_bytes());
    bytes.extend_from_slice(snapshot.bindings.collateral_token_usdce.as_str().as_bytes());
    bytes.extend_from_slice(snapshot.bindings.collateral_vault.as_str().as_bytes());
    bytes.push(u8::from(snapshot.bindings.adapter_has_wrapper_role));
    if let Some(address) = snapshot.bindings.neg_risk_adapter.as_ref() {
        bytes.extend_from_slice(address.as_str().as_bytes());
    }
    if let Some(address) = snapshot.bindings.wrapped_collateral.as_ref() {
        bytes.extend_from_slice(address.as_str().as_bytes());
    }
    if let Some(wallet) = snapshot.deposit_wallet.as_ref() {
        bytes.extend_from_slice(wallet.factory.as_str().as_bytes());
        bytes.extend_from_slice(wallet.beacon.as_str().as_bytes());
        bytes.extend_from_slice(wallet.wallet_implementation.as_str().as_bytes());
        bytes.extend_from_slice(wallet.factory_implementation.as_str().as_bytes());
        extend_fingerprint(&mut bytes, &wallet.factory_code);
        extend_fingerprint(&mut bytes, &wallet.factory_implementation_code);
        if let Some(code) = wallet.beacon_code.as_ref() {
            extend_fingerprint(&mut bytes, code);
        }
        extend_fingerprint(&mut bytes, &wallet.wallet_implementation_code);
    }
    CanonicalDigest::content_hash_bytes(&bytes)
}

fn extend_fingerprint(bytes: &mut Vec<u8>, fingerprint: &SettlementCodeFingerprint) {
    bytes.extend_from_slice(fingerprint.hash.as_str().as_bytes());
    bytes.extend_from_slice(&fingerprint.size.to_be_bytes());
}

fn typed_address(address: Address) -> Result<EvmAddress, SettlementReadinessReason> {
    EvmAddress::parse(format!("{address:#x}")).map_err(|error| {
        SettlementReadinessReason::RpcUnavailable {
            operation: "evm_address_decode".to_owned(),
            detail: error.to_string(),
        }
    })
}

fn typed_block_hash(block_hash: B256) -> Result<EvmBlockHash, RpcError> {
    EvmBlockHash::parse(format!("{block_hash:#x}")).map_err(|error| RpcError::AbiDecode {
        contract: "polygon_block_header".to_owned(),
        reason: error.to_string(),
    })
}

fn alloy_address(address: &EvmAddress) -> Result<Address, RpcError> {
    Address::from_str(address.as_str()).map_err(|error| RpcError::CallFailed {
        method: "settlement_address_parse".to_owned(),
        reason: error.to_string(),
    })
}

fn rpc_call_error(method: &'static str, error: &impl ToString) -> RpcError {
    RpcError::CallFailed {
        method: method.to_owned(),
        reason: error.to_string(),
    }
}

/// Read-only Alloy implementation used by bootstrap/readiness endpoints.
pub struct AlloySettlementChainReader {
    provider: DynProvider,
}

struct SettlementChainHead {
    chain_id: u64,
    block_number: u64,
    block_hash: EvmBlockHash,
    block_timestamp: DateTime<Utc>,
    block: BlockId,
}

impl AlloySettlementChainReader {
    pub fn connect(config: &OnchainConfig) -> Result<Self, RpcError> {
        let rpc_url = Url::parse(config.rpc_url()).map_err(|error| {
            RpcError::ConnectionFailed(format!(
                "configured Polygon RPC endpoint is invalid: {error}"
            ))
        })?;
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|error| {
                RpcError::ConnectionFailed(format!(
                    "failed to build settlement Polygon RPC client: {error}"
                ))
            })?;
        let transport = Http::with_client(http_client, rpc_url);
        let rpc_client = RpcClient::new(transport, false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(rpc_client).erased(),
        })
    }
}

#[async_trait]
impl SettlementChainReader for AlloySettlementChainReader {
    async fn inspect(
        &self,
        request: &SettlementInspectionRequest,
        topology: &WalletTopology,
    ) -> Result<SettlementChainSnapshot, SettlementChainReadError> {
        let head = self.read_chain_head(request.chain_id).await?;
        let adapter = alloy_address(&request.adapter)?;
        let conditional_tokens = alloy_address(&request.conditional_tokens)?;
        let collateral_token = alloy_address(&request.collateral_token)?;
        let usdce = alloy_address(&request.usdce)?;
        let adapter_view = CtfCollateralAdapterView::new(adapter, &self.provider);
        let bindings = self
            .read_adapter_bindings(request, adapter, head.block)
            .await?;
        let bindings = self
            .read_neg_risk_bindings(request, adapter, head.block, bindings)
            .await?;
        let legacy_neg_risk_adapter_code = match request.legacy_neg_risk_adapter.as_ref() {
            Some(address) => Some(
                self.read_code(
                    alloy_address(address)?,
                    head.block,
                    "eth_getCode(legacy_neg_risk_adapter)",
                )
                .await?,
            ),
            None => None,
        };
        let funder_code = self
            .read_code(topology.funder, head.block, "eth_getCode(funder)")
            .await?;
        let deposit_wallet = self
            .read_deposit_wallet(topology, head.block, funder_code.clone())
            .await?;
        let snapshot = SettlementChainSnapshot {
            chain_id: head.chain_id,
            block_number: head.block_number,
            block_hash: head.block_hash.clone(),
            block_timestamp: head.block_timestamp,
            adapter_code: self
                .read_code(adapter, head.block, "eth_getCode(adapter)")
                .await?,
            conditional_tokens_code: self
                .read_code(
                    conditional_tokens,
                    head.block,
                    "eth_getCode(conditional_tokens)",
                )
                .await?,
            collateral_token_code: self
                .read_code(
                    collateral_token,
                    head.block,
                    "eth_getCode(collateral_token)",
                )
                .await?,
            collateral_token_implementation_code: self
                .read_code(
                    alloy_address(&request.collateral_token_implementation)?,
                    head.block,
                    "eth_getCode(collateral_token_implementation)",
                )
                .await?,
            usdc_code: self
                .read_code(
                    alloy_address(&request.usdc)?,
                    head.block,
                    "eth_getCode(usdc)",
                )
                .await?,
            usdce_code: self
                .read_code(usdce, head.block, "eth_getCode(usdce)")
                .await?,
            collateral_vault_code: self
                .read_code(
                    alloy_address(&request.collateral_vault)?,
                    head.block,
                    "eth_getCode(collateral_vault)",
                )
                .await?,
            legacy_neg_risk_adapter_code,
            funder_code,
            deposit_wallet,
            bindings,
            adapter_paused: adapter_view
                .paused(usdce)
                .block(head.block)
                .call()
                .await
                .map_err(|error| rpc_call_error("adapter.paused", &error))?,
            operator_approved: ConditionalTokensApprovalView::new(
                conditional_tokens,
                &self.provider,
            )
            .isApprovedForAll(topology.funder, adapter)
            .block(head.block)
            .call()
            .await
            .map_err(|error| rpc_call_error("ctf.isApprovedForAll", &error))?,
            pusd_decimals: Erc20MetadataView::new(collateral_token, &self.provider)
                .decimals()
                .block(head.block)
                .call()
                .await
                .map_err(|error| rpc_call_error("pusd.decimals", &error))?,
            usdce_decimals: Erc20MetadataView::new(usdce, &self.provider)
                .decimals()
                .block(head.block)
                .call()
                .await
                .map_err(|error| rpc_call_error("usdce.decimals", &error))?,
        };
        self.recheck_canonical_head(head.block_number, head.block_hash)
            .await?;
        Ok(snapshot)
    }

    async fn canonical_block_hash(
        &self,
        block_number: u64,
    ) -> Result<Option<EvmBlockHash>, SettlementChainReadError> {
        self.provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .map_err(|error| rpc_call_error("eth_getBlockByNumber(settlement canonical)", &error))?
            .map(|block| typed_block_hash(block.hash()))
            .transpose()
            .map_err(Into::into)
    }

    async fn finalized_head(&self) -> Result<SettlementFinalizedHead, SettlementChainReadError> {
        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|error| rpc_call_error("eth_getBlockByNumber(finalized)", &error))?
            .ok_or_else(|| RpcError::CallFailed {
                method: "eth_getBlockByNumber(finalized)".to_owned(),
                reason: "Polygon RPC returned no finalized block".to_owned(),
            })?;
        Ok(SettlementFinalizedHead {
            block_number: block.header.number,
            block_hash: typed_block_hash(block.hash())?,
        })
    }
}

impl AlloySettlementChainReader {
    async fn read_chain_head(
        &self,
        expected_chain_id: u64,
    ) -> Result<SettlementChainHead, SettlementChainReadError> {
        let chain_id = self
            .provider
            .get_chain_id()
            .await
            .map_err(|error| rpc_call_error("eth_chainId", &error))?;
        if chain_id != expected_chain_id {
            return Err(SettlementChainReadError::WrongChain {
                expected: expected_chain_id,
                actual: chain_id,
            });
        }
        let block_number = self
            .provider
            .get_block_number()
            .await
            .map_err(|error| rpc_call_error("eth_blockNumber", &error))?;
        let observed_block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .map_err(|error| rpc_call_error("eth_getBlockByNumber", &error))?
            .ok_or_else(|| RpcError::AbiDecode {
                contract: "polygon_block_header".to_owned(),
                reason: "latest block is missing".to_owned(),
            })?;
        let observed_block_hash = observed_block.hash();
        let block_hash = typed_block_hash(observed_block_hash)?;
        let block_timestamp = i64::try_from(observed_block.header.timestamp)
            .ok()
            .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
            .ok_or_else(|| RpcError::AbiDecode {
                contract: "polygon_block_header".to_owned(),
                reason: "latest block timestamp is outside chrono range".to_owned(),
            })?;
        Ok(SettlementChainHead {
            chain_id,
            block_number,
            block_hash,
            block_timestamp,
            block: BlockId::hash_canonical(observed_block_hash),
        })
    }

    async fn read_adapter_bindings(
        &self,
        request: &SettlementInspectionRequest,
        adapter: Address,
        block: BlockId,
    ) -> Result<SettlementAdapterBindings, RpcError> {
        let view = CtfCollateralAdapterView::new(adapter, &self.provider);
        let collateral_token = alloy_address(&request.collateral_token)?;
        let collateral_view = CollateralTokenView::new(collateral_token, &self.provider);
        let implementation_word = self
            .provider
            .get_storage_at(collateral_token, ERC1967_IMPLEMENTATION_SLOT)
            .block_id(block)
            .await
            .map_err(|error| rpc_call_error("eth_getStorageAt(pusd.implementation)", &error))?;
        let implementation =
            Address::from_word(B256::from(implementation_word.to_be_bytes::<32>()));
        Ok(SettlementAdapterBindings {
            owner: typed_address(
                view.owner()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("adapter.owner", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            conditional_tokens: typed_address(
                view.CONDITIONAL_TOKENS()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("adapter.CONDITIONAL_TOKENS", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            collateral_token: typed_address(
                view.COLLATERAL_TOKEN()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("adapter.COLLATERAL_TOKEN", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            collateral_token_implementation: typed_address(implementation)
                .map_err(|reason| readiness_to_rpc(&reason))?,
            collateral_token_owner: typed_address(
                collateral_view
                    .owner()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("pusd.owner", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            usdc: typed_address(
                collateral_view
                    .USDC()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("pusd.USDC", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            usdce: typed_address(
                view.USDCE()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("adapter.USDCE", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            collateral_token_usdce: typed_address(
                collateral_view
                    .USDCE()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("pusd.USDCE", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            collateral_vault: typed_address(
                collateral_view
                    .VAULT()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("pusd.VAULT", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
            adapter_has_wrapper_role: collateral_view
                .hasAnyRole(adapter, U256::from(WRAPPER_ROLE))
                .block(block)
                .call()
                .await
                .map_err(|error| rpc_call_error("pusd.hasAnyRole(wrapper)", &error))?,
            neg_risk_adapter: None,
            wrapped_collateral: None,
            legacy_wrapped_collateral: None,
        })
    }

    async fn read_code(
        &self,
        address: Address,
        block: BlockId,
        method: &'static str,
    ) -> Result<Vec<u8>, RpcError> {
        self.provider
            .get_code_at(address)
            .block_id(block)
            .await
            .map_err(|error| rpc_call_error(method, &error))
            .map(|code| code.to_vec())
    }

    async fn read_deposit_wallet(
        &self,
        topology: &WalletTopology,
        block: BlockId,
        wallet_code: Vec<u8>,
    ) -> Result<Option<SettlementDepositWalletSnapshot>, RpcError> {
        if topology.kind != ExecutionWalletKind::DepositWallet {
            return Ok(None);
        }
        let view = DepositWalletView::new(topology.funder, &self.provider);
        let owner = view
            .owner()
            .block(block)
            .call()
            .await
            .map_err(|error| rpc_call_error("deposit_wallet.owner", &error))?;
        let factory = view
            .factory()
            .block(block)
            .call()
            .await
            .map_err(|error| rpc_call_error("deposit_wallet.factory", &error))?;
        let wallet_id = view
            .id()
            .block(block)
            .call()
            .await
            .map_err(|error| rpc_call_error("deposit_wallet.id", &error))?;
        let session_signer_valid_until = view
            .sessionSignerAuthorizedUntil(topology.signer)
            .block(block)
            .call()
            .await
            .map_err(|error| {
                rpc_call_error("deposit_wallet.sessionSignerAuthorizedUntil", &error)
            })?;
        let beacon = self
            .read_storage_address(topology.funder, ERC1967_BEACON_SLOT, block, "wallet.beacon")
            .await?;
        if beacon == Address::ZERO {
            return Err(RpcError::AbiDecode {
                contract: "deposit_wallet.beacon".to_owned(),
                reason: "current Deposit Wallet has an empty ERC-1967 beacon slot".to_owned(),
            });
        }
        let wallet_implementation = UpgradeableBeaconView::new(beacon, &self.provider)
            .implementation()
            .block(block)
            .call()
            .await
            .map_err(|error| rpc_call_error("deposit_wallet_beacon.implementation", &error))?;
        let beacon_code = self
            .read_code(beacon, block, "eth_getCode(deposit_wallet_beacon)")
            .await?;
        let expected = deposit_wallet_verification_catalog();
        let factory_implementation = self
            .read_storage_address(
                expected.factory,
                ERC1967_IMPLEMENTATION_SLOT,
                block,
                "deposit_wallet_factory.implementation",
            )
            .await?;
        Ok(Some(SettlementDepositWalletSnapshot {
            owner: typed_address(owner).map_err(|reason| readiness_to_rpc(&reason))?,
            session_signer_valid_until: EvmUint256::parse(session_signer_valid_until.to_string())
                .map_err(|error| RpcError::AbiDecode {
                contract: "deposit_wallet.session_signer_valid_until".to_owned(),
                reason: error.to_string(),
            })?,
            factory: typed_address(factory).map_err(|reason| readiness_to_rpc(&reason))?,
            wallet_id,
            beacon: typed_address(beacon).map_err(|reason| readiness_to_rpc(&reason))?,
            wallet_implementation: typed_address(wallet_implementation)
                .map_err(|reason| readiness_to_rpc(&reason))?,
            factory_implementation: typed_address(factory_implementation)
                .map_err(|reason| readiness_to_rpc(&reason))?,
            wallet_code: code_fingerprint(&wallet_code)?,
            factory_code: code_fingerprint(
                &self
                    .read_code(
                        expected.factory,
                        block,
                        "eth_getCode(deposit_wallet_factory)",
                    )
                    .await?,
            )?,
            factory_implementation_code: code_fingerprint(
                &self
                    .read_code(
                        factory_implementation,
                        block,
                        "eth_getCode(deposit_wallet_factory_implementation)",
                    )
                    .await?,
            )?,
            beacon_code: Some(code_fingerprint(&beacon_code)?),
            wallet_implementation_code: code_fingerprint(
                &self
                    .read_code(
                        wallet_implementation,
                        block,
                        "eth_getCode(deposit_wallet_implementation)",
                    )
                    .await?,
            )?,
        }))
    }

    async fn read_storage_address(
        &self,
        contract: Address,
        slot: U256,
        block: BlockId,
        label: &'static str,
    ) -> Result<Address, RpcError> {
        self.provider
            .get_storage_at(contract, slot)
            .block_id(block)
            .await
            .map(|word| Address::from_word(B256::from(word.to_be_bytes::<32>())))
            .map_err(|error| rpc_call_error(label, &error))
    }

    async fn recheck_canonical_head(
        &self,
        block_number: u64,
        observed_hash: EvmBlockHash,
    ) -> Result<(), SettlementChainReadError> {
        let current_hash = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .map_err(|error| rpc_call_error("eth_getBlockByNumber(canonical_recheck)", &error))?
            .map(|current| typed_block_hash(current.hash()))
            .transpose()?;
        if current_hash.as_ref() != Some(&observed_hash) {
            return Err(SettlementChainReadError::CanonicalBlockChanged {
                block_number,
                observed_hash,
                current_hash,
            });
        }
        Ok(())
    }

    async fn read_neg_risk_bindings(
        &self,
        request: &SettlementInspectionRequest,
        adapter: Address,
        block: BlockId,
        mut bindings: SettlementAdapterBindings,
    ) -> Result<SettlementAdapterBindings, RpcError> {
        let Some(legacy_address) = request.legacy_neg_risk_adapter.as_ref() else {
            return Ok(bindings);
        };
        let view = NegRiskCtfCollateralAdapterView::new(adapter, &self.provider);
        let legacy = alloy_address(legacy_address)?;
        bindings.neg_risk_adapter = Some(
            typed_address(
                view.NEG_RISK_ADAPTER()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("adapter.NEG_RISK_ADAPTER", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
        );
        bindings.wrapped_collateral = Some(
            typed_address(
                view.WRAPPED_COLLATERAL()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("adapter.WRAPPED_COLLATERAL", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
        );
        bindings.legacy_wrapped_collateral = Some(
            typed_address(
                LegacyNegRiskAdapterView::new(legacy, &self.provider)
                    .wcol()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| rpc_call_error("legacy_neg_risk.wcol", &error))?,
            )
            .map_err(|reason| readiness_to_rpc(&reason))?,
        );
        Ok(bindings)
    }
}

fn readiness_to_rpc(reason: &SettlementReadinessReason) -> RpcError {
    RpcError::CallFailed {
        method: "settlement_typed_decode".to_owned(),
        reason: format!("{reason:?}"),
    }
}

fn code_fingerprint(code: &[u8]) -> Result<SettlementCodeFingerprint, RpcError> {
    Ok(SettlementCodeFingerprint {
        hash: EvmCodeHash::parse(format!("{:#x}", keccak256(code))).map_err(|error| {
            RpcError::AbiDecode {
                contract: "runtime_code_hash".to_owned(),
                reason: error.to_string(),
            }
        })?,
        size: u64::try_from(code.len()).map_err(|error| RpcError::AbiDecode {
            contract: "runtime_code_size".to_owned(),
            reason: error.to_string(),
        })?,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use alloy::primitives::{Address, B256};
    use async_trait::async_trait;
    use chrono::{DateTime, TimeDelta, TimeZone, Utc};
    use polymarket_client_sdk_v2::clob::types::SignatureType;
    use quant_pivot_error::rpc::RpcError;
    use quant_pivot_models::{
        domain::quant::settlement_readiness::{
            SettlementContractBinding, SettlementCredentialKind, SettlementDeploymentEvidence,
            SettlementDeploymentSource, SettlementReadiness, SettlementReadinessReason,
            SettlementTokenKind,
        },
        enums::{quant::ExecutionWalletKind, settlement::SettlementRoute},
        types::{EvmAddress, EvmBlockHash, EvmCodeHash, EvmUint256},
    };

    use super::{
        ContractDeploymentVerifier, POLYGON_CHAIN_ID, SettlementAdapterBindings,
        SettlementChainReadError, SettlementChainReader, SettlementChainSnapshot,
        SettlementCodeFingerprint, SettlementCredentialAvailability, SettlementDeploymentCatalog,
        SettlementDepositWalletSnapshot, SettlementFinalizedHead, SettlementInspectionRequest,
        VerifiedSettlementDeployment,
    };
    use crate::wallet::{WalletTopology, deposit_wallet_runtime_code};

    const ADAPTER_CODE: &[u8] = &[0x60, 0x01, 0x60, 0x02];
    const COLLATERAL_PROXY_CODE: &[u8] = &[0x60, 0x03];
    const COLLATERAL_IMPLEMENTATION_CODE: &[u8] = &[0x60, 0x04, 0x60, 0x05];

    enum MockResult {
        Snapshot(Box<SettlementChainSnapshot>),
        WrongChain,
        CanonicalBlockChanged,
        RpcFailure,
    }

    struct MockChainReader {
        calls: Arc<AtomicUsize>,
        result: MockResult,
    }

    impl MockChainReader {
        fn snapshot(snapshot: SettlementChainSnapshot) -> (Self, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    calls: Arc::clone(&calls),
                    result: MockResult::Snapshot(Box::new(snapshot)),
                },
                calls,
            )
        }

        fn wrong_chain() -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                result: MockResult::WrongChain,
            }
        }

        fn rpc_failure() -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                result: MockResult::RpcFailure,
            }
        }

        fn canonical_block_changed() -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                result: MockResult::CanonicalBlockChanged,
            }
        }
    }

    #[async_trait]
    impl SettlementChainReader for MockChainReader {
        async fn inspect(
            &self,
            request: &SettlementInspectionRequest,
            _topology: &WalletTopology,
        ) -> Result<SettlementChainSnapshot, SettlementChainReadError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match &self.result {
                MockResult::Snapshot(snapshot) => Ok(snapshot.as_ref().clone()),
                MockResult::WrongChain => Err(SettlementChainReadError::WrongChain {
                    expected: request.chain_id,
                    actual: 1,
                }),
                MockResult::CanonicalBlockChanged => {
                    Err(SettlementChainReadError::CanonicalBlockChanged {
                        block_number: 73_000_000,
                        observed_hash: block_hash('3'),
                        current_hash: Some(block_hash('4')),
                    })
                }
                MockResult::RpcFailure => Err(RpcError::Timeout {
                    method: "eth_getCode".to_owned(),
                    elapsed_ms: 1_000,
                }
                .into()),
            }
        }

        async fn canonical_block_hash(
            &self,
            _block_number: u64,
        ) -> Result<Option<EvmBlockHash>, SettlementChainReadError> {
            match &self.result {
                MockResult::Snapshot(snapshot) => Ok(Some(snapshot.block_hash.clone())),
                MockResult::WrongChain => Err(SettlementChainReadError::WrongChain {
                    expected: POLYGON_CHAIN_ID,
                    actual: 1,
                }),
                MockResult::CanonicalBlockChanged => Ok(None),
                MockResult::RpcFailure => Err(RpcError::Timeout {
                    method: "eth_getBlockByNumber".to_owned(),
                    elapsed_ms: 1_000,
                }
                .into()),
            }
        }

        async fn finalized_head(
            &self,
        ) -> Result<SettlementFinalizedHead, SettlementChainReadError> {
            match &self.result {
                MockResult::Snapshot(snapshot) => Ok(SettlementFinalizedHead {
                    block_number: snapshot.block_number,
                    block_hash: snapshot.block_hash.clone(),
                }),
                MockResult::WrongChain => Err(SettlementChainReadError::WrongChain {
                    expected: POLYGON_CHAIN_ID,
                    actual: 1,
                }),
                MockResult::CanonicalBlockChanged => {
                    Err(SettlementChainReadError::CanonicalBlockChanged {
                        block_number: 73_000_000,
                        observed_hash: block_hash('3'),
                        current_hash: Some(block_hash('4')),
                    })
                }
                MockResult::RpcFailure => Err(RpcError::Timeout {
                    method: "eth_getBlockByNumber(finalized)".to_owned(),
                    elapsed_ms: 1_000,
                }
                .into()),
            }
        }
    }

    fn checked_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 22, 15, 10, 0)
            .single()
            .expect("valid test timestamp")
    }

    fn topology(kind: ExecutionWalletKind) -> WalletTopology {
        let address = Address::repeat_byte(0x11);
        let signature_type = match kind {
            ExecutionWalletKind::Eoa => SignatureType::Eoa,
            ExecutionWalletKind::Proxy => SignatureType::Proxy,
            ExecutionWalletKind::GnosisSafe => SignatureType::GnosisSafe,
            ExecutionWalletKind::DepositWallet => SignatureType::Poly1271,
        };
        WalletTopology {
            kind,
            signer: address,
            owner: address,
            funder: address,
            signature_type,
        }
    }

    fn deposit_topology() -> WalletTopology {
        WalletTopology {
            kind: ExecutionWalletKind::DepositWallet,
            signer: Address::from_str("0x26db8f3ab13163ebb41bab241c0eede026a8fc8a")
                .expect("fixture Deposit Wallet owner"),
            owner: Address::from_str("0x26db8f3ab13163ebb41bab241c0eede026a8fc8a")
                .expect("fixture Deposit Wallet owner"),
            funder: Address::from_str("0x4875924fbcac87a14b429b0af06e1c595ed3757c")
                .expect("fixture Deposit Wallet"),
            signature_type: SignatureType::Poly1271,
        }
    }

    fn fingerprint(hash: &str, size: u64) -> SettlementCodeFingerprint {
        SettlementCodeFingerprint {
            hash: EvmCodeHash::parse(hash).expect("fixture code hash"),
            size,
        }
    }

    fn current_deposit_wallet_snapshot() -> SettlementDepositWalletSnapshot {
        let owner =
            Address::from_str("0x26db8f3ab13163ebb41bab241c0eede026a8fc8a").expect("fixture owner");
        let mut wallet_id = [0_u8; 32];
        wallet_id[12..].copy_from_slice(owner.as_slice());
        SettlementDepositWalletSnapshot {
            owner: EvmAddress::parse(format!("{owner:#x}")).expect("typed fixture owner"),
            session_signer_valid_until: EvmUint256::parse("0").expect("fixture session expiry"),
            factory: EvmAddress::parse("0x00000000000fb5c9adea0298d729a0cb3823cc07")
                .expect("fixture factory"),
            wallet_id: B256::from(wallet_id),
            beacon: EvmAddress::parse("0x7a18edfe055488a3128f01f563e5b479d92ffc3a")
                .expect("fixture beacon"),
            wallet_implementation: EvmAddress::parse("0xf7f27c29e60fe6325bef8da7f93250353d2e3294")
                .expect("fixture wallet implementation"),
            factory_implementation: EvmAddress::parse("0x528cc05efac2b0d255e423272187efd41248abd7")
                .expect("fixture factory implementation"),
            wallet_code: fingerprint(
                "0x7c0d9aef3e7fcb57d58361e7d022587542f27b5f0c0f693a3874f4476d443b82",
                146,
            ),
            factory_code: fingerprint(
                "0xaaa52c8cc8a0e3fd27ce756cc6b4e70c51423e9b597b11f32d3e49f8b1fc890d",
                61,
            ),
            factory_implementation_code: fingerprint(
                "0xe6424f1008e46b4b657efacf9500ea7747cbbf3055d9d76459253ac2884793d2",
                9_250,
            ),
            beacon_code: Some(fingerprint(
                "0xf87b06a1302051471df08ff79a938757509569e16b7a7efa55a3ea7b29b0b9d1",
                1_540,
            )),
            wallet_implementation_code: fingerprint(
                "0xf5c1072460e64902af84d35f5bb1d0a15d80a88c5827b831a977fbc5a0684b96",
                20_858,
            ),
        }
    }

    fn block_hash(digit: char) -> EvmBlockHash {
        EvmBlockHash::parse(format!("0x{}", digit.to_string().repeat(64)))
            .expect("canonical block hash")
    }

    fn resolved_catalog() -> SettlementDeploymentCatalog {
        let mut catalog = SettlementDeploymentCatalog::official_current()
            .expect("built-in addresses are canonical");
        let code_hash =
            EvmCodeHash::parse(format!("{:#x}", alloy::primitives::keccak256(ADAPTER_CODE)))
                .expect("keccak hash is canonical");
        catalog.standard_adapter_code_hash = Some(code_hash.clone());
        catalog.neg_risk_adapter_code_hash = Some(code_hash);
        catalog.standard_adapter_code_size = Some(ADAPTER_CODE.len() as u64);
        catalog.neg_risk_adapter_code_size = Some(ADAPTER_CODE.len() as u64);
        catalog.collateral_token_proxy_code_hash = EvmCodeHash::parse(format!(
            "{:#x}",
            alloy::primitives::keccak256(COLLATERAL_PROXY_CODE)
        ))
        .expect("keccak hash is canonical");
        catalog.collateral_token_implementation_code_hash = EvmCodeHash::parse(format!(
            "{:#x}",
            alloy::primitives::keccak256(COLLATERAL_IMPLEMENTATION_CODE)
        ))
        .expect("keccak hash is canonical");
        catalog.collateral_token_proxy_code_size = COLLATERAL_PROXY_CODE.len() as u64;
        catalog.collateral_token_implementation_code_size =
            COLLATERAL_IMPLEMENTATION_CODE.len() as u64;
        catalog
    }

    fn valid_snapshot(
        catalog: &SettlementDeploymentCatalog,
        route: SettlementRoute,
        wallet_kind: ExecutionWalletKind,
    ) -> SettlementChainSnapshot {
        let neg_risk = route == SettlementRoute::NegRiskV2;
        SettlementChainSnapshot {
            chain_id: catalog.chain_id,
            block_number: 73_000_000,
            block_hash: block_hash('3'),
            block_timestamp: checked_at(),
            adapter_code: ADAPTER_CODE.to_vec(),
            conditional_tokens_code: vec![0x60],
            collateral_token_code: COLLATERAL_PROXY_CODE.to_vec(),
            collateral_token_implementation_code: COLLATERAL_IMPLEMENTATION_CODE.to_vec(),
            usdc_code: vec![0x60],
            usdce_code: vec![0x60],
            collateral_vault_code: vec![0x60],
            legacy_neg_risk_adapter_code: neg_risk.then(|| vec![0x60]),
            funder_code: if wallet_kind == ExecutionWalletKind::Eoa {
                Vec::new()
            } else {
                vec![0x60]
            },
            deposit_wallet: None,
            bindings: SettlementAdapterBindings {
                owner: catalog.owner.clone(),
                conditional_tokens: catalog.conditional_tokens.clone(),
                collateral_token: catalog.collateral_token.clone(),
                collateral_token_implementation: catalog.collateral_token_implementation.clone(),
                collateral_token_owner: catalog.owner.clone(),
                usdc: catalog.usdc.clone(),
                usdce: catalog.usdce.clone(),
                collateral_token_usdce: catalog.usdce.clone(),
                collateral_vault: catalog.collateral_vault.clone(),
                adapter_has_wrapper_role: true,
                neg_risk_adapter: neg_risk.then(|| catalog.legacy_neg_risk_adapter.clone()),
                wrapped_collateral: neg_risk.then(|| catalog.neg_risk_wrapped_collateral.clone()),
                legacy_wrapped_collateral: neg_risk
                    .then(|| catalog.neg_risk_wrapped_collateral.clone()),
            },
            adapter_paused: false,
            operator_approved: true,
            pusd_decimals: 6,
            usdce_decimals: 6,
        }
    }

    async fn blocked(
        catalog: SettlementDeploymentCatalog,
        snapshot: SettlementChainSnapshot,
        route: SettlementRoute,
        topology: WalletTopology,
        credentials: SettlementCredentialAvailability,
    ) -> SettlementReadiness {
        let (reader, _) = MockChainReader::snapshot(snapshot);
        ContractDeploymentVerifier::new(catalog, reader)
            .verify(route, &topology, credentials, checked_at())
            .await
            .expect_err("invalid evidence must not mint a capability")
    }

    fn assert_capability(
        capability: &VerifiedSettlementDeployment,
        catalog: &SettlementDeploymentCatalog,
        route: SettlementRoute,
        wallet_kind: ExecutionWalletKind,
    ) {
        assert_eq!(capability.route(), route);
        assert_eq!(capability.wallet_kind(), wallet_kind);
        assert_eq!(capability.verified_block(), 73_000_000);
        assert_eq!(capability.verified_block_hash(), &block_hash('3'));
        assert_eq!(
            capability.target(),
            match route {
                SettlementRoute::StandardV2 => &catalog.authority.standard_adapter,
                SettlementRoute::NegRiskV2 => &catalog.authority.neg_risk_adapter,
            }
        );
        assert_eq!(
            capability.target_code_hash(),
            match route {
                SettlementRoute::StandardV2 => catalog
                    .standard_adapter_code_hash
                    .as_ref()
                    .expect("resolved standard code hash"),
                SettlementRoute::NegRiskV2 => catalog
                    .neg_risk_adapter_code_hash
                    .as_ref()
                    .expect("resolved Neg Risk code hash"),
            }
        );
    }

    #[tokio::test]
    async fn repository_drift_is_typed_evidence_and_does_not_select_or_block_target() {
        let catalog = resolved_catalog();
        let snapshot = valid_snapshot(
            &catalog,
            SettlementRoute::StandardV2,
            ExecutionWalletKind::Eoa,
        );
        let (reader, calls) = MockChainReader::snapshot(snapshot);
        let capability = ContractDeploymentVerifier::new(catalog.clone(), reader)
            .verify(
                SettlementRoute::StandardV2,
                &topology(ExecutionWalletKind::Eoa),
                SettlementCredentialAvailability::DirectEoaSigner,
                checked_at(),
            )
            .await
            .expect("reference-source drift cannot override authoritative chain evidence");

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(capability.target(), &catalog.authority.standard_adapter);
        assert!(capability.advisories().iter().any(|evidence| matches!(
            evidence,
            SettlementDeploymentEvidence::RepositoryDocumentationDrift {
                source: SettlementDeploymentSource::CtfExchangeV2Readme,
                ..
            }
        )));
        assert_eq!(
            catalog.repository_reference.source,
            SettlementDeploymentSource::CtfExchangeV2Readme
        );
    }

    #[tokio::test]
    async fn verified_capability_requires_complete_standard_and_neg_risk_evidence() {
        for route in [SettlementRoute::StandardV2, SettlementRoute::NegRiskV2] {
            let catalog = resolved_catalog();
            let snapshot = valid_snapshot(&catalog, route, ExecutionWalletKind::Eoa);
            let (reader, calls) = MockChainReader::snapshot(snapshot);
            let capability = ContractDeploymentVerifier::new(catalog.clone(), reader)
                .verify(
                    route,
                    &topology(ExecutionWalletKind::Eoa),
                    SettlementCredentialAvailability::DirectEoaSigner,
                    checked_at(),
                )
                .await
                .expect("complete evidence mints the route capability");

            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_capability(&capability, &catalog, route, ExecutionWalletKind::Eoa);
        }
    }

    #[tokio::test]
    async fn missing_operator_approval_does_not_block_deployment_capability() {
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Eoa);
        let mut snapshot = valid_snapshot(&catalog, SettlementRoute::StandardV2, wallet.kind);
        snapshot.operator_approved = false;
        let (reader, _) = MockChainReader::snapshot(snapshot);
        let capability = ContractDeploymentVerifier::new(catalog, reader)
            .verify(
                SettlementRoute::StandardV2,
                &wallet,
                SettlementCredentialAvailability::DirectEoaSigner,
                checked_at(),
            )
            .await
            .expect("missing approval must still mint the approval-safe deployment capability");
        assert!(!capability.operator_approved());
    }

    #[tokio::test]
    async fn wrong_chain_and_rpc_failure_remain_typed() {
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Eoa);
        let wrong_chain =
            ContractDeploymentVerifier::new(catalog.clone(), MockChainReader::wrong_chain())
                .verify(
                    SettlementRoute::StandardV2,
                    &wallet,
                    SettlementCredentialAvailability::DirectEoaSigner,
                    checked_at(),
                )
                .await
                .expect_err("wrong chain must fail closed");
        assert!(matches!(
            wrong_chain.reasons.as_slice(),
            [SettlementReadinessReason::WrongChain {
                expected: 137,
                actual: 1
            }]
        ));

        let unavailable = ContractDeploymentVerifier::new(catalog, MockChainReader::rpc_failure())
            .verify(
                SettlementRoute::StandardV2,
                &wallet,
                SettlementCredentialAvailability::DirectEoaSigner,
                checked_at(),
            )
            .await
            .expect_err("RPC failure must fail closed");
        assert!(matches!(
            unavailable.reasons.as_slice(),
            [SettlementReadinessReason::RpcUnavailable { .. }]
        ));

        let reorg = ContractDeploymentVerifier::new(
            resolved_catalog(),
            MockChainReader::canonical_block_changed(),
        )
        .verify(
            SettlementRoute::StandardV2,
            &wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
            checked_at(),
        )
        .await
        .expect_err("a canonical hash change must fail closed");
        assert!(matches!(
            reorg.reasons.as_slice(),
            [SettlementReadinessReason::CanonicalBlockChanged { .. }]
        ));
    }

    #[tokio::test]
    async fn code_presence_hash_and_catalog_completeness_are_independent_gates() {
        let route = SettlementRoute::StandardV2;
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Eoa);

        let mut missing = valid_snapshot(&catalog, route, wallet.kind);
        missing.conditional_tokens_code.clear();
        let readiness = blocked(
            catalog.clone(),
            missing,
            route,
            wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::CodeMissing { contract, .. }
                if contract == "conditional_tokens"
        )));

        let mut wrong_hash = valid_snapshot(&catalog, route, wallet.kind);
        wrong_hash.adapter_code.push(0xff);
        let readiness = blocked(
            catalog.clone(),
            wrong_hash,
            route,
            wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
        )
        .await;
        assert!(
            readiness
                .reasons
                .iter()
                .any(|reason| matches!(reason, SettlementReadinessReason::CodeHashMismatch { .. }))
        );

        let mut incomplete = catalog.clone();
        incomplete.standard_adapter_code_hash = None;
        let readiness = blocked(
            incomplete,
            valid_snapshot(&catalog, route, wallet.kind),
            route,
            wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::CatalogIncomplete { field, .. }
                if field == "expected_adapter_code_hash"
        )));
    }

    #[tokio::test]
    async fn adapter_bindings_and_neg_risk_dependency_are_exact() {
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Eoa);
        let mut standard = valid_snapshot(&catalog, SettlementRoute::StandardV2, wallet.kind);
        standard.bindings.owner = catalog.usdce.clone();
        standard.bindings.collateral_token = catalog.usdce.clone();
        let readiness = blocked(
            catalog.clone(),
            standard,
            SettlementRoute::StandardV2,
            wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::ContractBindingMismatch {
                binding: SettlementContractBinding::Owner,
                ..
            }
        )));
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::ContractBindingMismatch {
                binding: SettlementContractBinding::CollateralToken,
                ..
            }
        )));

        let mut neg_risk = valid_snapshot(&catalog, SettlementRoute::NegRiskV2, wallet.kind);
        neg_risk.bindings.neg_risk_adapter = None;
        neg_risk.bindings.legacy_wrapped_collateral = None;
        let readiness = blocked(
            catalog,
            neg_risk,
            SettlementRoute::NegRiskV2,
            wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::ContractBindingUnavailable {
                binding: SettlementContractBinding::NegRiskAdapter
            }
        )));
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::ContractBindingUnavailable {
                binding: SettlementContractBinding::LegacyWrappedCollateral
            }
        )));
    }

    #[tokio::test]
    async fn collateral_token_proxy_implementation_and_wrapper_role_are_exact() {
        let catalog = resolved_catalog();
        let route = SettlementRoute::StandardV2;
        let wallet = topology(ExecutionWalletKind::Eoa);

        let mut upgraded = valid_snapshot(&catalog, route, wallet.kind);
        upgraded.collateral_token_implementation_code.push(0xff);
        let readiness = blocked(
            catalog.clone(),
            upgraded,
            route,
            wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::CodeHashMismatch { contract, .. }
                if contract == "collateral_token_implementation"
        )));

        let mut wrong_binding = valid_snapshot(&catalog, route, wallet.kind);
        wrong_binding.bindings.collateral_token_implementation = catalog.usdce.clone();
        wrong_binding.bindings.adapter_has_wrapper_role = false;
        let readiness = blocked(
            catalog,
            wrong_binding,
            route,
            wallet,
            SettlementCredentialAvailability::DirectEoaSigner,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::ContractBindingMismatch {
                binding: SettlementContractBinding::CollateralTokenImplementation,
                ..
            }
        )));
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::CollateralWrapperRoleMissing { .. }
        )));
    }

    #[tokio::test]
    async fn stale_or_future_chain_observation_cannot_mint_capability() {
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Eoa);
        for block_timestamp in [
            checked_at() - TimeDelta::seconds(121),
            checked_at() + TimeDelta::seconds(1),
        ] {
            let mut snapshot = valid_snapshot(&catalog, SettlementRoute::StandardV2, wallet.kind);
            snapshot.block_timestamp = block_timestamp;
            let readiness = blocked(
                catalog.clone(),
                snapshot,
                SettlementRoute::StandardV2,
                wallet,
                SettlementCredentialAvailability::DirectEoaSigner,
            )
            .await;
            assert!(readiness.reasons.iter().any(|reason| matches!(
                reason,
                SettlementReadinessReason::ChainObservationNotFresh { .. }
            )));
        }
    }

    #[test]
    fn official_catalog_freezes_provenance_and_current_fingerprint() {
        let catalog =
            SettlementDeploymentCatalog::official_current().expect("built-in catalog is canonical");
        assert_eq!(
            catalog.authority.provenance.source,
            SettlementDeploymentSource::ContractsDocumentation
        );
        assert_eq!(
            catalog.authority.provenance.source_url,
            "https://docs.polymarket.com/resources/contracts"
        );
        assert_eq!(
            catalog.authority.provenance.retrieved_at,
            "2026-07-22T15:08:33Z"
        );
        assert_eq!(
            catalog.corroboration.source,
            SettlementDeploymentSource::Changelog
        );
        assert_eq!(catalog.corroboration.revision, Some("2026-07-14"));
        assert_eq!(catalog.fingerprint_observed_block, 90_685_098);
        assert_eq!(
            catalog.fingerprint_observed_block_hash.as_str(),
            "0xb8a2e2119b1fbcfb609e62fae53cabcdb016a288ad4de13b228193643c94ec3a"
        );
        assert_eq!(
            catalog.authority.standard_adapter.as_str(),
            "0xada100db00ca00073811820692005400218fce1f"
        );
        assert_eq!(
            catalog
                .standard_adapter_code_hash
                .as_ref()
                .map(EvmCodeHash::as_str),
            Some("0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f")
        );
        assert_eq!(catalog.standard_adapter_code_size, Some(11_096));
        assert_eq!(
            catalog.authority.neg_risk_adapter.as_str(),
            "0xada2005600dec949baf300f4c6120000bdb6eaab"
        );
        assert_eq!(
            catalog
                .neg_risk_adapter_code_hash
                .as_ref()
                .map(EvmCodeHash::as_str),
            Some("0x3b892c7c2f80e7af69f28faf72a51c2d793f6b79b96011bdf0a1996319fcbe5b")
        );
        assert_eq!(catalog.neg_risk_adapter_code_size, Some(13_890));
    }

    #[tokio::test]
    async fn pause_decimals_topology_and_credentials_all_block() {
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Proxy);
        let mut snapshot = valid_snapshot(&catalog, SettlementRoute::StandardV2, wallet.kind);
        snapshot.adapter_paused = true;
        snapshot.pusd_decimals = 18;
        snapshot.funder_code.clear();
        let readiness = blocked(
            catalog,
            snapshot,
            SettlementRoute::StandardV2,
            wallet,
            SettlementCredentialAvailability::Missing,
        )
        .await;

        assert!(
            readiness
                .reasons
                .iter()
                .any(|reason| matches!(reason, SettlementReadinessReason::AdapterPaused { .. }))
        );
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::TokenDecimalsMismatch {
                token: SettlementTokenKind::Pusd,
                expected: 6,
                actual: 18
            }
        )));
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::WalletTopologyMismatch { .. }
        )));
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::CredentialMissing {
                required: SettlementCredentialKind::Relayer,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn observation_verification_skips_signing_credentials() {
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Proxy);
        let snapshot = valid_snapshot(&catalog, SettlementRoute::StandardV2, wallet.kind);
        let (reader, _) = MockChainReader::snapshot(snapshot);
        let capability = ContractDeploymentVerifier::new(catalog, reader)
            .verify_for_observation(SettlementRoute::StandardV2, &wallet, checked_at())
            .await
            .expect("external observation must mint fingerprint without relayer credentials");
        assert_eq!(capability.wallet_kind(), ExecutionWalletKind::Proxy);
        assert_eq!(capability.route(), SettlementRoute::StandardV2);
    }

    #[tokio::test]
    async fn money_verification_still_requires_signing_credentials() {
        let catalog = resolved_catalog();
        let wallet = topology(ExecutionWalletKind::Proxy);
        let snapshot = valid_snapshot(&catalog, SettlementRoute::StandardV2, wallet.kind);
        let readiness = blocked(
            catalog,
            snapshot,
            SettlementRoute::StandardV2,
            wallet,
            SettlementCredentialAvailability::Missing,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::CredentialMissing {
                required: SettlementCredentialKind::Relayer,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn deposit_wallet_requires_exact_factory_beacon_implementation_and_owner_lineage() {
        let catalog = resolved_catalog();
        let wallet = deposit_topology();
        let mut snapshot = valid_snapshot(
            &catalog,
            SettlementRoute::StandardV2,
            ExecutionWalletKind::DepositWallet,
        );
        snapshot.funder_code = deposit_wallet_runtime_code(wallet.signer);
        snapshot.deposit_wallet = Some(current_deposit_wallet_snapshot());
        let (reader, _) = MockChainReader::snapshot(snapshot.clone());
        let capability = ContractDeploymentVerifier::new(catalog.clone(), reader)
            .verify(
                SettlementRoute::StandardV2,
                &wallet,
                SettlementCredentialAvailability::Relayer,
                checked_at(),
            )
            .await
            .expect("exact Deposit Wallet identity mints deployment capability");
        assert_eq!(capability.wallet_kind(), ExecutionWalletKind::DepositWallet);

        snapshot
            .deposit_wallet
            .as_mut()
            .expect("fixture Deposit Wallet evidence")
            .factory = EvmAddress::parse("0x1111111111111111111111111111111111111111")
            .expect("wrong fixture factory");
        let readiness = blocked(
            catalog,
            snapshot,
            SettlementRoute::StandardV2,
            wallet,
            SettlementCredentialAvailability::Relayer,
        )
        .await;
        assert!(readiness.reasons.iter().any(|reason| matches!(
            reason,
            SettlementReadinessReason::ContractBindingMismatch {
                binding: SettlementContractBinding::DepositWalletFactory,
                ..
            }
        )));
    }
}
