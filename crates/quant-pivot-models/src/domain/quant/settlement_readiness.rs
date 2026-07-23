//! Typed, fail-closed settlement deployment readiness.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        quant::ExecutionWalletKind,
        settlement::{SettlementFailureCode, SettlementReadinessStatus, SettlementRoute},
    },
    types::{ContentHash, EvmAddress, EvmBlockHash, EvmCodeHash, EvmUint256},
};

/// Independently published official deployment source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementDeploymentSource {
    ContractsDocumentation,
    Changelog,
    CtfExchangeV2Readme,
}

/// Non-blocking provenance evidence retained alongside a verified capability.
/// These variants can explain publication drift but can never select a target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum SettlementDeploymentEvidence {
    RepositoryDocumentationDrift {
        route: SettlementRoute,
        source: SettlementDeploymentSource,
        revision: Option<String>,
    },
}

/// Reproducible Solidity source/compiler evidence for one current deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementArtifactReproducibilityEvidence {
    pub source_url: String,
    pub retrieved_at: String,
    pub source_bundle_hash: ContentHash,
    pub compiler_version: String,
    pub optimizer_runs: u32,
    pub evm_version: String,
    pub via_ir: bool,
    pub runtime_template_hash: EvmCodeHash,
    pub deployed_runtime_hash: EvmCodeHash,
    pub immutable_patch_exact_match: bool,
}

/// Adapter immutable or linked dependency checked on chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementContractBinding {
    Owner,
    ConditionalTokens,
    CollateralToken,
    CollateralTokenImplementation,
    CollateralTokenOwner,
    Usdc,
    Usdce,
    CollateralTokenUsdce,
    CollateralVault,
    DepositWalletOwner,
    DepositWalletFactory,
    DepositWalletImplementation,
    DepositWalletBeacon,
    NegRiskAdapter,
    WrappedCollateral,
    LegacyWrappedCollateral,
}

/// Collateral token metadata checked before enabling settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementTokenKind {
    Pusd,
    Usdce,
}

/// Credential class required by the resolved wallet topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementCredentialKind {
    DirectEoaSigner,
    Relayer,
}

/// Closed fail-closed reason set for V2 settlement admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum SettlementReadinessReason {
    CatalogIncomplete {
        route: SettlementRoute,
        field: String,
    },
    RpcUnavailable {
        operation: String,
        detail: String,
    },
    WrongChain {
        expected: u64,
        actual: u64,
    },
    CodeMissing {
        contract: String,
        address: EvmAddress,
    },
    CodeHashMismatch {
        contract: String,
        address: EvmAddress,
        expected: EvmCodeHash,
        actual: EvmCodeHash,
    },
    CodeSizeMismatch {
        contract: String,
        address: EvmAddress,
        expected: u64,
        actual: u64,
    },
    ContractBindingMismatch {
        binding: SettlementContractBinding,
        expected: EvmAddress,
        actual: EvmAddress,
    },
    ContractBindingUnavailable {
        binding: SettlementContractBinding,
    },
    AdapterPaused {
        adapter: EvmAddress,
        asset: EvmAddress,
    },
    OperatorApprovalMissing {
        funder: EvmAddress,
        adapter: EvmAddress,
    },
    ConditionNotResolved,
    InvalidBinaryPayoutVector {
        denominator: String,
        yes: String,
        no: String,
    },
    OutcomeInventoryMismatch {
        detail: String,
    },
    UnsettledExecutionOrders {
        count: u64,
    },
    AdapterResidualUsdce {
        adapter: EvmAddress,
        raw_balance: EvmUint256,
    },
    RedeemSimulationReverted {
        detail: String,
    },
    SubmissionPreflightFailed {
        failure_code: SettlementFailureCode,
        detail: String,
    },
    TokenDecimalsMismatch {
        token: SettlementTokenKind,
        expected: u8,
        actual: u8,
    },
    WalletTopologyMismatch {
        wallet_kind: ExecutionWalletKind,
        funder: EvmAddress,
        detail: String,
    },
    CollateralWrapperRoleMissing {
        collateral_token: EvmAddress,
        adapter: EvmAddress,
    },
    CredentialMissing {
        wallet_kind: ExecutionWalletKind,
        required: SettlementCredentialKind,
    },
    ChainObservationNotFresh {
        block_number: u64,
        block_timestamp: DateTime<Utc>,
        checked_at: DateTime<Utc>,
        max_age_seconds: i64,
    },
    CanonicalBlockChanged {
        block_number: u64,
        observed_hash: EvmBlockHash,
        current_hash: Option<EvmBlockHash>,
    },
}

/// Serializable truth rendered by readiness APIs and operator tooling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementReadiness {
    pub route: SettlementRoute,
    pub wallet_kind: ExecutionWalletKind,
    pub status: SettlementReadinessStatus,
    pub reasons: Vec<SettlementReadinessReason>,
    pub checked_block: Option<u64>,
    pub checked_block_hash: Option<EvmBlockHash>,
    pub deployment_digest: Option<ContentHash>,
    pub checked_at: DateTime<Utc>,
}

impl SettlementReadiness {
    #[must_use]
    pub fn blocked(
        route: SettlementRoute,
        wallet_kind: ExecutionWalletKind,
        reasons: Vec<SettlementReadinessReason>,
        checked_block: Option<u64>,
        checked_block_hash: Option<EvmBlockHash>,
        checked_at: DateTime<Utc>,
    ) -> Self {
        debug_assert!(!reasons.is_empty());
        Self {
            route,
            wallet_kind,
            status: SettlementReadinessStatus::Blocked,
            reasons,
            checked_block,
            checked_block_hash,
            deployment_digest: None,
            checked_at,
        }
    }

    #[must_use]
    pub const fn ready(
        route: SettlementRoute,
        wallet_kind: ExecutionWalletKind,
        checked_block: u64,
        checked_block_hash: EvmBlockHash,
        deployment_digest: ContentHash,
        checked_at: DateTime<Utc>,
    ) -> Self {
        Self {
            route,
            wallet_kind,
            status: SettlementReadinessStatus::Ready,
            reasons: Vec::new(),
            checked_block: Some(checked_block),
            checked_block_hash: Some(checked_block_hash),
            deployment_digest: Some(deployment_digest),
            checked_at,
        }
    }
}
