//! Settlement redeem HTTP contract types.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{
        pagination::PageRequest,
        quant::{
            settlement::{
                SettlementChainSubmissionInfo, SettlementRedeemInfo, SettlementRedeemLotInfo,
            },
            settlement_governance::SettlementGovernedActionInfo,
            settlement_inventory::SettlementInventoryLotInfo,
            settlement_readiness::{
                SettlementDeploymentEvidence, SettlementDeploymentSource, SettlementReadinessReason,
            },
        },
    },
    enums::{
        quant::{ExecutionWalletKind, ExitSettlementMode, OutcomeSide, RedeemPolicy},
        settlement::{
            SettlementAuthorizationState, SettlementCaseState, SettlementEffectivePolicy,
            SettlementFailureCode, SettlementGovernedActionKind, SettlementGovernedActionState,
            SettlementReadinessStatus, SettlementReconciliationState, SettlementRoute,
            SettlementSubmissionKind, SettlementSubmissionPurpose, SettlementSubmissionState,
            SettlementWritePolicy,
        },
    },
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash, EvmTransactionHash,
        ExecutionAccountId, MarketId, OrderIntentId, PositionId, RelayerTransactionId,
        SettlementActionIdempotencyKey, SettlementChainSubmissionId, SettlementEvidenceVersion,
        SettlementGovernedActionId, SettlementInventoryLotId, SettlementRedeemId,
        SettlementRedeemLotId, Shares, TokenId, Usd, UserId,
        settlement_payload::{
            SettlementBalanceEvidence, SettlementChainReceiptEvidence, SettlementFailureHistory,
        },
    },
};

/// One contributor in the exact immutable inventory currently governing a case.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementInventoryLotView {
    pub settlement_inventory_lot_id: SettlementInventoryLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub execution_account_id: ExecutionAccountId,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares: Shares,
    pub cost_basis_usd: Usd,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub position_version_at: DateTime<Utc>,
    pub intent_version_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl From<SettlementInventoryLotInfo> for SettlementInventoryLotView {
    fn from(info: SettlementInventoryLotInfo) -> Self {
        Self {
            settlement_inventory_lot_id: info.settlement_inventory_lot_id,
            settlement_redeem_id: info.settlement_redeem_id,
            inventory_digest: info.inventory_digest,
            contributor_lots_digest: info.contributor_lots_digest,
            execution_account_id: info.execution_account_id,
            position_id: info.position_id,
            order_intent_id: info.order_intent_id,
            token_id: info.token_id,
            side: info.side,
            shares: info.shares,
            cost_basis_usd: info.cost_basis_usd,
            settlement_mode: info.settlement_mode,
            redeem_policy: info.redeem_policy,
            position_version_at: info.position_version_at,
            intent_version_at: info.intent_version_at,
            created_at: info.created_at,
        }
    }
}

/// Outbound projection of one settlement redeem lot row.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementRedeemLotView {
    pub settlement_redeem_lot_id: SettlementRedeemLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares_redeemed: Shares,
    pub cost_basis_usd: Usd,
    pub payout_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub created_at: DateTime<Utc>,
}

impl From<SettlementRedeemLotInfo> for SettlementRedeemLotView {
    fn from(info: SettlementRedeemLotInfo) -> Self {
        Self {
            settlement_redeem_lot_id: info.settlement_redeem_lot_id,
            settlement_redeem_id: info.settlement_redeem_id,
            position_id: info.position_id,
            order_intent_id: info.order_intent_id,
            token_id: info.token_id,
            side: info.side,
            shares_redeemed: info.shares_redeemed,
            cost_basis_usd: info.cost_basis_usd,
            payout_usd: info.payout_usd,
            realized_pnl_usd: info.realized_pnl_usd,
            created_at: info.created_at,
        }
    }
}

/// Read-port aggregate: one redeem case with its current frozen inventory count.
///
/// Confirmed payout allocations are intentionally not used here: an unsubmitted
/// case must expose the contributor count that controls full-balance admission.
#[derive(Debug, Clone)]
pub struct SettlementRedeemSummary {
    pub redeem: SettlementRedeemInfo,
    pub inventory_lot_count: i64,
}

/// Outbound projection of one settlement redeem batch.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementRedeemView {
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub execution_account_id: ExecutionAccountId,
    pub funder_address: EvmAddress,
    pub wallet_kind: ExecutionWalletKind,
    pub route: SettlementRoute,
    pub effective_policy: SettlementEffectivePolicy,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub state: SettlementCaseState,
    pub readiness_status: SettlementReadinessStatus,
    pub readiness_reasons: Vec<SettlementReadinessReason>,
    pub readiness_advisories: Vec<SettlementDeploymentEvidence>,
    pub target_adapter: Option<EvmAddress>,
    pub target_code_hash: Option<EvmCodeHash>,
    pub deployment_digest: Option<ContentHash>,
    pub deployment_evidence_version: Option<SettlementEvidenceVersion>,
    pub verified_block_number: Option<i64>,
    pub verified_block_hash: Option<EvmBlockHash>,
    pub authorization_state: SettlementAuthorizationState,
    pub authorization_digest: Option<ContentHash>,
    pub authorization_expires_at: Option<DateTime<Utc>>,
    pub authorized_by: Option<UserId>,
    pub authorized_at: Option<DateTime<Utc>>,
    pub authorization_revoked_at: Option<DateTime<Utc>>,
    pub authorization_consumed_at: Option<DateTime<Utc>>,
    pub reconciliation_state: SettlementReconciliationState,
    /// Number of contributors in the exact current immutable inventory.
    pub inventory_lot_count: i64,
    pub expected_payout_usd: Option<Usd>,
    pub actual_payout_usd: Option<Usd>,
    /// Frozen YES/NO balances observed before preparation or confirmation.
    pub balance_before: Option<SettlementBalanceEvidence>,
    /// YES/NO balances observed at the confirmation receipt boundary.
    pub balance_after: Option<SettlementBalanceEvidence>,
    pub gas_fee_pol: Option<Decimal>,
    pub failure_code: Option<SettlementFailureCode>,
    pub attempt_count: i32,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<SettlementRedeemSummary> for SettlementRedeemView {
    fn from(summary: SettlementRedeemSummary) -> Self {
        let SettlementRedeemSummary {
            redeem,
            inventory_lot_count,
        } = summary;
        Self {
            settlement_redeem_id: redeem.settlement_redeem_id,
            market_id: redeem.market_id,
            yes_token_id: redeem.yes_token_id,
            no_token_id: redeem.no_token_id,
            execution_account_id: redeem.execution_account_id,
            funder_address: redeem.funder_address,
            wallet_kind: redeem.wallet_kind,
            route: redeem.route,
            effective_policy: redeem.effective_policy,
            inventory_digest: redeem.inventory_digest,
            contributor_lots_digest: redeem.contributor_lots_digest,
            state: redeem.state,
            readiness_status: redeem.readiness_status,
            readiness_reasons: redeem.readiness_evidence_json.reasons,
            readiness_advisories: redeem.readiness_evidence_json.advisories,
            target_adapter: redeem.target_adapter,
            target_code_hash: redeem.target_code_hash,
            deployment_digest: redeem.deployment_digest,
            deployment_evidence_version: redeem.deployment_evidence_version,
            verified_block_number: redeem.verified_block_number,
            verified_block_hash: redeem.verified_block_hash,
            authorization_state: redeem.authorization_state,
            authorization_digest: redeem.authorization_digest,
            authorization_expires_at: redeem.authorization_expires_at,
            authorized_by: redeem.authorized_by,
            authorized_at: redeem.authorized_at,
            authorization_revoked_at: redeem.authorization_revoked_at,
            authorization_consumed_at: redeem.authorization_consumed_at,
            reconciliation_state: redeem.reconciliation_state,
            inventory_lot_count,
            expected_payout_usd: redeem.expected_payout_usd,
            actual_payout_usd: redeem.actual_payout_usd,
            balance_before: redeem.balance_before_json,
            balance_after: redeem.balance_after_json,
            gas_fee_pol: redeem.gas_fee_pol,
            failure_code: redeem.failure_code,
            attempt_count: redeem.attempt_count,
            retry_count: redeem.retry_count,
            next_attempt_at: redeem.next_attempt_at,
            last_error: redeem.last_error,
            submitted_at: redeem.submitted_at,
            confirmed_at: redeem.confirmed_at,
            failed_at: redeem.failed_at,
            created_at: redeem.created_at,
            updated_at: redeem.updated_at,
        }
    }
}

/// Read-port aggregate separating pre-submission inventory from confirmed payouts.
#[derive(Debug, Clone)]
pub struct SettlementRedeemDetail {
    pub redeem: SettlementRedeemInfo,
    pub inventory_lots: Vec<SettlementInventoryLotInfo>,
    pub redeemed_lots: Vec<SettlementRedeemLotInfo>,
    pub submissions: Vec<SettlementChainSubmissionInfo>,
}

/// Safe operator projection of one durable chain submission.
///
/// Raw calldata and signed-envelope bytes are deliberately excluded. Their
/// immutable hashes remain visible for audit and exact replay verification.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementChainSubmissionView {
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub settlement_redeem_id: Option<SettlementRedeemId>,
    pub settlement_governed_action_id: Option<SettlementGovernedActionId>,
    pub canary_action_id: Option<SettlementGovernedActionId>,
    pub purpose: SettlementSubmissionPurpose,
    pub kind: SettlementSubmissionKind,
    pub state: SettlementSubmissionState,
    pub route: SettlementRoute,
    pub target_adapter: EvmAddress,
    pub target_code_hash: EvmCodeHash,
    pub call_target: EvmAddress,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub verified_block_number: i64,
    pub verified_block_hash: EvmBlockHash,
    pub prepared_block_number: Option<i64>,
    pub prepared_block_hash: Option<EvmBlockHash>,
    pub calldata_hash: EvmCalldataHash,
    pub relayer_transaction_id: Option<RelayerTransactionId>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub failure_code: Option<SettlementFailureCode>,
    pub failure_history: SettlementFailureHistory,
    pub receipt_evidence: Option<SettlementChainReceiptEvidence>,
    pub attempt_ordinal: i32,
    pub last_error: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub chain_hash_observed_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<SettlementChainSubmissionInfo> for SettlementChainSubmissionView {
    fn from(info: SettlementChainSubmissionInfo) -> Self {
        Self {
            settlement_chain_submission_id: info.settlement_chain_submission_id,
            settlement_redeem_id: info.settlement_redeem_id,
            settlement_governed_action_id: info.settlement_governed_action_id,
            canary_action_id: info.canary_action_id,
            purpose: info.purpose,
            kind: info.kind,
            state: info.state,
            route: info.route,
            target_adapter: info.target_adapter,
            target_code_hash: info.target_code_hash,
            call_target: info.call_target,
            deployment_digest: info.deployment_digest,
            deployment_evidence_version: info.deployment_evidence_version,
            verified_block_number: info.verified_block_number,
            verified_block_hash: info.verified_block_hash,
            prepared_block_number: info.prepared_block_number,
            prepared_block_hash: info.prepared_block_hash,
            calldata_hash: info.calldata_hash,
            relayer_transaction_id: info.relayer_transaction_id,
            transaction_hash: info.transaction_hash,
            failure_code: info.failure_code,
            failure_history: info.failure_history_json,
            receipt_evidence: info.receipt_evidence_json,
            attempt_ordinal: info.attempt_ordinal,
            last_error: info.last_error,
            dispatched_at: info.dispatched_at,
            chain_hash_observed_at: info.chain_hash_observed_at,
            confirmed_at: info.confirmed_at,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Settlement case detail including frozen contributors and confirmed allocations.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementRedeemDetailView {
    #[serde(flatten)]
    pub redeem: SettlementRedeemView,
    pub inventory_lots: Vec<SettlementInventoryLotView>,
    pub redeemed_lots: Vec<SettlementRedeemLotView>,
    pub submissions: Vec<SettlementChainSubmissionView>,
}

/// Exact compare-and-swap request for one `SemiAuto` batch authorization.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct SettlementAuthorizationRequest {
    pub digest: ContentHash,
    #[validate(length(min = 1, max = 500))]
    pub reason: String,
}

/// One official deployment publication with retrieval provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettlementDeploymentProvenanceView {
    pub source: SettlementDeploymentSource,
    pub source_url: String,
    pub revision: Option<String>,
    pub retrieved_at: String,
}

/// Live, signer-free deployment readiness for one configured route and wallet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettlementRouteReadinessView {
    pub route: SettlementRoute,
    pub wallet_kind: ExecutionWalletKind,
    pub status: SettlementReadinessStatus,
    pub blocking_reasons: Vec<SettlementReadinessReason>,
    pub advisories: Vec<SettlementDeploymentEvidence>,
    pub authority: SettlementDeploymentProvenanceView,
    pub corroboration: Option<SettlementDeploymentProvenanceView>,
    pub target_adapter: EvmAddress,
    pub runtime_code_hash: EvmCodeHash,
    pub observed_block_number: Option<u64>,
    pub observed_block_hash: Option<EvmBlockHash>,
    pub deployment_digest: Option<ContentHash>,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub operator_approved: Option<bool>,
    pub checked_at: DateTime<Utc>,
}

/// Complete read-only truth for settlement deployment and write admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettlementReadinessView {
    pub settlement_write_policy: SettlementWritePolicy,
    pub routes: Vec<SettlementRouteReadinessView>,
}

/// Governed money-moving workflow whose preflight is available to operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementGovernedAction {
    OperatorApproval,
    OperatorRevocation,
    Canary,
}

/// Closed action-level gate, separate from deployment-readiness reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementGovernedActionBlockReason {
    SettlementWritePolicyDisabled,
    DeploymentNotReady,
    OperatorApprovalAlreadySatisfied,
    OperatorApprovalRequired,
    RuntimeModeNotSemiAuto,
    RuntimeModeWritePolicyMismatch,
    SettlementCaseNotFound,
    SettlementCaseScopeMismatch,
    ManualOnlyInventory,
    ExecutionNotQuiescent,
    SettlementAuthorizationNotApproved,
    CanaryPayoutLimitExceeded,
}

/// Signer-free preflight. A token is present only when every gate is met.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettlementGovernedActionPreflightView {
    pub action: SettlementGovernedAction,
    pub allowed: bool,
    pub blocking_reasons: Vec<SettlementGovernedActionBlockReason>,
    pub scope: Option<SettlementGovernedActionScope>,
    pub preflight_token: Option<ContentHash>,
    pub expires_at: Option<DateTime<Utc>>,
    pub readiness: SettlementReadinessView,
}

/// Complete operator-owned scope signed by the short-lived preflight digest.
///
/// Deployment block observations remain server-owned evidence and are refreshed
/// during apply; every business choice and static deployment identity is echoed
/// back so the apply request cannot widen the preflight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum SettlementGovernedActionScope {
    OperatorApproval {
        execution_account_id: ExecutionAccountId,
        route: SettlementRoute,
        wallet_kind: ExecutionWalletKind,
        target_adapter: EvmAddress,
        deployment_digest: ContentHash,
        deployment_evidence_version: SettlementEvidenceVersion,
        desired_approval: bool,
        expires_at: DateTime<Utc>,
    },
    Canary {
        execution_account_id: ExecutionAccountId,
        route: SettlementRoute,
        wallet_kind: ExecutionWalletKind,
        target_adapter: EvmAddress,
        deployment_digest: ContentHash,
        deployment_evidence_version: SettlementEvidenceVersion,
        settlement_redeem_id: SettlementRedeemId,
        authorization_digest: ContentHash,
        maximum_payout_usd: Usd,
        expires_at: DateTime<Utc>,
    },
}

impl SettlementGovernedActionScope {
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        match self {
            Self::OperatorApproval { expires_at, .. } | Self::Canary { expires_at, .. } => {
                *expires_at
            }
        }
    }
}

/// Exact route-scoped request for an operator approval or revocation preflight.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct SettlementOperatorApprovalPreflightRequest {
    pub route: SettlementRoute,
    pub desired_approval: bool,
}

/// Exact route-scoped request for a controlled canary preflight.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct SettlementCanaryPreflightRequest {
    pub route: SettlementRoute,
    pub settlement_redeem_id: SettlementRedeemId,
    pub maximum_payout_usd: Usd,
}

/// Apply request bound to one server-issued preflight and idempotency key.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct SettlementGovernedActionApplyRequest {
    pub preflight_token: ContentHash,
    pub scope: SettlementGovernedActionScope,
    pub idempotency_key: SettlementActionIdempotencyKey,
    #[validate(length(min = 1, max = 500))]
    pub reason: String,
}

/// CAS revocation of an unconsumed governed action.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct SettlementGovernedActionRevokeRequest {
    pub scope_digest: ContentHash,
    #[validate(length(min = 1, max = 500))]
    pub reason: String,
}

/// Safe operator projection. Raw calldata and signed envelopes stay below the
/// Web boundary and are exposed only through immutable hashes on the submission.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementGovernedActionView {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub execution_account_id: ExecutionAccountId,
    pub settlement_redeem_id: Option<SettlementRedeemId>,
    pub kind: SettlementGovernedActionKind,
    pub state: SettlementGovernedActionState,
    pub route: Option<SettlementRoute>,
    pub target_adapter: Option<EvmAddress>,
    pub deployment_digest: Option<ContentHash>,
    pub deployment_evidence_version: Option<SettlementEvidenceVersion>,
    pub verified_block_number: Option<i64>,
    pub verified_block_hash: Option<EvmBlockHash>,
    pub desired_approval: Option<bool>,
    pub authorization_digest: Option<ContentHash>,
    pub payout_ceiling_usd: Option<Usd>,
    pub scope_digest: ContentHash,
    pub idempotency_key: SettlementActionIdempotencyKey,
    pub authorization_reason: String,
    pub authorized_by: UserId,
    pub revoked_by: Option<UserId>,
    pub revocation_reason: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub authorized_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub failure_code: Option<SettlementFailureCode>,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<SettlementGovernedActionInfo> for SettlementGovernedActionView {
    fn from(info: SettlementGovernedActionInfo) -> Self {
        Self {
            settlement_governed_action_id: info.settlement_governed_action_id,
            execution_account_id: info.execution_account_id,
            settlement_redeem_id: info.settlement_redeem_id,
            kind: info.kind,
            state: info.state,
            route: info.route,
            target_adapter: info.target_adapter,
            deployment_digest: info.deployment_digest,
            deployment_evidence_version: info.deployment_evidence_version,
            verified_block_number: info.verified_block_number,
            verified_block_hash: info.verified_block_hash,
            desired_approval: info.desired_approval,
            authorization_digest: info.authorization_digest,
            payout_ceiling_usd: info.payout_ceiling_usd,
            scope_digest: info.scope_digest,
            idempotency_key: info.idempotency_key,
            authorization_reason: info.authorization_reason,
            authorized_by: info.authorized_by,
            revoked_by: info.revoked_by,
            revocation_reason: info.revocation_reason,
            expires_at: info.expires_at,
            authorized_at: info.authorized_at,
            consumed_at: info.consumed_at,
            revoked_at: info.revoked_at,
            failure_code: info.failure_code,
            retry_count: info.retry_count,
            next_attempt_at: info.next_attempt_at,
            last_error: info.last_error,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SettlementGovernedActionDetailView {
    #[serde(flatten)]
    pub action: SettlementGovernedActionView,
    pub submission: Option<SettlementChainSubmissionView>,
}

/// Paginated governed-action queue filters.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct SettlementGovernedActionListQuery {
    pub kind: Option<SettlementGovernedActionKind>,
    pub state: Option<SettlementGovernedActionState>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Paginated filter for listing settlement redeem batches.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct SettlementRedeemListQuery {
    pub state: Option<SettlementCaseState>,
    pub market_id: Option<MarketId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}
