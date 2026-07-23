//! Governed settlement actions and external-chain observation cursors.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::{quant_settlement_external_cursor, quant_settlement_governed_action},
    enums::settlement::{
        SettlementFailureCode, SettlementGovernedActionKind, SettlementGovernedActionState,
        SettlementRoute,
    },
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash, EvmTransactionHash,
        ExecutionAccountId, RelayerTransactionId, SettlementActionIdempotencyKey,
        SettlementChainSubmissionId, SettlementEvidenceVersion, SettlementExternalCursorId,
        SettlementGovernedActionId, SettlementRedeemId, Usd, UserId, WorkerId,
        settlement_payload::SettlementChainReceiptEvidence,
    },
};

use super::settlement::{NewSettlementChainSubmission, SettlementChainSubmissionInfo};

/// Immutable governed-action scope plus its recoverable execution lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_settlement_governed_action::Entity")]
pub struct SettlementGovernedActionInfo {
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
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    SettlementGovernedActionInfo,
    quant_settlement_governed_action::Model,
    {
        settlement_governed_action_id, execution_account_id, settlement_redeem_id,
        kind, state, route, target_adapter, deployment_digest,
        deployment_evidence_version, verified_block_number, verified_block_hash,
        desired_approval, authorization_digest, payout_ceiling_usd, scope_digest, idempotency_key,
        authorization_reason, authorized_by, revoked_by, revocation_reason, expires_at,
        authorized_at, consumed_at, revoked_at, failure_code, retry_count,
        claim_owner, lease_expires_at, next_attempt_at, last_error, created_at,
        updated_at,
    }
);

/// Insert payload for one exact, append-only governed action request.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_governed_action::ActiveModel")]
pub struct NewSettlementGovernedAction {
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
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

/// Durable scan cursor pinned to one account and deployment fingerprint.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_settlement_external_cursor::Entity")]
pub struct SettlementExternalCursorInfo {
    pub settlement_external_cursor_id: SettlementExternalCursorId,
    pub execution_account_id: ExecutionAccountId,
    pub chain_id: i64,
    pub route: SettlementRoute,
    pub target_adapter: EvmAddress,
    pub target_code_hash: EvmCodeHash,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub next_block_number: i64,
    pub last_observed_block_number: Option<i64>,
    pub last_observed_block_hash: Option<EvmBlockHash>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    SettlementExternalCursorInfo,
    quant_settlement_external_cursor::Model,
    {
        settlement_external_cursor_id, execution_account_id, chain_id, route,
        target_adapter, target_code_hash, deployment_digest,
        deployment_evidence_version, next_block_number, last_observed_block_number,
        last_observed_block_hash, updated_at,
    }
);

/// Initial external scanner position for one exact deployment identity.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_external_cursor::ActiveModel")]
pub struct NewSettlementExternalCursor {
    pub settlement_external_cursor_id: SettlementExternalCursorId,
    pub execution_account_id: ExecutionAccountId,
    pub chain_id: i64,
    pub route: SettlementRoute,
    pub target_adapter: EvmAddress,
    pub target_code_hash: EvmCodeHash,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub next_block_number: i64,
    pub last_observed_block_number: Option<i64>,
    pub last_observed_block_hash: Option<EvmBlockHash>,
}

/// Compare-and-swap advancement after a canonical block range is persisted.
#[derive(Debug, Clone)]
pub struct AdvanceSettlementExternalCursor {
    pub settlement_external_cursor_id: SettlementExternalCursorId,
    pub expected_next_block_number: i64,
    pub next_block_number: i64,
    pub last_observed_block_number: i64,
    pub last_observed_block_hash: EvmBlockHash,
}

/// Atomically journal all matched external submissions and advance the exact
/// canonical cursor range. An empty submission list is valid.
#[derive(Debug, Clone)]
pub struct PersistExternalSettlementScan {
    pub cursor: AdvanceSettlementExternalCursor,
    pub submissions: Vec<NewSettlementChainSubmission>,
    pub observed_at: DateTime<Utc>,
}

/// One leased governed action plus its optional durable submission.
#[derive(Debug, Clone)]
pub struct SettlementGovernedActionWorkClaim {
    pub action: SettlementGovernedActionInfo,
    pub submission: Option<SettlementChainSubmissionInfo>,
}

/// Atomic journal write before any governed-action transport call.
#[derive(Debug, Clone)]
pub struct PersistPreparedGovernedActionSubmission {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub expected_scope_digest: ContentHash,
    pub owner: WorkerId,
    pub submission: NewSettlementChainSubmission,
    pub persisted_at: DateTime<Utc>,
}

/// Exact prepared-envelope CAS immediately before dispatch.
#[derive(Debug, Clone)]
pub struct BeginGovernedActionDispatch {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub expected_scope_digest: ContentHash,
    pub expected_target_adapter: EvmAddress,
    pub expected_deployment_digest: ContentHash,
    pub expected_calldata_hash: EvmCalldataHash,
    pub expected_signed_envelope_hash: ContentHash,
    pub owner: WorkerId,
    pub dispatching_at: DateTime<Utc>,
}

/// Durable direct-EOA broadcast transition.
#[derive(Debug, Clone)]
pub struct RecordGovernedActionEoaBroadcast {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub expected_signed_envelope_hash: ContentHash,
    pub owner: WorkerId,
    pub observed_at: DateTime<Utc>,
}

/// Durable relayer acceptance transition.
#[derive(Debug, Clone)]
pub struct RecordGovernedActionRelayerAcceptance {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub expected_signed_envelope_hash: ContentHash,
    pub relayer_transaction_id: RelayerTransactionId,
    pub owner: WorkerId,
    pub observed_at: DateTime<Utc>,
}

/// Bind a polled EVM hash to the exact opaque relayer identity.
#[derive(Debug, Clone)]
pub struct RecordGovernedActionRelayerChainHash {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub expected_relayer_transaction_id: RelayerTransactionId,
    pub transaction_hash: EvmTransactionHash,
    pub owner: WorkerId,
    pub observed_at: DateTime<Utc>,
}

/// Bounded retry for a pre-submission governed action.
#[derive(Debug, Clone)]
pub struct ScheduleGovernedActionRetry {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub expected_scope_digest: ContentHash,
    pub owner: WorkerId,
    pub failure_code: SettlementFailureCode,
    pub next_attempt_at: DateTime<Utc>,
    pub last_error: String,
    pub scheduled_at: DateTime<Utc>,
}

/// Defer polling of an existing governed-action identity without classifying
/// normal receipt/finality latency as a retry failure.
#[derive(Debug, Clone)]
pub struct ScheduleGovernedActionWork {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub expected_scope_digest: ContentHash,
    pub owner: WorkerId,
    pub next_attempt_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
}

/// Terminal pre-submission failure. No durable transport identity exists, so
/// the action can be closed without fabricating a chain submission.
#[derive(Debug, Clone)]
pub struct FailSettlementGovernedAction {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub expected_scope_digest: ContentHash,
    pub owner: WorkerId,
    pub failure_code: SettlementFailureCode,
    pub last_error: String,
    pub failed_at: DateTime<Utc>,
}

/// Finalized operator-approval or revocation proof.
#[derive(Debug, Clone)]
pub struct ConfirmSettlementGovernedAction {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub expected_scope_digest: ContentHash,
    pub owner: WorkerId,
    pub receipt_evidence: SettlementChainReceiptEvidence,
    pub confirmed_at: DateTime<Utc>,
}

/// Durable business-evidence failure; no retry may create another call.
#[derive(Debug, Clone)]
pub struct RequireGovernedActionReconciliation {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub expected_scope_digest: ContentHash,
    pub owner: WorkerId,
    pub failure_code: SettlementFailureCode,
    pub last_error: String,
    pub observed_at: DateTime<Utc>,
}

/// Operator revocation command for an unconsumed action.
#[derive(Debug, Clone)]
pub struct RevokeSettlementGovernedAction {
    pub settlement_governed_action_id: SettlementGovernedActionId,
    pub expected_scope_digest: ContentHash,
    pub actor: UserId,
    pub reason: String,
    pub revoked_at: DateTime<Utc>,
}
