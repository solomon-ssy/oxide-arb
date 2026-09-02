//! Settlement case, chain-submission, and immutable lot persistence DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{data_plane::DomainEventEnvelope, quant::PositionExit},
    entities::{
        quant_settlement_authorization, quant_settlement_chain_submission,
        quant_settlement_redeem_lot,
    },
    enums::{
        quant::{ExecutionWalletKind, OutcomeSide},
        settlement::{
            SettlementAuthorizationState, SettlementCaseState, SettlementEffectivePolicy,
            SettlementFailureCode, SettlementReadinessStatus, SettlementReconciliationState,
            SettlementRoute, SettlementSubmissionKind, SettlementSubmissionPurpose,
            SettlementSubmissionState,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash, EvmTransactionHash,
        EvmUint256, ExecutionAccountId, MarketId, OrderIntentId, RelayerTransactionId,
        SettlementAuthorizationId, SettlementChainSubmissionId, SettlementEvidenceVersion,
        SettlementGovernedActionId, SettlementRedeemId, SettlementRedeemLotId, Shares,
        StrategyPositionLotId, TokenId, Usd, UserId, WorkerId,
        settlement_payload::{
            SettlementBalanceEvidence, SettlementChainReceiptEvidence, SettlementFailureHistory,
            SettlementPayoutVector, SettlementReadinessEvidence, SettlementReceiptEvidence,
        },
    },
};

const SETTLEMENT_AUTHORIZATION_DOMAIN: &str = "quant-pivot.settlement-authorization";
const SETTLEMENT_AUTHORIZATION_SCHEMA_VERSION: u32 = 1;

/// One recoverable settlement case for a unique `(market, funder)` pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementRedeemInfo {
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub execution_account_id: ExecutionAccountId,
    pub resolution_content_hash: ContentHash,
    pub resolution_outcome: String,
    pub resolved_at: DateTime<Utc>,
    pub funder_address: EvmAddress,
    pub wallet_kind: ExecutionWalletKind,
    pub route: SettlementRoute,
    pub effective_policy: SettlementEffectivePolicy,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub state: SettlementCaseState,
    pub readiness_status: SettlementReadinessStatus,
    pub readiness_evidence_json: SettlementReadinessEvidence,
    pub target_adapter: Option<EvmAddress>,
    pub target_code_hash: Option<EvmCodeHash>,
    pub deployment_digest: Option<ContentHash>,
    pub deployment_evidence_version: Option<SettlementEvidenceVersion>,
    pub verified_block_number: Option<i64>,
    pub verified_block_hash: Option<EvmBlockHash>,
    pub current_authorization_id: Option<SettlementAuthorizationId>,
    pub authorization_state: SettlementAuthorizationState,
    pub authorization_digest: Option<ContentHash>,
    pub authorization_expires_at: Option<DateTime<Utc>>,
    pub authorized_by: Option<UserId>,
    pub authorized_at: Option<DateTime<Utc>>,
    pub authorization_revoked_at: Option<DateTime<Utc>>,
    pub authorization_consumed_at: Option<DateTime<Utc>>,
    pub reconciliation_state: SettlementReconciliationState,
    pub payout_vector_json: SettlementPayoutVector,
    pub balance_before_json: Option<SettlementBalanceEvidence>,
    pub balance_after_json: Option<SettlementBalanceEvidence>,
    pub expected_payout_usd: Option<Usd>,
    pub actual_payout_usd: Option<Usd>,
    pub gas_fee_pol: Option<Decimal>,
    pub failure_code: Option<SettlementFailureCode>,
    pub attempt_count: i32,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub prepared_at: Option<DateTime<Utc>>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Minimal deterministic settlement fact that blocks account-recovery sealing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementRecoveryBlocker {
    pub settlement_redeem_id: SettlementRedeemId,
    pub state: SettlementCaseState,
    pub reconciliation_state: SettlementReconciliationState,
    pub inventory_digest: ContentHash,
    pub updated_at: DateTime<Utc>,
}

/// Insert payload for a newly discovered settlement case.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_redeem::ActiveModel")]
pub struct NewSettlementRedeem {
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub execution_account_id: ExecutionAccountId,
    pub resolution_content_hash: ContentHash,
    pub resolution_outcome: String,
    pub resolved_at: DateTime<Utc>,
    pub route: SettlementRoute,
    pub effective_policy: SettlementEffectivePolicy,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub state: SettlementCaseState,
    pub readiness_status: SettlementReadinessStatus,
    pub readiness_evidence_json: SettlementReadinessEvidence,
    pub target_adapter: Option<EvmAddress>,
    pub target_code_hash: Option<EvmCodeHash>,
    pub deployment_digest: Option<ContentHash>,
    pub deployment_evidence_version: Option<SettlementEvidenceVersion>,
    pub verified_block_number: Option<i64>,
    pub verified_block_hash: Option<EvmBlockHash>,
    pub current_authorization_id: Option<SettlementAuthorizationId>,
    pub reconciliation_state: SettlementReconciliationState,
    pub payout_vector_json: SettlementPayoutVector,
    pub balance_before_json: Option<SettlementBalanceEvidence>,
    pub balance_after_json: Option<SettlementBalanceEvidence>,
    pub expected_payout_usd: Option<Usd>,
    pub actual_payout_usd: Option<Usd>,
    pub gas_fee_pol: Option<Decimal>,
    pub failure_code: Option<SettlementFailureCode>,
    pub attempt_count: i32,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub prepared_at: Option<DateTime<Utc>>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One immutable prepared submission and its recoverable chain identity.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_settlement_chain_submission::Entity")]
pub struct SettlementChainSubmissionInfo {
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
    pub conditional_tokens: EvmAddress,
    pub collateral_token: EvmAddress,
    pub usdce: EvmAddress,
    pub call_target: EvmAddress,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub verified_block_number: i64,
    pub verified_block_hash: EvmBlockHash,
    pub prepared_block_number: Option<i64>,
    pub prepared_block_hash: Option<EvmBlockHash>,
    pub calldata_hash: EvmCalldataHash,
    pub calldata: Vec<u8>,
    pub signed_envelope: Option<Vec<u8>>,
    pub signed_envelope_hash: Option<ContentHash>,
    pub prepared_nonce: Option<EvmUint256>,
    pub gas_limit: Option<EvmUint256>,
    pub relayer_transaction_id: Option<RelayerTransactionId>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub failure_code: Option<SettlementFailureCode>,
    pub failure_history_json: SettlementFailureHistory,
    pub receipt_evidence_json: Option<SettlementChainReceiptEvidence>,
    pub attempt_ordinal: i32,
    pub last_error: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub chain_hash_observed_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Atomic settlement-submission write result with the post-commit case view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementSubmissionOutcome {
    pub redeem: SettlementRedeemInfo,
    pub submission: SettlementChainSubmissionInfo,
}

info_from_model!(
    SettlementChainSubmissionInfo,
    quant_settlement_chain_submission::Model,
    {
        settlement_chain_submission_id, settlement_redeem_id,
        settlement_governed_action_id, canary_action_id, purpose, kind,
        state, route, target_adapter, target_code_hash,
        conditional_tokens, collateral_token, usdce, call_target, deployment_digest,
        deployment_evidence_version, verified_block_number, verified_block_hash,
        prepared_block_number, prepared_block_hash, calldata_hash, calldata,
        signed_envelope, signed_envelope_hash, prepared_nonce, gas_limit,
        relayer_transaction_id, transaction_hash, failure_code,
        failure_history_json, receipt_evidence_json, attempt_ordinal, last_error,
        dispatched_at, chain_hash_observed_at, confirmed_at, created_at, updated_at,
    }
);

/// Insert payload for a durable prepared or externally observed submission.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_chain_submission::ActiveModel")]
pub struct NewSettlementChainSubmission {
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
    pub conditional_tokens: EvmAddress,
    pub collateral_token: EvmAddress,
    pub usdce: EvmAddress,
    pub call_target: EvmAddress,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub verified_block_number: i64,
    pub verified_block_hash: EvmBlockHash,
    pub prepared_block_number: Option<i64>,
    pub prepared_block_hash: Option<EvmBlockHash>,
    pub calldata_hash: EvmCalldataHash,
    pub calldata: Vec<u8>,
    pub signed_envelope: Option<Vec<u8>>,
    pub signed_envelope_hash: Option<ContentHash>,
    pub prepared_nonce: Option<EvmUint256>,
    pub gas_limit: Option<EvmUint256>,
    pub relayer_transaction_id: Option<RelayerTransactionId>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub failure_code: Option<SettlementFailureCode>,
    pub failure_history_json: SettlementFailureHistory,
    pub receipt_evidence_json: Option<SettlementChainReceiptEvidence>,
    pub attempt_ordinal: i32,
    pub last_error: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub chain_hash_observed_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

/// One immutable operator-authorization attempt.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_settlement_authorization::Entity")]
pub struct SettlementAuthorizationInfo {
    pub settlement_authorization_id: SettlementAuthorizationId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub attempt_ordinal: i32,
    pub state: SettlementAuthorizationState,
    pub scope_digest: ContentHash,
    pub staged_by: WorkerId,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<UserId>,
    pub approved_at: Option<DateTime<Utc>>,
    pub revoked_by: Option<UserId>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    SettlementAuthorizationInfo,
    quant_settlement_authorization::Model,
    {
        settlement_authorization_id,
        settlement_redeem_id,
        attempt_ordinal,
        state,
        scope_digest,
        staged_by,
        expires_at,
        approved_by,
        approved_at,
        revoked_by,
        revoked_at,
        consumed_at,
        expired_at,
        created_at,
    }
);

/// Insert payload for one append-only authorization attempt.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_authorization::ActiveModel")]
pub struct NewSettlementAuthorization {
    pub settlement_authorization_id: SettlementAuthorizationId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub attempt_ordinal: i32,
    pub state: SettlementAuthorizationState,
    pub scope_digest: ContentHash,
    pub staged_by: WorkerId,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<UserId>,
    pub approved_at: Option<DateTime<Utc>>,
    pub revoked_by: Option<UserId>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
}

/// Exclusive durable work claim. Recovery claims always carry the active
/// immutable submission; new-submission claims never do.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementWorkClaim {
    pub redeem: SettlementRedeemInfo,
    pub authorization: Option<SettlementAuthorizationInfo>,
    pub active_submission: Option<SettlementChainSubmissionInfo>,
}

/// Canonical, exact scope approved for one operator-authorized settlement batch.
///
/// The expiry and next attempt ordinal are signed into the digest, so approval
/// cannot be reused after either time or lifecycle state advances. Canonical
/// block identity is deliberately excluded: the worker must mint a newer live
/// capability immediately before signing, while the operator authorizes the
/// stable deployment, inventory, and payout scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementAuthorizationScope {
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub funder_address: EvmAddress,
    pub wallet_kind: ExecutionWalletKind,
    pub route: SettlementRoute,
    pub target_adapter: EvmAddress,
    pub target_code_hash: EvmCodeHash,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub payout_vector: SettlementPayoutVector,
    pub balance_before: SettlementBalanceEvidence,
    pub expected_payout_usd: Usd,
    pub attempt_ordinal: i32,
    pub expires_at: DateTime<Utc>,
}

impl SettlementAuthorizationScope {
    /// Domain-separated RFC 8785/BLAKE3 digest shown to and approved by the
    /// operator. Field ordering in Rust does not affect canonical JSON keys.
    pub fn digest(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed(
            SETTLEMENT_AUTHORIZATION_DOMAIN,
            SETTLEMENT_AUTHORIZATION_SCHEMA_VERSION,
            self,
        )
    }
}

/// Transition a prepared operator-authorized case into `Pending` authorization.
#[derive(Debug, Clone)]
pub struct StageSettlementAuthorization {
    pub settlement_redeem_id: SettlementRedeemId,
    pub owner: WorkerId,
    pub digest: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub expected_target_adapter: EvmAddress,
    pub expected_deployment_digest: ContentHash,
    pub staged_at: DateTime<Utc>,
}

/// CAS approval of exactly one pending authorization digest.
#[derive(Debug, Clone)]
pub struct ApproveSettlementAuthorization {
    pub settlement_redeem_id: SettlementRedeemId,
    pub digest: ContentHash,
    pub actor: UserId,
    pub approved_at: DateTime<Utc>,
}

/// CAS revocation of exactly one approved authorization digest.
#[derive(Debug, Clone)]
pub struct RevokeSettlementAuthorization {
    pub settlement_redeem_id: SettlementRedeemId,
    pub digest: ContentHash,
    pub actor: UserId,
    pub revoked_at: DateTime<Utc>,
}

/// Atomic broadcast-before-journal write guarded by the case lease and the
/// frozen authorization/deployment scope.
#[derive(Debug, Clone)]
pub struct PersistPreparedSettlementSubmission {
    pub owner: WorkerId,
    pub expected_authorization_digest: Option<ContentHash>,
    pub expected_canary_action_id: Option<SettlementGovernedActionId>,
    pub submission: NewSettlementChainSubmission,
    pub persisted_at: DateTime<Utc>,
}

/// Final compare-and-swap before any transport receives the durable envelope.
#[derive(Debug, Clone)]
pub struct BeginSettlementDispatch {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub owner: WorkerId,
    pub expected_target_adapter: EvmAddress,
    pub expected_deployment_digest: ContentHash,
    pub expected_calldata_hash: EvmCalldataHash,
    pub expected_signed_envelope_hash: ContentHash,
    pub dispatching_at: DateTime<Utc>,
}

/// Durable acceptance of a direct EOA broadcast. The transaction hash was
/// computed and persisted before broadcast; this transition never accepts a
/// replacement hash from transport.
#[derive(Debug, Clone)]
pub struct RecordEoaSettlementBroadcast {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub owner: WorkerId,
    pub expected_signed_envelope_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
}

/// Durable acceptance of an opaque relayer identity, kept separate from the
/// later EVM transaction hash.
#[derive(Debug, Clone)]
pub struct RecordRelayerSettlementAcceptance {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub owner: WorkerId,
    pub expected_signed_envelope_hash: ContentHash,
    pub relayer_transaction_id: RelayerTransactionId,
    pub observed_at: DateTime<Utc>,
}

/// Per-lot allocation within a confirmed settlement case.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_settlement_redeem_lot::Entity")]
pub struct SettlementRedeemLotInfo {
    pub settlement_redeem_lot_id: SettlementRedeemLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares_redeemed: Shares,
    pub cost_basis_usd: Usd,
    pub payout_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    SettlementRedeemLotInfo,
    quant_settlement_redeem_lot::Model,
    {
        settlement_redeem_lot_id, settlement_redeem_id, strategy_position_lot_id,
        order_intent_id, token_id, side, shares_redeemed, cost_basis_usd,
        payout_usd, realized_pnl_usd, created_at,
    }
);

/// Insert payload for `quant_settlement_redeem_lot`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_redeem_lot::ActiveModel")]
pub struct NewSettlementRedeemLot {
    pub settlement_redeem_lot_id: SettlementRedeemLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares_redeemed: Shares,
    pub cost_basis_usd: Usd,
    pub payout_usd: Usd,
    pub realized_pnl_usd: Usd,
}

/// One position-lot close applied by a confirmed settlement transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementRedeemLotWrite {
    pub lot: NewSettlementRedeemLot,
    pub position_exit: PositionExit,
}

/// Atomic ledger write for a confirmed settlement transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmSettlementRedeem {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub owner: WorkerId,
    pub receipt_evidence_json: SettlementReceiptEvidence,
    pub balance_after_json: SettlementBalanceEvidence,
    pub actual_payout_usd: Usd,
    pub gas_fee_pol: Option<Decimal>,
    pub confirmed_at: DateTime<Utc>,
    pub lots: Vec<SettlementRedeemLotWrite>,
    pub outbox_event: DomainEventEnvelope,
}

/// Durable evidence failure that must not release capital or close lots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequireSettlementReconciliation {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub owner: WorkerId,
    pub failure_code: SettlementFailureCode,
    pub detail: String,
    pub observed_at: DateTime<Utc>,
}

/// Relayer polling discovered the chain identity for an already durable
/// opaque relayer transaction ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordRelayerSettlementChainHash {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: SettlementChainSubmissionId,
    pub owner: WorkerId,
    pub expected_relayer_transaction_id: RelayerTransactionId,
    pub transaction_hash: EvmTransactionHash,
    pub observed_at: DateTime<Utc>,
}

/// Atomic result of one leased, signer-free case preflight.
#[derive(Debug, Clone)]
pub struct PersistSettlementPreflight {
    pub settlement_redeem_id: SettlementRedeemId,
    pub owner: WorkerId,
    pub expected_inventory_digest: ContentHash,
    pub readiness_status: SettlementReadinessStatus,
    pub readiness_evidence: SettlementReadinessEvidence,
    pub target_adapter: Option<EvmAddress>,
    pub target_code_hash: Option<EvmCodeHash>,
    pub deployment_digest: Option<ContentHash>,
    pub deployment_evidence_version: Option<SettlementEvidenceVersion>,
    pub verified_block_number: Option<i64>,
    pub verified_block_hash: Option<EvmBlockHash>,
    pub payout_vector: SettlementPayoutVector,
    pub balance_before: Option<SettlementBalanceEvidence>,
    pub expected_payout_usd: Option<Usd>,
    pub failure_code: Option<SettlementFailureCode>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
}

/// Durable bounded retry without changing an existing submission identity.
#[derive(Debug, Clone)]
pub struct ScheduleSettlementRetry {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: Option<SettlementChainSubmissionId>,
    pub owner: WorkerId,
    pub failure_code: SettlementFailureCode,
    pub detail: String,
    pub next_attempt_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

/// Defer one case or schedule its next identity/receipt poll without recording
/// a business failure or increasing the retry backoff.
#[derive(Debug, Clone)]
pub struct ScheduleSettlementWork {
    pub settlement_redeem_id: SettlementRedeemId,
    pub settlement_chain_submission_id: Option<SettlementChainSubmissionId>,
    pub owner: WorkerId,
    pub next_attempt_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}
