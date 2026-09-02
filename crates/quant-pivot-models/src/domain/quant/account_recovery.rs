//! Account recovery incident and chain-execution association contracts.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use super::strategy_position_lot::NewStrategyPositionLot;

use crate::{
    entities::{
        quant_account_clean_funder_blocker, quant_account_execution_association,
        quant_account_recovery_incident, quant_account_recovery_manifest,
    },
    enums::execution::{
        AccountChainExecutionRole, AccountExecutionAssociationKind, AccountRecoveryIncidentKind,
        AccountRecoveryIncidentStatus,
    },
    types::{
        AccountChainExecutionId, AccountRecoveryIncidentId, AccountRecoveryManifestId, ContentHash,
        EvmBlockHash, ExecutionAccountId, ExecutionOrderId, OrderId, Shares, StrategyPositionLotId,
        TokenId, Usd, UserId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel, JsonSchema)]
#[sea_orm(entity = "quant_account_recovery_incident::Entity")]
pub struct AccountRecoveryIncidentInfo {
    pub account_recovery_incident_id: AccountRecoveryIncidentId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: AccountRecoveryIncidentKind,
    pub status: AccountRecoveryIncidentStatus,
    pub trigger_chain_execution_id: Option<AccountChainExecutionId>,
    pub reason: String,
    pub opened_at: DateTime<Utc>,
    pub seal_hash: Option<ContentHash>,
    pub sealed_by: Option<UserId>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    AccountRecoveryIncidentInfo,
    quant_account_recovery_incident::Model,
    {
        account_recovery_incident_id,
        execution_account_id,
        kind,
        status,
        trigger_chain_execution_id,
        reason,
        opened_at,
        seal_hash,
        sealed_by,
        sealed_at,
        revision,
        created_at,
        updated_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_recovery_incident::ActiveModel")]
pub struct NewAccountRecoveryIncident {
    pub account_recovery_incident_id: AccountRecoveryIncidentId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: AccountRecoveryIncidentKind,
    pub status: AccountRecoveryIncidentStatus,
    pub trigger_chain_execution_id: Option<AccountChainExecutionId>,
    pub reason: String,
    pub opened_at: DateTime<Utc>,
    pub seal_hash: Option<ContentHash>,
    pub sealed_by: Option<UserId>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_account_execution_association::Entity")]
pub struct AccountExecutionAssociationInfo {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub kind: AccountExecutionAssociationKind,
    pub execution_order_id: Option<ExecutionOrderId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub evidence_hash: ContentHash,
    pub associated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    AccountExecutionAssociationInfo,
    quant_account_execution_association::Model,
    {
        account_chain_execution_id,
        kind,
        execution_order_id,
        recovery_incident_id,
        evidence_hash,
        associated_at,
        created_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_execution_association::ActiveModel")]
pub struct NewAccountExecutionAssociation {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub kind: AccountExecutionAssociationKind,
    pub execution_order_id: Option<ExecutionOrderId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub evidence_hash: ContentHash,
    pub associated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountExecutionAssociationOutcome {
    pub association: AccountExecutionAssociationInfo,
    pub incident: Option<AccountRecoveryIncidentInfo>,
    pub incident_created: bool,
}

/// One venue or finalized-chain token balance used by account recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountRecoveryTokenBalance {
    pub token_id: TokenId,
    pub shares: Shares,
}

/// Open internal lot state before incident execution allocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountRecoveryLotBalance {
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub token_id: TokenId,
    pub shares: Shares,
    pub cost_usd: Usd,
    pub opened_at: DateTime<Utc>,
}

/// Deterministic post-recovery quantity assigned to one existing lot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountRecoveryLotAllocation {
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub token_id: TokenId,
    pub before_shares: Shares,
    pub after_shares: Shares,
    pub before_cost_usd: Usd,
    pub after_cost_usd: Usd,
    pub realized_pnl_delta_usd: Usd,
    pub closed_at: Option<DateTime<Utc>>,
}

/// Remaining shares from one incident BUY that must become a recovery-origin lot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountRecoveryCreatedLot {
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub account_chain_execution_id: AccountChainExecutionId,
    pub token_id: TokenId,
    pub acquired_shares: Shares,
    pub remaining_shares: Shares,
    pub acquired_cost_usd: Usd,
    pub remaining_cost_usd: Usd,
    pub realized_pnl_delta_usd: Usd,
    pub closed_at: Option<DateTime<Utc>>,
}

/// Finalized incident execution needed to explain a position delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountRecoveryExecutionDelta {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub token_id: TokenId,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub shares_delta: Decimal,
    pub principal_usd: Usd,
    pub exact_fee_usd: Usd,
    pub available_at: DateTime<Utc>,
}

/// Operator-owned allocation for one ambiguous external SELL across open lots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountRecoverySellAllocation {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub shares: Shares,
}

/// Typed reason an account recovery manifest cannot be sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountRecoveryMismatch {
    PauseIncomplete,
    VenueSnapshotUnstable,
    OpenOrdersPresent {
        order_ids: Vec<OrderId>,
    },
    ReservedCapitalPresent {
        reserved_usd: Usd,
    },
    CollateralMismatch {
        clob_usd: Usd,
        chain_usd: Usd,
    },
    PositionSourceMismatch {
        token_id: TokenId,
        data_api_shares: Shares,
        chain_shares: Shares,
    },
    PositionLedgerMismatch {
        token_id: TokenId,
        expected_shares: Shares,
        venue_shares: Shares,
    },
    TokenMetadataMissing {
        token_id: TokenId,
    },
    IncidentExecutionIncomplete {
        account_chain_execution_id: AccountChainExecutionId,
    },
    LotAllocationRequired {
        account_chain_execution_id: AccountChainExecutionId,
        token_id: TokenId,
        sold_shares: Shares,
        candidate_lot_ids: Vec<StrategyPositionLotId>,
    },
    LotAllocationInvalid {
        account_chain_execution_id: AccountChainExecutionId,
    },
    PendingSettlement {
        count: u64,
    },
    CleanFunderRequired {
        account_chain_execution_id: AccountChainExecutionId,
        role: AccountChainExecutionRole,
    },
}

/// Fully materialized, source-independent input to deterministic recovery assessment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountRecoveryAssessmentInput {
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub execution_account_id: ExecutionAccountId,
    pub observed_at: DateTime<Utc>,
    pub finalized_block_number: i64,
    pub finalized_block_hash: EvmBlockHash,
    pub clob_snapshot_hash: ContentHash,
    pub data_api_snapshot_hash: ContentHash,
    pub chain_snapshot_hash: ContentHash,
    pub settlement_snapshot_hash: ContentHash,
    pub pause_confirmed: bool,
    pub venue_snapshot_stable: bool,
    pub clob_collateral_usd: Usd,
    pub chain_collateral_usd: Usd,
    pub reserved_usd: Usd,
    pub open_order_ids: Vec<OrderId>,
    pub unmapped_token_ids: Vec<TokenId>,
    pub invalid_execution_ids: Vec<AccountChainExecutionId>,
    pub clean_funder_blocker: Option<AccountCleanFunderBlockerEvidence>,
    pub data_api_positions: Vec<AccountRecoveryTokenBalance>,
    pub chain_positions: Vec<AccountRecoveryTokenBalance>,
    pub open_lots: Vec<AccountRecoveryLotBalance>,
    pub incident_executions: Vec<AccountRecoveryExecutionDelta>,
    pub explicit_sell_allocations: Vec<AccountRecoverySellAllocation>,
    pub pending_settlement_count: u64,
}

/// Deterministic assessment persisted as the account recovery manifest payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountRecoveryAssessment {
    pub allocations: Vec<AccountRecoveryLotAllocation>,
    pub created_lots: Vec<AccountRecoveryCreatedLot>,
    pub mismatches: Vec<AccountRecoveryMismatch>,
    pub evidence_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_account_recovery_manifest::Entity")]
pub struct AccountRecoveryManifestInfo {
    pub account_recovery_manifest_id: AccountRecoveryManifestId,
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub attempt_no: i32,
    pub observed_at: DateTime<Utc>,
    pub finalized_block_number: i64,
    pub finalized_block_hash: EvmBlockHash,
    pub converged: bool,
    pub input_json: AccountRecoveryAssessmentInput,
    pub assessment_json: AccountRecoveryAssessment,
    pub created_lots_json: AccountRecoveryCreatedLots,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    AccountRecoveryManifestInfo,
    quant_account_recovery_manifest::Model,
    {
        account_recovery_manifest_id,
        recovery_incident_id,
        attempt_no,
        observed_at,
        finalized_block_number,
        finalized_block_hash,
        converged,
        input_json,
        assessment_json,
        created_lots_json,
        evidence_hash,
        created_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_recovery_manifest::ActiveModel")]
pub struct NewAccountRecoveryManifest {
    pub account_recovery_manifest_id: AccountRecoveryManifestId,
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub attempt_no: i32,
    pub observed_at: DateTime<Utc>,
    pub finalized_block_number: i64,
    pub finalized_block_hash: EvmBlockHash,
    pub converged: bool,
    pub input_json: AccountRecoveryAssessmentInput,
    pub assessment_json: AccountRecoveryAssessment,
    pub created_lots_json: AccountRecoveryCreatedLots,
    pub evidence_hash: ContentHash,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct AccountRecoveryCreatedLots(pub Vec<NewStrategyPositionLot>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountRecoveryManifestDraft {
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub input: AccountRecoveryAssessmentInput,
    pub assessment: AccountRecoveryAssessment,
    pub created_lots: Vec<NewStrategyPositionLot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealAccountRecoveryIncident {
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub account_recovery_manifest_id: AccountRecoveryManifestId,
    pub expected_revision: i64,
    pub actor: UserId,
    pub sealed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeAccountRecoveryIncident {
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub expected_revision: i64,
    pub finalized_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountCleanFunderBlockerEvidence {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub role: AccountChainExecutionRole,
    pub evidence_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_account_clean_funder_blocker::Entity")]
pub struct AccountCleanFunderBlockerInfo {
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub account_chain_execution_id: AccountChainExecutionId,
    pub role: AccountChainExecutionRole,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    AccountCleanFunderBlockerInfo,
    quant_account_clean_funder_blocker::Model,
    {
        recovery_incident_id,
        account_chain_execution_id,
        role,
        evidence_hash,
        created_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_clean_funder_blocker::ActiveModel")]
pub struct NewAccountCleanFunderBlocker {
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub account_chain_execution_id: AccountChainExecutionId,
    pub role: AccountChainExecutionRole,
    pub evidence_hash: ContentHash,
}

impl AccountRecoveryAssessment {
    #[must_use]
    pub const fn converged(&self) -> bool {
        self.mismatches.is_empty()
    }
}
