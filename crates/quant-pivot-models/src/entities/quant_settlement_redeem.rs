//! `quant_settlement_redeem` table entity.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use super::{
    market, quant_execution_account, quant_settlement_authorization,
    quant_settlement_chain_submission, quant_settlement_governed_action,
    quant_settlement_inventory_lot, quant_settlement_redeem_lot,
};
use crate::{
    enums::settlement::{
        SettlementCaseState, SettlementEffectivePolicy, SettlementFailureCode,
        SettlementReadinessStatus, SettlementReconciliationState, SettlementRoute,
    },
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCodeHash, ExecutionAccountId, MarketId,
        SettlementAuthorizationId, SettlementEvidenceVersion, SettlementRedeemId, TokenId, Usd,
        WorkerId,
        settlement_payload::{
            SettlementBalanceEvidence, SettlementPayoutVector, SettlementReadinessEvidence,
        },
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_redeem")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub execution_account_id: ExecutionAccountId,
    pub resolution_content_hash: ContentHash,
    #[sea_orm(column_type = "Text")]
    pub resolution_outcome: String,
    pub resolved_at: DateTime<Utc>,
    pub route: SettlementRoute,
    pub effective_policy: SettlementEffectivePolicy,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub state: SettlementCaseState,
    pub readiness_status: SettlementReadinessStatus,
    #[sea_orm(column_type = "JsonBinary")]
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
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub prepared_at: Option<DateTime<Utc>>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "RedeemLot")]
    pub redeem_lot: HasMany<quant_settlement_redeem_lot::Entity>,
    #[sea_orm(has_many, relation_enum = "ChainSubmission")]
    pub chain_submission: HasMany<quant_settlement_chain_submission::Entity>,
    #[sea_orm(has_many, relation_enum = "Authorization")]
    pub authorization: HasMany<quant_settlement_authorization::Entity>,
    #[sea_orm(has_many, relation_enum = "GovernedAction")]
    pub governed_action: HasMany<quant_settlement_governed_action::Entity>,
    #[sea_orm(has_many, relation_enum = "InventoryLot")]
    pub inventory_lot: HasMany<quant_settlement_inventory_lot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<market::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CurrentAuthorization",
        from = "current_authorization_id",
        to = "settlement_authorization_id"
    )]
    pub current_authorization: BelongsTo<Option<quant_settlement_authorization::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
