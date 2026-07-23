//! `quant_settlement_governed_action` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_execution_account, quant_settlement_redeem, user};
use crate::{
    enums::settlement::{
        SettlementFailureCode, SettlementGovernedActionKind, SettlementGovernedActionState,
        SettlementRoute,
    },
    types::{
        ContentHash, EvmAddress, EvmBlockHash, ExecutionAccountId, SettlementActionIdempotencyKey,
        SettlementEvidenceVersion, SettlementGovernedActionId, SettlementRedeemId, Usd, UserId,
        WorkerId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_governed_action")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    #[sea_orm(column_type = "Text")]
    pub authorization_reason: String,
    pub authorized_by: UserId,
    pub revoked_by: Option<UserId>,
    #[sea_orm(column_type = "Text", nullable)]
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
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "SettlementRedeem",
        from = "settlement_redeem_id",
        to = "settlement_redeem_id"
    )]
    pub settlement_redeem: BelongsTo<Option<quant_settlement_redeem::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "AuthorizedBy",
        from = "authorized_by",
        to = "id"
    )]
    pub authorized_by_user: BelongsTo<user::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RevokedBy",
        from = "revoked_by",
        to = "id"
    )]
    pub revoked_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
