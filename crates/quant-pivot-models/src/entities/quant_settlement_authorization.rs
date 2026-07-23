//! `quant_settlement_authorization` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_settlement_redeem, user};
use crate::{
    enums::settlement::SettlementAuthorizationState,
    types::{ContentHash, SettlementAuthorizationId, SettlementRedeemId, UserId, WorkerId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_authorization")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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

    #[sea_orm(
        belongs_to,
        relation_enum = "SettlementRedeem",
        from = "settlement_redeem_id",
        to = "settlement_redeem_id"
    )]
    pub settlement_redeem: BelongsTo<quant_settlement_redeem::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ApprovedBy",
        from = "approved_by",
        to = "id"
    )]
    pub approved_by_user: BelongsTo<Option<user::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RevokedBy",
        from = "revoked_by",
        to = "id"
    )]
    pub revoked_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
