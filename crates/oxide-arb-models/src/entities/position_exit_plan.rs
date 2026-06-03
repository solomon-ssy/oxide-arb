//! `position_exit_plan` table entity.

use crate::{
    enums::fact::{ExitAction, ExitPlanStatus, ExitTriggerType},
    types::{ExitPlanId, MarketId, PositionId, Price, Shares, TokenId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "position_exit_plan")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub exit_plan_id: ExitPlanId,
    pub position_id: PositionId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub trigger_type: ExitTriggerType,
    pub action: ExitAction,
    pub target_shares: Shares,
    pub min_exit_price: Price,
    #[sea_orm(column_type = "JsonBinary")]
    pub reason: Json,
    #[sea_orm(column_type = "Text")]
    pub policy_version: String,
    #[sea_orm(column_type = "Text")]
    pub created_by: String,
    pub status: ExitPlanStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
