//! `position_exit_execution` table entity.

use crate::{
    enums::fact::{ExitExecutionOutcome, ExitOrderType},
    types::{ExitExecutionId, ExitPlanId, OrderId, Price, Shares, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "position_exit_execution")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub exit_execution_id: ExitExecutionId,
    pub exit_plan_id: ExitPlanId,
    pub order_id: Option<OrderId>,
    pub order_type: ExitOrderType,
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub avg_exit_price: Option<Price>,
    pub fee_usd: Usd,
    pub realized_exit_pnl_usd: Usd,
    pub outcome: ExitExecutionOutcome,
    #[sea_orm(column_type = "Text", nullable)]
    pub failure_reason: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
