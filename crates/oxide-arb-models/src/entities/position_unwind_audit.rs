//! `position_unwind_audit` table entity.

use crate::{
    enums::fact::UnwindAuditEventType,
    types::{ExitExecutionId, ExitPlanId, PositionId, UnwindAuditId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "position_unwind_audit")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub unwind_audit_id: UnwindAuditId,
    pub position_id: PositionId,
    pub exit_plan_id: Option<ExitPlanId>,
    pub exit_execution_id: Option<ExitExecutionId>,
    pub event_type: UnwindAuditEventType,
    #[sea_orm(column_type = "JsonBinary")]
    pub before_position: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub after_position: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub book_context: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub token_balance_context: Json,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    #[sea_orm(column_type = "Text")]
    pub actor: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
