//! `control_factor_audit_event` table entity (append-only global hash chain).

use crate::{
    enums::control_factor::{AuditResourceType, ControlAuditEventType, OperatorRole},
    types::AuditEventId,
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_audit_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: AuditEventId,
    pub sequence: i64,
    pub event_type: ControlAuditEventType,
    #[sea_orm(column_type = "Text")]
    pub actor: String,
    pub actor_role: OperatorRole,
    pub resource_type: AuditResourceType,
    #[sea_orm(column_type = "Text")]
    pub resource_id: String,
    #[sea_orm(column_type = "Text")]
    pub request_id: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub before_hash: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub after_hash: Option<String>,
    #[sea_orm(column_type = "JsonBinary")]
    pub diff: Json,
    #[sea_orm(column_type = "Text", nullable)]
    pub prev_event_hash: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub event_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
