//! `operation_log` table entity (append-only activity log).

use crate::{
    enums::{
        operation_log::{OperationCategory, OperationOutcome},
        rbac::ResourceType,
    },
    types::{AuditEventId, OperationLogId, UserId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "operation_log")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: OperationLogId,
    pub occurred_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text")]
    pub request_id: String,
    pub actor_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text", nullable)]
    pub actor_username: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub acting_role: Option<String>,
    pub category: OperationCategory,
    #[sea_orm(column_type = "Text")]
    pub action: String,
    pub resource_type: Option<ResourceType>,
    #[sea_orm(column_type = "Text", nullable)]
    pub resource_id: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub http_method: String,
    #[sea_orm(column_type = "Text")]
    pub http_path: String,
    pub http_status: i16,
    pub outcome: OperationOutcome,
    #[sea_orm(column_type = "Text", nullable)]
    pub client_ip: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub user_agent: Option<String>,
    pub latency_ms: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub detail: Json,
    pub governance_audit_event_id: Option<AuditEventId>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
