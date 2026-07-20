//! `operation_log` table entity (append-only activity log).

use crate::{
    enums::{
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        rbac::ResourceType,
    },
    types::{
        AuditEventId, ContentHash, CorrelationId, OperationAction, OperationDetailDocument,
        OperationLogId, RoleCode, UserId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "operation_log")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: OperationLogId,
    pub occurred_at: DateTime<Utc>,
    pub request_id: CorrelationId,
    pub actor_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text", nullable)]
    pub actor_username: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub acting_role: Option<RoleCode>,
    pub category: OperationCategory,
    pub action: OperationAction,
    pub resource_type: Option<ResourceType>,
    #[sea_orm(column_type = "Text", nullable)]
    pub resource_id: Option<String>,
    pub http_method: OperationHttpMethod,
    #[sea_orm(column_type = "Text")]
    pub http_path: String,
    pub http_status: i16,
    pub outcome: OperationOutcome,
    pub client_ip: Option<IpNetwork>,
    #[sea_orm(column_type = "Text", nullable)]
    pub user_agent: Option<String>,
    pub latency_ms: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub detail: OperationDetailDocument,
    pub before_hash: Option<ContentHash>,
    pub after_hash: Option<ContentHash>,
    pub governance_audit_event_id: Option<AuditEventId>,
    pub governance_audit_sequence: Option<i64>,
}

impl ActiveModelBehavior for ActiveModel {}
