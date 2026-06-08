//! Operation-log DTOs (append-only activity trail).

use crate::{
    enums::{
        operation_log::{OperationCategory, OperationOutcome},
        rbac::ResourceType,
    },
    types::{AuditEventId, OperationLogId, UserId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Insert payload for one operation-log row.
///
/// `occurred_at` is intentionally omitted — the database write-default is the
/// single source of truth for the timestamp. `detail` must already be redacted
/// (never contains passwords, tokens, or PII).
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::operation_log::ActiveModel")]
pub struct NewOperationLog {
    pub id: OperationLogId,
    pub request_id: String,
    pub actor_user_id: Option<UserId>,
    pub actor_username: Option<String>,
    pub acting_role: Option<String>,
    pub category: OperationCategory,
    pub action: String,
    pub resource_type: Option<ResourceType>,
    pub resource_id: Option<String>,
    pub http_method: String,
    pub http_path: String,
    pub http_status: i16,
    pub outcome: OperationOutcome,
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
    pub latency_ms: i32,
    pub detail: serde_json::Value,
    pub governance_audit_event_id: Option<AuditEventId>,
}

/// DB row projection for the `operation_log` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::operation_log::Entity")]
pub struct OperationLogInfo {
    pub id: OperationLogId,
    pub occurred_at: DateTime<Utc>,
    pub request_id: String,
    pub actor_user_id: Option<UserId>,
    pub actor_username: Option<String>,
    pub acting_role: Option<String>,
    pub category: OperationCategory,
    pub action: String,
    pub resource_type: Option<ResourceType>,
    pub resource_id: Option<String>,
    pub http_method: String,
    pub http_path: String,
    pub http_status: i16,
    pub outcome: OperationOutcome,
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
    pub latency_ms: i32,
    pub detail: serde_json::Value,
    pub governance_audit_event_id: Option<AuditEventId>,
}

info_from_model!(OperationLogInfo, crate::entities::operation_log::Model, {
    id, occurred_at, request_id, actor_user_id, actor_username, acting_role,
    category, action, resource_type, resource_id, http_method, http_path,
    http_status, outcome, client_ip, user_agent, latency_ms, detail,
    governance_audit_event_id,
});

/// Pagination + filter parameters for querying the operation log.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperationLogQuery {
    pub actor_user_id: Option<UserId>,
    pub category: Option<OperationCategory>,
    pub resource_type: Option<ResourceType>,
    pub outcome: Option<OperationOutcome>,
    pub request_id: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub page: u64,
    pub size: u64,
}
