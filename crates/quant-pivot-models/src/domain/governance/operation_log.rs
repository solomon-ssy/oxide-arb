//! Operation-log DTOs (append-only activity trail).

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, entity::prelude::IpNetwork};
use serde::{Deserialize, Serialize};

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

/// Insert payload for one operation-log row.
///
/// `occurred_at` is intentionally omitted — the database write-default is the
/// single source of truth for the timestamp. `detail` must already be redacted
/// (never contains passwords, tokens, or PII).
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::operation_log::ActiveModel")]
pub struct NewOperationLog {
    pub id: OperationLogId,
    pub request_id: CorrelationId,
    pub actor_user_id: Option<UserId>,
    pub actor_username: Option<String>,
    pub acting_role: Option<RoleCode>,
    pub category: OperationCategory,
    pub action: OperationAction,
    pub resource_type: Option<ResourceType>,
    pub resource_id: Option<String>,
    pub http_method: OperationHttpMethod,
    pub http_path: String,
    pub http_status: i16,
    pub outcome: OperationOutcome,
    pub client_ip: Option<IpNetwork>,
    pub user_agent: Option<String>,
    pub latency_ms: i32,
    pub detail: OperationDetailDocument,
    pub before_hash: Option<ContentHash>,
    pub after_hash: Option<ContentHash>,
    pub governance_audit_event_id: Option<AuditEventId>,
    pub governance_audit_sequence: Option<i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::operation_log::Entity")]
pub struct OperationLogInfo {
    pub id: OperationLogId,
    pub occurred_at: DateTime<Utc>,
    pub request_id: CorrelationId,
    pub actor_user_id: Option<UserId>,
    pub actor_username: Option<String>,
    pub acting_role: Option<RoleCode>,
    pub category: OperationCategory,
    pub action: OperationAction,
    pub resource_type: Option<ResourceType>,
    pub resource_id: Option<String>,
    pub http_method: OperationHttpMethod,
    pub http_path: String,
    pub http_status: i16,
    pub outcome: OperationOutcome,
    pub client_ip: Option<IpNetwork>,
    pub user_agent: Option<String>,
    pub latency_ms: i32,
    pub detail: OperationDetailDocument,
    pub before_hash: Option<ContentHash>,
    pub after_hash: Option<ContentHash>,
    pub governance_audit_event_id: Option<AuditEventId>,
    pub governance_audit_sequence: Option<i64>,
}

info_from_model!(OperationLogInfo, crate::entities::operation_log::Model, {
    id, occurred_at, request_id, actor_user_id, actor_username, acting_role,
    category, action, resource_type, resource_id, http_method, http_path,
    http_status, outcome, client_ip, user_agent, latency_ms, detail,
    before_hash, after_hash, governance_audit_event_id, governance_audit_sequence,
});
