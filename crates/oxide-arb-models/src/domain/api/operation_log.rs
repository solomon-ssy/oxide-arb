//! Operation-log API contract (outbound read projection).
//!
//! The append-only operation log carries no credentials, but it still flows out
//! through a dedicated `*View` (per the DTO paradigm) so the wire contract is
//! decoupled from the persistence projection and stays a single, discoverable
//! source of truth for clients / `OpenAPI` generation.

use crate::{
    domain::{OperationLogInfo, pagination::PageRequest},
    enums::{
        operation_log::{OperationCategory, OperationOutcome},
        rbac::ResourceType,
    },
    types::{AuditEventId, OperationLogId, UserId},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Outbound view of one append-only operation-log row.
#[derive(Debug, Serialize)]
pub struct OperationLogView {
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
    /// Redacted detail summary / diff stamped by the handler (never raw bodies).
    pub detail: serde_json::Value,
    /// Hard link to the governance hash-chain event, when this row mirrors one.
    pub governance_audit_event_id: Option<AuditEventId>,
}

impl From<OperationLogInfo> for OperationLogView {
    fn from(info: OperationLogInfo) -> Self {
        Self {
            id: info.id,
            occurred_at: info.occurred_at,
            request_id: info.request_id,
            actor_user_id: info.actor_user_id,
            actor_username: info.actor_username,
            acting_role: info.acting_role,
            category: info.category,
            action: info.action,
            resource_type: info.resource_type,
            resource_id: info.resource_id,
            http_method: info.http_method,
            http_path: info.http_path,
            http_status: info.http_status,
            outcome: info.outcome,
            client_ip: info.client_ip,
            user_agent: info.user_agent,
            latency_ms: info.latency_ms,
            detail: info.detail,
            governance_audit_event_id: info.governance_audit_event_id,
        }
    }
}

/// Pagination + filter parameters for querying the operation log.
///
/// The pagination window is the shared [`PageRequest`], flattened so the query
/// string stays flat alongside the filters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperationLogQuery {
    pub actor_user_id: Option<UserId>,
    pub category: Option<OperationCategory>,
    pub resource_type: Option<ResourceType>,
    pub outcome: Option<OperationOutcome>,
    pub request_id: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl OperationLogQuery {
    /// Return a copy with the embedded pagination window normalized.
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
}
