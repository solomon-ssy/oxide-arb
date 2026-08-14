//! Permission-scoped, cursor-paginated Activity Center contract.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::runtime_activity::{
    RuntimeActivityActionKind, RuntimeActivityDomain, RuntimeActivityStatus,
};

/// Untrusted Activity Center query parameters.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RuntimeActivityListQuery {
    pub domain: Option<RuntimeActivityDomain>,
    pub status: Option<RuntimeActivityStatus>,
    pub cursor: Option<String>,
    pub limit: Option<u64>,
}

impl RuntimeActivityListQuery {
    pub const DEFAULT_LIMIT: u64 = 25;
    pub const MAX_LIMIT: u64 = 100;

    /// Effective bounded result count, or `None` when caller input is invalid.
    #[must_use]
    pub const fn normalized_limit(&self) -> Option<u64> {
        let limit = match self.limit {
            Some(limit) => limit,
            None => Self::DEFAULT_LIMIT,
        };
        match limit {
            limit @ 1..=Self::MAX_LIMIT => Some(limit),
            _ => None,
        }
    }
}

/// Decoded opaque keyset cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeActivityCursor {
    pub updated_at: DateTime<Utc>,
    pub domain: RuntimeActivityDomain,
    pub activity_id: String,
}

/// Repository-facing permission scope and keyset window.
#[derive(Debug, Clone)]
pub struct RuntimeActivityReadQuery {
    pub visible_domains: Vec<RuntimeActivityDomain>,
    pub domain: Option<RuntimeActivityDomain>,
    pub status: Option<RuntimeActivityStatus>,
    pub cursor: Option<RuntimeActivityCursor>,
    pub limit: u64,
}

/// Typed reference used by workspace inspectors and audit deep-links.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeActivityEntityView {
    pub kind: String,
    pub id: String,
}

/// One lifecycle-valid action backed by an existing domain endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeActivityActionView {
    pub kind: RuntimeActivityActionKind,
    pub permission_code: String,
}

/// One row projected from an existing durable fact ledger.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeActivityView {
    pub activity_id: String,
    pub domain: RuntimeActivityDomain,
    pub kind: String,
    pub status: RuntimeActivityStatus,
    pub source_status: String,
    pub entity: RuntimeActivityEntityView,
    pub related_entity: Option<RuntimeActivityEntityView>,
    pub progress_pct: Option<f64>,
    pub detail: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub target_route: String,
    pub available_actions: Vec<RuntimeActivityActionView>,
    /// Repository-computed lifecycle eligibility; never exposed on the wire.
    #[serde(skip)]
    pub action_eligible: bool,
}

/// Count for one permission-visible fact family.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeActivityDomainCountView {
    pub domain: RuntimeActivityDomain,
    pub count: u64,
}

/// Counts are computed after RBAC/domain/status filtering and before pagination.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeActivitySummaryView {
    pub total: u64,
    pub by_domain: Vec<RuntimeActivityDomainCountView>,
}

/// Keyset page returned by `GET /api/runtime/activities`.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeActivityPageView {
    pub summary: RuntimeActivitySummaryView,
    pub items: Vec<RuntimeActivityView>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}
