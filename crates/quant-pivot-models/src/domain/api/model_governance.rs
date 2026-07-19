//! Model-governance admin HTTP contract (Phase 3.7).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | POST | `/research/models/{id}/publish` | `publication:publish` | Promote a candidate/shadow version |
//! | POST | `/research/models/{id}/retire` | `publication:retire` | Retire published version without restore |
//! | POST | `/research/models/{id}/bind-publish-path-set` | `publication:create` | Bind CPCV path set for publish gates |
//!
//! Governed endpoints require `X-Acting-Role` (Casbin `ActingRoleGoverned`) and record the
//! actor on `quant_model_governance_audit`.

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::types::BacktestPathSetId;

/// Inbound body for `POST /research/models/{id}/publish`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PublishModelRequest {
    /// Operator reason recorded on the governance audit row.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Inbound body for `POST /research/models/{id}/retire`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RetireModelRequest {
    /// Operator reason recorded on the governance audit row.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Inbound body for `POST /research/models/{id}/bind-publish-path-set`.
///
/// Pins the CPCV path set that publish/promote quality gates must evaluate
/// (Phase 11.5 remediation — replaces implicit "latest path set").
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct BindPublishPathSetRequest {
    /// Path set that belongs to this model version.
    pub path_set_id: BacktestPathSetId,
    /// Operator reason recorded on the governance audit row.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}
