//! Model-governance admin HTTP contract (Phase 3.7).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | POST | `/research/models/{id}/publish` | `publication:publish` | Promote a candidate/shadow version |
//! | POST | `/research/models/{id}/rollback` | `publication:rollback` | Retire published version, restore predecessor |
//! | POST | `/research/models/{id}/retire` | `publication:retire` | Retire published version without restore |
//!
//! Both endpoints require `X-Acting-Role` (Casbin `ActingRoleGoverned`) and record the
//! actor on `quant_model_governance_audit`.

use serde::Deserialize;
use validator::Validate;

/// Inbound body for `POST /research/models/{id}/publish`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PublishModelRequest {
    /// Operator reason recorded on the governance audit row.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Inbound body for `POST /research/models/{id}/rollback`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RollbackModelRequest {
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
