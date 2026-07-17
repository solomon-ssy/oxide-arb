//! Model-governance audit ledger persistence DTOs (append-only WORM trail).

use crate::{
    entities::quant_model_governance_audit,
    enums::quant::{ModelGovernanceAction, PublicationStatus},
    types::{AuditEventId, ModelGovernanceAuditId, ModelVersionId, TrainingDatasetId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// One persisted model-governance audit row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_model_governance_audit::Entity")]
pub struct ModelGovernanceAuditInfo {
    pub audit_id: ModelGovernanceAuditId,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub action: ModelGovernanceAction,
    pub actor_username: String,
    pub actor_role: Option<String>,
    pub reason: String,
    pub before_status: PublicationStatus,
    pub after_status: PublicationStatus,
    pub before_hash: Option<String>,
    pub after_hash: Option<String>,
    pub quality_gate_passed: bool,
    pub rollback_target_version_id: Option<ModelVersionId>,
    pub shadow_window_secs: Option<i64>,
    pub detail_json: serde_json::Value,
    pub audit_event_id: Option<AuditEventId>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ModelGovernanceAuditInfo,
    quant_model_governance_audit::Model,
    {
        audit_id,
        model_version_id,
        training_dataset_id,
        action,
        actor_username,
        actor_role,
        reason,
        before_status,
        after_status,
        before_hash,
        after_hash,
        quality_gate_passed,
        rollback_target_version_id,
        shadow_window_secs,
        detail_json,
        audit_event_id,
        created_at,
    }
);

/// Insert payload for `quant_model_governance_audit` (omits DB-managed `created_at`).
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_governance_audit::ActiveModel")]
pub struct NewModelGovernanceAudit {
    pub audit_id: ModelGovernanceAuditId,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub action: ModelGovernanceAction,
    pub actor_username: String,
    pub actor_role: Option<String>,
    pub reason: String,
    pub before_status: PublicationStatus,
    pub after_status: PublicationStatus,
    pub before_hash: Option<String>,
    pub after_hash: Option<String>,
    pub quality_gate_passed: bool,
    pub rollback_target_version_id: Option<ModelVersionId>,
    pub shadow_window_secs: Option<i64>,
    pub detail_json: serde_json::Value,
    pub audit_event_id: Option<AuditEventId>,
}
