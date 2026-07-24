//! Model-governance audit ledger persistence DTOs (append-only WORM trail).

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_model_governance_audit,
    enums::quant::{ModelGovernanceAction, PublicationStatus},
    types::{
        AuditEventId, BacktestPathSetId, CalibrationArtifactId, ContentHash, FeatureParityStateId,
        ModelGovernanceAuditId, ModelVersionId, Probability, RoleCode, TrainingDatasetId, UserId,
    },
};

/// Action-specific, closed audit evidence for model-governance transitions.
///
/// The relational `action` column is indexed for filtering; its value must
/// match this document's `action` tag (enforced by the boot schema). IDs and
/// content hashes remain distinct types instead of sharing overloaded strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, tag = "action", rename_all = "snake_case")]
pub enum ModelGovernanceAuditDetail {
    Retire {
        retired_version_id: ModelVersionId,
        artifact_hash: ContentHash,
    },
    BindPublishPathSet {
        previous_path_set_id: Option<BacktestPathSetId>,
        path_set_id: BacktestPathSetId,
        path_set_hash: ContentHash,
    },
    Publish {
        artifact_hash: ContentHash,
        gate_report_hash: ContentHash,
        shadow_samples: u64,
        shadow_mean_overlap: Probability,
        feature_parity_state_id: FeatureParityStateId,
        required_shadow_window_secs: i64,
    },
    BindCalibration {
        source_version_id: ModelVersionId,
        source_artifact_hash: ContentHash,
        calibrated_version_id: ModelVersionId,
        calibrated_artifact_hash: ContentHash,
        calibrator_id: CalibrationArtifactId,
    },
}

/// One persisted model-governance audit row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_model_governance_audit::Entity")]
pub struct ModelGovernanceAuditInfo {
    pub audit_id: ModelGovernanceAuditId,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub action: ModelGovernanceAction,
    pub actor_user_id: Option<UserId>,
    pub actor_username: String,
    pub actor_role: Option<RoleCode>,
    pub reason: String,
    pub before_status: PublicationStatus,
    pub after_status: PublicationStatus,
    pub detail: ModelGovernanceAuditDetail,
    pub audit_event_id: AuditEventId,
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
        actor_user_id,
        actor_username,
        actor_role,
        reason,
        before_status,
        after_status,
        detail,
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
    pub actor_user_id: Option<UserId>,
    pub actor_username: String,
    pub actor_role: Option<RoleCode>,
    pub reason: String,
    pub before_status: PublicationStatus,
    pub after_status: PublicationStatus,
    pub detail: ModelGovernanceAuditDetail,
    pub audit_event_id: AuditEventId,
}

#[cfg(test)]
mod tests {
    use super::ModelGovernanceAuditDetail;

    #[test]
    fn persisted_rejects_unknown_shape() {
        let unknown_action = serde_json::json!({
            "action": "rollback",
            "target_version_id": "01900000-0000-7000-8000-000000000000"
        });
        assert!(serde_json::from_value::<ModelGovernanceAuditDetail>(unknown_action).is_err());

        let unknown_field = serde_json::json!({
            "action": "retire",
            "retired_version_id": "01900000-0000-7000-8000-000000000000",
            "artifact_hash": format!("blake3:{}", "0".repeat(64)),
            "unexpected": true
        });
        assert!(serde_json::from_value::<ModelGovernanceAuditDetail>(unknown_field).is_err());
    }
}
