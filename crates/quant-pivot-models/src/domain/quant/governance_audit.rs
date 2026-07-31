//! Model-governance audit ledger persistence DTOs (append-only WORM trail).

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use super::{
    model_route_bootstrap::ModelRouteBootstrapRecord, promotion_permit::ModelRoutePromotionRecord,
};
use crate::{
    entities::quant_model_governance_audit,
    enums::quant::ModelGovernanceAction,
    types::{
        AuditEventId, CalibrationArtifactId, ContentHash, ModelGovernanceAuditId, ModelVersionId,
        PromotionPermitId, RoleCode, TrainingDatasetId, UserId,
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
    BootstrapRoute {
        record: Box<ModelRouteBootstrapRecord>,
    },
    PromoteRoute {
        record: Box<ModelRoutePromotionRecord>,
    },
    SealCalibration {
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
    pub detail: ModelGovernanceAuditDetail,
    pub audit_event_id: AuditEventId,
    pub created_at: DateTime<Utc>,
}

impl ModelGovernanceAuditInfo {
    /// Whether a retry carries the exact immutable audit payload.
    #[must_use]
    pub fn matches_new(&self, audit: &NewModelGovernanceAudit) -> bool {
        self.audit_id == audit.audit_id
            && self.model_version_id == audit.model_version_id
            && self.training_dataset_id == audit.training_dataset_id
            && self.action == audit.action
            && self.actor_user_id == audit.actor_user_id
            && self.actor_username == audit.actor_username
            && self.actor_role == audit.actor_role
            && self.reason == audit.reason
            && self.detail == audit.detail
            && self.audit_event_id == audit.audit_event_id
    }
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
        detail,
        audit_event_id,
        created_at,
    }
);

/// Insert payload for `quant_model_governance_audit` (omits DB-managed `created_at`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
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
    pub detail: ModelGovernanceAuditDetail,
    pub audit_event_id: AuditEventId,
}

/// Insert payload for the only model-route promotion audit subtype.
///
/// The normalized permit/hash columns are required by the schema only for
/// `PromoteRoute`; other governance DTOs omit them and therefore insert NULL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_governance_audit::ActiveModel")]
pub struct NewRoutePromotionAudit {
    pub audit_id: ModelGovernanceAuditId,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub action: ModelGovernanceAction,
    pub actor_user_id: Option<UserId>,
    pub actor_username: String,
    pub actor_role: Option<RoleCode>,
    pub reason: String,
    pub detail: ModelGovernanceAuditDetail,
    pub audit_event_id: AuditEventId,
    pub promotion_permit_id: PromotionPermitId,
    pub promotion_transaction_hash: ContentHash,
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
            "action": "seal_calibration",
            "source_version_id": "01900000-0000-7000-8000-000000000000",
            "source_artifact_hash": format!("blake3:{}", "0".repeat(64)),
            "calibrated_version_id": "01900000-0000-7000-8000-000000000001",
            "calibrated_artifact_hash": format!("blake3:{}", "1".repeat(64)),
            "calibrator_id": "01900000-0000-7000-8000-000000000002",
            "unexpected": true
        });
        assert!(serde_json::from_value::<ModelGovernanceAuditDetail>(unknown_field).is_err());
    }
}
