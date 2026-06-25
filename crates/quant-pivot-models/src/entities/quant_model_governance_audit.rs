//! `quant_model_governance_audit` table entity.

use crate::{
    enums::quant::{ModelGovernanceAction, ModelPublicationStatus},
    types::{AuditEventId, ModelGovernanceAuditId, ModelVersionId, TrainingDatasetId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_governance_audit")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub audit_id: ModelGovernanceAuditId,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub action: ModelGovernanceAction,
    pub actor_username: String,
    pub actor_role: Option<String>,
    pub reason: String,
    pub before_status: ModelPublicationStatus,
    pub after_status: ModelPublicationStatus,
    pub before_hash: Option<String>,
    pub after_hash: Option<String>,
    pub quality_gate_passed: bool,
    pub rollback_target_version_id: Option<ModelVersionId>,
    pub shadow_window_secs: Option<i64>,
    #[sea_orm(column_type = "JsonBinary")]
    pub detail_json: Json,
    pub audit_event_id: Option<AuditEventId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_model_version::Entity",
        from = "Column::ModelVersionId",
        to = "super::quant_model_version::Column::ModelVersionId"
    )]
    ModelVersion,
    #[sea_orm(
        belongs_to = "super::quant_training_dataset::Entity",
        from = "Column::TrainingDatasetId",
        to = "super::quant_training_dataset::Column::TrainingDatasetId"
    )]
    TrainingDataset,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl Related<super::quant_training_dataset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TrainingDataset.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
