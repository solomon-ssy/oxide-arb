//! `quant_model_governance_audit` table entity.

use crate::{
    enums::quant::{ModelGovernanceAction, PublicationStatus},
    types::{AuditEventId, ModelGovernanceAuditId, ModelVersionId, TrainingDatasetId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
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
    pub before_status: PublicationStatus,
    pub after_status: PublicationStatus,
    pub before_hash: Option<String>,
    pub after_hash: Option<String>,
    pub quality_gate_passed: bool,
    pub shadow_window_secs: Option<i64>,
    #[sea_orm(column_type = "JsonBinary")]
    pub detail_json: Json,
    pub audit_event_id: Option<AuditEventId>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<Option<super::quant_model_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TrainingDataset",
        from = "training_dataset_id",
        to = "training_dataset_id"
    )]
    pub training_dataset: BelongsTo<Option<super::quant_training_dataset::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
