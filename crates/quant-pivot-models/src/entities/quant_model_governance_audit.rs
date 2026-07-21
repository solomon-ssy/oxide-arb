//! `quant_model_governance_audit` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_model_version, quant_training_dataset};
use crate::{
    domain::quant::ModelGovernanceAuditDetail,
    enums::quant::{ModelGovernanceAction, PublicationStatus},
    types::{
        AuditEventId, ModelGovernanceAuditId, ModelVersionId, RoleCode, TrainingDatasetId, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_governance_audit")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    #[sea_orm(column_type = "JsonBinary")]
    pub detail: ModelGovernanceAuditDetail,
    pub audit_event_id: AuditEventId,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<Option<quant_model_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TrainingDataset",
        from = "training_dataset_id",
        to = "training_dataset_id"
    )]
    pub training_dataset: BelongsTo<Option<quant_training_dataset::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
