//! `quant_research_job` table entity.

use crate::{
    enums::quant::{ResearchJobKind, ResearchJobStatus},
    types::{
        DatasetCoverage, DecisionPolicySnapshotId, ModelSpecId, ResearchJobError, ResearchJobId,
        ResearchJobProgress,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_research_job")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_id: ResearchJobId,
    pub kind: ResearchJobKind,
    pub status: ResearchJobStatus,
    pub model_spec_id: Option<ModelSpecId>,
    pub decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    #[sea_orm(column_type = "JsonBinary")]
    pub params_json: Json,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub progress_json: Option<ResearchJobProgress>,
    pub result_ref: Option<Uuid>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub error_json: Option<ResearchJobError>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub coverage_json: Option<DatasetCoverage>,
    pub requested_by: Option<String>,
    pub acting_role: String,
    pub parent_job_id: Option<ResearchJobId>,
    pub recovery_attempt: i32,
    pub max_recovery_attempts: i32,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelSpec",
        from = "model_spec_id",
        to = "model_spec_id"
    )]
    pub model_spec: BelongsTo<Option<super::quant_model_spec::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<Option<super::decision_policy_snapshot::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
