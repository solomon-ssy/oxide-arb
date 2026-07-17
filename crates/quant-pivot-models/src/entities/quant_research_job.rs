//! `quant_research_job` table entity.

use crate::{
    enums::quant::{ResearchJobKind, ResearchJobStatus},
    types::{
        DatasetCoverage, ModelSpecId, ResearchJobError, ResearchJobId, ResearchJobProgress,
        RuntimeConfigVersionId,
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
    pub runtime_config_version_id: Option<RuntimeConfigVersionId>,
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
}

impl ActiveModelBehavior for ActiveModel {}
