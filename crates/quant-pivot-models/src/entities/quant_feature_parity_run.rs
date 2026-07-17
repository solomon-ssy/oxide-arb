//! `quant_feature_parity_run` lifecycle entity.

use crate::{
    enums::quant::{FeatureParityRunKind, FeatureParityRunStatus},
    types::{
        ContentHash, FeatureParityRunId, ModelVersionId, RecommendationReportId, TrainingDatasetId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feature_parity_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub run_id: FeatureParityRunId,
    pub kind: FeatureParityRunKind,
    pub status: FeatureParityRunStatus,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub report_id: Option<RecommendationReportId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub triggered_by: String,
    pub requested_by: Option<String>,
    pub acting_role: String,
    pub reason: String,
    pub total_count: i64,
    pub compared_count: i64,
    pub matched_count: i64,
    pub mismatched_count: i64,
    pub pending_materialization_count: i64,
    pub feature_contract_hash: Option<ContentHash>,
    pub transform_hash: Option<ContentHash>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub pending_since: Option<DateTime<Utc>>,
    pub containment_completed_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "Subject")]
    pub subject: HasMany<super::quant_feature_parity_subject::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
