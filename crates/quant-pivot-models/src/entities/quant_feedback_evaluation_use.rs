//! `quant_feedback_evaluation_use` one-time holdout consumption ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::{DatasetPurpose, FeedbackEvaluationPurpose},
    types::{
        ArtifactUri, ContentHash, FeedbackCycleId, FeedbackEvaluationUseId, ModelVersionId,
        ResearchProfileArtifactId, ResearchProfileRef, TrainingDatasetId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_evaluation_use")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub feedback_evaluation_use_id: FeedbackEvaluationUseId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub purpose: FeedbackEvaluationPurpose,
    pub dataset_purpose: DatasetPurpose,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub evaluation_dataset_id: TrainingDatasetId,
    pub evaluation_dataset_hash: ContentHash,
    pub evaluation_artifact_bytes_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub comparison_contract_hash: ContentHash,
    pub semantic_use_hash: ContentHash,
    pub cpcv_artifact_uri: ArtifactUri,
    pub cpcv_artifact_hash: ContentHash,
    pub evaluation_use_hash: ContentHash,
    pub reserved_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
