//! `quant_training_dataset` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    decision_policy_snapshot, quant_model_spec, quant_model_version, quant_source_slice,
    research_profile_artifact,
};
use crate::{
    enums::{
        model::ModelFamily,
        quant::{DatasetPurpose, FeedbackCohort, TrainingDatasetStatus},
    },
    types::{
        ArtifactUri, ContentHash, DatasetCohortManifest, DatasetCoverage, DatasetManifest,
        DatasetSourceLineage, DecisionPolicySnapshotId, ModelSpecId, ResearchProfileArtifactId,
        SchemaVersion, SourceSliceId, TrainingDatasetId, TrainingHorizonsSecs,
        TrainingSampleSources, factor::FactorServingPlane,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_training_dataset")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_spec_definition_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub factor_serving_plane: FactorServingPlane,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub source_slice_id: SourceSliceId,
    pub pit_cutoff: DateTime<Utc>,
    #[sea_orm(column_type = "JsonBinary")]
    pub source_lineage: DatasetSourceLineage,
    pub feedback_cohort: Option<FeedbackCohort>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub cohort_manifest: Option<DatasetCohortManifest>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: TrainingDatasetStatus,
    pub purpose: DatasetPurpose,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: Option<ContentHash>,
    pub dataset_hash: Option<ContentHash>,
    pub manifest_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub manifest: Option<DatasetManifest>,
    pub artifact_bytes_hash: Option<ContentHash>,
    pub parquet_uri: Option<ArtifactUri>,
    pub sample_count: Option<i64>,
    pub knowledge_lag_secs: i64,
    pub sample_interval_secs: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub horizons_secs: TrainingHorizonsSecs,
    pub feature_schema_version: SchemaVersion,
    #[sea_orm(column_type = "JsonBinary")]
    pub sample_sources: Option<TrainingSampleSources>,
    #[sea_orm(column_type = "JsonBinary")]
    pub coverage: Option<DatasetCoverage>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub failure_detail: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelSpec",
        from = "model_spec_id",
        to = "model_spec_id"
    )]
    pub model_spec: BelongsTo<quant_model_spec::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<research_profile_artifact::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "SourceSlice",
        from = "source_slice_id",
        to = "source_slice_id"
    )]
    pub source_slice: BelongsTo<quant_source_slice::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
    #[sea_orm(has_many, relation_enum = "ModelVersion")]
    pub model_version: HasMany<quant_model_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
