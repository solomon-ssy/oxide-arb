//! `quant_model_version` table entity.

use crate::{
    enums::{common::MarketCategory, quant::PublicationStatus},
    types::{
        BacktestPathSetId, ContentHash, ModelSpecId, ModelVersionId, ResearchProfileRef,
        TradePolicyArtifactId, TrainingDatasetId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
    /// Queryable copy of the immutable artifact scope. Runtime loading still
    /// verifies the artifact bytes; catalog reads never deserialize N objects.
    pub category_scope: Option<MarketCategory>,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    pub publish_path_set_id: Option<BacktestPathSetId>,
    #[sea_orm(column_type = "JsonBinary")]
    pub metrics_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub training_objective_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub quality_gate_report: Json,
    pub publication_status: PublicationStatus,
    pub published_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelSpec",
        from = "model_spec_id",
        to = "model_spec_id"
    )]
    pub model_spec: BelongsTo<super::quant_model_spec::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TrainingDataset",
        from = "training_dataset_id",
        to = "training_dataset_id"
    )]
    pub training_dataset: BelongsTo<Option<super::quant_training_dataset::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TradePolicyArtifact",
        from = "trade_policy_artifact_id",
        to = "artifact_id"
    )]
    pub trade_policy_artifact: BelongsTo<Option<super::quant_trade_policy_artifact::Entity>>,
    #[sea_orm(has_many, relation_enum = "ModelRun")]
    pub model_run: HasMany<super::quant_model_run::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
