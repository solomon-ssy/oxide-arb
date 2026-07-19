//! `quant_trade_policy_validation` immutable validation-run ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::TradePolicyValidationStatus,
    types::{ContentHash, TradePolicyArtifactId, TradePolicyValidationRunId, TrainingDatasetId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_trade_policy_validation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub validation_run_id: TradePolicyValidationRunId,
    pub artifact_id: TradePolicyArtifactId,
    pub artifact_hash: ContentHash,
    pub source_dataset_id: TrainingDatasetId,
    pub source_dataset_hash: ContentHash,
    pub source_slice_manifest_hash: ContentHash,
    pub evidence_manifest_hash: ContentHash,
    pub status: TradePolicyValidationStatus,
    pub total_rows: i64,
    pub passed_rows: i64,
    pub failed_rows: i64,
    pub validation_hash: Option<ContentHash>,
    #[sea_orm(column_type = "Text", nullable)]
    pub failure_detail: Option<String>,
    pub actor_id: Uuid,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Artifact",
        from = "artifact_id",
        to = "artifact_id"
    )]
    pub artifact: BelongsTo<super::quant_trade_policy_artifact::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "SourceDataset",
        from = "source_dataset_id",
        to = "training_dataset_id"
    )]
    pub source_dataset: BelongsTo<super::quant_training_dataset::Entity>,
    #[sea_orm(has_many, relation_enum = "ValidationRow")]
    pub validation_row: HasMany<super::quant_trade_policy_validation_row::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
