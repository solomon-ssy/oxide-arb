//! `quant_trade_policy_validation` immutable validation-run ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::TradePolicyValidationStatus,
    types::{ContentHash, TradePolicyArtifactId, TradePolicyValidationRunId, TrainingDatasetId},
};

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
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
