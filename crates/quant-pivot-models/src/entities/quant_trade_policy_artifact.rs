//! `quant_trade_policy_artifact` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::TradePolicyStatus,
    types::{ContentHash, TradePolicyArtifactId, TradePolicyArtifactPayload, TrainingDatasetId},
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_trade_policy_artifact")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload_json: TradePolicyArtifactPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
