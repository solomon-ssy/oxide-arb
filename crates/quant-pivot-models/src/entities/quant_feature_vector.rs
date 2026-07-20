//! `quant_feature_vector` table entity.

use crate::{
    domain::DecisionBoundary,
    enums::quant::DataQualityStatus,
    types::{
        ContentHash, DecisionCaptureEvidence, FeatureSourceRefs, FeatureVectorId,
        FeatureVectorPayload, MarketId, SchemaVersion, TokenId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feature_vector")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub feature_vector_id: FeatureVectorId,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub decision_at: DateTime<Utc>,
    #[sea_orm(column_type = "JsonBinary")]
    pub decision_boundary: DecisionBoundary,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: FeatureVectorPayload,
    #[sea_orm(column_type = "JsonBinary")]
    pub source_refs: FeatureSourceRefs,
    #[sea_orm(column_type = "JsonBinary")]
    pub decision_capture: DecisionCaptureEvidence,
    pub decision_capture_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<super::market::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
