//! Feature vector persistence DTOs.

use crate::{
    enums::quant::DataQualityStatus,
    types::{ContentHash, FeatureVectorId, MarketId, SchemaVersion, TokenId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Persisted point-in-time feature vector metadata.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_feature_vector::Entity")]
pub struct FeatureVectorInfo {
    pub feature_vector_id: FeatureVectorId,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub as_of: DateTime<Utc>,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    pub payload: serde_json::Value,
    pub source_refs: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(FeatureVectorInfo, crate::entities::quant_feature_vector::Model, {
    feature_vector_id, market_id, token_id, as_of, feature_schema_version,
    feature_hash, data_quality, staleness_ms, payload, source_refs, created_at,
});

/// Insert payload for `quant_feature_vector`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feature_vector::ActiveModel")]
pub struct NewFeatureVector {
    pub feature_vector_id: FeatureVectorId,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub as_of: DateTime<Utc>,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    pub payload: serde_json::Value,
    pub source_refs: serde_json::Value,
}

/// Runtime feature payload before persistence assigns queryable metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVectorModel {
    pub vector: NewFeatureVector,
}
