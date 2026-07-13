//! Feature vector persistence DTOs.

use crate::{
    domain::DecisionBoundary,
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
    pub decision_at: DateTime<Utc>,
    /// Full source-visibility contract. `None` is reserved for pre-v10 audit rows.
    pub decision_boundary: Option<DecisionBoundary>,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    pub payload: serde_json::Value,
    pub source_refs: serde_json::Value,
    /// Full v10 decision capture. `None` is reserved for retired legacy rows.
    pub decision_capture: Option<serde_json::Value>,
    pub decision_capture_hash: Option<ContentHash>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(FeatureVectorInfo, crate::entities::quant_feature_vector::Model, {
    feature_vector_id, market_id, token_id, decision_at, decision_boundary, feature_schema_version,
    feature_hash, data_quality, staleness_ms, payload, source_refs, decision_capture,
    decision_capture_hash, created_at,
});

/// Insert payload for `quant_feature_vector`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feature_vector::ActiveModel")]
pub struct NewFeatureVector {
    pub feature_vector_id: FeatureVectorId,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub decision_at: DateTime<Utc>,
    /// Always populated by v10 writers; optional only at the persistence layer
    /// so legacy audit rows are not given invented cutoffs.
    pub decision_boundary: Option<DecisionBoundary>,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    pub payload: serde_json::Value,
    pub source_refs: serde_json::Value,
    /// Always populated by the online v10 writer; nullable only so pre-v10
    /// audit rows are not assigned invented captures.
    pub decision_capture: Option<serde_json::Value>,
    pub decision_capture_hash: Option<ContentHash>,
}

/// Runtime feature payload before persistence assigns queryable metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVectorModel {
    pub vector: NewFeatureVector,
}
