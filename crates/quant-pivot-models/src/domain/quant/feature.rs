//! Feature vector persistence DTOs.

use crate::{
    domain::DecisionBoundary,
    enums::quant::DataQualityStatus,
    types::{
        ContentHash, DecisionCaptureEvidence, FeatureSourceRefs, FeatureVectorId,
        FeatureVectorPayload, MarketId, SchemaVersion, TokenId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// Persisted point-in-time feature vector metadata.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feature_vector::Entity")]
pub struct FeatureVectorInfo {
    pub feature_vector_id: FeatureVectorId,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub decision_at: DateTime<Utc>,
    /// Full source-visibility contract committed by every clean-boot writer.
    pub decision_boundary: DecisionBoundary,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    pub payload: FeatureVectorPayload,
    pub source_refs: FeatureSourceRefs,
    pub decision_capture: DecisionCaptureEvidence,
    pub decision_capture_hash: ContentHash,
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
    pub decision_boundary: DecisionBoundary,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    pub payload: FeatureVectorPayload,
    pub source_refs: FeatureSourceRefs,
    pub decision_capture: DecisionCaptureEvidence,
    pub decision_capture_hash: ContentHash,
}

/// Runtime feature payload before persistence assigns queryable metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVectorModel {
    pub vector: NewFeatureVector,
}
