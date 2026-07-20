//! Feature compute types → Postgres insert DTO projection.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{DecisionBoundary, NewFeatureVector},
    enums::quant::DataQualityStatus,
    types::{
        ContentHash, DecisionCaptureEvidence, FeatureSourceRefs, FeatureVectorId,
        FeatureVectorPayload, MarketId, SchemaVersion, TokenId,
    },
};

use super::FeatureVector;
use crate::hashing::ResearchHasher;

pub struct FeatureVectorPersistenceProjection {
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub decision_at: chrono::DateTime<chrono::Utc>,
    pub decision_boundary: DecisionBoundary,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    pub payload: FeatureVectorPayload,
    pub source_refs: FeatureSourceRefs,
}

impl FeatureVector {
    pub(crate) fn persistence_projection(
        &self,
        boundary: &DecisionBoundary,
    ) -> QuantResult<FeatureVectorPersistenceProjection> {
        if self.decision_at != boundary.decision_at() {
            return Err(ResearchError::Serialization {
                detail: format!(
                    "feature vector decision time {} does not match boundary {}",
                    self.decision_at,
                    boundary.decision_at()
                ),
            }
            .into());
        }
        Ok(FeatureVectorPersistenceProjection {
            market_id: self.market_id.clone(),
            token_id: self.token_id.clone(),
            decision_at: self.decision_at,
            decision_boundary: boundary.clone(),
            feature_schema_version: self.generic_schema_version,
            feature_hash: ResearchHasher::feature_vector(self)?,
            data_quality: self.data_quality,
            staleness_ms: i64::try_from(self.max_known_staleness_ms().ok_or_else(|| {
                ResearchError::Serialization {
                    detail: "feature vector has no known cell staleness".to_owned(),
                }
            })?)
            .map_err(|_| ResearchError::Serialization {
                detail: "feature vector staleness does not fit i64".to_owned(),
            })?,
            payload: FeatureVectorPayload {
                generic: self.generic.clone(),
                domain: self.domain.clone(),
            },
            source_refs: FeatureSourceRefs(self.evidence_refs()),
        })
    }

    /// Build the complete immutable row; a durable vector can never omit its
    /// decision capture or its hash commitment.
    pub fn try_to_new(
        &self,
        boundary: &DecisionBoundary,
        capture: &DecisionCaptureEvidence,
    ) -> QuantResult<NewFeatureVector> {
        let projection = self.persistence_projection(boundary)?;
        if capture.snapshot.boundary != *boundary
            || capture.snapshot.market_id != self.market_id
            || self.token_id.as_ref() != Some(&capture.snapshot.token_id)
            || capture.data_quality != self.data_quality
        {
            return Err(ResearchError::Serialization {
                detail: format!(
                    "feature vector {} decision capture does not match its identity/boundary",
                    self.market_id
                ),
            }
            .into());
        }
        let decision_capture_hash = ResearchHasher::canonical(capture)?;
        Ok(NewFeatureVector {
            feature_vector_id: FeatureVectorId::from_v7(),
            market_id: projection.market_id,
            token_id: projection.token_id,
            decision_at: projection.decision_at,
            decision_boundary: projection.decision_boundary,
            feature_schema_version: projection.feature_schema_version,
            feature_hash: projection.feature_hash,
            data_quality: projection.data_quality,
            staleness_ms: projection.staleness_ms,
            payload: projection.payload,
            source_refs: projection.source_refs,
            decision_capture: capture.clone(),
            decision_capture_hash,
        })
    }
}
