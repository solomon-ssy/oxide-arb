//! Feature compute types → Postgres insert DTO projection.

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{domain::NewFeatureVector, types::FeatureVectorId};
use serde_json::json;

use super::FeatureVector;
use crate::hashing::ResearchHasher;

impl FeatureVector {
    /// Project this feature vector into a `quant_feature_vector` insert payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the canonical hash cannot be computed or the provenance
    /// references cannot be serialized.
    pub fn try_to_new(&self) -> QuantResult<NewFeatureVector> {
        let feature_hash = ResearchHasher::feature_vector(self)?;
        let payload = json!({
            "values": self.values,
            "substitutions": self.substitutions,
        });
        let source_refs = serde_json::to_value(&self.source_refs)
            .map_err(|err| QuantError::Internal(format!("serialize feature source_refs: {err}")))?;

        Ok(NewFeatureVector {
            feature_vector_id: FeatureVectorId::from_v7(),
            market_id: self.market_id.clone(),
            token_id: self.token_id.clone(),
            as_of: self.as_of,
            feature_schema_version: self.schema_version,
            feature_hash,
            data_quality: self.data_quality,
            staleness_ms: i64::try_from(self.staleness_ms).unwrap_or(i64::MAX),
            payload,
            source_refs,
        })
    }
}
