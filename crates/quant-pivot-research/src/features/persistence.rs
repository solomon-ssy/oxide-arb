//! Feature compute types → Postgres insert DTO projection.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{DecisionBoundary, NewFeatureVector},
    types::FeatureVectorId,
};
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
    pub fn try_to_new(&self, boundary: &DecisionBoundary) -> QuantResult<NewFeatureVector> {
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
        let feature_hash = ResearchHasher::feature_vector(self)?;
        let payload = json!({
            "generic": self.generic,
            "domain": self.domain,
        });
        let source_refs: Vec<_> = self
            .iter_cells()
            .filter_map(|(_, cell)| cell.evidence.as_ref())
            .collect();
        let source_refs =
            serde_json::to_value(source_refs).map_err(|err| ResearchError::Serialization {
                detail: format!("serialize feature source_refs: {err}"),
            })?;

        Ok(NewFeatureVector {
            feature_vector_id: FeatureVectorId::from_v7(),
            market_id: self.market_id.clone(),
            token_id: self.token_id.clone(),
            decision_at: self.decision_at,
            decision_boundary: Some(boundary.clone()),
            feature_schema_version: self.generic_schema_version,
            feature_hash,
            data_quality: self.data_quality,
            staleness_ms: i64::try_from(self.max_known_staleness_ms().ok_or_else(|| {
                ResearchError::Serialization {
                    detail: "feature vector has no known cell staleness".to_owned(),
                }
            })?)
            .map_err(|_| ResearchError::Serialization {
                detail: "feature vector staleness does not fit i64".to_owned(),
            })?,
            payload,
            source_refs,
            decision_capture: None,
            decision_capture_hash: None,
        })
    }
}
