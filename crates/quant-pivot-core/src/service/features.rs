//! Feature-plane persistence mapping: research compute types → Postgres DTOs.
//!
//! The research [`FeatureVector`] is the compute truth; Postgres stores a
//! canonical projection of it. The `payload` is the canonical JSON of the typed
//! values plus the audited substitutions (never an opaque, lossy blob the
//! compute path reads back), and `feature_hash` is the canonical vector digest.

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{domain::NewFeatureVector, types::FeatureVectorId};
use quant_pivot_research::{features::FeatureVector, hashing::ResearchHasher};
use serde_json::json;

/// Map a research feature vector into a `quant_feature_vector` insert payload.
///
/// # Errors
///
/// Returns an error when the canonical hash cannot be computed or the provenance
/// references cannot be serialized.
pub fn map_feature_vector_to_new(vector: &FeatureVector) -> QuantResult<NewFeatureVector> {
    let feature_hash = ResearchHasher::feature_vector(vector)?;
    let payload = json!({
        "values": vector.values,
        "substitutions": vector.substitutions,
    });
    let source_refs = serde_json::to_value(&vector.source_refs)
        .map_err(|err| QuantError::Internal(format!("serialize feature source_refs: {err}")))?;

    Ok(NewFeatureVector {
        feature_vector_id: FeatureVectorId::from_v7(),
        market_id: vector.market_id.clone(),
        token_id: vector.token_id.clone(),
        as_of: vector.as_of,
        feature_schema_version: vector.schema_version,
        feature_hash,
        data_quality: vector.data_quality,
        staleness_ms: i64::try_from(vector.staleness_ms).unwrap_or(i64::MAX),
        payload,
        source_refs,
    })
}
