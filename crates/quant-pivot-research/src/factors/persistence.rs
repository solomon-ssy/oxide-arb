//! Factor compute types → Postgres insert DTO projection.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{NewFactorDefinition, NewFactorValue},
    enums::quant::PublicationStatus,
    types::{FactorValueId, FeatureVectorId, MarketId, ModelRunId, SchemaVersion},
};

use super::{FactorDefinitionSpec, FactorValue, factor_definition_id};

/// Round-scoped identifiers required to project a research [`FactorValue`] into
/// Postgres (not carried on the compute type itself).
pub struct FactorValueInsertContext<'a> {
    /// The owning online round.
    pub model_run_id: &'a ModelRunId,
    /// The persisted source feature vector.
    pub feature_vector_id: &'a FeatureVectorId,
    /// The market the factor was computed for.
    pub market_id: &'a MarketId,
    /// Decision time.
    pub as_of: DateTime<Utc>,
}

impl FactorValue {
    /// Project this factor value into a `quant_factor_value` insert payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the explanation cannot be serialized to JSON.
    pub fn try_to_new(&self, ctx: &FactorValueInsertContext<'_>) -> QuantResult<NewFactorValue> {
        let explanation = serde_json::to_value(&self.explanation).map_err(|err| {
            ResearchError::Serialization {
                detail: format!("serialize factor explanation: {err}"),
            }
        })?;
        Ok(NewFactorValue {
            factor_value_id: FactorValueId::from_v7(),
            factor_definition_id: self.definition_id.clone(),
            feature_vector_id: ctx.feature_vector_id.clone(),
            model_run_id: ctx.model_run_id.clone(),
            market_id: ctx.market_id.clone(),
            as_of: ctx.as_of,
            raw_value: self.raw_value,
            normalized_score: self.normalized_score(),
            normalization_source: self.normalization_source(),
            indeterminate_reason: self.indeterminate_reason(),
            direction: self.direction,
            confidence: self.confidence,
            explanation,
        })
    }
}

impl FactorDefinitionSpec {
    /// Project this governed spec into a `quant_factor_definition` insert payload.
    ///
    /// The definition id is deterministic (UUID v5 of the factor name), so this
    /// payload upserts idempotently. `input_schema_version` binds the feature schema
    /// the factor consumes; the output schema version is the factor definition's own
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error when the spec cannot be serialized to canonical JSON.
    pub fn try_to_new(
        &self,
        input_schema_version: SchemaVersion,
    ) -> QuantResult<NewFactorDefinition> {
        let definition_json =
            serde_json::to_value(self).map_err(|err| ResearchError::Serialization {
                detail: format!("serialize factor definition: {err}"),
            })?;
        Ok(NewFactorDefinition {
            factor_definition_id: factor_definition_id(self.name.as_str()),
            name: self.name.as_str().to_owned(),
            factor_family: self.family,
            scope: self.family.definition_scope(),
            input_schema_version,
            output_schema_version: SchemaVersion::FIRST,
            definition_json,
            status: PublicationStatus::Draft,
            created_by: None,
        })
    }
}
