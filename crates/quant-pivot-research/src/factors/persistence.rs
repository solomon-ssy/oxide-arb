//! Factor compute types → Postgres insert DTO projection.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{NewFactorDefinition, NewFactorValue},
    enums::quant::PublicationStatus,
    types::{FactorValueId, FeatureVectorId, MarketId, ModelRunId, SchemaVersion},
};

use super::{
    FactorDefinitionIdentity, FactorDefinitionSpec, FactorValue, factor_definition_identity,
};

/// Round-scoped identifiers required to project a research [`FactorValue`] into
/// Postgres (not carried on the compute type itself).
pub struct FactorValueInsertContext<'a> {
    /// The owning online round.
    pub model_run_id: &'a ModelRunId,
    /// The persisted source feature vector.
    pub feature_vector_id: &'a FeatureVectorId,
    /// The market the factor was computed for.
    pub market_id: &'a MarketId,
    /// Frozen decision time.
    pub decision_at: DateTime<Utc>,
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
            decision_at: ctx.decision_at,
            value_state: self.value_state(),
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
    /// `identity` is resolved by the owning [`super::FactorEngine`] from the
    /// canonical definition plus the active feature contract. The repository is
    /// insert-only and treats re-registration of the exact same content as an
    /// idempotent read.
    ///
    /// # Errors
    ///
    /// Returns an error when the spec cannot be serialized to canonical JSON.
    pub fn try_to_new(
        &self,
        input_schema_version: SchemaVersion,
        identity: &FactorDefinitionIdentity,
    ) -> QuantResult<NewFactorDefinition> {
        let expected = factor_definition_identity(self, &identity.feature_contract_hash)?;
        if expected != *identity {
            return Err(ResearchError::FactorComputation {
                detail: format!(
                    "factor definition identity does not match canonical content for `{}`",
                    self.name
                ),
            }
            .into());
        }
        let definition_json =
            serde_json::to_value(self).map_err(|err| ResearchError::Serialization {
                detail: format!("serialize factor definition: {err}"),
            })?;
        Ok(NewFactorDefinition {
            factor_definition_id: identity.factor_definition_id.clone(),
            definition_hash: identity.definition_hash.clone(),
            feature_contract_hash: identity.feature_contract_hash.clone(),
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
