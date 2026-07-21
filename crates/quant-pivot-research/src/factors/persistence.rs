//! Factor compute types → Postgres insert DTO projection.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::{NewFactorDefinition, NewFactorValue},
    enums::quant::PublicationStatus,
    types::{
        FactorValueId, FeatureVectorId, MarketId, ModelRunId, SchemaVersion,
        factor::FactorDefinitionDocument,
    },
};

use super::{FactorDefinitionIdentity, FactorValue, factor_definition_identity};

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
            explanation: self.explanation.clone(),
        })
    }
}

/// Project a governed spec into a `quant_factor_definition` insert payload.
///
/// `identity` is resolved by the owning [`super::FactorEngine`] from the
/// canonical definition plus the active feature contract. The repository is
/// insert-only and treats re-registration of the exact same content as an
/// idempotent read.
///
/// # Errors
///
/// Returns an error when the spec cannot be serialized to canonical JSON.
pub fn factor_definition_to_new(
    definition: &FactorDefinitionDocument,
    input_schema_version: SchemaVersion,
    identity: &FactorDefinitionIdentity,
) -> QuantResult<NewFactorDefinition> {
    let expected = factor_definition_identity(definition, &identity.feature_contract_hash)?;
    if expected != *identity {
        return Err(ResearchError::FactorComputation {
            detail: format!(
                "factor definition identity does not match canonical content for `{}`",
                definition.name
            ),
        }
        .into());
    }
    Ok(NewFactorDefinition {
        factor_definition_id: identity.factor_definition_id.clone(),
        definition_hash: identity.definition_hash.clone(),
        feature_contract_hash: identity.feature_contract_hash.clone(),
        name: definition.name.as_str().to_owned(),
        factor_family: definition.family,
        scope: definition.family.definition_scope(),
        input_schema_version,
        output_schema_version: SchemaVersion::FIRST,
        definition: definition.clone(),
        status: PublicationStatus::Draft,
        created_by: None,
    })
}
