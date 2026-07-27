//! Factor compute types → Postgres insert DTO projection.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::{FactorDefinitionInfo, FactorValueInfo, NewFactorValue},
    enums::factor::FactorValueState,
    types::{FactorValueId, FeatureVectorId, MarketId, ModelRunId, factor::FactorDefinitionRef},
};

use super::{FactorValue, NormalizedFactor};

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
    /// Rehydrate one immutable factor value and verify its governed definition.
    pub fn try_from_persistence(
        value: &FactorValueInfo,
        definition: &FactorDefinitionInfo,
    ) -> QuantResult<Self> {
        let revision = FactorDefinitionRef::try_from(definition).map_err(|error| {
            ResearchError::Serialization {
                detail: format!(
                    "rebuild persisted factor definition {}: {error}",
                    definition.factor_definition_id
                ),
            }
        })?;
        value
            .validate_against(&revision)
            .map_err(|error| ResearchError::Serialization {
                detail: format!(
                    "rebuild persisted factor value {}: {error}",
                    value.factor_value_id
                ),
            })?;
        let document = revision.definition();
        let normalization = match (
            value.value_state,
            value.raw_value,
            value.normalized_score,
            value.normalization_source,
            value.indeterminate_reason,
        ) {
            (FactorValueState::Scored, Some(_), Some(score), Some(source), None) => {
                NormalizedFactor::Scored {
                    score,
                    source,
                    // Clamp diagnostics are display-only and are deliberately
                    // not part of persisted scoring semantics.
                    clamp: None,
                }
            }
            (FactorValueState::MissingInput, None, None, None, None) => {
                NormalizedFactor::MissingInput
            }
            (FactorValueState::NotApplicable, None, None, None, None) => {
                NormalizedFactor::NotApplicable
            }
            (FactorValueState::Indeterminate, _, None, None, Some(reason)) => {
                NormalizedFactor::Indeterminate { reason }
            }
            _ => {
                return Err(ResearchError::Serialization {
                    detail: format!(
                        "persisted factor value {} has an invalid state tuple",
                        value.factor_value_id
                    ),
                }
                .into());
            }
        };
        Ok(Self {
            definition_id: value.factor_definition_id,
            name: document.name.clone(),
            family: document.family,
            raw_value: value.raw_value,
            normalization,
            direction: value.direction,
            confidence: value.confidence,
            explanation: value.explanation.clone(),
            input_feature_refs: document.input_features.clone(),
        })
    }

    /// Project this factor value into a `quant_factor_value` insert payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the explanation cannot be serialized to JSON.
    pub fn try_to_new(&self, ctx: &FactorValueInsertContext<'_>) -> QuantResult<NewFactorValue> {
        Ok(NewFactorValue {
            factor_value_id: FactorValueId::from_v7(),
            factor_definition_id: self.definition_id,
            feature_vector_id: *ctx.feature_vector_id,
            model_run_id: *ctx.model_run_id,
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

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::{FactorDefinitionInfo, FactorValueInfo},
        enums::{
            factor::{FactorFamily, FactorNormalization, FactorValueState, NormalizationSource},
            quant::FactorDirection,
        },
        types::{
            ContentHash, FactorValueId, FeatureVectorId, MarketId, ModelRunId, Probability,
            SchemaVersion,
            factor::{
                FactorComputationContract, FactorContextEffect, FactorDefinitionDocument,
                FactorDefinitionRef, FactorExplanation, FactorOutputSemantics,
            },
            stable_name::{FactorName, FeatureName},
        },
    };
    use rust_decimal_macros::dec;

    use super::{FactorValue, NormalizedFactor};

    fn factor_rows() -> (FactorDefinitionInfo, FactorValueInfo) {
        let created_at = Utc.timestamp_opt(1_000, 0).single().expect("created-at");
        let definition = FactorDefinitionDocument {
            name: FactorName::new("feedback_depth"),
            family: FactorFamily::Liquidity,
            input_features: vec![FeatureName::new("book.depth_usd")],
            output: FactorOutputSemantics::Context {
                effect: FactorContextEffect::HigherIsSupportive,
            },
            normalization: FactorNormalization::MinMax,
            owner: "feedback-test".to_owned(),
            required: true,
            computation: FactorComputationContract {
                semantic_version: 1,
                semantic_key: "quant-pivot/test-feedback-depth@1".to_owned(),
            },
        };
        let feature_contract_hash = ContentHash::from_bytes([1; 32]);
        let revision = FactorDefinitionRef::try_seal(
            definition.clone(),
            feature_contract_hash,
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
        )
        .expect("factor revision");
        let definition_info = FactorDefinitionInfo {
            factor_definition_id: revision.factor_definition_id(),
            definition_hash: revision.definition_hash(),
            feature_contract_hash,
            name: definition.name.to_string(),
            factor_family: definition.family,
            scope: definition.family.definition_scope(),
            input_schema_version: SchemaVersion::FIRST,
            output_schema_version: SchemaVersion::FIRST,
            definition,
            created_at,
        };
        let value_info = FactorValueInfo {
            factor_value_id: FactorValueId::from_v7(),
            factor_definition_id: revision.factor_definition_id(),
            feature_vector_id: FeatureVectorId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("feedback-factor-market"),
            decision_at: created_at,
            value_state: FactorValueState::Scored,
            raw_value: Some(dec!(1200)),
            normalized_score: Some(Probability::new(dec!(0.8))),
            normalization_source: Some(NormalizationSource::PerMarket),
            indeterminate_reason: None,
            direction: FactorDirection::Neutral,
            confidence: Probability::new(dec!(0.9)),
            explanation: FactorExplanation {
                headline: "depth supports fill quality".to_owned(),
                drivers: Vec::new(),
            },
            created_at,
        };
        (definition_info, value_info)
    }

    #[test]
    fn persistence_roundtrip_rejects_tamper() {
        let (definition, value) = factor_rows();
        let factor =
            FactorValue::try_from_persistence(&value, &definition).expect("rehydrate factor");
        assert_eq!(factor.name, definition.definition.name);
        assert_eq!(
            factor.normalization,
            NormalizedFactor::Scored {
                score: Probability::new(dec!(0.8)),
                source: NormalizationSource::PerMarket,
                clamp: None,
            }
        );

        let mut tampered_definition = definition.clone();
        tampered_definition.definition_hash = ContentHash::from_bytes([9; 32]);
        assert!(FactorValue::try_from_persistence(&value, &tampered_definition).is_err());

        let mut invalid_state = value;
        invalid_state.normalized_score = None;
        assert!(FactorValue::try_from_persistence(&invalid_state, &definition).is_err());
    }
}
