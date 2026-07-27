//! Factor registry and factor value persistence DTOs.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_factor_definition,
    enums::{
        factor::{
            FactorDefinitionScope, FactorFamily, FactorIndeterminateReason, FactorValueState,
            NormalizationSource,
        },
        quant::FactorDirection,
    },
    types::{
        ContentHash, FactorDefinitionId, FactorValueId, FeatureVectorId, MarketId, ModelRunId,
        ModelVersionId, Probability, SchemaVersion,
        factor::{
            FactorDefinitionDocument, FactorDefinitionRef, FactorDefinitionRevisionError,
            FactorExplanation, FactorRawValue,
        },
    },
};

/// Immutable, content-addressed factor definition row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_factor_definition::Entity")]
pub struct FactorDefinitionInfo {
    pub factor_definition_id: FactorDefinitionId,
    /// Content address of the canonical definition and its feature contract.
    pub definition_hash: ContentHash,
    /// Frozen feature contract consumed by this revision.
    pub feature_contract_hash: ContentHash,
    pub name: String,
    pub factor_family: FactorFamily,
    pub scope: FactorDefinitionScope,
    pub input_schema_version: SchemaVersion,
    pub output_schema_version: SchemaVersion,
    pub definition: FactorDefinitionDocument,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    FactorDefinitionInfo,
    quant_factor_definition::Model,
    {
        factor_definition_id,
        definition_hash,
        feature_contract_hash,
        name,
        factor_family,
        scope,
        input_schema_version,
        output_schema_version,
        definition,
        created_at,
    }
);

/// Insert payload for `quant_factor_definition`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_factor_definition::ActiveModel")]
pub struct NewFactorDefinition {
    pub factor_definition_id: FactorDefinitionId,
    pub definition_hash: ContentHash,
    pub feature_contract_hash: ContentHash,
    pub name: String,
    pub factor_family: FactorFamily,
    pub scope: FactorDefinitionScope,
    pub input_schema_version: SchemaVersion,
    pub output_schema_version: SchemaVersion,
    pub definition: FactorDefinitionDocument,
}

/// Persisted factor-definition columns do not reconstruct one sealed revision.
#[derive(Debug, Error)]
pub enum FactorDefinitionProjectionError {
    #[error(transparent)]
    Revision(#[from] FactorDefinitionRevisionError),
    #[error("factor-definition column `{field}` does not match the sealed revision")]
    ProjectionMismatch { field: &'static str },
}

/// Explicit result of one member of an atomic factor-registration batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome", content = "definition")]
pub enum FactorRegistrationOutcome {
    Inserted(FactorDefinitionInfo),
    AlreadyPresent(FactorDefinitionInfo),
}

impl From<FactorDefinitionRef> for NewFactorDefinition {
    fn from(revision: FactorDefinitionRef) -> Self {
        let definition = revision.definition().clone();
        Self {
            factor_definition_id: revision.factor_definition_id(),
            definition_hash: revision.definition_hash(),
            feature_contract_hash: revision.feature_contract_hash(),
            name: definition.name.to_string(),
            factor_family: definition.family,
            scope: definition.family.definition_scope(),
            input_schema_version: revision.input_schema_version(),
            output_schema_version: revision.output_schema_version(),
            definition,
        }
    }
}

impl TryFrom<&NewFactorDefinition> for FactorDefinitionRef {
    type Error = FactorDefinitionProjectionError;

    fn try_from(definition: &NewFactorDefinition) -> Result<Self, Self::Error> {
        let revision = Self::try_seal(
            definition.definition.clone(),
            definition.feature_contract_hash,
            definition.input_schema_version,
            definition.output_schema_version,
        )?;
        for (matches, field) in [
            (
                revision.factor_definition_id() == definition.factor_definition_id,
                "factor_definition_id",
            ),
            (
                revision.definition_hash() == definition.definition_hash,
                "definition_hash",
            ),
            (revision.factor_name().as_str() == definition.name, "name"),
            (
                revision.definition().family == definition.factor_family,
                "factor_family",
            ),
            (
                revision.definition().family.definition_scope() == definition.scope,
                "scope",
            ),
            (
                revision.definition() == &definition.definition,
                "definition",
            ),
        ] {
            if !matches {
                return Err(FactorDefinitionProjectionError::ProjectionMismatch { field });
            }
        }
        Ok(revision)
    }
}

impl TryFrom<&FactorDefinitionInfo> for FactorDefinitionRef {
    type Error = FactorDefinitionProjectionError;

    fn try_from(definition: &FactorDefinitionInfo) -> Result<Self, Self::Error> {
        let revision = Self::try_seal(
            definition.definition.clone(),
            definition.feature_contract_hash,
            definition.input_schema_version,
            definition.output_schema_version,
        )?;
        for (matches, field) in [
            (
                revision.factor_definition_id() == definition.factor_definition_id,
                "factor_definition_id",
            ),
            (
                revision.definition_hash() == definition.definition_hash,
                "definition_hash",
            ),
            (revision.factor_name().as_str() == definition.name, "name"),
            (
                revision.definition().family == definition.factor_family,
                "factor_family",
            ),
            (
                revision.definition().family.definition_scope() == definition.scope,
                "scope",
            ),
            (
                revision.definition() == &definition.definition,
                "definition",
            ),
        ] {
            if !matches {
                return Err(FactorDefinitionProjectionError::ProjectionMismatch { field });
            }
        }
        Ok(revision)
    }
}

/// Persisted factor value row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_factor_value::Entity")]
pub struct FactorValueInfo {
    pub factor_value_id: FactorValueId,
    pub factor_definition_id: FactorDefinitionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub market_id: MarketId,
    pub decision_at: DateTime<Utc>,
    /// Authoritative factor-value state (scored / missing-input / not-applicable
    /// / indeterminate).
    pub value_state: FactorValueState,
    pub raw_value: Option<Decimal>,
    /// `None` when the factor was missing-input, not-applicable, or indeterminate.
    pub normalized_score: Option<Probability>,
    /// How the score was derived (`None` when not scored).
    pub normalization_source: Option<NormalizationSource>,
    /// Why the factor was indeterminate (`None` unless `value_state` is
    /// `Indeterminate`).
    pub indeterminate_reason: Option<FactorIndeterminateReason>,
    pub direction: FactorDirection,
    pub confidence: Probability,
    pub explanation: FactorExplanation,
    pub created_at: DateTime<Utc>,
}

info_from_model!(FactorValueInfo, crate::entities::quant_factor_value::Model, {
    factor_value_id, factor_definition_id, feature_vector_id, model_run_id, market_id, decision_at,
    value_state, raw_value, normalized_score, normalization_source, indeterminate_reason,
    direction, confidence, explanation, created_at,
});

/// Insert payload for `quant_factor_value`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_factor_value::ActiveModel")]
pub struct NewFactorValue {
    pub factor_value_id: FactorValueId,
    pub factor_definition_id: FactorDefinitionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub market_id: MarketId,
    pub decision_at: DateTime<Utc>,
    /// Authoritative factor-value state (scored / missing-input / not-applicable
    /// / indeterminate).
    pub value_state: FactorValueState,
    pub raw_value: Option<Decimal>,
    /// `None` when the factor was missing-input, not-applicable, or indeterminate.
    pub normalized_score: Option<Probability>,
    /// How the score was derived (`None` when not scored).
    pub normalization_source: Option<NormalizationSource>,
    /// Why the factor was indeterminate (`None` unless `value_state` is
    /// `Indeterminate`).
    pub indeterminate_reason: Option<FactorIndeterminateReason>,
    pub direction: FactorDirection,
    pub confidence: Probability,
    pub explanation: FactorExplanation,
}

/// A persisted factor value does not match its sealed definition or row shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FactorValueProjectionError {
    #[error("factor definition id does not match the sealed revision")]
    DefinitionMismatch,
    #[error("factor value state tuple is invalid")]
    InvalidStateTuple,
    #[error("factor confidence must be in [0, 1] with at most 18 decimal places")]
    InvalidConfidence,
    #[error("normalized factor score must be in [0, 1] with at most 18 decimal places")]
    InvalidNormalizedScore,
    #[error("raw factor value exceeds numeric(28,12)")]
    InvalidRawValue,
    #[error("factor explanation headline or driver ordering is invalid")]
    InvalidExplanation,
    #[error("factor explanation driver is not declared by the sealed definition")]
    InvalidDriverLineage,
    #[error("factor raw value is outside the definition normalization domain")]
    InvalidNormalizationInput,
    #[error("factor direction does not match the definition output semantics")]
    DirectionMismatch,
}

struct FactorValueProjection<'a> {
    factor_definition_id: FactorDefinitionId,
    value_state: FactorValueState,
    raw_value: Option<Decimal>,
    normalized_score: Option<Probability>,
    normalization_source: Option<NormalizationSource>,
    indeterminate_reason: Option<FactorIndeterminateReason>,
    direction: FactorDirection,
    confidence: Probability,
    explanation: &'a FactorExplanation,
}

impl FactorValueProjection<'_> {
    fn validate_against(
        &self,
        revision: &FactorDefinitionRef,
    ) -> Result<(), FactorValueProjectionError> {
        if self.factor_definition_id != revision.factor_definition_id() {
            return Err(FactorValueProjectionError::DefinitionMismatch);
        }
        let valid_state = match self.value_state {
            FactorValueState::Scored => {
                self.raw_value.is_some()
                    && self.normalized_score.is_some()
                    && self.normalization_source.is_some()
                    && self.indeterminate_reason.is_none()
            }
            FactorValueState::MissingInput | FactorValueState::NotApplicable => {
                self.raw_value.is_none()
                    && self.normalized_score.is_none()
                    && self.normalization_source.is_none()
                    && self.indeterminate_reason.is_none()
                    && self.confidence.is_zero()
            }
            FactorValueState::Indeterminate => {
                self.normalized_score.is_none()
                    && self.normalization_source.is_none()
                    && self.indeterminate_reason.is_some()
                    && self.confidence.is_zero()
            }
        };
        if !valid_state {
            return Err(FactorValueProjectionError::InvalidStateTuple);
        }
        let confidence = self.confidence.inner();
        if !(Decimal::ZERO..=Decimal::ONE).contains(&confidence)
            || confidence.scale() > Probability::PRECISION.1
        {
            return Err(FactorValueProjectionError::InvalidConfidence);
        }
        if self.normalized_score.is_some_and(|score| {
            !(Decimal::ZERO..=Decimal::ONE).contains(&score.inner())
                || score.inner().scale() > Probability::PRECISION.1
        }) {
            return Err(FactorValueProjectionError::InvalidNormalizedScore);
        }
        if self
            .raw_value
            .is_some_and(|raw| FactorRawValue::try_from(raw).is_err())
        {
            return Err(FactorValueProjectionError::InvalidRawValue);
        }
        if self.explanation.headline.trim().is_empty()
            || self.explanation.headline.len() > 4096
            || !self
                .explanation
                .drivers
                .windows(2)
                .all(|pair| pair[0].feature_name < pair[1].feature_name)
        {
            return Err(FactorValueProjectionError::InvalidExplanation);
        }
        let definition = revision.definition();
        if !self
            .explanation
            .drivers
            .iter()
            .all(|driver| definition.input_features.contains(&driver.feature_name))
        {
            return Err(FactorValueProjectionError::InvalidDriverLineage);
        }
        if self
            .raw_value
            .is_some_and(|raw| definition.normalization_input(raw).is_none())
        {
            return Err(FactorValueProjectionError::InvalidNormalizationInput);
        }
        let expected_direction = self
            .raw_value
            .and_then(|raw| definition.contribution_direction(raw))
            .unwrap_or_default();
        if self.direction != expected_direction {
            return Err(FactorValueProjectionError::DirectionMismatch);
        }
        Ok(())
    }
}

impl NewFactorValue {
    /// Verify this insert payload against its exact immutable definition.
    pub fn validate_against(
        &self,
        revision: &FactorDefinitionRef,
    ) -> Result<(), FactorValueProjectionError> {
        FactorValueProjection::from(self).validate_against(revision)
    }
}

impl FactorValueInfo {
    /// Verify this persisted row against its exact immutable definition.
    pub fn validate_against(
        &self,
        revision: &FactorDefinitionRef,
    ) -> Result<(), FactorValueProjectionError> {
        FactorValueProjection::from(self).validate_against(revision)
    }
}

impl<'a> From<&'a NewFactorValue> for FactorValueProjection<'a> {
    fn from(value: &'a NewFactorValue) -> Self {
        Self {
            factor_definition_id: value.factor_definition_id,
            value_state: value.value_state,
            raw_value: value.raw_value,
            normalized_score: value.normalized_score,
            normalization_source: value.normalization_source,
            indeterminate_reason: value.indeterminate_reason,
            direction: value.direction,
            confidence: value.confidence,
            explanation: &value.explanation,
        }
    }
}

impl<'a> From<&'a FactorValueInfo> for FactorValueProjection<'a> {
    fn from(value: &'a FactorValueInfo) -> Self {
        Self {
            factor_definition_id: value.factor_definition_id,
            value_state: value.value_state,
            raw_value: value.raw_value,
            normalized_score: value.normalized_score,
            normalization_source: value.normalization_source,
            indeterminate_reason: value.indeterminate_reason,
            direction: value.direction,
            confidence: value.confidence,
            explanation: &value.explanation,
        }
    }
}

/// Latest scored factor value for one exact market/model/definition binding.
///
/// This is the online PIT snapshot consumed by entry/exit evaluators. It is
/// intentionally separate from the frozen factor breakdown embedded in a
/// recommendation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestFactorSnapshotInfo {
    pub factor_value_id: FactorValueId,
    pub factor_definition_id: FactorDefinitionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub definition_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub market_id: MarketId,
    pub raw_value: Decimal,
    pub normalized_value: Decimal,
    pub confidence: Decimal,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub snapshot_hash: ContentHash,
}

/// One value inside a coherent latest factor snapshot bundle.
///
/// Unlike the recommendation's frozen contribution breakdown, this is the
/// current persisted factor-plane fact and carries its exact governed
/// definition revision and owning serving run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestFactorSnapshotValueInfo {
    pub factor_value_id: FactorValueId,
    pub factor_definition_id: FactorDefinitionId,
    pub definition_hash: ContentHash,
    pub name: String,
    pub family: FactorFamily,
    pub value_state: FactorValueState,
    pub raw_value: Option<Decimal>,
    pub normalized_score: Option<Probability>,
    pub normalization_source: Option<NormalizationSource>,
    pub indeterminate_reason: Option<FactorIndeterminateReason>,
    pub direction: FactorDirection,
    pub confidence: Probability,
    pub explanation: FactorExplanation,
}

/// Latest complete factor plane from one exact feature vector and serving run.
///
/// Values from different feature vectors or runs are never mixed:
/// cross-sectional normalization only has meaning inside the exact batch that
/// produced it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestFactorSnapshotBundleInfo {
    pub model_run_id: ModelRunId,
    pub feature_vector_id: FeatureVectorId,
    pub model_version_id: ModelVersionId,
    pub market_id: MarketId,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub values: Vec<LatestFactorSnapshotValueInfo>,
    pub snapshot_hash: ContentHash,
}
