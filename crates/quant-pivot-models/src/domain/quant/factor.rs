//! Factor registry and factor value persistence DTOs.

use crate::{
    entities::quant_factor_definition,
    enums::{
        factor::{
            FactorDefinitionScope, FactorFamily, FactorIndeterminateReason, FactorValueState,
            NormalizationSource,
        },
        quant::{FactorDirection, PublicationStatus},
    },
    types::{
        ContentHash, FactorDefinitionId, FactorValueId, FeatureVectorId, MarketId, ModelRunId,
        ModelVersionId, Probability, SchemaVersion,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Governed factor definition row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
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
    pub definition_json: serde_json::Value,
    pub status: PublicationStatus,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
        definition_json,
        status,
        created_by,
        created_at,
        updated_at,
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
    pub definition_json: serde_json::Value,
    pub status: PublicationStatus,
    pub created_by: Option<Uuid>,
}

/// Persisted factor value row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
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
    pub explanation: serde_json::Value,
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
    pub explanation: serde_json::Value,
}

/// Runtime factor bundle generated from one feature vector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorValueModel {
    pub values: Vec<NewFactorValue>,
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
    pub explanation: serde_json::Value,
}

/// Latest complete factor plane from one exact serving run.
///
/// Values from different runs are never mixed: cross-sectional normalization
/// only has meaning inside the batch that produced it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestFactorSnapshotBundleInfo {
    pub model_run_id: ModelRunId,
    pub model_version_id: ModelVersionId,
    pub market_id: MarketId,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub values: Vec<LatestFactorSnapshotValueInfo>,
    pub snapshot_hash: ContentHash,
}
