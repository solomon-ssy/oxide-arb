//! Factor registry and factor value persistence DTOs.

use crate::{
    enums::{
        factor::{
            FactorDefinitionScope, FactorFamily, FactorIndeterminateReason, NormalizationSource,
        },
        quant::{FactorDirection, PublicationStatus},
    },
    types::{
        FactorDefinitionId, FactorValueId, FeatureVectorId, MarketId, ModelRunId, Probability,
        SchemaVersion,
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
    crate::entities::quant_factor_definition::Model,
    {
        factor_definition_id,
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
    pub as_of: DateTime<Utc>,
    pub raw_value: Option<Decimal>,
    /// `None` when the factor was missing-input or indeterminate.
    pub normalized_score: Option<Probability>,
    /// How the score was derived (`None` when not scored).
    pub normalization_source: Option<NormalizationSource>,
    /// Why the factor was indeterminate (`None` unless indeterminate).
    pub indeterminate_reason: Option<FactorIndeterminateReason>,
    pub direction: FactorDirection,
    pub confidence: Probability,
    pub explanation: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(FactorValueInfo, crate::entities::quant_factor_value::Model, {
    factor_value_id, factor_definition_id, feature_vector_id, model_run_id, market_id, as_of,
    raw_value, normalized_score, normalization_source, indeterminate_reason, direction, confidence,
    explanation, created_at,
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
    pub as_of: DateTime<Utc>,
    pub raw_value: Option<Decimal>,
    /// `None` when the factor was missing-input or indeterminate.
    pub normalized_score: Option<Probability>,
    /// How the score was derived (`None` when not scored).
    pub normalization_source: Option<NormalizationSource>,
    /// Why the factor was indeterminate (`None` unless indeterminate).
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
