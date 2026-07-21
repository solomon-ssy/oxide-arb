//! `quant_factor_value` table entity.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use super::{market, quant_factor_definition, quant_feature_vector, quant_model_run};
use crate::{
    enums::{
        factor::{FactorIndeterminateReason, FactorValueState, NormalizationSource},
        quant::FactorDirection,
    },
    types::{
        FactorDefinitionId, FactorValueId, FeatureVectorId, MarketId, ModelRunId, Probability,
        factor::FactorExplanation,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_factor_value")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub factor_value_id: FactorValueId,
    pub factor_definition_id: FactorDefinitionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub market_id: MarketId,
    pub decision_at: DateTime<Utc>,
    /// Authoritative factor-value state (scored / missing-input / not-applicable
    /// / indeterminate). Orthogonal to `indeterminate_reason`.
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
    #[sea_orm(column_type = "JsonBinary")]
    pub explanation: FactorExplanation,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "FactorDefinition",
        from = "factor_definition_id",
        to = "factor_definition_id"
    )]
    pub factor_definition: BelongsTo<quant_factor_definition::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "FeatureVector",
        from = "feature_vector_id",
        to = "feature_vector_id"
    )]
    pub feature_vector: BelongsTo<quant_feature_vector::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<market::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<quant_model_run::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
