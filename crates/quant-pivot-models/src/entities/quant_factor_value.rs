//! `quant_factor_value` table entity.

use crate::{
    enums::{
        factor::{FactorIndeterminateReason, FactorValueState, NormalizationSource},
        quant::FactorDirection,
    },
    types::{
        FactorDefinitionId, FactorValueId, FeatureVectorId, MarketId, ModelRunId, Probability,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_factor_value")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub factor_value_id: FactorValueId,
    pub factor_definition_id: FactorDefinitionId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub market_id: MarketId,
    pub as_of: DateTime<Utc>,
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
    pub explanation: Json,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_factor_definition::Entity",
        from = "Column::FactorDefinitionId",
        to = "super::quant_factor_definition::Column::FactorDefinitionId"
    )]
    FactorDefinition,
    #[sea_orm(
        belongs_to = "super::quant_feature_vector::Entity",
        from = "Column::FeatureVectorId",
        to = "super::quant_feature_vector::Column::FeatureVectorId"
    )]
    FeatureVector,
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
}

impl Related<super::quant_factor_definition::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::FactorDefinition.def()
    }
}

impl Related<super::quant_feature_vector::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::FeatureVector.def()
    }
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
