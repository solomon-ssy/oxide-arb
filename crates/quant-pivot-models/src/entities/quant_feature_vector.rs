//! `quant_feature_vector` table entity.

use crate::{
    domain::DecisionBoundary,
    enums::quant::DataQualityStatus,
    types::{ContentHash, FeatureVectorId, MarketId, SchemaVersion, TokenId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feature_vector")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub feature_vector_id: FeatureVectorId,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub decision_at: DateTime<Utc>,
    #[sea_orm(column_type = "JsonBinary")]
    pub decision_boundary: Option<DecisionBoundary>,
    pub feature_schema_version: SchemaVersion,
    pub feature_hash: ContentHash,
    pub data_quality: DataQualityStatus,
    pub staleness_ms: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub source_refs: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub decision_capture: Option<Json>,
    pub decision_capture_hash: Option<ContentHash>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
