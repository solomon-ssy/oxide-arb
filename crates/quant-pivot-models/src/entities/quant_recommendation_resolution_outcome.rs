//! `quant_recommendation_resolution_outcome` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{market, quant_recommendation};
use crate::{
    enums::quant::RecommendationResolutionKind,
    types::{ContentHash, MarketId, PayoutRatio, RecommendationId, SchemaVersion, TokenId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation_resolution_outcome")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub resolution_kind: RecommendationResolutionKind,
    pub token_payout_ratio: PayoutRatio,
    pub resolved_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub source_checkpoint_hash: ContentHash,
    pub resolution_fact_hash: ContentHash,
    pub resolution_fact_log_index: i64,
    pub resolution_fact_schema_version: SchemaVersion,
    pub outcome_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<quant_recommendation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<market::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
