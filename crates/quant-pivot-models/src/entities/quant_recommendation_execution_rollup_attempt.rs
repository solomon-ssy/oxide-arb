//! Ordered immutable attempt bindings for a recommendation execution rollup.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_execution_attempt_outcome, quant_recommendation_execution_rollup};
use crate::types::{ContentHash, OrderIntentId, RecommendationId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation_execution_rollup_attempt")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub sequence: i32,
    pub order_intent_id: OrderIntentId,
    pub attempt_outcome_hash: ContentHash,
    pub terminal_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Rollup",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub rollup: BelongsTo<quant_recommendation_execution_rollup::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "AttemptOutcome",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub attempt_outcome: BelongsTo<quant_execution_attempt_outcome::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
