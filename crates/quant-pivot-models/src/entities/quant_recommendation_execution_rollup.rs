//! Immutable recommendation-level execution rollup.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_recommendation, quant_recommendation_execution_rollup_attempt};
use crate::types::{ContentHash, RecommendationId, Shares, Usd};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation_execution_rollup")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub intent_count: i32,
    pub attempt_count: i32,
    pub unfilled_attempt_count: i32,
    pub partially_filled_attempt_count: i32,
    pub fully_filled_attempt_count: i32,
    pub total_requested_shares: Shares,
    pub total_filled_shares: Shares,
    pub total_entry_fee_usd: Option<Usd>,
    pub total_exit_fee_usd: Option<Usd>,
    pub total_settlement_payout_usd: Option<Usd>,
    pub total_realized_pnl_usd: Usd,
    pub first_attempt_terminal_at: Option<DateTime<Utc>>,
    pub last_attempt_terminal_at: Option<DateTime<Utc>>,
    pub terminal_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub attempt_set_hash: ContentHash,
    pub rollup_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<quant_recommendation::Entity>,
    #[sea_orm(has_many, relation_enum = "Attempt")]
    pub attempts: HasMany<quant_recommendation_execution_rollup_attempt::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
