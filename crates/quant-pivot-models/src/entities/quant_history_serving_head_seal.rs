//! Immutable version of the monotonic finalized-history serving head.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_exchange_history_plan, quant_history_serving_head_seal};
use crate::{
    domain::data_plane::ExchangeHistoryFrontier,
    types::{ContentHash, HistoryServingHeadSealId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_history_serving_head_seal")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub serving_head_seal_id: HistoryServingHeadSealId,
    #[sea_orm(unique)]
    pub seal_hash: ContentHash,
    pub plan_id: Uuid,
    pub frontier: ExchangeHistoryFrontier,
    pub previous_seal_id: Option<HistoryServingHeadSealId>,
    pub window_from_block: i64,
    pub accepted_through_block: i64,
    pub effective_through_at: DateTime<Utc>,
    pub policy_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(belongs_to, from = "plan_id", to = "plan_id")]
    pub plan: BelongsTo<quant_exchange_history_plan::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "PreviousSeal",
        from = "previous_seal_id",
        to = "serving_head_seal_id"
    )]
    pub previous_seal: BelongsTo<Option<quant_history_serving_head_seal::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
