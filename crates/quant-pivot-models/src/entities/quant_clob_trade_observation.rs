//! Append-only authenticated CLOB trade lifecycle observation.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_execution_account, quant_execution_order, quant_order_intent, quant_venue_incentive_event,
};
use crate::{
    enums::{common::Side, execution::ExecutionOrderPhase, fee::FeeLiquidityRole},
    types::{
        Bps, ClobTradeObservationId, ContentHash, ExecutionAccountId, ExecutionOrderId, MarketId,
        OrderId, OrderIntentId, Price, Shares, TokenId, Usd, VenueTradeId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_clob_trade_observation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub clob_trade_observation_id: ClobTradeObservationId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub execution_account_id: ExecutionAccountId,
    #[sea_orm(unique)]
    pub venue_trade_id: VenueTradeId,
    pub venue_bucket_index: i32,
    pub venue_order_id: OrderId,
    pub order_phase: ExecutionOrderPhase,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub liquidity_role: FeeLiquidityRole,
    pub shares: Shares,
    pub price: Price,
    pub principal_usd: Usd,
    pub provisional_fee_usd: Usd,
    pub provisional_fee_rate_bps: Bps,
    pub matched_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionOrder",
        from = "execution_order_id",
        to = "execution_order_id"
    )]
    pub execution_order: BelongsTo<quant_execution_order::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<quant_order_intent::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(has_many, relation_enum = "IncentiveEvents")]
    pub incentive_events: HasMany<quant_venue_incentive_event::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
