//! `quant_execution_order` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    market, quant_clob_trade_observation, quant_execution_trade_ref,
    quant_execution_transaction_ref, quant_order_intent, quant_reconciliation,
};
use crate::{
    enums::{
        common::Side,
        execution::{ExecutionOrderPhase, OrderTypeKind, VenueOrderStatus},
        quant::ExecutionOrderState,
    },
    types::{
        ExecutionOrderId, MarketId, OrderId, OrderIntentId, PreparedVenueOrder, Price, Shares,
        TokenId, Usd,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_order")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub order_phase: ExecutionOrderPhase,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub order_type: OrderTypeKind,
    pub price: Price,
    pub shares: Shares,
    pub cost_usd: Usd,
    #[sea_orm(column_type = "JsonBinary")]
    pub prepared_order_json: PreparedVenueOrder,
    pub venue_order_id: Option<OrderId>,
    pub venue_status: Option<VenueOrderStatus>,
    pub state: ExecutionOrderState,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub gtd_expiration_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<quant_order_intent::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<market::Entity>,
    #[sea_orm(has_one, relation_enum = "Reconciliation")]
    pub reconciliation: HasOne<quant_reconciliation::Entity>,
    #[sea_orm(has_many, relation_enum = "TradeRefs")]
    pub trade_refs: HasMany<quant_execution_trade_ref::Entity>,
    #[sea_orm(has_many, relation_enum = "Fills")]
    pub fills: HasMany<quant_clob_trade_observation::Entity>,
    #[sea_orm(has_many, relation_enum = "TransactionRefs")]
    pub transaction_refs: HasMany<quant_execution_transaction_ref::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
