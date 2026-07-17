//! `quant_execution_order` table entity.

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
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

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
    pub order_intent: BelongsTo<super::quant_order_intent::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<super::market::Entity>,
    #[sea_orm(has_one, relation_enum = "Reconciliation")]
    pub reconciliation: HasOne<super::quant_reconciliation::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
