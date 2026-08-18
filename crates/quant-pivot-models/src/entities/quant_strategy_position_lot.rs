//! `quant_strategy_position_lot` strategy-owned lot entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    event, market, quant_account_recovery_incident, quant_execution_account, quant_order_intent,
    quant_settlement_inventory_lot,
};
use crate::{
    enums::{
        common::MarketCategory,
        execution::{PositionLedgerState, StrategyPositionOriginKind},
        quant::{AccountSource, OutcomeSide},
    },
    types::{
        AccountRecoveryIncidentId, EventId, ExecutionAccountId, MarketId, OrderIntentId, Price,
        Shares, StrategyPositionLotId, TokenId, Usd,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_strategy_position_lot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub origin_kind: StrategyPositionOriginKind,
    pub order_intent_id: Option<OrderIntentId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub execution_account_id: ExecutionAccountId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    #[sea_orm(column_type = r#"custom("qp_market_category")"#)]
    pub category: MarketCategory,
    pub side: OutcomeSide,
    pub state: PositionLedgerState,
    pub shares: Shares,
    pub avg_price: Price,
    pub cost_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub source: AccountSource,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,

    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<Option<quant_order_intent::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RecoveryIncident",
        from = "recovery_incident_id",
        to = "account_recovery_incident_id"
    )]
    pub recovery_incident: BelongsTo<Option<quant_account_recovery_incident::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<market::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Event",
        from = "event_id",
        to = "event_id"
    )]
    pub event: BelongsTo<Option<event::Entity>>,
    #[sea_orm(has_many, relation_enum = "SettlementInventoryLot")]
    pub settlement_inventory_lot: HasMany<quant_settlement_inventory_lot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
