//! `positions` table entity.

use crate::{
    enums::common::{
        PositionStatus, RedeemResolutionSource, RedeemStatus, SettlementAccountingStatus,
        SettlementTrigger, Side,
    },
    enums::legacy::LegacyExecutionMode,
    types::{MarketId, PositionId, Price, Shares, TokenId, TradeId, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "position")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub position_id: PositionId,
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub execution_mode: LegacyExecutionMode,
    pub shares: Shares,
    pub avg_entry_price: Price,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub unrealized_pnl: Usd,
    pub realized_pnl: Usd,
    pub status: PositionStatus,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
    pub winning_token_id: Option<TokenId>,
    pub settlement_payout_usd: Option<Usd>,
    pub redeem_tx_hash: Option<String>,
    pub redeem_status: RedeemStatus,
    pub redeem_attempts: i32,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub oracle_verdict: Option<serde_json::Value>,
    pub settlement_trigger: Option<SettlementTrigger>,
    pub settlement_accounting_status: SettlementAccountingStatus,
    pub settlement_accounting_error: Option<String>,
    pub settlement_accounted_at: Option<DateTime<Utc>>,
    pub redeem_terminal_reason: Option<String>,
    pub redeem_neg_risk: bool,
    pub redeem_route: String,
    pub redeem_holder_address: Option<String>,
    pub redeem_resolution: RedeemResolutionSource,
    pub redeem_gas_limit: i64,
    /// On-chain gas cost (USD) for the redeem transaction, when known.
    pub redeem_gas_paid_usd: Option<Usd>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
    #[sea_orm(
        belongs_to = "super::trade::Entity",
        from = "Column::TradeId",
        to = "super::trade::Column::TradeId"
    )]
    Trade,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl Related<super::trade::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Trade.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
