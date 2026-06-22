//! `trades` table entity.

use crate::{
    enums::common::{
        MarketCategory, Side, TradeBusinessOutcome, TradeReconcileResolution, TradeState,
    },
    enums::legacy::LegacyExecutionMode,
    types::{
        Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, ReservationId, Shares,
        TokenId, TradeId, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "trade")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub trade_id: TradeId,
    pub execution_id: ExecutionId,
    pub reservation_id: ReservationId,
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub price: Price,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub detected_edge_bps: Option<Bps>,
    pub detected_profit_usd: Option<Usd>,
    pub net_profit_usd: Option<Usd>,
    pub order_id: Option<OrderId>,
    #[sea_orm(column_type = "Text", nullable)]
    pub tx_hash: Option<String>,
    /// Lifecycle state machine — the single source of truth for the trade row.
    pub state: TradeState,
    /// Business outcome maintained by repository state transitions.
    /// `None` while in-flight (`intent`/`submitted`).
    pub business_outcome: Option<TradeBusinessOutcome>,
    /// Frozen scored-opportunity snapshot captured at dispatch (post-trade audit).
    #[sea_orm(column_type = "JsonBinary")]
    pub scored_snapshot: Json,
    /// Market category captured at dispatch (post-trade audit, self-describing row).
    pub category: MarketCategory,
    /// Set when an orphaned/ambiguous trade needs operator/reconciliation review.
    pub needs_reconcile: bool,
    /// Explicit terminal conclusion for a trade that entered reconciliation.
    pub reconcile_resolution: Option<TradeReconcileResolution>,
    /// Time at which reconciliation produced `reconcile_resolution`.
    pub reconciled_at: Option<DateTime<Utc>>,
    /// Human-readable operator/worker note explaining the reconciliation result.
    #[sea_orm(column_type = "Text", nullable)]
    pub reconcile_note: Option<String>,
    /// CTF token balance snapshot taken immediately before venue submit (Live).
    pub pre_submit_ctf_balance: Option<Shares>,
    /// Number of deferrals while reconciliation evidence was insufficient.
    pub reconcile_attempts: i32,
    /// Earliest time the reconciliation worker should re-scan this row.
    pub reconcile_defer_until: Option<DateTime<Utc>>,
    /// Current relay lease owner for `*_processing` rows.
    #[sea_orm(column_type = "Text", nullable)]
    pub post_trade_claim_owner: Option<String>,
    /// Time at which the current relay lease was acquired.
    pub post_trade_claimed_at: Option<DateTime<Utc>>,
    /// Number of relay claim attempts for this trade.
    pub post_trade_attempts: i32,
    pub execution_mode: LegacyExecutionMode,
    pub latency_ms: Option<i32>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    /// Wall-clock at which the order was submitted to the venue (orphan-scan anchor).
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
        belongs_to = "super::event::Entity",
        from = "Column::EventId",
        to = "super::event::Column::EventId"
    )]
    Event,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl Related<super::event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Event.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
