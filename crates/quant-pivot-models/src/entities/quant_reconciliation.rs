//! `quant_reconciliation` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_execution_order, quant_order_intent};
use crate::{
    enums::execution::ReconciliationResult,
    types::{
        ExecutionOrderId, OrderIntentId, Price, ReconciliationEvidenceChain, ReconciliationId,
        Shares, Usd,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_reconciliation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub reconciliation_id: ReconciliationId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub result: ReconciliationResult,
    #[sea_orm(column_type = "JsonBinary")]
    pub evidence_json: ReconciliationEvidenceChain,
    pub venue_filled_shares: Option<Shares>,
    pub venue_avg_price: Option<Price>,
    pub expected_cash_delta_usd: Option<Usd>,
    pub venue_cash_delta_usd: Option<Usd>,
    pub realized_pnl_usd: Option<Usd>,
    pub expected_fee_usd: Option<Usd>,
    pub observed_fee_usd: Option<Usd>,
    pub fee_delta_usd: Option<Usd>,
    #[sea_orm(column_type = "Text", nullable)]
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

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
}

impl ActiveModelBehavior for ActiveModel {}
