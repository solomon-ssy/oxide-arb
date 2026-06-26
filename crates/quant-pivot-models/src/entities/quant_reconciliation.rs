//! `quant_reconciliation` table entity.

use crate::{
    enums::execution::ReconciliationResult,
    types::{
        ExecutionOrderId, OrderIntentId, Price, ReconciliationEvidenceChain, ReconciliationId,
        Shares, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

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
    pub discrepancy_usd: Option<Usd>,
    #[sea_orm(column_type = "Text", nullable)]
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_execution_order::Entity",
        from = "Column::ExecutionOrderId",
        to = "super::quant_execution_order::Column::ExecutionOrderId"
    )]
    ExecutionOrder,
    #[sea_orm(
        belongs_to = "super::quant_order_intent::Entity",
        from = "Column::OrderIntentId",
        to = "super::quant_order_intent::Column::OrderIntentId"
    )]
    OrderIntent,
}

impl Related<super::quant_execution_order::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ExecutionOrder.def()
    }
}

impl Related<super::quant_order_intent::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OrderIntent.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
