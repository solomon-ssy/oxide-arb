//! `quant_execution_trade_ref` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_execution_order;
use crate::{
    enums::execution::VenueTradeStatus,
    types::{EvmTransactionHash, ExecutionOrderId, ExecutionTradeRefId, VenueTradeId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_trade_ref")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub execution_trade_ref_id: ExecutionTradeRefId,
    pub execution_order_id: ExecutionOrderId,
    #[sea_orm(unique)]
    pub venue_trade_id: VenueTradeId,
    pub trade_status: Option<VenueTradeStatus>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionOrder",
        from = "execution_order_id",
        to = "execution_order_id"
    )]
    pub execution_order: BelongsTo<quant_execution_order::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
