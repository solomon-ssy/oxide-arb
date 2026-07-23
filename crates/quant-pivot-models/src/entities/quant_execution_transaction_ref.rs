//! `quant_execution_transaction_ref` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_execution_order;
use crate::types::{EvmTransactionHash, ExecutionOrderId, ExecutionTransactionRefId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_transaction_ref")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub execution_transaction_ref_id: ExecutionTransactionRefId,
    pub execution_order_id: ExecutionOrderId,
    pub transaction_hash: EvmTransactionHash,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionOrder",
        from = "execution_order_id",
        to = "execution_order_id"
    )]
    pub execution_order: BelongsTo<quant_execution_order::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
