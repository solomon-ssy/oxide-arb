//! `quant_execution_fee_measurement` append-only fee-provenance ledger entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_execution_fill;
use crate::{
    enums::fee::FeeMeasurementStage,
    types::{
        Bps, ContentHash, EvmAddress, EvmTransactionHash, ExecutionFeeMeasurementId,
        ExecutionFillId, Usd,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_fee_measurement")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub execution_fee_measurement_id: ExecutionFeeMeasurementId,
    pub execution_fill_id: ExecutionFillId,
    pub stage: FeeMeasurementStage,
    pub fee_usd: Usd,
    pub fee_rate_bps: Option<Bps>,
    #[sea_orm(column_type = "Text")]
    pub source_identity: String,
    pub chain_id: Option<i64>,
    pub protocol_version: Option<i32>,
    pub exchange_address: Option<EvmAddress>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub log_index: Option<i64>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionFill",
        from = "execution_fill_id",
        to = "execution_fill_id"
    )]
    pub execution_fill: BelongsTo<quant_execution_fill::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
