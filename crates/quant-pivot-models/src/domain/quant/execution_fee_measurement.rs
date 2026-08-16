//! Append-only execution-fee measurement persistence contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_execution_fee_measurement,
    enums::fee::FeeMeasurementStage,
    types::{
        Bps, ContentHash, EvmAddress, EvmTransactionHash, ExecutionFeeMeasurementId,
        ExecutionFillId, Usd,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_execution_fee_measurement::Entity")]
pub struct ExecutionFeeMeasurementInfo {
    pub execution_fee_measurement_id: ExecutionFeeMeasurementId,
    pub execution_fill_id: ExecutionFillId,
    pub stage: FeeMeasurementStage,
    pub fee_usd: Usd,
    pub fee_rate_bps: Option<Bps>,
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
}

info_from_model!(
    ExecutionFeeMeasurementInfo,
    quant_execution_fee_measurement::Model,
    {
        execution_fee_measurement_id,
        execution_fill_id,
        stage,
        fee_usd,
        fee_rate_bps,
        source_identity,
        chain_id,
        protocol_version,
        exchange_address,
        transaction_hash,
        log_index,
        observed_at,
        available_at,
        evidence_hash,
        created_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_execution_fee_measurement::ActiveModel")]
pub struct NewExecutionFeeMeasurement {
    pub execution_fee_measurement_id: ExecutionFeeMeasurementId,
    pub execution_fill_id: ExecutionFillId,
    pub stage: FeeMeasurementStage,
    pub fee_usd: Usd,
    pub fee_rate_bps: Option<Bps>,
    pub source_identity: String,
    pub chain_id: Option<i64>,
    pub protocol_version: Option<i32>,
    pub exchange_address: Option<EvmAddress>,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub log_index: Option<i64>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
}
