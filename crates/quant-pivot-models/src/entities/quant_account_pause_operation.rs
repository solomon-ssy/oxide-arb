//! Durable per-exchange account pause-state operation.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_account_recovery_incident;
use crate::{
    enums::{
        execution::{AccountPauseOperationKind, AccountPauseOperationState},
        settlement::SettlementSubmissionKind,
    },
    types::{
        AccountPauseOperationId, AccountRecoveryIncidentId, ContentHash, EvmAddress, EvmBlockHash,
        EvmCalldataHash, EvmTransactionHash, EvmUint256, RelayerTransactionId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_account_pause_operation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub account_pause_operation_id: AccountPauseOperationId,
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub exchange_address: EvmAddress,
    pub operation_kind: AccountPauseOperationKind,
    pub state: AccountPauseOperationState,
    pub submission_kind: SettlementSubmissionKind,
    pub requested_block: i64,
    pub interval_blocks: Option<i64>,
    pub effective_block: Option<i64>,
    pub prepared_block_number: i64,
    pub prepared_block_hash: EvmBlockHash,
    pub prepared_nonce: EvmUint256,
    pub gas_limit: Option<EvmUint256>,
    pub calldata_hash: EvmCalldataHash,
    pub deployment_digest: ContentHash,
    pub signed_envelope: Vec<u8>,
    pub signed_envelope_hash: ContentHash,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub relayer_transaction_id: Option<RelayerTransactionId>,
    pub confirmation_block_number: Option<i64>,
    pub confirmation_block_hash: Option<EvmBlockHash>,
    pub confirmation_transaction_hash: Option<EvmTransactionHash>,
    pub confirmation_log_index: Option<i64>,
    #[sea_orm(column_type = "Text", nullable)]
    pub failure_detail: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RecoveryIncident",
        from = "recovery_incident_id",
        to = "account_recovery_incident_id"
    )]
    pub recovery_incident: BelongsTo<quant_account_recovery_incident::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
