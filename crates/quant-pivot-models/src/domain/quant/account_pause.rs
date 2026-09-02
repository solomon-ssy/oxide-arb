//! Durable per-exchange account pause-state operation contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_account_pause_operation,
    enums::{
        execution::{AccountPauseOperationKind, AccountPauseOperationState},
        settlement::SettlementSubmissionKind,
    },
    types::{
        AccountPauseOperationId, AccountRecoveryIncidentId, ContentHash, EvmAddress, EvmBlockHash,
        EvmCalldataHash, EvmTransactionHash, EvmUint256, RelayerTransactionId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_account_pause_operation::Entity")]
pub struct AccountPauseOperationInfo {
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
    pub failure_detail: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    AccountPauseOperationInfo,
    quant_account_pause_operation::Model,
    {
        account_pause_operation_id,
        recovery_incident_id,
        exchange_address,
        operation_kind,
        state,
        submission_kind,
        requested_block,
        interval_blocks,
        effective_block,
        prepared_block_number,
        prepared_block_hash,
        prepared_nonce,
        gas_limit,
        calldata_hash,
        deployment_digest,
        signed_envelope,
        signed_envelope_hash,
        transaction_hash,
        relayer_transaction_id,
        confirmation_block_number,
        confirmation_block_hash,
        confirmation_transaction_hash,
        confirmation_log_index,
        failure_detail,
        dispatched_at,
        confirmed_at,
        created_at,
        updated_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_pause_operation::ActiveModel")]
pub struct NewAccountPauseOperation {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountPauseDispatch {
    EoaAccepted,
    RelayerAccepted(RelayerTransactionId),
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPauseConfirmation {
    pub block_number: i64,
    pub block_hash: EvmBlockHash,
    pub transaction_hash: EvmTransactionHash,
    pub log_index: i64,
    pub confirmed_at: DateTime<Utc>,
}
