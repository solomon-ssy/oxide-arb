//! Finalized account-scoped chain execution contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_account_chain_execution,
    enums::{common::Side, execution::AccountChainExecutionRole},
    types::{
        AccountChainExecutionId, ContentHash, EvmAddress, EvmBlockHash, EvmTransactionHash,
        ExecutionAccountId, OrderId, Shares, TokenId, Usd,
    },
};

/// Durable ordering key of an accepted finalized exchange event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountChainEventCursor {
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_account_chain_execution::Entity")]
pub struct AccountChainExecutionInfo {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub execution_account_id: ExecutionAccountId,
    pub role: AccountChainExecutionRole,
    pub chain_id: i64,
    pub protocol_version: i32,
    pub exchange_address: EvmAddress,
    pub block_number: i64,
    pub block_hash: EvmBlockHash,
    pub transaction_hash: EvmTransactionHash,
    pub transaction_index: i64,
    pub log_index: i64,
    pub order_id: OrderId,
    pub maker_address: EvmAddress,
    pub taker_address: EvmAddress,
    pub order_side: Side,
    pub order_token_id: TokenId,
    pub maker_amount_raw: String,
    pub taker_amount_raw: String,
    pub account_side: Option<Side>,
    pub account_token_id: Option<TokenId>,
    pub shares: Option<Shares>,
    pub principal_usd: Option<Usd>,
    pub exact_fee_usd: Option<Usd>,
    pub builder_code: Option<String>,
    pub metadata: Option<String>,
    pub source_event_hash: ContentHash,
    pub availability_policy_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    AccountChainExecutionInfo,
    quant_account_chain_execution::Model,
    {
        account_chain_execution_id,
        execution_account_id,
        role,
        chain_id,
        protocol_version,
        exchange_address,
        block_number,
        block_hash,
        transaction_hash,
        transaction_index,
        log_index,
        order_id,
        maker_address,
        taker_address,
        order_side,
        order_token_id,
        maker_amount_raw,
        taker_amount_raw,
        account_side,
        account_token_id,
        shares,
        principal_usd,
        exact_fee_usd,
        builder_code,
        metadata,
        source_event_hash,
        availability_policy_hash,
        observed_at,
        available_at,
        created_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_chain_execution::ActiveModel")]
pub struct NewAccountChainExecution {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub execution_account_id: ExecutionAccountId,
    pub role: AccountChainExecutionRole,
    pub chain_id: i64,
    pub protocol_version: i32,
    pub exchange_address: EvmAddress,
    pub block_number: i64,
    pub block_hash: EvmBlockHash,
    pub transaction_hash: EvmTransactionHash,
    pub transaction_index: i64,
    pub log_index: i64,
    pub order_id: OrderId,
    pub maker_address: EvmAddress,
    pub taker_address: EvmAddress,
    pub order_side: Side,
    pub order_token_id: TokenId,
    pub maker_amount_raw: String,
    pub taker_amount_raw: String,
    pub account_side: Option<Side>,
    pub account_token_id: Option<TokenId>,
    pub shares: Option<Shares>,
    pub principal_usd: Option<Usd>,
    pub exact_fee_usd: Option<Usd>,
    pub builder_code: Option<String>,
    pub metadata: Option<String>,
    pub source_event_hash: ContentHash,
    pub availability_policy_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountChainExecutionInsertOutcome {
    pub inserted: u64,
    pub replayed: u64,
}
