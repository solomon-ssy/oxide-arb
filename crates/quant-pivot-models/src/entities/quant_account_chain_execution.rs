//! Finalized account-scoped exchange execution facts.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_account_clean_funder_blocker, quant_account_execution_association,
    quant_account_recovery_incident, quant_execution_account,
};
use crate::{
    enums::{common::Side, execution::AccountChainExecutionRole},
    types::{
        AccountChainExecutionId, ContentHash, EvmAddress, EvmBlockHash, EvmTransactionHash,
        ExecutionAccountId, OrderId, Shares, TokenId, Usd,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_account_chain_execution")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    #[sea_orm(column_type = "Text")]
    pub maker_amount_raw: String,
    #[sea_orm(column_type = "Text")]
    pub taker_amount_raw: String,
    pub account_side: Option<Side>,
    pub account_token_id: Option<TokenId>,
    pub shares: Option<Shares>,
    pub principal_usd: Option<Usd>,
    pub exact_fee_usd: Option<Usd>,
    #[sea_orm(column_type = "Text", nullable)]
    pub builder_code: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub metadata: Option<String>,
    pub source_event_hash: ContentHash,
    pub availability_policy_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(has_one, relation_enum = "Association")]
    pub association: HasOne<quant_account_execution_association::Entity>,
    #[sea_orm(has_many, relation_enum = "TriggeredRecoveryIncident")]
    pub triggered_recovery_incident: HasMany<quant_account_recovery_incident::Entity>,
    #[sea_orm(has_one, relation_enum = "CleanFunderBlocker")]
    pub clean_funder_blocker: HasOne<quant_account_clean_funder_blocker::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
