//! `quant_settlement_external_cursor` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_execution_account;
use crate::{
    enums::settlement::SettlementRoute,
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCodeHash, ExecutionAccountId,
        SettlementEvidenceVersion, SettlementExternalCursorId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_external_cursor")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub settlement_external_cursor_id: SettlementExternalCursorId,
    pub execution_account_id: ExecutionAccountId,
    pub chain_id: i64,
    pub route: SettlementRoute,
    pub target_adapter: EvmAddress,
    pub target_code_hash: EvmCodeHash,
    pub deployment_digest: ContentHash,
    pub deployment_evidence_version: SettlementEvidenceVersion,
    pub next_block_number: i64,
    pub last_observed_block_number: Option<i64>,
    pub last_observed_block_hash: Option<EvmBlockHash>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
