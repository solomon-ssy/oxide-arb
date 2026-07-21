//! `quant_trade_tape_block_cursor` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    domain::data_plane::{TradeTapeBlockCursorStatus, TradeTapeSourceKind},
    types::EvmAddress,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_trade_tape_block_cursor")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source: TradeTapeSourceKind,
    #[sea_orm(primary_key, auto_increment = false)]
    pub contract_address: EvmAddress,
    pub last_finalized_block: i64,
    pub last_log_index: i32,
    pub head_lag_blocks: i64,
    pub status: TradeTapeBlockCursorStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
