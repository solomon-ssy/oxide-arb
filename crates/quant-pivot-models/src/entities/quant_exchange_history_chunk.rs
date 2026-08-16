//! `quant_exchange_history_chunk` accepted-frontier control entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    domain::data_plane::{
        ExchangeHistoryChunkStatus, ExchangeHistoryContinuityBasis, ExchangeHistoryFrontier,
    },
    types::{ContentHash, EvmBlockHash},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_exchange_history_chunk")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub chunk_id: Uuid,
    pub frontier: ExchangeHistoryFrontier,
    pub from_block: i64,
    pub to_block: i64,
    pub status: ExchangeHistoryChunkStatus,
    pub attempt_count: i32,
    pub hypersync_count: Option<i64>,
    pub attestor_count: Option<i64>,
    pub hypersync_digest: Option<ContentHash>,
    pub attestor_digest: Option<ContentHash>,
    pub first_block_hash: Option<EvmBlockHash>,
    pub last_block_hash: Option<EvmBlockHash>,
    pub archive_height: Option<i64>,
    pub continuity_basis: Option<ExchangeHistoryContinuityBasis>,
    pub continuity_block: Option<i64>,
    pub continuity_hash: Option<EvmBlockHash>,
    pub effective_through_at: Option<DateTime<Utc>>,
    pub state_revision: Option<i64>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
