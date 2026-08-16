//! Exact accepted chunk revisions frozen by one serving-head version.

use sea_orm::entity::prelude::*;

use super::{quant_exchange_history_chunk, quant_history_serving_head_seal};
use crate::{domain::data_plane::ExchangeHistoryFrontier, types::HistoryServingHeadSealId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_history_serving_head_seal_chunk")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub serving_head_seal_id: HistoryServingHeadSealId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub chunk_id: Uuid,
    pub frontier: ExchangeHistoryFrontier,
    pub state_revision: i64,
    pub from_block: i64,
    pub to_block: i64,

    #[sea_orm(belongs_to, from = "serving_head_seal_id", to = "serving_head_seal_id")]
    pub seal: BelongsTo<quant_history_serving_head_seal::Entity>,
    #[sea_orm(belongs_to, from = "chunk_id", to = "chunk_id")]
    pub chunk: BelongsTo<quant_exchange_history_chunk::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
