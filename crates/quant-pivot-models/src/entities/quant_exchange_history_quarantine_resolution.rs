//! Append-only exchange-history quarantine resolution ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_exchange_history_chunk, quant_exchange_history_quarantine};
use crate::{domain::data_plane::ExchangeHistoryQuarantineDisposition, types::ContentHash};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_exchange_history_quarantine_resolution")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub resolution_id: Uuid,
    #[sea_orm(unique)]
    pub quarantine_id: Uuid,
    pub disposition: ExchangeHistoryQuarantineDisposition,
    pub replacement_chunk_id: Uuid,
    pub evidence_hash: ContentHash,
    pub actor: String,
    pub detail: String,
    pub resolved_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Quarantine",
        from = "quarantine_id",
        to = "quarantine_id"
    )]
    pub quarantine: BelongsTo<quant_exchange_history_quarantine::Entity>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ReplacementChunk",
        from = "replacement_chunk_id",
        to = "chunk_id"
    )]
    pub replacement_chunk: BelongsTo<quant_exchange_history_chunk::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
