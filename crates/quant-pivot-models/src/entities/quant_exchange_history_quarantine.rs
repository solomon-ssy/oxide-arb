//! `quant_exchange_history_quarantine` immutable rejection evidence entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_exchange_history_chunk;
use crate::{
    domain::data_plane::{ExchangeHistoryQuarantineEvidence, ExchangeHistoryQuarantineKind},
    types::ContentHash,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_exchange_history_quarantine")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub quarantine_id: Uuid,
    pub chunk_id: Uuid,
    pub kind: ExchangeHistoryQuarantineKind,
    #[sea_orm(column_type = "JsonBinary")]
    pub evidence: ExchangeHistoryQuarantineEvidence,
    pub evidence_hash: ContentHash,
    pub quarantined_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Chunk",
        from = "chunk_id",
        to = "chunk_id"
    )]
    pub chunk: BelongsTo<quant_exchange_history_chunk::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
