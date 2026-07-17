//! Current same-source crypto price transition projection.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::types::{ContentHash, DomainInstrumentKey, DomainSourceId, Usd};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_crypto_price_projection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: DomainSourceId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub instrument_key: DomainInstrumentKey,
    pub previous_price: Option<Usd>,
    pub current_price: Usd,
    /// `PostgreSQL` `BIGINT` storage representation; converted to domain `u64` at the repository boundary.
    pub source_sequence: i64,
    pub event_time: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub report_hash: ContentHash,
    pub gap_generation: i64,
    pub source_healthy: bool,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
