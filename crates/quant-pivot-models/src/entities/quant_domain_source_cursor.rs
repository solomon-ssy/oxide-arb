//! `quant_domain_source_cursor` table entity.

use crate::{
    domain::{DomainCursorStatus, DomainSourceCheckpoint},
    types::{ContentHash, DomainInstrumentKey, DomainSourceId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_domain_source_cursor")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: DomainSourceId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub instrument_key: DomainInstrumentKey,
    #[sea_orm(column_type = "JsonBinary")]
    pub checkpoint_json: DomainSourceCheckpoint,
    pub checkpoint_hash: ContentHash,
    pub status: DomainCursorStatus,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
