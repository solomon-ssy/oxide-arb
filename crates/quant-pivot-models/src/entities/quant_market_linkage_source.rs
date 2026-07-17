//! Typed source-role projection for a market linkage revision.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::domain::LinkageSourceRole,
    types::{ContentHash, DomainInstrumentKey, DomainSourceId, MarketLinkageId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_market_linkage_source")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub linkage_id: MarketLinkageId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub role: LinkageSourceRole,
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: DomainSourceId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub instrument_key: DomainInstrumentKey,
    pub binding_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "MarketLinkage",
        from = "linkage_id",
        to = "linkage_id"
    )]
    pub market_linkage: BelongsTo<super::quant_market_linkage::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
