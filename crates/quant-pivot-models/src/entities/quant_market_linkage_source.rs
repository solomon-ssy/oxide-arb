//! Typed source-role projection for a market linkage revision.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::domain::LinkageSourceRole,
    types::{ContentHash, DomainInstrumentKey, DomainSourceId, MarketLinkageId},
};

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
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_market_linkage::Entity",
        from = "Column::LinkageId",
        to = "super::quant_market_linkage::Column::LinkageId"
    )]
    MarketLinkage,
}

impl Related<super::quant_market_linkage::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::MarketLinkage.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
