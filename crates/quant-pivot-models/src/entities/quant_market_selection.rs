//! `quant_market_selection` table entity.

use crate::types::{
    ContentHash, MarketSelectionId, RuntimeConfigVersionId, SelectionExclusionSummary,
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_market_selection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_selection_id: MarketSelectionId,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub selector_hash: ContentHash,
    pub market_count: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub exclusion_summary: SelectionExclusionSummary,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::quant_market_selection_member::Entity")]
    Member,
}

impl Related<super::quant_market_selection_member::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Member.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
