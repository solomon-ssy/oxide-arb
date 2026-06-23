//! `quant_market_selection` table entity.

use crate::{
    jsonb_newtype,
    types::{MarketSelectionId, RuntimeConfigVersionId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

jsonb_newtype! {
    /// Structured JSONB list of market ids included in a selection snapshot.
    pub struct SelectionIncludedMarketIds(Vec<String>);
}

jsonb_newtype! {
    /// Structured JSONB list of market ids excluded from a selection snapshot.
    pub struct SelectionExcludedMarketIds(Vec<String>);
}

jsonb_newtype! {
    /// Structured JSONB summary of exclusion reasons for a selection snapshot.
    pub struct SelectionExclusionSummary {
        stale_book_count: u32,
        insufficient_liquidity_count: u32,
        excluded_by_operator_count: u32,
        other_count: u32,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_market_selection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_selection_id: MarketSelectionId,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    #[sea_orm(column_type = "Text")]
    pub selector_hash: String,
    pub market_count: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub included_market_ids: SelectionIncludedMarketIds,
    #[sea_orm(column_type = "JsonBinary")]
    pub excluded_market_ids: SelectionExcludedMarketIds,
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
