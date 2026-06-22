//! `quant_universe_snapshot` table entity.

use crate::{
    jsonb_newtype,
    types::{RuntimeConfigVersionId, UniverseSnapshotId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

jsonb_newtype! {
    /// Structured JSONB list of market ids included in a universe snapshot.
    pub struct UniverseIncludedMarketIds(Vec<String>);
}

jsonb_newtype! {
    /// Structured JSONB list of market ids excluded from a universe snapshot.
    pub struct UniverseExcludedMarketIds(Vec<String>);
}

jsonb_newtype! {
    /// Structured JSONB summary of exclusion reasons for a universe snapshot.
    pub struct UniverseExclusionSummary {
        stale_book_count: u32,
        insufficient_liquidity_count: u32,
        excluded_by_operator_count: u32,
        other_count: u32,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_universe_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub universe_snapshot_id: UniverseSnapshotId,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    #[sea_orm(column_type = "Text")]
    pub selector_hash: String,
    pub market_count: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub included_market_ids: UniverseIncludedMarketIds,
    #[sea_orm(column_type = "JsonBinary")]
    pub excluded_market_ids: UniverseExcludedMarketIds,
    #[sea_orm(column_type = "JsonBinary")]
    pub exclusion_summary: UniverseExclusionSummary,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::quant_universe_member::Entity")]
    Member,
}

impl Related<super::quant_universe_member::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Member.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
