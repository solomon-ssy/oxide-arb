//! `quant_market_selection` table entity.

use crate::types::{
    ContentHash, MarketSelectionId, RuntimeConfigVersionId, SelectionExclusionSummary,
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_market_selection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_selection_id: MarketSelectionId,
    pub decision_at: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub selector_hash: ContentHash,
    pub market_count: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub exclusion_summary: SelectionExclusionSummary,
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "Member")]
    pub member: HasMany<super::quant_market_selection_member::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
