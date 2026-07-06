//! `events` table entity.

use crate::{
    enums::market::EventStatus,
    types::{CatalogMarketIds, EventId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: EventId,
    #[sea_orm(column_type = "Text")]
    pub title: String,
    #[sea_orm(column_type = "Text")]
    pub slug: String,
    pub status: EventStatus,
    /// Raw Gamma tag slugs — the official categorization source.
    pub tags: Vec<String>,
    pub neg_risk: bool,
    /// Ordered Gamma `condition_id`s at sync time (mirrors `EventRegistryInfo.market_ids`).
    pub catalog_market_ids: CatalogMarketIds,
    pub end_date: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub raw_gamma: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::market::Entity")]
    Market,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
