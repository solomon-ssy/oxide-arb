//! `events` table entity.

use crate::{
    enums::market::EventStatus,
    types::{CatalogMarketIds, ContentHash, EventId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: EventId,
    #[sea_orm(column_type = "Text")]
    pub title: String,
    #[sea_orm(column_type = "Text")]
    pub slug: String,
    /// Recurring-series slug (Tier-0 linkage anchor), when present.
    #[sea_orm(column_type = "Text", nullable)]
    pub series_slug: Option<String>,
    pub status: EventStatus,
    /// Raw Gamma tag slugs — the official categorization source.
    pub tags: Vec<String>,
    pub neg_risk: bool,
    /// Ordered Gamma `condition_id`s at sync time (mirrors `EventRegistryInfo.market_ids`).
    pub catalog_market_ids: CatalogMarketIds,
    pub end_date: Option<DateTime<Utc>>,
    pub content_hash: ContentHash,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "Market")]
    pub market: HasMany<super::market::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
