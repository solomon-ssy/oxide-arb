//! Gamma catalog persistence DTOs for the `event` table.

use crate::{
    enums::market::EventStatus,
    types::{CatalogMarketIds, EventId},
};
use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveValue, DeriveIntoActiveModel, DerivePartialModel, FromQueryResult, IntoActiveValue,
};
use serde::{Deserialize, Serialize};

/// DB row projection matching `entities::event::Model` columns exactly.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::event::Entity")]
pub struct EventInfo {
    pub event_id: EventId,
    pub title: String,
    pub slug: String,
    /// Recurring-series slug (Tier-0 linkage anchor), when present.
    pub series_slug: Option<String>,
    pub status: EventStatus,
    pub tags: Vec<String>,
    pub neg_risk: bool,
    /// Ordered Gamma `condition_id`s at sync time (mirrors `EventRegistryInfo.market_ids`).
    pub catalog_market_ids: CatalogMarketIds,
    pub end_date: Option<DateTime<Utc>>,
    pub raw_gamma: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(EventInfo, crate::entities::event::Model, {
    event_id,
    title,
    slug,
    series_slug,
    status,
    tags,
    neg_risk,
    catalog_market_ids,
    end_date,
    raw_gamma,
    created_at,
    updated_at,
});

/// Structured write wrapper for raw Gamma tag slugs.
///
/// Entity column type is `Vec<String>`; this DTO newtype maps via
/// `IntoActiveValue<Vec<String>>` rather than entity-side JSON derives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTags(pub Vec<String>);

impl From<Vec<String>> for EventTags {
    fn from(value: Vec<String>) -> Self {
        Self(value)
    }
}

impl IntoActiveValue<Vec<String>> for EventTags {
    fn into_active_value(self) -> ActiveValue<Vec<String>> {
        ActiveValue::Set(self.0)
    }
}

/// Upsert payload for a Polymarket event catalog row.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::event::ActiveModel")]
pub struct UpsertEvent {
    pub event_id: EventId,
    pub title: String,
    pub slug: String,
    pub series_slug: Option<String>,
    pub status: EventStatus,
    pub tags: EventTags,
    pub neg_risk: bool,
    pub catalog_market_ids: CatalogMarketIds,
    pub end_date: Option<DateTime<Utc>>,
    pub raw_gamma: Option<serde_json::Value>,
}
