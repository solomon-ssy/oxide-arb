//! `markets` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use super::event;
use crate::{
    enums::{
        catalog::CatalogFilterReason,
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{ContentHash, EventId, MarketId, TokenId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "market")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_id: MarketId,
    pub event_id: EventId,
    #[sea_orm(column_type = "Text")]
    pub question: String,
    #[sea_orm(column_type = "Text")]
    pub slug: String,
    /// Market rules text used as a resolution-source grounding anchor.
    #[sea_orm(column_type = "Text", nullable)]
    pub description: Option<String>,
    /// Category memberships inherited from the parent event's Gamma tags.
    #[sea_orm(column_type = r#"custom("qp_market_category[]")"#)]
    pub categories: Vec<MarketCategory>,
    pub status: MarketStatus,
    #[sea_orm(column_type = r#"custom("qp_catalog_filter_reason[]")"#)]
    pub filter_reasons: Vec<CatalogFilterReason>,
    #[sea_orm(column_type = "Text", nullable)]
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub content_hash: ContentHash,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Event",
        from = "event_id",
        to = "event_id"
    )]
    pub event: BelongsTo<event::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
