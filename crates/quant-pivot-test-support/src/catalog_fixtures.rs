//! Minimal event/market upsert fixtures for catalog integration tests.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{UpsertEvent, UpsertMarket},
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    hashing::CanonicalDigest,
    types::{EventId, MarketId, TokenId},
};

/// Build a minimal event upsert row.
#[must_use]
pub fn make_event(id: &str, title: &str, slug: &str, category: MarketCategory) -> UpsertEvent {
    let content_hash = CanonicalDigest::content_hash_json(&(id, title, slug, category))
        .expect("catalog event fixture must be serializable");
    UpsertEvent {
        event_id: EventId::new(id),
        title: title.into(),
        slug: slug.into(),
        series_slug: None,
        status: EventStatus::Active,
        tags: vec![category.as_str().to_owned()].into(),
        neg_risk: false,
        catalog_market_ids: Vec::new().into(),
        end_date: None,
        content_hash,
    }
}

/// Build a minimal market upsert row with deterministic YES/NO token ids.
#[must_use]
pub fn make_market(
    market_id: &str,
    event_id: &str,
    question: &str,
    slug: &str,
    category: MarketCategory,
    end_date: Option<DateTime<Utc>>,
) -> UpsertMarket {
    let content_hash = CanonicalDigest::content_hash_json(&(
        market_id, event_id, question, slug, category, &end_date,
    ))
    .expect("catalog market fixture must be serializable");
    UpsertMarket {
        market_id: MarketId::new(market_id),
        event_id: EventId::new(event_id),
        question: question.into(),
        slug: slug.into(),
        description: None,
        categories: CategorySet::from(category),
        status: MarketStatus::Active,
        filter_reasons: CatalogFilterReasonSet::default(),
        outcome: None,
        yes_token_id: TokenId::new("12345"),
        no_token_id: TokenId::new("67890"),
        tick_size: TickSize::Hundredth,
        neg_risk: false,
        start_date: None,
        end_date,
        resolved_at: None,
        content_hash,
    }
}
