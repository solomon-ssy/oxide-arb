//! Market/event fixture builders for the catalog repository integration tests.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{UpsertEvent, UpsertMarket},
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    types::{EventId, MarketId, TokenId},
};

pub fn make_event(id: &str, title: &str, slug: &str, category: MarketCategory) -> UpsertEvent {
    UpsertEvent {
        event_id: EventId::new(id),
        title: title.into(),
        slug: slug.into(),
        status: EventStatus::Active,
        tags: vec![category.as_str().to_owned()].into(),
        neg_risk: false,
        end_date: None,
        raw_gamma: None,
    }
}

pub fn make_market(
    market_id: &str,
    event_id: &str,
    question: &str,
    slug: &str,
    category: MarketCategory,
    end_date: Option<DateTime<Utc>>,
) -> UpsertMarket {
    UpsertMarket {
        market_id: MarketId::new(market_id),
        event_id: EventId::new(event_id),
        question: question.into(),
        slug: slug.into(),
        categories: CategorySet::from(category),
        status: MarketStatus::Active,
        outcome: None,
        yes_token_id: TokenId::new("12345"),
        no_token_id: TokenId::new("67890"),
        tick_size: TickSize::Hundredth,
        neg_risk: false,
        end_date,
        resolved_at: None,
        fees_enabled: true,
        fee_rate: None,
        fee_exponent: None,
        fee_taker_only: None,
        fee_rebate_rate: None,
        fee_source: None,
        fee_observed_at: None,
    }
}
