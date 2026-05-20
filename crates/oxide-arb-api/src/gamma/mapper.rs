//! Maps raw Gamma API DTOs to domain types.

use chrono::{DateTime, Utc};
use oxide_arb_models::domain::market::{EventEntry, MarketEntry, TokenDescriptor};
use oxide_arb_models::enums::common::{MarketCategory, TickSize};
use oxide_arb_models::enums::market::MarketStatus;
use oxide_arb_models::types::{EventId, MarketId, TokenId, Usd};
use rust_decimal::Decimal;

use super::types::{RawGammaEvent, RawGammaMarket};

/// Extract per-token fee metadata from a Gamma sync page.
///
/// Returns `(token_id, fees_enabled, category)` tuples for [`FeeCalculator::ingest_gamma_markets`].
pub fn collect_fee_sync(raw_events: &[RawGammaEvent]) -> Vec<(TokenId, bool, MarketCategory)> {
    let mut out = Vec::new();
    for ev in raw_events {
        let markets = ev.markets.as_deref().unwrap_or(&[]);
        for m in markets {
            let category = MarketCategory::from(m.category.as_deref());
            let fees_enabled = m.fees_enabled.unwrap_or(true);
            for t in m.tokens.as_deref().unwrap_or(&[]) {
                out.push((TokenId::new(&t.token_id), fees_enabled, category));
            }
        }
    }
    out
}

pub fn map_event(raw: RawGammaEvent) -> EventEntry {
    let event_id = EventId::new(&raw.id);
    let markets = raw.markets.unwrap_or_default();
    let market_ids: Vec<MarketId> = markets
        .iter()
        .map(|m| MarketId::new(&m.condition_id))
        .collect();

    EventEntry {
        event_id,
        title: raw.title,
        slug: raw.slug,
        market_ids,
        neg_risk: raw.neg_risk.unwrap_or(false),
        created_at: parse_datetime(raw.created_at.as_ref()).unwrap_or_else(Utc::now),
        updated_at: parse_datetime(raw.updated_at.as_ref()).unwrap_or_else(Utc::now),
    }
}

pub fn map_market(raw: RawGammaMarket, event_id: &EventId) -> MarketEntry {
    let tokens: Vec<TokenDescriptor> = raw
        .tokens
        .unwrap_or_default()
        .into_iter()
        .map(|t| TokenDescriptor {
            token_id: TokenId::new(&t.token_id),
            outcome: t.outcome,
            neg_risk: raw.neg_risk.unwrap_or(false),
        })
        .collect();

    let tick_size = raw
        .minimum_tick_size
        .as_deref()
        .and_then(TickSize::from_str_value)
        .unwrap_or(TickSize::Hundredth);

    let category = MarketCategory::from(raw.category.as_deref());
    let status = if raw.closed.unwrap_or(false) {
        MarketStatus::Settled
    } else if raw.active.unwrap_or(true) {
        MarketStatus::Active
    } else {
        MarketStatus::Paused
    };

    MarketEntry {
        market_id: MarketId::new(&raw.condition_id),
        event_id: event_id.clone(),
        question: raw.question,
        slug: raw.slug.unwrap_or_default(),
        category,
        status,
        neg_risk: raw.neg_risk.unwrap_or(false),
        tick_size,
        tokens,
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: raw.minimum_order_size.unwrap_or(Decimal::ONE),
        volume_24h: Usd::ZERO,
        created_at: parse_datetime(raw.created_at.as_ref()).unwrap_or_else(Utc::now),
        updated_at: parse_datetime(raw.updated_at.as_ref()).unwrap_or_else(Utc::now),
    }
}

fn parse_datetime(s: Option<&String>) -> Option<DateTime<Utc>> {
    s?.parse::<DateTime<Utc>>().ok()
}
