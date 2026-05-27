//! Maps raw Gamma API DTOs to domain types.

use super::types::{RawGammaEvent, RawGammaMarket};
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::market::{EventRegistryInfo, MarketRegistryInfo, TokenInfo, UpsertEvent, UpsertMarket},
    enums::{
        common::{MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    types::{EventId, MarketId, TokenId, Usd},
};
use rust_decimal::Decimal;

/// Complete output of a Gamma catalog sync — persistence DTOs and in-memory views.
pub struct GammaCatalogBatch {
    pub events: Vec<UpsertEvent>,
    pub markets: Vec<UpsertMarket>,
    pub registry_events: Vec<EventRegistryInfo>,
    pub registry_markets: Vec<MarketRegistryInfo>,
    pub fee_data: Vec<(TokenId, bool, MarketCategory)>,
}

/// Extract per-token fee metadata from a Gamma sync page.
///
/// Returns `(token_id, fees_enabled, category)` tuples for `FeeCalculator::ingest_gamma_markets`.
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

/// Parse a Gamma sync payload into a `GammaCatalogBatch` containing both
/// persistence DTOs and in-memory registry views.
pub fn parse_sync_payload(raw_events: Vec<RawGammaEvent>) -> GammaCatalogBatch {
    let fee_data = collect_fee_sync(&raw_events);
    let mut upsert_events = Vec::with_capacity(raw_events.len());
    let mut upsert_markets = Vec::new();
    let mut registry_events = Vec::with_capacity(raw_events.len());
    let mut registry_markets = Vec::new();

    for raw in raw_events {
        let event_id = EventId::new(&raw.id);
        let raw_markets_clone = raw.markets.clone().unwrap_or_default();

        upsert_events.push(map_upsert_event(&raw, &event_id));

        for rm in raw_markets_clone {
            let (upsert, registry) = map_market_dual(rm, &event_id);
            upsert_markets.push(upsert);
            registry_markets.push(registry);
        }

        registry_events.push(map_event(raw));
    }

    GammaCatalogBatch {
        events: upsert_events,
        markets: upsert_markets,
        registry_events,
        registry_markets,
        fee_data,
    }
}

fn map_upsert_event(raw: &RawGammaEvent, event_id: &EventId) -> UpsertEvent {
    UpsertEvent {
        event_id: event_id.clone(),
        title: raw.title.clone(),
        slug: raw.slug.clone(),
        category: MarketCategory::Other,
        status: EventStatus::Active,
        neg_risk: raw.neg_risk.unwrap_or(false),
        end_date: None,
        raw_gamma: serde_json::to_value(raw).ok(),
    }
}

fn map_market_dual(raw: RawGammaMarket, event_id: &EventId) -> (UpsertMarket, MarketRegistryInfo) {
    let tokens: Vec<TokenInfo> = raw
        .tokens
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|t| TokenInfo {
            token_id: TokenId::new(&t.token_id),
            outcome: t.outcome,
            neg_risk: raw.neg_risk.unwrap_or(false),
        })
        .collect();

    let tick_size = raw
        .minimum_tick_size
        .as_deref()
        .and_then(|s| s.parse::<TickSize>().ok())
        .unwrap_or(TickSize::Hundredth);

    let category = MarketCategory::from(raw.category.as_deref());
    let status = if raw.closed.unwrap_or(false) {
        MarketStatus::Settled
    } else if raw.active.unwrap_or(true) {
        MarketStatus::Active
    } else {
        MarketStatus::Paused
    };

    let market_id = MarketId::new(&raw.condition_id);

    let yes_token = tokens
        .iter()
        .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
        .map_or_else(
            || {
                tokens
                    .first()
                    .map_or_else(|| TokenId::new(""), |t| t.token_id.clone())
            },
            |t| t.token_id.clone(),
        );
    let no_token = tokens
        .iter()
        .find(|t| t.outcome.eq_ignore_ascii_case("no"))
        .map_or_else(
            || {
                tokens
                    .last()
                    .map_or_else(|| TokenId::new(""), |t| t.token_id.clone())
            },
            |t| t.token_id.clone(),
        );

    let created = parse_datetime(raw.created_at.as_ref()).unwrap_or_else(Utc::now);
    let updated = parse_datetime(raw.updated_at.as_ref()).unwrap_or_else(Utc::now);

    let upsert = UpsertMarket {
        market_id: market_id.clone(),
        event_id: event_id.clone(),
        question: raw.question.clone(),
        slug: raw.slug.clone().unwrap_or_default(),
        category,
        status,
        outcome: None,
        yes_token_id: yes_token.clone(),
        no_token_id: no_token.clone(),
        tick_size,
        neg_risk: raw.neg_risk.unwrap_or(false),
        end_date: None,
        resolved_at: None,
    };

    let registry = MarketRegistryInfo {
        market_id,
        event_id: event_id.clone(),
        token_yes: yes_token,
        token_no: no_token,
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
        created_at: created,
        updated_at: updated,
    };

    (upsert, registry)
}

pub fn map_event(raw: RawGammaEvent) -> EventRegistryInfo {
    let event_id = EventId::new(&raw.id);
    let markets = raw.markets.unwrap_or_default();
    let market_ids: Vec<MarketId> = markets
        .iter()
        .map(|m| MarketId::new(&m.condition_id))
        .collect();

    EventRegistryInfo {
        event_id,
        title: raw.title,
        slug: raw.slug,
        market_ids,
        neg_risk: raw.neg_risk.unwrap_or(false),
        created_at: parse_datetime(raw.created_at.as_ref()).unwrap_or_else(Utc::now),
        updated_at: parse_datetime(raw.updated_at.as_ref()).unwrap_or_else(Utc::now),
    }
}

pub fn map_market(raw: RawGammaMarket, event_id: &EventId) -> MarketRegistryInfo {
    let tokens: Vec<TokenInfo> = raw
        .tokens
        .unwrap_or_default()
        .into_iter()
        .map(|t| TokenInfo {
            token_id: TokenId::new(&t.token_id),
            outcome: t.outcome,
            neg_risk: raw.neg_risk.unwrap_or(false),
        })
        .collect();

    let tick_size = raw
        .minimum_tick_size
        .as_deref()
        .and_then(|s| s.parse::<TickSize>().ok())
        .unwrap_or(TickSize::Hundredth);

    let category = MarketCategory::from(raw.category.as_deref());
    let status = if raw.closed.unwrap_or(false) {
        MarketStatus::Settled
    } else if raw.active.unwrap_or(true) {
        MarketStatus::Active
    } else {
        MarketStatus::Paused
    };

    let yes_token = tokens
        .iter()
        .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
        .map_or_else(|| TokenId::new(""), |t| t.token_id.clone());
    let no_token = tokens
        .iter()
        .find(|t| t.outcome.eq_ignore_ascii_case("no"))
        .map_or_else(|| TokenId::new(""), |t| t.token_id.clone());

    MarketRegistryInfo {
        market_id: MarketId::new(&raw.condition_id),
        event_id: event_id.clone(),
        token_yes: yes_token,
        token_no: no_token,
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
