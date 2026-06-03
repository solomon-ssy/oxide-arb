//! Maps raw Gamma API DTOs to domain types.

use super::types::{RawGammaEvent, RawGammaMarket};
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::{
        fee::MarketFeeSchedule,
        market::{EventRegistryInfo, MarketRegistryInfo, TokenInfo, UpsertEvent, UpsertMarket},
    },
    enums::{
        common::{MarketCategory, TickSize},
        fee::FeeSource,
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
    pub fee_data: Vec<MarketFeeSchedule>,
}

/// Extract market-scoped fee schedules from a Gamma sync page.
pub fn collect_fee_sync(raw_events: &[RawGammaEvent]) -> Vec<MarketFeeSchedule> {
    let mut out = Vec::new();
    for ev in raw_events {
        let markets = ev.markets.as_deref().unwrap_or(&[]);
        for m in markets {
            if let Some(schedule) = (MarketFeeScheduleParts {
                raw: m,
                observed_at: Utc::now(),
            })
            .into()
            {
                out.push(schedule);
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

    for mut raw in raw_events {
        let event_id = EventId::new(&raw.id);

        upsert_events.push(map_upsert_event(&raw, &event_id));
        registry_events.push(map_event_ref(&raw));

        for rm in raw.markets.take().unwrap_or_default() {
            let (upsert, registry) = map_market_dual(rm, &event_id);
            upsert_markets.push(upsert);
            registry_markets.push(registry);
        }
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
        end_date: parse_gamma_datetime(raw.end_date.as_deref()),
        raw_gamma: serde_json::to_value(raw).ok(),
    }
}

fn map_market_dual(raw: RawGammaMarket, event_id: &EventId) -> (UpsertMarket, MarketRegistryInfo) {
    let tokens = token_infos(&raw);
    let tick_size = raw
        .minimum_tick_size
        .as_deref()
        .and_then(|s| s.parse::<TickSize>().ok())
        .unwrap_or(TickSize::Hundredth);
    let category = MarketCategory::from(raw.category.as_deref());
    let status = market_status(&raw);
    let market_id = MarketId::new(&raw.condition_id);
    let (yes_token, no_token) = token_pair_or_fallback(&tokens);
    let created = raw
        .created_at
        .as_deref()
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        .unwrap_or_else(Utc::now);
    let updated = raw
        .updated_at
        .as_deref()
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        .unwrap_or_else(Utc::now);
    let end_date = parse_gamma_datetime(raw.end_date.as_deref());
    let fee_schedule: Option<MarketFeeSchedule> = MarketFeeScheduleParts {
        raw: &raw,
        observed_at: updated,
    }
    .into();
    let outcome = raw
        .winning_outcome
        .clone()
        .or_else(|| raw.outcome.clone())
        .filter(|value| !value.trim().is_empty());
    let resolved_at = if raw.closed.unwrap_or(false) {
        raw.resolved_at
            .as_deref()
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
            .or(Some(updated))
    } else {
        None
    };

    let upsert = MarketUpsertParts {
        raw: &raw,
        market_id: &market_id,
        event_id,
        category,
        status,
        outcome: outcome.clone(),
        yes_token: &yes_token,
        no_token: &no_token,
        tick_size,
        end_date,
        resolved_at,
        fee_schedule: fee_schedule.as_ref(),
    }
    .into();

    let registry = MarketRegistryParts {
        raw,
        market_id,
        event_id,
        yes_token,
        no_token,
        category,
        status,
        outcome,
        tick_size,
        tokens,
        fee_schedule,
        end_date,
        resolved_at,
        created,
        updated,
    }
    .into();

    (upsert, registry)
}

fn token_infos(raw: &RawGammaMarket) -> Vec<TokenInfo> {
    raw.tokens
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|t| TokenInfo {
            token_id: TokenId::new(&t.token_id),
            outcome: t.outcome,
            neg_risk: raw.neg_risk.unwrap_or(false),
        })
        .collect()
}

fn market_status(raw: &RawGammaMarket) -> MarketStatus {
    if raw.closed.unwrap_or(false) {
        MarketStatus::Settled
    } else if raw.active.unwrap_or(true) {
        MarketStatus::Active
    } else {
        MarketStatus::Paused
    }
}

fn token_pair_or_fallback(tokens: &[TokenInfo]) -> (TokenId, TokenId) {
    let yes = tokens
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
    let no = tokens
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
    (yes, no)
}

fn parse_gamma_datetime(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .filter(|raw| !raw.trim().is_empty())
        .and_then(|raw| raw.parse::<DateTime<Utc>>().ok())
}

struct MarketUpsertParts<'a> {
    raw: &'a RawGammaMarket,
    market_id: &'a MarketId,
    event_id: &'a EventId,
    category: MarketCategory,
    status: MarketStatus,
    outcome: Option<String>,
    yes_token: &'a TokenId,
    no_token: &'a TokenId,
    tick_size: TickSize,
    end_date: Option<DateTime<Utc>>,
    resolved_at: Option<DateTime<Utc>>,
    fee_schedule: Option<&'a MarketFeeSchedule>,
}

impl From<MarketUpsertParts<'_>> for UpsertMarket {
    fn from(parts: MarketUpsertParts<'_>) -> Self {
        Self {
            market_id: parts.market_id.clone(),
            event_id: parts.event_id.clone(),
            question: parts.raw.question.clone(),
            slug: parts.raw.slug.clone().unwrap_or_default(),
            category: parts.category,
            status: parts.status,
            outcome: parts.outcome,
            yes_token_id: parts.yes_token.clone(),
            no_token_id: parts.no_token.clone(),
            tick_size: parts.tick_size,
            neg_risk: parts.raw.neg_risk.unwrap_or(false),
            end_date: parts.end_date,
            resolved_at: parts.resolved_at,
            fees_enabled: parts
                .fee_schedule
                .is_none_or(|schedule| schedule.fees_enabled),
            fee_rate: parts.fee_schedule.map(|schedule| schedule.fee_rate),
            fee_exponent: parts.fee_schedule.map(|schedule| schedule.exponent),
            fee_taker_only: parts.fee_schedule.map(|schedule| schedule.taker_only),
            fee_rebate_rate: parts.fee_schedule.and_then(|schedule| schedule.rebate_rate),
            fee_source: parts
                .fee_schedule
                .map(|schedule| schedule.source.as_str().to_owned()),
            fee_observed_at: parts.fee_schedule.map(|schedule| schedule.observed_at),
        }
    }
}

struct MarketRegistryParts<'a> {
    raw: RawGammaMarket,
    market_id: MarketId,
    event_id: &'a EventId,
    yes_token: TokenId,
    no_token: TokenId,
    category: MarketCategory,
    status: MarketStatus,
    outcome: Option<String>,
    tick_size: TickSize,
    tokens: Vec<TokenInfo>,
    fee_schedule: Option<MarketFeeSchedule>,
    end_date: Option<DateTime<Utc>>,
    resolved_at: Option<DateTime<Utc>>,
    created: DateTime<Utc>,
    updated: DateTime<Utc>,
}

impl From<MarketRegistryParts<'_>> for MarketRegistryInfo {
    fn from(parts: MarketRegistryParts<'_>) -> Self {
        Self {
            market_id: parts.market_id,
            event_id: parts.event_id.clone(),
            token_yes: parts.yes_token,
            token_no: parts.no_token,
            question: parts.raw.question,
            slug: parts.raw.slug.unwrap_or_default(),
            category: parts.category,
            status: parts.status,
            outcome: parts.outcome,
            neg_risk: parts.raw.neg_risk.unwrap_or(false),
            tick_size: parts.tick_size,
            tokens: parts.tokens,
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: parts.raw.minimum_order_size.unwrap_or(Decimal::ONE),
            volume_24h: Usd::ZERO,
            fee_schedule: parts.fee_schedule,
            end_date: parts.end_date,
            resolved_at: parts.resolved_at,
            created_at: parts.created,
            updated_at: parts.updated,
        }
    }
}

pub fn map_event(raw: RawGammaEvent) -> EventRegistryInfo {
    let id = raw.id;
    let title = raw.title;
    let slug = raw.slug;
    let neg_risk = raw.neg_risk.unwrap_or(false);
    let created_at = raw.created_at;
    let updated_at = raw.updated_at;
    let markets = raw.markets.unwrap_or_default();
    map_event_parts(
        &id,
        title,
        slug,
        neg_risk,
        created_at.as_deref(),
        updated_at.as_deref(),
        &markets,
    )
}

fn map_event_ref(raw: &RawGammaEvent) -> EventRegistryInfo {
    map_event_parts(
        &raw.id,
        raw.title.clone(),
        raw.slug.clone(),
        raw.neg_risk.unwrap_or(false),
        raw.created_at.as_deref(),
        raw.updated_at.as_deref(),
        raw.markets.as_deref().unwrap_or_default(),
    )
}

fn map_event_parts(
    id: &str,
    title: String,
    slug: String,
    neg_risk: bool,
    created_at: Option<&str>,
    updated_at: Option<&str>,
    markets: &[RawGammaMarket],
) -> EventRegistryInfo {
    let event_id = EventId::new(id);
    let market_ids: Vec<MarketId> = markets
        .iter()
        .map(|m| MarketId::new(&m.condition_id))
        .collect();

    EventRegistryInfo {
        event_id,
        title,
        slug,
        market_ids,
        neg_risk,
        created_at: created_at
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
            .unwrap_or_else(Utc::now),
        updated_at: updated_at
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
            .unwrap_or_else(Utc::now),
    }
}

pub fn map_market(raw: RawGammaMarket, event_id: &EventId) -> MarketRegistryInfo {
    let updated_at = raw
        .updated_at
        .as_deref()
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        .unwrap_or_else(Utc::now);
    let fee_schedule: Option<MarketFeeSchedule> = MarketFeeScheduleParts {
        raw: &raw,
        observed_at: updated_at,
    }
    .into();
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
    let outcome = raw
        .winning_outcome
        .clone()
        .or_else(|| raw.outcome.clone())
        .filter(|value| !value.trim().is_empty());
    let resolved_at = if raw.closed.unwrap_or(false) {
        raw.resolved_at
            .as_deref()
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
            .or(Some(updated_at))
    } else {
        None
    };

    let yes_token = tokens
        .iter()
        .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
        .map_or_else(|| TokenId::new(""), |t| t.token_id.clone());
    let no_token = tokens
        .iter()
        .find(|t| t.outcome.eq_ignore_ascii_case("no"))
        .map_or_else(|| TokenId::new(""), |t| t.token_id.clone());
    let created_at = raw
        .created_at
        .as_deref()
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        .unwrap_or_else(Utc::now);

    MarketRegistryInfo {
        market_id: MarketId::new(&raw.condition_id),
        event_id: event_id.clone(),
        token_yes: yes_token,
        token_no: no_token,
        question: raw.question,
        slug: raw.slug.unwrap_or_default(),
        category,
        status,
        outcome,
        neg_risk: raw.neg_risk.unwrap_or(false),
        tick_size,
        tokens,
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: raw.minimum_order_size.unwrap_or(Decimal::ONE),
        volume_24h: Usd::ZERO,
        fee_schedule,
        end_date: parse_gamma_datetime(raw.end_date.as_deref()),
        resolved_at,
        created_at,
        updated_at,
    }
}

struct MarketFeeScheduleParts<'a> {
    raw: &'a RawGammaMarket,
    observed_at: DateTime<Utc>,
}

impl From<MarketFeeScheduleParts<'_>> for Option<MarketFeeSchedule> {
    fn from(parts: MarketFeeScheduleParts<'_>) -> Self {
        let fee = parts.raw.fee_schedule.as_ref()?;
        Some(MarketFeeSchedule {
            market_id: MarketId::new(&parts.raw.condition_id),
            fees_enabled: parts.raw.fees_enabled.unwrap_or(true),
            fee_rate: fee.rate?,
            exponent: fee.exponent?,
            taker_only: fee.taker_only.unwrap_or(true),
            rebate_rate: fee.rebate_rate,
            source: FeeSource::GammaFeeSchedule,
            observed_at: parts.observed_at,
        })
    }
}
