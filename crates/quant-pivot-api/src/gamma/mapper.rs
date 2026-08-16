//! Maps normalized Gamma catalog rows to domain types.
//!
//! `CatalogMarket` construction already enforces the binary-market invariant,
//! so downstream persistence and the in-memory registry share one canonical
//! normalized representation.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::market::MarketError;
use quant_pivot_models::{
    domain::market::{
        EventRegistryInfo, MarketRegistryInfo, TokenInfo, fee::MarketMakerRebateSchedule,
        resolve_binary_pair_exact,
    },
    enums::common::CategorySet,
    hashing::CanonicalDigest,
    types::{EventId, MarketId, TokenId},
};

use super::catalog::{CatalogEvent, CatalogMarket, FilteredPrelistingMarket, RejectedMarket};
use crate::gamma::CatalogMarketReject;

/// Source timestamps retained separately from the live registry's resolved
/// clock fields so the durable ledger can distinguish source time from a
/// conservative availability-time fallback.
#[derive(Debug, Clone, Copy)]
pub struct CatalogSourceTimestamps {
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Complete output of a Gamma catalog sync after wire validation.
pub struct GammaCatalogBatch {
    pub registry_events: Vec<EventRegistryInfo>,
    pub registry_markets: Vec<MarketRegistryInfo>,
    pub event_source_timestamps: HashMap<EventId, CatalogSourceTimestamps>,
    pub market_source_timestamps: HashMap<MarketId, CatalogSourceTimestamps>,
    /// Legitimate Gamma pre-listing objects excluded from the venue projection.
    pub filtered: Vec<FilteredPrelistingMarket>,
    /// Markets dropped during normalization, for sync summaries and metrics.
    pub rejected: Vec<RejectedMarket>,
}

impl From<Vec<CatalogEvent>> for GammaCatalogBatch {
    fn from(events: Vec<CatalogEvent>) -> Self {
        let available_at = Utc::now();
        let event_source_timestamps = events
            .iter()
            .map(|event| {
                (
                    EventId::new(&event.id),
                    CatalogSourceTimestamps {
                        created_at: event.created_at,
                        updated_at: event.updated_at,
                    },
                )
            })
            .collect();
        let market_source_timestamps = events
            .iter()
            .flat_map(|event| {
                event.markets.iter().map(|market| {
                    (
                        MarketId::new(&market.condition_id),
                        CatalogSourceTimestamps {
                            created_at: market.created_at,
                            updated_at: market.updated_at,
                        },
                    )
                })
            })
            .collect();
        let mut registry_events = Vec::with_capacity(events.len());
        let mut registry_markets = Vec::new();
        let mut filtered = Vec::new();
        let mut rejected = Vec::new();

        for mut event in events {
            let event_id = EventId::new(&event.id);
            let event_categories = event.categories;
            filtered.append(&mut event.filtered_markets);
            rejected.append(&mut event.rejected_markets);
            registry_events.push(EventRegistryInfo::from(&event));

            for market in event.markets {
                let condition_id = market.condition_id.clone();
                let ctx = CatalogMarketMapCtx {
                    event_id: event_id.clone(),
                    categories: event_categories,
                    available_at,
                };
                match MarketRegistryInfo::try_from(CatalogMarketWithCtx { market, ctx }) {
                    Ok(registry) => registry_markets.push(registry),
                    Err(error) => rejected.push(RejectedMarket {
                        condition_id,
                        reject: CatalogMarketReject::InvalidTokenPair {
                            reason: error.to_string(),
                        },
                        raw_payload: None,
                    }),
                }
            }
        }

        Self {
            registry_events,
            registry_markets,
            event_source_timestamps,
            market_source_timestamps,
            filtered,
            rejected,
        }
    }
}

impl From<&CatalogEvent> for EventRegistryInfo {
    fn from(event: &CatalogEvent) -> Self {
        Self {
            event_id: EventId::new(&event.id),
            title: event.title.clone(),
            slug: event.slug.clone(),
            series_slug: event.series_slug.clone(),
            status: event.status,
            market_ids: event
                .markets
                .iter()
                .map(|market| MarketId::new(&market.condition_id))
                .collect(),
            categories: event.categories,
            tags: event.tags.clone(),
            neg_risk: event.neg_risk,
            end_date: event.end_date,
            created_at: event.created_at.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            updated_at: event
                .updated_at
                .or(event.created_at)
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        }
    }
}

/// Context required to map a catalog market into persistence and registry rows.
pub struct CatalogMarketMapCtx {
    pub event_id: EventId,
    pub categories: CategorySet,
    pub available_at: DateTime<Utc>,
}

/// Catalog market plus mapping context for domain conversion.
pub struct CatalogMarketWithCtx {
    pub market: CatalogMarket,
    pub ctx: CatalogMarketMapCtx,
}

impl TryFrom<CatalogMarketWithCtx> for MarketRegistryInfo {
    type Error = MarketError;

    fn try_from(value: CatalogMarketWithCtx) -> Result<Self, Self::Error> {
        let CatalogMarketWithCtx { market, ctx } = value;
        let tokens = TokenInfoPair::from(&market);
        let market_id = MarketId::new(&market.condition_id);
        let (yes_token, no_token) = resolve_binary_pair_exact(&market_id, &tokens.0)?;
        let status = market.status;
        let created = market.created_at;
        let updated = market
            .updated_at
            .or(market.created_at)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        let outcome = market
            .settlement
            .as_ref()
            .map(|settlement| settlement.winning_outcome.clone());
        // `closedTime` is the only source resolution clock. A settled market
        // without it remains unknown; substituting `updatedAt` (or wall clock)
        // would fabricate label maturity and a live settlement transition.
        let resolved_at = market.resolved_at;
        let maker_rebate_schedule = market
            .maker_rebate_schedule
            .as_ref()
            .map(|source| {
                let schedule_hash = CanonicalDigest::content_hash_typed(
                    "quant-pivot/market-maker-rebate-schedule",
                    1,
                    &(
                        &market_id,
                        source.fees_enabled,
                        source.platform_rate,
                        source.exponent,
                        source.taker_only,
                        source.rebate_rate,
                        source.effective_at,
                        ctx.available_at,
                        source.catalog_change_hash,
                    ),
                )
                .map_err(|error| MarketError::CatalogSerialization {
                    entity: "market_maker_rebate_schedule",
                    id: market_id.to_string(),
                    reason: error.to_string(),
                })?;
                Ok::<_, MarketError>(MarketMakerRebateSchedule {
                    market_id: market_id.clone(),
                    fees_enabled: source.fees_enabled,
                    platform_rate: source.platform_rate,
                    exponent: source.exponent,
                    taker_only: source.taker_only,
                    rebate_rate: source.rebate_rate,
                    effective_at: source.effective_at,
                    available_at: ctx.available_at,
                    catalog_change_hash: source.catalog_change_hash,
                    schedule_hash,
                })
            })
            .transpose()?;

        let registry = Self {
            market_id,
            event_id: ctx.event_id,
            token_yes: yes_token,
            token_no: no_token,
            question: market.question,
            slug: market.slug.unwrap_or_default(),
            description: market.description,
            categories: ctx.categories,
            status,
            filter_reasons: market.filter_reasons,
            outcome,
            neg_risk: market.neg_risk,
            tick_size: market.tick_size,
            tokens: tokens.0.to_vec(),
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: market.min_order_size,
            liquidity_usd: market.liquidity_usd,
            volume_24h: market.volume_24h_usd,
            maker_rebate_schedule,
            start_date: market.start_date,
            end_date: market.end_date,
            resolved_at,
            created_at: created,
            updated_at: updated,
        };

        Ok(registry)
    }
}

struct TokenInfoPair([TokenInfo; 2]);

impl From<&CatalogMarket> for TokenInfoPair {
    fn from(market: &CatalogMarket) -> Self {
        let leg = |index: usize| TokenInfo {
            token_id: TokenId::new(&market.tokens[index].token_id),
            outcome: market.tokens[index].outcome.clone(),
            neg_risk: market.neg_risk,
        };
        Self([leg(0), leg(1)])
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        domain::market::UpsertMarket,
        enums::{common::MarketCategory, market::MarketStatus},
    };

    use super::*;
    use crate::gamma::wire::WireEvent;

    fn batch_from(json: &str) -> GammaCatalogBatch {
        let wire: WireEvent = serde_json::from_str(json).expect("wire event");
        GammaCatalogBatch::from(vec![CatalogEvent::from(wire)])
    }

    #[test]
    fn upsert_registry_share_categories() {
        let batch = batch_from(
            r#"{
                "id": "evt-1",
                "title": "E",
                "slug": "e",
                "tags": [{ "label": "Sports", "slug": "sports" }],
                "markets": [{
                    "conditionId": "0xabc",
                    "question": "Team A vs Team B?",
                    "clobTokenIds": ["11", "22"],
                    "outcomes": ["Team A", "Team B"]
                }]
            }"#,
        );

        let registry = &batch.registry_markets[0];
        let upsert =
            UpsertMarket::from_registry(registry).expect("normalized market remains persistable");
        assert_eq!(upsert.yes_token_id, registry.token_yes);
        assert_eq!(upsert.no_token_id, registry.token_no);
        assert_eq!(upsert.yes_token_id.as_str(), "11");
        assert_eq!(upsert.no_token_id.as_str(), "22");
        assert_eq!(upsert.categories, registry.categories);
        assert_eq!(registry.primary_category(), MarketCategory::Sports);
        assert_eq!(upsert.event_id, registry.event_id);
        assert!(batch.rejected.is_empty());
    }

    #[test]
    fn settled_market_carries_time() {
        let batch = batch_from(
            r#"{
                "id": "evt-1",
                "title": "E",
                "slug": "e",
                "markets": [{
                    "conditionId": "0xdone",
                    "question": "Over/Under?",
                    "closed": true,
                    "active": false,
                    "umaResolutionStatus": "resolved",
                    "closedTime": "2026-06-11 04:05:01+00",
                    "updatedAt": "2026-06-11T05:00:00Z",
                    "clobTokenIds": ["111", "222"],
                    "outcomes": ["Over", "Under"],
                    "outcomePrices": ["1", "0"]
                }]
            }"#,
        );

        let registry = &batch.registry_markets[0];
        assert_eq!(registry.status, MarketStatus::Settled);
        assert_eq!(registry.outcome.as_deref(), Some("Over"));
        let resolved_at = registry.resolved_at.expect("resolved_at from closedTime");
        assert_eq!(resolved_at.to_rfc3339(), "2026-06-11T04:05:01+00:00");
    }

    #[test]
    fn missing_close_keeps_unknown() {
        let batch = batch_from(
            r#"{
                "id": "evt-1",
                "title": "E",
                "slug": "e",
                "markets": [{
                    "conditionId": "0xdone",
                    "question": "Over/Under?",
                    "closed": true,
                    "active": false,
                    "umaResolutionStatus": "resolved",
                    "updatedAt": "2026-06-11T05:00:00Z",
                    "clobTokenIds": ["111", "222"],
                    "outcomes": ["Over", "Under"],
                    "outcomePrices": ["1", "0"]
                }]
            }"#,
        );

        assert_eq!(batch.registry_markets[0].status, MarketStatus::Settled);
        assert!(batch.registry_markets[0].resolved_at.is_none());
    }

    #[test]
    fn rejected_markets_into_batch() {
        let batch = batch_from(
            r#"{
                "id": "evt-1",
                "title": "E",
                "slug": "e",
                "markets": [{
                    "conditionId": "0xmulti",
                    "question": "?",
                    "clobTokenIds": ["1", "2", "3"],
                    "outcomes": ["A", "B", "C"]
                }]
            }"#,
        );
        assert!(batch.registry_markets.is_empty());
        assert_eq!(batch.rejected.len(), 1);
        assert_eq!(batch.rejected[0].condition_id, "0xmulti");
    }
}
