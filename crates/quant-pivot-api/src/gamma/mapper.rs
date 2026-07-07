//! Maps normalized Gamma catalog rows to domain types.
//!
//! `CatalogMarket` construction already enforces the binary-market invariant,
//! so persistence (`UpsertMarket`) and registry (`MarketRegistryInfo`) rows
//! are produced from the same canonical token pair — the two stores can never
//! disagree about a market's YES/NO legs.

use crate::gamma::CatalogMarketReject;

use super::catalog::{CatalogEvent, CatalogMarket, RejectedMarket};
use chrono::{DateTime, Utc};
use quant_pivot_error::market::MarketError;
use quant_pivot_models::{
    domain::{
        fee::{MarketFeeColumns, MarketFeeSchedule},
        market::{
            EventRegistryInfo, MarketRegistryInfo, TokenInfo, UpsertEvent, UpsertMarket,
            resolve_binary_pair_exact,
        },
    },
    enums::{common::CategorySet, fee::FeeSource, market::MarketStatus},
    types::{EventId, MarketId, TokenId},
};

/// Complete output of a Gamma catalog sync — persistence DTOs and in-memory views.
pub struct GammaCatalogBatch {
    pub events: Vec<UpsertEvent>,
    pub markets: Vec<UpsertMarket>,
    pub registry_events: Vec<EventRegistryInfo>,
    pub registry_markets: Vec<MarketRegistryInfo>,
    pub fee_data: Vec<MarketFeeSchedule>,
    /// Markets dropped during normalization, for sync summaries and metrics.
    pub rejected: Vec<RejectedMarket>,
}

impl From<Vec<CatalogEvent>> for GammaCatalogBatch {
    fn from(events: Vec<CatalogEvent>) -> Self {
        let fee_data = events
            .iter()
            .flat_map(|event| {
                event
                    .markets
                    .iter()
                    .filter_map(|market| market.fee_schedule_with_observed_at(market.updated_at))
            })
            .collect();

        let mut upsert_events = Vec::with_capacity(events.len());
        let mut upsert_markets = Vec::new();
        let mut registry_events = Vec::with_capacity(events.len());
        let mut registry_markets = Vec::new();
        let mut rejected = Vec::new();

        for mut event in events {
            let event_id = EventId::new(&event.id);
            let event_categories = event.categories;
            rejected.append(&mut event.rejected_markets);
            upsert_events.push(UpsertEvent::from(&event));
            registry_events.push(EventRegistryInfo::from(&event));

            for market in event.markets {
                let condition_id = market.condition_id.clone();
                let ctx = CatalogMarketMapCtx {
                    event_id: event_id.clone(),
                    categories: event_categories,
                };
                match TryFrom::try_from(CatalogMarketWithCtx { market, ctx }) {
                    Ok((upsert, registry)) => {
                        upsert_markets.push(upsert);
                        registry_markets.push(registry);
                    }
                    Err(error) => rejected.push(RejectedMarket {
                        condition_id,
                        reject: CatalogMarketReject::InvalidTokenPair {
                            reason: error.to_string(),
                        },
                    }),
                }
            }
        }

        Self {
            events: upsert_events,
            markets: upsert_markets,
            registry_events,
            registry_markets,
            fee_data,
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
            market_ids: event
                .markets
                .iter()
                .map(|market| MarketId::new(&market.condition_id))
                .collect(),
            categories: event.categories,
            tags: event.tags.clone(),
            neg_risk: event.neg_risk,
            created_at: event.created_at.unwrap_or_else(Utc::now),
            updated_at: event.updated_at.unwrap_or_else(Utc::now),
        }
    }
}

impl From<&CatalogEvent> for UpsertEvent {
    fn from(event: &CatalogEvent) -> Self {
        Self {
            event_id: EventId::new(&event.id),
            title: event.title.clone(),
            slug: event.slug.clone(),
            status: event.status,
            tags: event.tags.clone().into(),
            neg_risk: event.neg_risk,
            catalog_market_ids: event
                .markets
                .iter()
                .map(|market| MarketId::new(&market.condition_id))
                .collect::<Vec<_>>()
                .into(),
            end_date: event.end_date,
            raw_gamma: Some(event.raw_wire.clone()),
        }
    }
}

/// Context required to map a catalog market into persistence and registry rows.
pub struct CatalogMarketMapCtx {
    pub event_id: EventId,
    pub categories: CategorySet,
}

/// Catalog market plus mapping context for domain conversion.
pub struct CatalogMarketWithCtx {
    pub market: CatalogMarket,
    pub ctx: CatalogMarketMapCtx,
}

impl TryFrom<CatalogMarketWithCtx> for (UpsertMarket, MarketRegistryInfo) {
    type Error = MarketError;

    fn try_from(value: CatalogMarketWithCtx) -> Result<Self, Self::Error> {
        let CatalogMarketWithCtx { market, ctx } = value;
        let tokens = TokenInfoPair::from(&market);
        let market_id = MarketId::new(&market.condition_id);
        let (yes_token, no_token) = resolve_binary_pair_exact(&market_id, &tokens.0)?;
        let status = market.status;
        let created = market.created_at.unwrap_or_else(Utc::now);
        let updated = market.updated_at.unwrap_or_else(Utc::now);
        let fee_schedule = market.fee_schedule_with_observed_at(Some(updated));
        let fee_columns = fee_schedule.as_ref().map_or_else(
            MarketFeeColumns::disabled,
            MarketFeeSchedule::to_market_fee_columns,
        );
        let outcome = market
            .settlement
            .as_ref()
            .map(|settlement| settlement.winning_outcome.clone());
        let resolved_at =
            (status == MarketStatus::Settled).then(|| market.resolved_at.unwrap_or(updated));

        let upsert = UpsertMarket {
            market_id: market_id.clone(),
            event_id: ctx.event_id.clone(),
            question: market.question.clone(),
            slug: market.slug.clone().unwrap_or_default(),
            categories: ctx.categories,
            status,
            outcome: outcome.clone(),
            yes_token_id: yes_token.clone(),
            no_token_id: no_token.clone(),
            tick_size: market.tick_size,
            neg_risk: market.neg_risk,
            end_date: market.end_date,
            resolved_at,
            fees_enabled: fee_columns.fees_enabled,
            fee_rate: fee_columns.fee_rate,
            fee_exponent: fee_columns.fee_exponent,
            fee_taker_only: fee_columns.fee_taker_only,
            fee_rebate_rate: fee_columns.fee_rebate_rate,
            fee_source: fee_columns.fee_source,
            fee_observed_at: fee_columns.fee_observed_at,
        };

        let registry = MarketRegistryInfo {
            market_id,
            event_id: ctx.event_id,
            token_yes: yes_token,
            token_no: no_token,
            question: market.question,
            slug: market.slug.unwrap_or_default(),
            categories: ctx.categories,
            status,
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
            fee_schedule,
            end_date: market.end_date,
            resolved_at,
            created_at: created,
            updated_at: updated,
        };

        Ok((upsert, registry))
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

impl CatalogMarket {
    pub fn fee_schedule_with_observed_at(
        &self,
        observed_at: Option<DateTime<Utc>>,
    ) -> Option<MarketFeeSchedule> {
        let fee = self.fee_schedule.as_ref()?;
        Some(MarketFeeSchedule {
            market_id: MarketId::new(&self.condition_id),
            fees_enabled: self.fees_enabled,
            fee_rate: fee.rate?,
            exponent: fee.exponent?,
            taker_only: fee.taker_only.unwrap_or(true),
            rebate_rate: fee.rebate_rate,
            source: FeeSource::GammaFeeSchedule,
            observed_at: observed_at.unwrap_or_else(Utc::now),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gamma::wire::WireEvent;
    use quant_pivot_models::enums::common::MarketCategory;

    fn batch_from(json: &str) -> GammaCatalogBatch {
        let wire: WireEvent = serde_json::from_str(json).expect("wire event");
        GammaCatalogBatch::from(vec![CatalogEvent::from(wire)])
    }

    #[test]
    fn upsert_and_registry_share_the_same_pair_and_categories() {
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

        let upsert = &batch.markets[0];
        let registry = &batch.registry_markets[0];
        assert_eq!(upsert.yes_token_id, registry.token_yes);
        assert_eq!(upsert.no_token_id, registry.token_no);
        assert_eq!(upsert.yes_token_id.as_str(), "11");
        assert_eq!(upsert.no_token_id.as_str(), "22");
        assert_eq!(upsert.categories, registry.categories);
        assert_eq!(registry.fee_category(), MarketCategory::Sports);
        assert_eq!(upsert.event_id, registry.event_id);
        assert!(batch.rejected.is_empty());
    }

    #[test]
    fn settled_market_carries_winning_outcome_and_close_time() {
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

        let upsert = &batch.markets[0];
        assert_eq!(upsert.status, MarketStatus::Settled);
        assert_eq!(upsert.outcome.as_deref(), Some("Over"));
        let resolved_at = upsert.resolved_at.expect("resolved_at from closedTime");
        assert_eq!(resolved_at.to_rfc3339(), "2026-06-11T04:05:01+00:00");
    }

    #[test]
    fn rejected_markets_flow_into_the_batch() {
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
        assert!(batch.markets.is_empty());
        assert_eq!(batch.rejected.len(), 1);
        assert_eq!(batch.rejected[0].condition_id, "0xmulti");
    }
}
