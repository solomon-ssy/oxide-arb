//! Maps normalized Gamma catalog rows to domain types.
//!
//! `CatalogMarket` construction already enforces the binary-market invariant,
//! so persistence (`UpsertMarket`) and registry (`MarketRegistryInfo`) rows
//! are produced from the same canonical token pair — the two stores can never
//! disagree about a market's YES/NO legs.

use super::catalog::{CatalogEvent, CatalogMarket, RejectedMarket};
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::{
        fee::MarketFeeSchedule,
        market::{
            EventRegistryInfo, MarketRegistryInfo, TokenInfo, UpsertEvent, UpsertMarket,
            resolve_binary_pair_exact,
        },
    },
    enums::{common::CategorySet, fee::FeeSource, market::MarketStatus},
    types::{EventId, MarketId, TokenId, Usd},
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
            upsert_events.push(event.to_upsert_event());
            registry_events.push(event.to_registry_info());

            for market in event.markets {
                let (upsert, registry) =
                    market.into_upsert_and_registry(event_id.clone(), event_categories);
                upsert_markets.push(upsert);
                registry_markets.push(registry);
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

impl CatalogEvent {
    /// Maps a normalized catalog event to the in-memory registry view.
    pub fn to_registry_info(&self) -> EventRegistryInfo {
        EventRegistryInfo {
            event_id: EventId::new(&self.id),
            title: self.title.clone(),
            slug: self.slug.clone(),
            market_ids: self
                .markets
                .iter()
                .map(|market| MarketId::new(&market.condition_id))
                .collect(),
            categories: self.categories,
            tags: self.tags.clone(),
            neg_risk: self.neg_risk,
            created_at: self.created_at.unwrap_or_else(Utc::now),
            updated_at: self.updated_at.unwrap_or_else(Utc::now),
        }
    }

    /// Maps a normalized catalog event to the persistence upsert DTO.
    pub fn to_upsert_event(&self) -> UpsertEvent {
        UpsertEvent {
            event_id: EventId::new(&self.id),
            title: self.title.clone(),
            slug: self.slug.clone(),
            status: self.status,
            tags: self.tags.clone().into(),
            neg_risk: self.neg_risk,
            end_date: self.end_date,
            raw_gamma: Some(self.raw_wire.clone()),
        }
    }
}

impl CatalogMarket {
    /// Maps a normalized catalog market to persistence and registry domain rows.
    ///
    /// `CatalogMarket` construction guarantees the binary invariant, so the
    /// canonical pair resolution cannot fail here; both rows are built from
    /// the exact same (YES, NO) pair.
    pub fn into_upsert_and_registry(
        self,
        event_id: EventId,
        event_categories: CategorySet,
    ) -> (UpsertMarket, MarketRegistryInfo) {
        let tokens = token_infos(&self);
        let market_id = MarketId::new(&self.condition_id);
        // Infallible: the binary invariant is encoded in `CatalogMarket.tokens`.
        let (yes_token, no_token) = resolve_binary_pair_exact(&tokens);
        let status = self.status;
        let created = self.created_at.unwrap_or_else(Utc::now);
        let updated = self.updated_at.unwrap_or_else(Utc::now);
        let fee_schedule = self.fee_schedule_with_observed_at(Some(updated));
        let outcome = self
            .settlement
            .as_ref()
            .map(|settlement| settlement.winning_outcome.clone());
        let resolved_at =
            (status == MarketStatus::Settled).then(|| self.resolved_at.unwrap_or(updated));

        let upsert = UpsertMarket {
            market_id: market_id.clone(),
            event_id: event_id.clone(),
            question: self.question.clone(),
            slug: self.slug.clone().unwrap_or_default(),
            categories: event_categories,
            status,
            outcome: outcome.clone(),
            yes_token_id: yes_token.clone(),
            no_token_id: no_token.clone(),
            tick_size: self.tick_size,
            neg_risk: self.neg_risk,
            end_date: self.end_date,
            resolved_at,
            fees_enabled: fee_schedule
                .as_ref()
                .is_none_or(|schedule| schedule.fees_enabled),
            fee_rate: fee_schedule.as_ref().map(|schedule| schedule.fee_rate),
            fee_exponent: fee_schedule.as_ref().map(|schedule| schedule.exponent),
            fee_taker_only: fee_schedule.as_ref().map(|schedule| schedule.taker_only),
            fee_rebate_rate: fee_schedule
                .as_ref()
                .and_then(|schedule| schedule.rebate_rate),
            fee_source: fee_schedule
                .as_ref()
                .map(|schedule| schedule.source.as_str().to_owned()),
            fee_observed_at: fee_schedule.as_ref().map(|schedule| schedule.observed_at),
        };

        let registry = MarketRegistryInfo {
            market_id,
            event_id,
            token_yes: yes_token,
            token_no: no_token,
            question: self.question,
            slug: self.slug.unwrap_or_default(),
            categories: event_categories,
            status,
            outcome,
            neg_risk: self.neg_risk,
            tick_size: self.tick_size,
            tokens: tokens.to_vec(),
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: self.min_order_size,
            volume_24h: Usd::ZERO,
            fee_schedule,
            end_date: self.end_date,
            resolved_at,
            created_at: created,
            updated_at: updated,
        };

        (upsert, registry)
    }

    /// Maps a normalized catalog market to the in-memory registry view.
    pub fn into_registry_info(
        self,
        event_id: EventId,
        event_categories: CategorySet,
    ) -> MarketRegistryInfo {
        self.into_upsert_and_registry(event_id, event_categories).1
    }

    /// Typed fee schedule with an explicit observation timestamp.
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

fn token_infos(market: &CatalogMarket) -> [TokenInfo; 2] {
    let leg = |index: usize| TokenInfo {
        token_id: TokenId::new(&market.tokens[index].token_id),
        outcome: market.tokens[index].outcome.clone(),
        neg_risk: market.neg_risk,
    };
    [leg(0), leg(1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gamma::wire::WireEvent;
    use oxide_arb_models::enums::common::MarketCategory;

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
