//! Live neg-risk structural-drift monitor port (Phase 11.2.1).

use std::cmp::Reverse;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        NegRiskEventDriftView, NegRiskLegView, StructuralMonitorPort, market::registry::NegRiskLeg,
    },
    types::{EventId, Price},
};
use rust_decimal::Decimal;

use crate::pipeline::{book_store::BookStore, market_registry::MarketRegistry};

/// Live neg-risk structural-drift monitor backed by the in-memory registry +
/// book store.
pub struct CoreStructuralMonitor {
    market_registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
}

impl CoreStructuralMonitor {
    #[must_use]
    pub const fn new(market_registry: Arc<MarketRegistry>, book_store: Arc<BookStore>) -> Self {
        Self {
            market_registry,
            book_store,
        }
    }

    /// Resolve one leg's live best ask and question.
    fn leg_view(&self, leg: &NegRiskLeg) -> NegRiskLegView {
        let best_ask = self
            .book_store
            .load(&leg.yes_token_id)
            .and_then(|book| book.best_ask())
            .map(Price::inner);
        let question = self
            .market_registry
            .get_market(&leg.market_id)
            .map(|market| market.question.clone())
            .unwrap_or_default();
        NegRiskLegView {
            market_id: leg.market_id.clone(),
            yes_token_id: leg.yes_token_id.clone(),
            question,
            best_ask,
        }
    }
}

#[async_trait]
impl StructuralMonitorPort for CoreStructuralMonitor {
    async fn negrisk_events(&self) -> QuantResult<Vec<NegRiskEventDriftView>> {
        let as_of = Utc::now();
        // Distinct neg-risk events among active markets, deterministically ordered.
        let mut event_ids: Vec<EventId> = self
            .market_registry
            .active_markets()
            .iter()
            .filter_map(|market_id| self.market_registry.get_market(market_id))
            .filter(|market| market.neg_risk)
            .map(|market| market.event_id.clone())
            .collect();
        event_ids.sort();
        event_ids.dedup();

        let mut events = Vec::new();
        for event_id in event_ids {
            let Some(event) = self.market_registry.get_event(&event_id) else {
                continue;
            };
            let leg_set = self.market_registry.neg_risk_leg_set(&event_id);
            if leg_set.expected_legs == 0 {
                continue;
            }
            let leg_views: Vec<NegRiskLegView> =
                leg_set.legs.iter().map(|leg| self.leg_view(leg)).collect();
            let (ask_sum, drift) = leg_sum(&leg_views);
            events.push(NegRiskEventDriftView {
                event_id: event.event_id,
                title: event.title,
                leg_count: u32::try_from(leg_set.expected_legs).unwrap_or(u32::MAX),
                ask_sum,
                drift,
                legs: leg_views,
                as_of,
            });
        }
        // Most-mispriced first; events without a full ask sum sink to the end.
        events.sort_by_key(|event| Reverse(drift_magnitude(event.drift)));
        Ok(events)
    }
}

/// The best-ask sum across legs and its drift from 1, or `None` when any leg
/// lacks a published ask (never a fabricated sum).
fn leg_sum(legs: &[NegRiskLegView]) -> (Option<Decimal>, Option<Decimal>) {
    let mut sum = Decimal::ZERO;
    for leg in legs {
        let Some(ask) = leg.best_ask else {
            return (None, None);
        };
        sum += ask;
    }
    (Some(sum), Some(sum - Decimal::ONE))
}

/// Absolute drift magnitude for ordering (unavailable drift sorts last).
fn drift_magnitude(drift: Option<Decimal>) -> Decimal {
    drift.map_or(Decimal::NEGATIVE_ONE, |value| value.abs())
}
