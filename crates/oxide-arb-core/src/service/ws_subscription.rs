//! Keeps CLOB websocket subscriptions aligned with the trading hotset.
//!
//! The full Gamma catalog is too large to mirror blindly into Polymarket WS
//! connections. The engine baseline is therefore selected by
//! [`MarketDataSubscriptionPolicy`] and registered under
//! [`SubscriptionSource::Engine`]; the web plane remains an overlay and can
//! never unsubscribe an engine-owned token.

use crate::pipeline::{market_registry::MarketRegistry, universe_filter::MarketUniverseFilter};
use chrono::{Duration, Utc};
use oxide_arb_api::ws::{ClobWsManager, SubscriptionSource};
use oxide_arb_models::{
    domain::market::MarketRegistryInfo, enums::market::MarketStatus, types::TokenId,
};
use std::sync::Arc;

/// Bounded selector for the trading engine's market-data hotset.
#[derive(Debug, Clone, Copy)]
pub struct MarketDataSubscriptionPolicy {
    max_subscription_tokens: usize,
    endgame_window_hours: u64,
}

pub struct WsSubscriptionCoordinator {
    ws_manager: Arc<ClobWsManager>,
    policy: MarketDataSubscriptionPolicy,
}

struct Candidate {
    market: Arc<MarketRegistryInfo>,
    rank_ms: i64,
}

impl WsSubscriptionCoordinator {
    pub const fn new(ws_manager: Arc<ClobWsManager>, policy: MarketDataSubscriptionPolicy) -> Self {
        Self { ws_manager, policy }
    }

    /// Reconcile the engine baseline to the policy-selected trading hotset.
    pub fn sync_engine_hotset(
        &self,
        registry: &MarketRegistry,
        universe: &MarketUniverseFilter,
    ) -> usize {
        let desired = self.policy.select_tokens(registry, universe);
        self.ws_manager
            .sync_tokens(SubscriptionSource::Engine, &desired);
        desired.len()
    }

    #[must_use]
    pub fn subscribed_count(&self) -> usize {
        self.ws_manager
            .source_subscription_count(SubscriptionSource::Engine)
    }
}

impl MarketDataSubscriptionPolicy {
    #[must_use]
    pub const fn new(max_subscription_tokens: usize, endgame_window_hours: u64) -> Self {
        Self {
            max_subscription_tokens,
            endgame_window_hours,
        }
    }

    /// Return a stable, bounded token list for the engine WS baseline.
    #[must_use]
    pub fn select_tokens(
        &self,
        registry: &MarketRegistry,
        universe: &MarketUniverseFilter,
    ) -> Vec<TokenId> {
        let mut candidates = self.collect_candidates(registry, universe);
        candidates.sort_by(|left, right| {
            left.rank_ms.cmp(&right.rank_ms).then(
                left.market
                    .market_id
                    .as_str()
                    .cmp(right.market.market_id.as_str()),
            )
        });

        let mut tokens = Vec::with_capacity(self.max_subscription_tokens);
        for candidate in candidates {
            if tokens.len().saturating_add(2) > self.max_subscription_tokens {
                break;
            }
            tokens.push(candidate.market.token_yes.clone());
            tokens.push(candidate.market.token_no.clone());
        }
        tokens.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        tokens.dedup_by(|a, b| a.as_str() == b.as_str());
        tokens
    }

    fn collect_candidates(
        &self,
        registry: &MarketRegistry,
        universe: &MarketUniverseFilter,
    ) -> Vec<Candidate> {
        let now = Utc::now();
        let hours = i64::try_from(self.endgame_window_hours).unwrap_or(i64::MAX);
        let cutoff = now + Duration::hours(hours);
        let active = registry.active_markets();
        let mut candidates = Vec::new();

        for market_id in active.iter() {
            let Some(market) = registry.get_market(market_id) else {
                continue;
            };
            if market.status != MarketStatus::Active || !universe.is_enabled(market.categories) {
                continue;
            }
            let rank_ms = match market.end_date {
                Some(end_date) if end_date <= cutoff => end_date
                    .signed_duration_since(now)
                    .num_milliseconds()
                    .max(0),
                Some(_) => continue,
                None => i64::MAX,
            };
            candidates.push(Candidate { market, rank_ms });
        }

        candidates
    }
}
