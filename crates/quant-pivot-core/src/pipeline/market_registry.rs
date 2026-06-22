use crate::pipeline::universe_filter::MarketUniverseFilter;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use quant_pivot_models::{
    domain::market::{EventRegistryInfo, MarketRegistryInfo},
    enums::market::MarketStatus,
    types::{EventId, MarketId, TokenId},
};
use std::{collections::HashSet, sync::Arc};

/// Market metadata registry with bidirectional token ↔ market lookup.
///
/// Populated by Gamma API sync; read by the Scanner/DataPipeline hot path.
pub struct MarketRegistry {
    markets: DashMap<MarketId, Arc<MarketRegistryInfo>>,
    token_to_market: DashMap<TokenId, MarketId>,
    events: DashMap<EventId, EventRegistryInfo>,
    active_markets: ArcSwap<Vec<MarketId>>,
}

impl MarketRegistry {
    pub fn new() -> Self {
        Self {
            markets: DashMap::new(),
            token_to_market: DashMap::new(),
            events: DashMap::new(),
            active_markets: ArcSwap::from_pointee(Vec::new()),
        }
    }

    fn store_active_ids(&self, ids: Vec<MarketId>) {
        self.active_markets.store(Arc::new(ids));
    }

    fn push_active(&self, market_id: &MarketId) {
        self.active_markets.rcu(|current| {
            if current.iter().any(|id| id == market_id) {
                return current.clone();
            }
            let mut next = current.to_vec();
            next.push(market_id.clone());
            Arc::new(next)
        });
    }

    fn remove_active(&self, market_id: &MarketId) {
        self.active_markets.rcu(|current| {
            if !current.iter().any(|id| id == market_id) {
                return current.clone();
            }
            Arc::new(
                current
                    .iter()
                    .filter(|id| *id != market_id)
                    .cloned()
                    .collect(),
            )
        });
    }

    /// Register a single market. Rebuilds token→market index.
    pub fn register_market(&self, mut entry: MarketRegistryInfo) {
        if entry.resolve_token_pair().is_err() {
            tracing::warn!(market_id = %entry.market_id, "skipping market with invalid token pair");
            return;
        }
        for td in &entry.tokens {
            self.token_to_market
                .insert(td.token_id.clone(), entry.market_id.clone());
        }
        let is_active = entry.status == MarketStatus::Active;
        let market_id = entry.market_id.clone();
        self.markets.insert(market_id.clone(), Arc::new(entry));

        if is_active {
            self.push_active(&market_id);
        } else {
            self.remove_active(&market_id);
        }
    }

    /// Batch register markets (after Gamma full sync). Rebuilds active list once at the end.
    pub fn register_markets(&self, entries: Vec<MarketRegistryInfo>) {
        if entries.is_empty() {
            return;
        }

        let mut active = self.active_markets().iter().cloned().collect::<Vec<_>>();

        for mut entry in entries {
            if entry.resolve_token_pair().is_err() {
                tracing::warn!(market_id = %entry.market_id, "skipping market with invalid token pair");
                continue;
            }
            for td in &entry.tokens {
                self.token_to_market
                    .insert(td.token_id.clone(), entry.market_id.clone());
            }

            let is_active = entry.status == MarketStatus::Active;
            let market_id = entry.market_id.clone();
            self.markets.insert(market_id.clone(), Arc::new(entry));

            if is_active {
                if !active.iter().any(|id| id == &market_id) {
                    active.push(market_id);
                }
            } else {
                active.retain(|id| id != &market_id);
            }
        }

        self.store_active_ids(active);
    }

    /// Batch register events from Gamma sync.
    pub fn register_events(&self, events: impl IntoIterator<Item = EventRegistryInfo>) {
        for event in events {
            self.register_event(event);
        }
    }

    /// Mark active markets absent from the latest full sync as paused.
    ///
    /// Returns the deactivated entries for downstream persistence.
    pub fn deactivate_stale(&self, seen_ids: &HashSet<MarketId>) -> Vec<MarketRegistryInfo> {
        let stale_ids: Vec<MarketId> = self
            .active_markets()
            .iter()
            .filter(|id| !seen_ids.contains(id))
            .cloned()
            .collect();

        if stale_ids.is_empty() {
            return Vec::new();
        }

        let mut deactivated = Vec::with_capacity(stale_ids.len());
        for id in stale_ids {
            if let Some(market) = self.get_market(&id) {
                let mut market = (*market).clone();
                market.status = MarketStatus::Paused;
                deactivated.push(market);
            }
        }

        self.register_markets(deactivated.clone());
        deactivated
    }

    /// Register or update an event.
    pub fn register_event(&self, entry: EventRegistryInfo) {
        self.events.insert(entry.event_id.clone(), entry);
    }

    /// Reverse lookup: token → market.
    pub fn market_for_token(&self, token_id: &TokenId) -> Option<MarketId> {
        self.token_to_market
            .get(token_id)
            .map(|r| r.value().clone())
    }

    /// Get a shared market entry.
    pub fn get_market(&self, market_id: &MarketId) -> Option<Arc<MarketRegistryInfo>> {
        self.markets.get(market_id).map(|r| Arc::clone(r.value()))
    }

    /// Return whether a market is negative-risk without cloning the full entry.
    pub fn neg_risk(&self, market_id: &MarketId) -> Option<bool> {
        self.markets.get(market_id).map(|entry| entry.neg_risk)
    }

    /// Return (YES token, NO token) for a market.
    ///
    /// Tokens are identified by their `outcome` field: "Yes" and "No".
    pub fn token_pair(&self, market_id: &MarketId) -> Option<(TokenId, TokenId)> {
        self.markets
            .get(market_id)
            .map(|entry| (entry.token_yes.clone(), entry.token_no.clone()))
    }

    /// Wait-free read of the active market ID list.
    #[must_use]
    pub fn active_markets(&self) -> Arc<Vec<MarketId>> {
        self.active_markets.load_full()
    }

    /// Active YES/NO catalog tokens bounded by the tradeable-universe filter.
    ///
    /// This is a catalog helper, not the engine WS subscription policy. Trading
    /// subscriptions must go through `MarketDataSubscriptionPolicy`.
    #[must_use]
    pub fn active_catalog_tokens(&self, universe: &MarketUniverseFilter) -> Vec<TokenId> {
        let active = self.active_markets();
        let mut tokens = Vec::with_capacity(active.len() * 2);
        for market_id in active.iter() {
            let Some(market) = self.get_market(market_id) else {
                continue;
            };
            if market.status != MarketStatus::Active {
                continue;
            }
            if !universe.is_enabled(market.categories) {
                continue;
            }
            tokens.push(market.token_yes.clone());
            tokens.push(market.token_no.clone());
        }
        tokens.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        tokens.dedup_by(|a, b| a.as_str() == b.as_str());
        tokens
    }

    /// Rebuild the active-market list from scratch by scanning all entries.
    pub fn refresh_active(&self) {
        let mut ids = Vec::new();
        for entry in &self.markets {
            if entry.value().status == MarketStatus::Active {
                ids.push(entry.key().clone());
            }
        }
        self.store_active_ids(ids);
    }

    /// Total number of registered markets.
    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}

impl Default for MarketRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use quant_pivot_models::{
        domain::market::TokenInfo,
        enums::common::{CategorySet, MarketCategory, TickSize},
        types::Usd,
    };
    use rust_decimal_macros::dec;
    fn sample_market(id: &str, status: MarketStatus) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            token_yes: TokenId::new(format!("{id}-yes")),
            token_no: TokenId::new(format!("{id}-no")),
            question: "Test?".into(),
            slug: "test".into(),
            categories: CategorySet::from(MarketCategory::Other),
            status,
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-yes")),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-no")),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(5),
            volume_24h: Usd::ZERO,
            fee_schedule: None,
            end_date: None,
            resolved_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn register_and_lookup() {
        let reg = MarketRegistry::new();
        reg.register_market(sample_market("m1", MarketStatus::Active));
        assert_eq!(reg.market_count(), 1);
        assert_eq!(reg.active_markets().len(), 1);

        let mid = reg
            .market_for_token(&TokenId::new("m1-yes"))
            .expect("token should map to market");
        assert_eq!(mid, MarketId::new("m1"));

        let (yes, no) = reg.token_pair(&MarketId::new("m1")).unwrap();
        assert_eq!(yes, TokenId::new("m1-yes"));
        assert_eq!(no, TokenId::new("m1-no"));
    }

    #[test]
    fn inactive_market_not_in_active_list() {
        let reg = MarketRegistry::new();
        reg.register_market(sample_market("m1", MarketStatus::Settled));
        assert!(reg.active_markets().is_empty());
    }

    #[test]
    fn batch_register() {
        let reg = MarketRegistry::new();
        reg.register_markets(vec![
            sample_market("m1", MarketStatus::Active),
            sample_market("m2", MarketStatus::Active),
        ]);
        assert_eq!(reg.market_count(), 2);
        assert_eq!(reg.active_markets().len(), 2);
    }

    #[test]
    fn active_markets_snapshot_is_wait_free() {
        let reg = MarketRegistry::new();
        reg.register_market(sample_market("m1", MarketStatus::Active));
        let snapshot = reg.active_markets();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].as_str(), "m1");
    }
}
