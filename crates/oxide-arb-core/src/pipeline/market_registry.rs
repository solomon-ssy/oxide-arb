use std::collections::HashSet;

use dashmap::DashMap;
use parking_lot::RwLock;

use oxide_arb_models::domain::market::{EventRegistryInfo, MarketRegistryInfo};
use oxide_arb_models::enums::market::MarketStatus;
use oxide_arb_models::types::{EventId, MarketId, TokenId};

/// Market metadata registry with bidirectional token ↔ market lookup.
///
/// Populated by Gamma API sync; read by the Scanner/DataPipeline hot path.
pub struct MarketRegistry {
    markets: DashMap<MarketId, MarketRegistryInfo>,
    token_to_market: DashMap<TokenId, MarketId>,
    events: DashMap<EventId, EventRegistryInfo>,
    active_market_ids: RwLock<Vec<MarketId>>,
}

impl MarketRegistry {
    pub fn new() -> Self {
        Self {
            markets: DashMap::new(),
            token_to_market: DashMap::new(),
            events: DashMap::new(),
            active_market_ids: RwLock::new(Vec::new()),
        }
    }

    /// Register a single market. Rebuilds token→market index.
    pub fn register_market(&self, entry: MarketRegistryInfo) {
        for td in &entry.tokens {
            self.token_to_market
                .insert(td.token_id.clone(), entry.market_id.clone());
        }
        let is_active = entry.status == MarketStatus::Active;
        let market_id = entry.market_id.clone();
        self.markets.insert(market_id.clone(), entry);

        let mut active = self.active_market_ids.write();
        if is_active {
            if !active.contains(&market_id) {
                active.push(market_id);
            }
        } else {
            active.retain(|id| id != &market_id);
        }
    }

    /// Batch register markets (after Gamma full sync). Rebuilds active list once at the end.
    pub fn register_markets(&self, entries: Vec<MarketRegistryInfo>) {
        if entries.is_empty() {
            return;
        }

        let mut active = self.active_market_ids.read().clone();

        for entry in entries {
            for td in &entry.tokens {
                self.token_to_market
                    .insert(td.token_id.clone(), entry.market_id.clone());
            }

            let is_active = entry.status == MarketStatus::Active;
            let market_id = entry.market_id.clone();
            self.markets.insert(market_id.clone(), entry);

            if is_active {
                if !active.contains(&market_id) {
                    active.push(market_id);
                }
            } else {
                active.retain(|id| id != &market_id);
            }
        }

        *self.active_market_ids.write() = active;
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
            .active_market_ids()
            .into_iter()
            .filter(|id| !seen_ids.contains(id))
            .collect();

        if stale_ids.is_empty() {
            return Vec::new();
        }

        let mut deactivated = Vec::with_capacity(stale_ids.len());
        for id in stale_ids {
            if let Some(mut market) = self.get_market(&id) {
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

    /// Get a clone of the market entry.
    pub fn get_market(&self, market_id: &MarketId) -> Option<MarketRegistryInfo> {
        self.markets.get(market_id).map(|r| r.value().clone())
    }

    /// Return (YES token, NO token) for a market.
    ///
    /// Tokens are identified by their `outcome` field: "Yes" and "No".
    pub fn token_pair(&self, market_id: &MarketId) -> Option<(TokenId, TokenId)> {
        let entry = self.markets.get(market_id)?;
        let yes = entry
            .tokens
            .iter()
            .find(|t| t.outcome == "Yes")
            .map(|t| t.token_id.clone())?;
        let no = entry
            .tokens
            .iter()
            .find(|t| t.outcome == "No")
            .map(|t| t.token_id.clone())?;
        drop(entry);
        Some((yes, no))
    }

    /// Snapshot of all active market IDs.
    pub fn active_market_ids(&self) -> Vec<MarketId> {
        self.active_market_ids.read().clone()
    }

    /// Rebuild the active-market list from scratch by scanning all entries.
    pub fn refresh_active(&self) {
        let mut ids = Vec::new();
        for entry in &self.markets {
            if entry.value().status == MarketStatus::Active {
                ids.push(entry.key().clone());
            }
        }
        *self.active_market_ids.write() = ids;
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
    use oxide_arb_models::domain::market::TokenInfo;
    use oxide_arb_models::enums::common::{MarketCategory, TickSize};
    use rust_decimal_macros::dec;

    fn sample_market(id: &str, status: MarketStatus) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            question: "Test?".into(),
            slug: "test".into(),
            category: MarketCategory::Other,
            status,
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
            volume_24h: oxide_arb_models::types::Usd::ZERO,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn register_and_lookup() {
        let reg = MarketRegistry::new();
        reg.register_market(sample_market("m1", MarketStatus::Active));
        assert_eq!(reg.market_count(), 1);
        assert_eq!(reg.active_market_ids().len(), 1);

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
        assert!(reg.active_market_ids().is_empty());
    }

    #[test]
    fn batch_register() {
        let reg = MarketRegistry::new();
        reg.register_markets(vec![
            sample_market("m1", MarketStatus::Active),
            sample_market("m2", MarketStatus::Active),
        ]);
        assert_eq!(reg.market_count(), 2);
        assert_eq!(reg.active_market_ids().len(), 2);
    }
}
