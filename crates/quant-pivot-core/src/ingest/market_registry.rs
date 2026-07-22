use std::{collections::HashSet, sync::Arc};

use quant_pivot_models::{
    domain::market::{
        EventRegistryInfo, MarketRegistryInfo,
        registry::{CatalogMarketLeg, NegRiskLegSet},
    },
    enums::market::MarketStatus,
    types::{EventId, MarketId, TokenId, TokenKey},
};

use crate::ingest::{
    data_plane_index::{DataPlane, DataPlaneIndex},
    market_filter::MarketFilter,
};

/// Market metadata registry with bidirectional token ↔ market lookup.
///
/// Populated by Gamma API sync; read by the data pipeline and selection paths.
pub struct MarketRegistry {
    data_plane: Arc<DataPlane>,
}

impl MarketRegistry {
    pub const fn new(data_plane: Arc<DataPlane>) -> Self {
        Self { data_plane }
    }

    #[must_use]
    pub const fn data_plane(&self) -> &Arc<DataPlane> {
        &self.data_plane
    }

    /// Register a single market and atomically publish a complete new index.
    pub fn register_market(&self, entry: MarketRegistryInfo) {
        let market_id = entry.market_id.clone();
        if self.data_plane.register_markets(vec![entry]) == 0 {
            tracing::warn!(%market_id, "skipping market with invalid token pair");
        }
    }

    /// Batch register markets and publish the resulting catalog once.
    pub fn register_markets(&self, entries: Vec<MarketRegistryInfo>) {
        let expected = entries.len();
        let registered = self.data_plane.register_markets(entries);
        if registered != expected {
            tracing::warn!(
                expected,
                registered,
                "skipped markets with invalid token pairs"
            );
        }
    }

    /// Batch register events from Gamma sync.
    pub fn register_events(&self, events: impl IntoIterator<Item = EventRegistryInfo>) {
        self.data_plane.register_events(events);
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
        self.data_plane.register_events([entry]);
    }

    /// Reverse lookup: token → market.
    pub fn market_for_token(&self, token_id: &TokenId) -> Option<MarketId> {
        self.data_plane.with_index(|index| {
            let token = index.token_key(token_id)?;
            Some(index.token_metadata(token)?.market_id.clone())
        })
    }

    /// Dense hot-path token → market lookup.
    #[must_use]
    pub fn market_for_key(&self, token: TokenKey) -> Option<MarketId> {
        self.data_plane
            .with_index(|index| Some(index.token_metadata(token)?.market_id.clone()))
    }

    /// Resolve a process-local key back to its boundary identifier.
    #[must_use]
    pub fn token_id(&self, token: TokenKey) -> Option<TokenId> {
        self.data_plane
            .with_index(|index| Some(index.token_metadata(token)?.token_id.clone()))
    }

    /// Get a shared market entry.
    pub fn get_market(&self, market_id: &MarketId) -> Option<Arc<MarketRegistryInfo>> {
        self.data_plane
            .with_index(|index| index.market(market_id).map(Arc::clone))
    }

    /// Return whether a market is negative-risk without cloning the full entry.
    pub fn neg_risk(&self, market_id: &MarketId) -> Option<bool> {
        self.data_plane
            .with_index(|index| index.market(market_id).map(|entry| entry.neg_risk))
    }

    /// The event a market belongs to, if catalogued.
    #[must_use]
    pub fn get_event(&self, event_id: &EventId) -> Option<EventRegistryInfo> {
        self.data_plane
            .with_index(|index| index.event(event_id).map(|entry| entry.as_ref().clone()))
    }

    /// Every YES-leg token of a neg-risk event, deterministically ordered by
    /// `(market_id, yes_token_id)` (stable for hashing / online-offline parity).
    ///
    /// Returns empty for an unknown event or a non-neg-risk event.
    #[must_use]
    pub fn neg_risk_leg_set(&self, event_id: &EventId) -> NegRiskLegSet {
        self.data_plane.with_index(|index| {
            let Some(event) = index.event(event_id) else {
                return NegRiskLegSet::empty();
            };
            if !event.neg_risk {
                return NegRiskLegSet::empty();
            }
            NegRiskLegSet::from_catalog(&event.market_ids, |market_id| {
                index.market(market_id).map(|entry| {
                    if entry.neg_risk {
                        CatalogMarketLeg::NegRisk {
                            yes_token_id: entry.token_yes.clone(),
                        }
                    } else {
                        CatalogMarketLeg::NonNegRisk
                    }
                })
            })
        })
    }

    /// Return (YES token, NO token) for a market.
    ///
    /// Tokens are identified by their `outcome` field: "Yes" and "No".
    pub fn token_pair(&self, market_id: &MarketId) -> Option<(TokenId, TokenId)> {
        self.data_plane.with_index(|index| {
            let pair = index.market_token_pair(market_id)?;
            Some((
                index.token_metadata(pair.yes)?.token_id.clone(),
                index.token_metadata(pair.no)?.token_id.clone(),
            ))
        })
    }

    /// Wait-free read of the active market ID list.
    #[must_use]
    pub fn active_markets(&self) -> Arc<[MarketId]> {
        self.data_plane
            .with_index(DataPlaneIndex::active_markets_owned)
    }

    /// Active YES/NO catalog tokens bounded by the tradeable market filter.
    ///
    /// This is a catalog helper, not the engine WS subscription policy. Trading
    /// subscriptions must go through `MarketDataSubscriptionPolicy`.
    #[must_use]
    pub fn active_catalog_tokens(&self, market_filter: &MarketFilter) -> Vec<TokenId> {
        let mut tokens = self.data_plane.with_index(|index| {
            let mut tokens = Vec::with_capacity(index.active_markets().len() * 2);
            for market_id in index.active_markets() {
                let Some(market) = index.market(market_id) else {
                    continue;
                };
                if !market_filter.is_enabled(market.categories) {
                    continue;
                }
                tokens.push(market.token_yes.clone());
                tokens.push(market.token_no.clone());
            }
            tokens
        });
        tokens.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        tokens.dedup_by(|a, b| a.as_str() == b.as_str());
        tokens
    }

    /// Total number of registered markets.
    pub fn market_count(&self) -> usize {
        self.data_plane.with_index(DataPlaneIndex::market_count)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::market::TokenInfo,
        enums::{
            catalog::CatalogFilterReasonSet,
            common::{CategorySet, MarketCategory, TickSize},
            market::EventStatus,
        },
    };
    use rust_decimal_macros::dec;

    use super::*;
    fn sample_market(id: &str, status: MarketStatus) -> MarketRegistryInfo {
        sample_market_for_event(id, "evt-1", status, false)
    }

    fn sample_market_for_event(
        id: &str,
        event_id: &str,
        status: MarketStatus,
        neg_risk: bool,
    ) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new(event_id),
            token_yes: TokenId::new(format!("{id}-yes")),
            token_no: TokenId::new(format!("{id}-no")),
            question: "Test?".into(),
            slug: "test".into(),
            description: None,
            categories: CategorySet::from(MarketCategory::Other),
            status,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-yes")),
                    outcome: "Yes".into(),
                    neg_risk,
                },
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-no")),
                    outcome: "No".into(),
                    neg_risk,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(5),
            liquidity_usd: None,
            volume_24h: None,
            start_date: None,
            end_date: None,
            resolved_at: None,
            created_at: Some(Utc::now()),
            updated_at: Utc::now(),
        }
    }

    fn sample_neg_risk_event(event_id: &str, market_ids: Vec<&str>) -> EventRegistryInfo {
        EventRegistryInfo {
            event_id: EventId::new(event_id),
            title: "Neg-risk event".into(),
            slug: event_id.into(),
            series_slug: None,
            status: EventStatus::Active,
            market_ids: market_ids.into_iter().map(MarketId::new).collect(),
            categories: CategorySet::from(MarketCategory::Crypto),
            tags: Vec::new(),
            neg_risk: true,
            end_date: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn register_and_lookup() {
        let reg = MarketRegistry::new(Arc::new(DataPlane::new()));
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
        let reg = MarketRegistry::new(Arc::new(DataPlane::new()));
        reg.register_market(sample_market("m1", MarketStatus::Settled));
        assert!(reg.active_markets().is_empty());
    }

    #[test]
    fn batch_register() {
        let reg = MarketRegistry::new(Arc::new(DataPlane::new()));
        reg.register_markets(vec![
            sample_market("m1", MarketStatus::Active),
            sample_market("m2", MarketStatus::Active),
        ]);
        assert_eq!(reg.market_count(), 2);
        assert_eq!(reg.active_markets().len(), 2);
    }

    #[test]
    fn active_markets_snapshot_is_wait_free() {
        let reg = MarketRegistry::new(Arc::new(DataPlane::new()));
        reg.register_market(sample_market("m1", MarketStatus::Active));
        let snapshot = reg.active_markets();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].as_str(), "m1");
    }

    #[test]
    fn neg_risk_leg_set_counts_only_neg_risk_event_members() {
        let reg = MarketRegistry::new(Arc::new(DataPlane::new()));
        let event_id = "evt-negrisk-mixed";
        reg.register_event(sample_neg_risk_event(
            event_id,
            vec!["m-leg-a", "m-leg-b", "m-binary"],
        ));
        reg.register_market(sample_market_for_event(
            "m-leg-a",
            event_id,
            MarketStatus::Active,
            true,
        ));
        reg.register_market(sample_market_for_event(
            "m-leg-b",
            event_id,
            MarketStatus::Active,
            true,
        ));
        reg.register_market(sample_market_for_event(
            "m-binary",
            event_id,
            MarketStatus::Active,
            false,
        ));

        let set = reg.neg_risk_leg_set(&EventId::new(event_id));
        assert_eq!(
            set.expected_legs, 2,
            "binary member must not inflate expected"
        );
        assert_eq!(set.legs.len(), 2);
    }

    #[test]
    fn neg_risk_leg_set_expects_unregistered_catalog_member() {
        let reg = MarketRegistry::new(Arc::new(DataPlane::new()));
        let event_id = "evt-negrisk-partial";
        reg.register_event(sample_neg_risk_event(
            event_id,
            vec!["m-present", "m-missing"],
        ));
        reg.register_market(sample_market_for_event(
            "m-present",
            event_id,
            MarketStatus::Active,
            true,
        ));

        let set = reg.neg_risk_leg_set(&EventId::new(event_id));
        assert_eq!(
            set.expected_legs, 2,
            "unregistered catalog leg still expected"
        );
        assert_eq!(set.legs.len(), 1);
    }
}
