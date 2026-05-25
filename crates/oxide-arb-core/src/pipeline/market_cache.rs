use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};

use oxide_arb_models::enums::common::{MarketCategory, TickSize};
use oxide_arb_models::types::{EventId, MarketId, TokenId};

use super::market_registry::MarketRegistry;

/// Pre-computed scan entry for hot-path iteration.
///
/// Avoids repeated `DashMap` lookups and `MarketRegistryInfo` destructuring
/// during the Scanner's periodic sweep.
#[derive(Debug, Clone)]
pub struct CachedMarketScanEntry {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub category: MarketCategory,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

/// Lock-free cache of active markets for the Scanner hot path.
///
/// Backed by `ArcSwap` for wait-free reads. Rebuilt on Gamma sync
/// (~every 5 minutes), so reads never block writes and vice versa.
pub struct MarketCache {
    scan_entries: ArcSwap<Vec<CachedMarketScanEntry>>,
    registry: Arc<MarketRegistry>,
}

impl MarketCache {
    pub fn new(registry: Arc<MarketRegistry>) -> Self {
        let cache = Self {
            scan_entries: ArcSwap::from_pointee(Vec::new()),
            registry,
        };
        cache.rebuild();
        cache
    }

    /// Reconstruct the cache from the current registry state.
    pub fn rebuild(&self) {
        let active_ids = self.registry.active_market_ids();
        let mut entries = Vec::with_capacity(active_ids.len());

        for market_id in &active_ids {
            let Some(market) = self.registry.get_market(market_id) else {
                continue;
            };
            let Some((token_yes, token_no)) = self.registry.token_pair(market_id) else {
                continue;
            };

            entries.push(CachedMarketScanEntry {
                market_id: market.market_id,
                event_id: market.event_id,
                token_yes,
                token_no,
                category: market.category,
                tick_size: market.tick_size,
                neg_risk: market.neg_risk,
                settlement_deadline: None,
            });
        }

        self.scan_entries.store(Arc::new(entries));
    }

    /// Wait-free snapshot of cached entries.
    pub fn entries(&self) -> Arc<Vec<CachedMarketScanEntry>> {
        self.scan_entries.load_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::market_registry::MarketRegistry;
    use chrono::Utc;
    use oxide_arb_models::domain::market::{MarketRegistryInfo, TokenInfo};
    use oxide_arb_models::enums::market::MarketStatus;
    use rust_decimal_macros::dec;

    fn sample_entry(id: &str) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            question: "Q?".into(),
            slug: "q".into(),
            category: MarketCategory::Other,
            status: MarketStatus::Active,
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
    fn rebuild_populates_entries() {
        let reg = Arc::new(MarketRegistry::new());
        reg.register_market(sample_entry("m1"));
        reg.register_market(sample_entry("m2"));

        let cache = MarketCache::new(reg);
        let entries = cache.entries();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn empty_registry_yields_empty_cache() {
        let reg = Arc::new(MarketRegistry::new());
        let cache = MarketCache::new(reg);
        assert!(cache.entries().is_empty());
    }
}
