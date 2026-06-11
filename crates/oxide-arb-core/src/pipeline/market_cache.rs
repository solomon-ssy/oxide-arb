use super::{market_registry::MarketRegistry, universe_filter::MarketUniverseFilter};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    enums::common::{CategorySet, MarketCategory, TickSize},
    types::{EventId, MarketId, TokenId},
};
use std::{collections::HashMap, sync::Arc};

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
    /// Category memberships (universe-filter source of truth).
    pub categories: CategorySet,
    /// Pre-derived `categories.fee_category()` — cached at rebuild so the
    /// per-sweep scan never re-derives it.
    pub fee_category: MarketCategory,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

/// Lock-free cache of active markets for the Scanner hot path.
///
/// Backed by `ArcSwap` for wait-free reads. Rebuilt on Gamma sync
/// (~every 5 minutes) and on runtime-config activation (universe filter
/// changes), so reads never block writes and vice versa.
pub struct MarketCache {
    scan_entries: ArcSwap<Vec<Arc<CachedMarketScanEntry>>>,
    index: ArcSwap<HashMap<MarketId, Arc<CachedMarketScanEntry>>>,
    registry: Arc<MarketRegistry>,
    universe: Arc<MarketUniverseFilter>,
}

impl MarketCache {
    pub fn new(registry: Arc<MarketRegistry>, universe: Arc<MarketUniverseFilter>) -> Self {
        let cache = Self {
            scan_entries: ArcSwap::from_pointee(Vec::new()),
            index: ArcSwap::from_pointee(HashMap::new()),
            registry,
            universe,
        };
        cache.rebuild();
        cache
    }

    /// Reconstruct the cache from the current registry state, admitting only
    /// markets that pass the universe filter.
    pub fn rebuild(&self) {
        let active_ids = self.registry.active_markets();
        let mut entries = Vec::with_capacity(active_ids.len());
        let mut index = HashMap::with_capacity(active_ids.len());

        for market_id in active_ids.iter() {
            let Some(market) = self.registry.get_market(market_id) else {
                continue;
            };
            if !self.universe.is_enabled(market.categories) {
                continue;
            }

            let entry = Arc::new(CachedMarketScanEntry {
                market_id: market.market_id.clone(),
                event_id: market.event_id.clone(),
                token_yes: market.token_yes.clone(),
                token_no: market.token_no.clone(),
                categories: market.categories,
                fee_category: market.fee_category(),
                tick_size: market.tick_size,
                neg_risk: market.neg_risk,
                settlement_deadline: market.end_date,
            });
            index.insert(entry.market_id.clone(), Arc::clone(&entry));
            entries.push(entry);
        }

        self.scan_entries.store(Arc::new(entries));
        self.index.store(Arc::new(index));
    }

    /// Wait-free snapshot of cached entries.
    pub fn entries(&self) -> Arc<Vec<Arc<CachedMarketScanEntry>>> {
        self.scan_entries.load_full()
    }

    /// O(1) lookup for a single market scan entry (Arc clone, no struct copy).
    pub fn get(&self, market_id: &MarketId) -> Option<Arc<CachedMarketScanEntry>> {
        self.index.load().get(market_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::market_registry::MarketRegistry;
    use chrono::Utc;
    use oxide_arb_models::{
        domain::market::{MarketRegistryInfo, TokenInfo},
        enums::market::MarketStatus,
        types::Usd,
    };
    use rust_decimal_macros::dec;

    fn sample_entry(id: &str, categories: CategorySet) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            token_yes: TokenId::new(format!("{id}-yes")),
            token_no: TokenId::new(format!("{id}-no")),
            question: "Q?".into(),
            slug: "q".into(),
            categories,
            status: MarketStatus::Active,
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

    fn admit_all() -> Arc<MarketUniverseFilter> {
        Arc::new(MarketUniverseFilter::default())
    }

    #[test]
    fn rebuild_populates_entries() {
        let reg = Arc::new(MarketRegistry::new());
        reg.register_market(sample_entry("m1", CategorySet::EMPTY));
        reg.register_market(sample_entry("m2", CategorySet::EMPTY));

        let cache = MarketCache::new(reg, admit_all());
        let entries = cache.entries();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn get_returns_entry_by_market_id() {
        let reg = Arc::new(MarketRegistry::new());
        reg.register_market(sample_entry("m1", CategorySet::EMPTY));

        let cache = MarketCache::new(reg, admit_all());
        let entry = cache.get(&MarketId::new("m1")).unwrap();
        assert_eq!(entry.market_id.as_str(), "m1");
        assert_eq!(entry.fee_category, MarketCategory::Other);
    }

    #[test]
    fn empty_registry_yields_empty_cache() {
        let reg = Arc::new(MarketRegistry::new());
        let cache = MarketCache::new(reg, admit_all());
        assert!(cache.entries().is_empty());
        assert!(cache.get(&MarketId::new("missing")).is_none());
    }

    #[test]
    fn universe_filter_bounds_rebuild() {
        let reg = Arc::new(MarketRegistry::new());
        reg.register_market(sample_entry(
            "m-sports",
            CategorySet::from(MarketCategory::Sports),
        ));
        reg.register_market(sample_entry(
            "m-politics",
            CategorySet::from(MarketCategory::Politics),
        ));

        let universe = Arc::new(MarketUniverseFilter::new(&[MarketCategory::Politics]));
        let cache = MarketCache::new(Arc::clone(&reg), Arc::clone(&universe));
        assert_eq!(cache.entries().len(), 1);
        assert!(cache.get(&MarketId::new("m-politics")).is_some());
        assert!(cache.get(&MarketId::new("m-sports")).is_none());

        // Hot reload widens the universe; rebuild re-admits the sports market.
        universe.reload(&[]);
        cache.rebuild();
        assert_eq!(cache.entries().len(), 2);
    }
}
