use super::{market_filter::MarketFilter, market_registry::MarketRegistry};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::common::{CategorySet, MarketCategory, TickSize},
    types::{EventId, MarketId, TokenId},
};
use std::{collections::HashMap, sync::Arc};

/// Pre-computed scan entry for hot-path iteration.
///
/// Avoids repeated `DashMap` lookups and `MarketRegistryInfo` destructuring
/// during catalog-driven market sweeps (selection / report builders).
#[derive(Debug, Clone)]
pub struct CachedMarketScanEntry {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    /// Category memberships (market-filter source of truth).
    pub categories: CategorySet,
    /// Pre-derived `categories.fee_category()` — cached at rebuild so the
    /// per-sweep scan never re-derives it.
    pub fee_category: MarketCategory,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

/// Lock-free cache of active markets for ingest and selection hot paths.
///
/// Backed by `ArcSwap` for wait-free reads. Rebuilt on Gamma sync
/// (~every 5 minutes) and on runtime-config activation (market filter
/// changes), so reads never block writes and vice versa.
pub struct MarketCache {
    scan_entries: ArcSwap<Vec<Arc<CachedMarketScanEntry>>>,
    index: ArcSwap<HashMap<MarketId, Arc<CachedMarketScanEntry>>>,
    registry: Arc<MarketRegistry>,
    market_filter: Arc<MarketFilter>,
}

impl MarketCache {
    pub fn new(registry: Arc<MarketRegistry>, market_filter: Arc<MarketFilter>) -> Self {
        let cache = Self {
            scan_entries: ArcSwap::from_pointee(Vec::new()),
            index: ArcSwap::from_pointee(HashMap::new()),
            registry,
            market_filter,
        };
        cache.rebuild();
        cache
    }

    /// Reconstruct the cache from the current registry state, admitting only
    /// markets that pass the market filter.
    pub fn rebuild(&self) {
        let active_ids = self.registry.active_markets();
        let mut entries = Vec::with_capacity(active_ids.len());
        let mut index = HashMap::with_capacity(active_ids.len());

        for market_id in active_ids.iter() {
            let Some(market) = self.registry.get_market(market_id) else {
                continue;
            };
            if !self.market_filter.is_enabled(market.categories) {
                continue;
            }
            if market
                .end_date
                .is_none_or(|deadline| deadline <= Utc::now())
            {
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
        self.index.load().get(market_id).map(Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::market_registry::MarketRegistry;
    use chrono::Utc;
    use quant_pivot_models::{
        domain::market::{MarketRegistryInfo, TokenInfo},
        enums::market::MarketStatus,
    };
    use rust_decimal_macros::dec;

    fn sample_entry(id: &str, categories: CategorySet) -> MarketRegistryInfo {
        sample_entry_with_end_date(
            id,
            categories,
            Some(Utc::now() + chrono::Duration::hours(2)),
        )
    }

    fn sample_entry_with_end_date(
        id: &str,
        categories: CategorySet,
        end_date: Option<DateTime<Utc>>,
    ) -> MarketRegistryInfo {
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
            liquidity_usd: None,
            volume_24h: None,
            fee_schedule: None,
            end_date,
            resolved_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn admit_all() -> Arc<MarketFilter> {
        Arc::new(MarketFilter::default())
    }

    #[test]
    fn past_deadline_markets_are_excluded_from_cache() {
        let reg = Arc::new(MarketRegistry::new());
        reg.register_market(sample_entry_with_end_date(
            "past",
            CategorySet::EMPTY,
            Some(Utc::now() - chrono::Duration::hours(1)),
        ));
        reg.register_market(sample_entry("future", CategorySet::EMPTY));

        let cache = MarketCache::new(reg, admit_all());
        assert_eq!(cache.entries().len(), 1);
        assert!(cache.get(&MarketId::new("future")).is_some());
        assert!(cache.get(&MarketId::new("past")).is_none());
    }

    #[test]
    fn no_end_date_markets_are_excluded_from_cache() {
        let reg = Arc::new(MarketRegistry::new());
        reg.register_market(sample_entry_with_end_date(
            "no-date",
            CategorySet::EMPTY,
            None,
        ));

        let cache = MarketCache::new(reg, admit_all());
        assert!(cache.entries().is_empty());
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
    fn market_filter_bounds_rebuild() {
        let reg = Arc::new(MarketRegistry::new());
        reg.register_market(sample_entry(
            "m-sports",
            CategorySet::from(MarketCategory::Sports),
        ));
        reg.register_market(sample_entry(
            "m-politics",
            CategorySet::from(MarketCategory::Politics),
        ));

        let market_filter = Arc::new(MarketFilter::new(&[MarketCategory::Politics]));
        let cache = MarketCache::new(Arc::clone(&reg), Arc::clone(&market_filter));
        assert_eq!(cache.entries().len(), 1);
        assert!(cache.get(&MarketId::new("m-politics")).is_some());
        assert!(cache.get(&MarketId::new("m-sports")).is_none());

        // Hot reload widens the market filter; rebuild re-admits the sports market.
        market_filter.reload(&[]);
        cache.rebuild();
        assert_eq!(cache.entries().len(), 2);
    }
}
