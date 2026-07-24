//! Hot-reloadable category filter bounding the tradeable market set.
//!
//! The full Gamma catalog is always ingested and persisted; this filter only
//! bounds the *hot* set — WebSocket subscriptions and the scanner sweep — so
//! narrowing it never loses settlement or evidence data. Reads are lock-free
//! (`ArcSwap`); reloads come exclusively from runtime-config activation.

use std::sync::Arc;

use arc_swap::ArcSwap;
use quant_pivot_models::enums::common::{CategorySet, MarketCategory};

/// Lock-free view of `market_data.enabled_categories`.
///
/// Matching is any-match against an entry's [`CategorySet`]: an event tagged
/// politics + geopolitics passes when either category is enabled. The empty
/// filter admits everything (the default).
pub struct MarketFilter {
    enabled: ArcSwap<CategorySet>,
}

impl MarketFilter {
    /// Build from the configured category list (empty = admit all).
    #[must_use]
    pub fn new(enabled_categories: &[MarketCategory]) -> Self {
        Self {
            enabled: ArcSwap::from_pointee(CategorySet::from(enabled_categories)),
        }
    }

    /// Whether a market with `categories` belongs to the tradeable market set.
    ///
    /// Markets without any recognized category only match the empty
    /// (admit-all) filter — operators narrowing the market set explicitly opt
    /// out of uncategorized markets.
    #[must_use]
    #[inline]
    pub fn is_enabled(&self, categories: CategorySet) -> bool {
        let enabled = **self.enabled.load();
        enabled.is_empty() || enabled.intersects(categories)
    }

    /// Swap in a new enabled set (runtime-config activation path).
    pub fn reload(&self, enabled_categories: &[MarketCategory]) {
        self.enabled
            .store(Arc::new(CategorySet::from(enabled_categories)));
    }

    /// Current enabled set (for logging / introspection).
    #[must_use]
    pub fn enabled(&self) -> CategorySet {
        **self.enabled.load()
    }
}

impl Default for MarketFilter {
    fn default() -> Self {
        Self::new(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_admits_everything() {
        let filter = MarketFilter::default();
        assert!(filter.is_enabled(CategorySet::from(MarketCategory::Sports)));
        assert!(filter.is_enabled(CategorySet::EMPTY));
    }

    #[test]
    fn narrowed_filter_matches_membership() {
        let filter = MarketFilter::new(&[MarketCategory::Geopolitics]);
        let multi: CategorySet = [MarketCategory::Politics, MarketCategory::Geopolitics]
            .into_iter()
            .collect();
        assert!(filter.is_enabled(multi));
        assert!(!filter.is_enabled(CategorySet::from(MarketCategory::Sports)));
        assert!(
            !filter.is_enabled(CategorySet::EMPTY),
            "uncategorized markets must not match a narrowed filter"
        );
    }

    #[test]
    fn reload_swaps_enabled_set() {
        let filter = MarketFilter::new(&[MarketCategory::Sports]);
        assert!(filter.is_enabled(CategorySet::from(MarketCategory::Sports)));
        filter.reload(&[MarketCategory::Crypto]);
        assert!(!filter.is_enabled(CategorySet::from(MarketCategory::Sports)));
        assert!(filter.is_enabled(CategorySet::from(MarketCategory::Crypto)));
    }
}
