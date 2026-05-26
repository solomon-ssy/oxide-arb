//! Per-market execution in-flight guard — prevents double-fire on the same market
//! while allowing parallel execution across different markets.

use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use oxide_arb_models::types::MarketId;

/// Global registry of markets currently in an execution pipeline.
pub struct MarketInFlightRegistry {
    active: DashMap<MarketId, ()>,
}

impl MarketInFlightRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: DashMap::new(),
        }
    }

    /// Attempt to mark `market_id` as in-flight. Returns a guard that releases on drop.
    #[must_use]
    pub fn try_acquire(self: &Arc<Self>, market_id: &MarketId) -> Option<InFlightGuard> {
        match self.active.entry(market_id.clone()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(v) => {
                v.insert(());
                Some(InFlightGuard {
                    market_id: market_id.clone(),
                    registry: Arc::clone(self),
                })
            }
        }
    }

    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}

impl Default for MarketInFlightRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII release of a market in-flight slot.
pub struct InFlightGuard {
    market_id: MarketId,
    registry: Arc<MarketInFlightRegistry>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.registry.active.remove(&self.market_id);
    }
}
