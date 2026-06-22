use arc_swap::ArcSwap;
use chrono::Utc;
use oxide_arb_models::domain::TradeIntegritySnapshot;
use std::sync::Arc;

/// Lock-free publisher for [`TradeIntegritySnapshot`].
pub struct TradeIntegrityStoreHandle {
    snapshot: ArcSwap<TradeIntegritySnapshot>,
}

impl TradeIntegrityStoreHandle {
    pub fn new(initial: TradeIntegritySnapshot) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(initial),
        }
    }

    #[must_use]
    pub fn load(&self) -> Arc<TradeIntegritySnapshot> {
        self.snapshot.load_full()
    }

    pub fn publish(&self, next: TradeIntegritySnapshot) {
        self.snapshot.store(Arc::new(next));
    }
}

impl Default for TradeIntegrityStoreHandle {
    fn default() -> Self {
        Self::new(TradeIntegritySnapshot::zero(Utc::now()))
    }
}
