//! Coalesced wake-up for immediate resting-order terms guards.

use std::{collections::HashSet, mem, sync::Arc};

use parking_lot::Mutex;
use quant_pivot_models::types::MarketId;
use tokio::sync::Notify;

#[derive(Clone, Default)]
/// Process-local coalescing signal emitted only after execution-term commits.
pub struct TermsDriftWake {
    inner: Arc<TermsDriftWakeInner>,
}

#[derive(Default)]
struct TermsDriftWakeInner {
    markets: Mutex<HashSet<MarketId>>,
    notify: Notify,
}

impl TermsDriftWake {
    /// Merge affected markets and wake one guard worker.
    pub fn publish(&self, market_ids: impl IntoIterator<Item = MarketId>) {
        let mut markets = self.inner.markets.lock();
        let before = markets.len();
        markets.extend(market_ids);
        let changed = markets.len() != before;
        drop(markets);
        if changed {
            self.inner.notify.notify_one();
        }
    }

    /// Wait until at least one affected market is pending.
    pub async fn notified(&self) {
        self.inner.notify.notified().await;
    }

    #[must_use]
    /// Atomically drain the currently coalesced affected-market set.
    pub fn take_markets(&self) -> HashSet<MarketId> {
        mem::take(&mut *self.inner.markets.lock())
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::types::MarketId;

    use super::TermsDriftWake;

    #[test]
    fn publish_coalesces_markets() {
        let wake = TermsDriftWake::default();
        let first = MarketId::new("first");
        let second = MarketId::new("second");

        wake.publish([first.clone(), second.clone(), first.clone()]);

        let markets = wake.take_markets();
        assert_eq!(markets.len(), 2);
        assert!(markets.contains(&first));
        assert!(markets.contains(&second));
        assert!(wake.take_markets().is_empty());
    }

    #[tokio::test]
    async fn early_publish_notifies() {
        let wake = TermsDriftWake::default();
        let market = MarketId::new("market");

        wake.publish([market.clone()]);
        wake.notified().await;

        assert!(wake.take_markets().contains(&market));
    }
}
