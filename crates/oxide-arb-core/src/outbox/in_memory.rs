//! In-memory outbox buffer for synchronous post-trade spill (PR-1).
//!
//! Production flusher integration lands in PR-8; until then spilled jobs are
//! replayed directly by the execution outcome drain.

use std::collections::VecDeque;
use std::sync::Arc;

use oxide_arb_error::OxideError;
use parking_lot::Mutex;

use crate::execution::execution_pipeline::PostTradeJob;
use crate::observability::metrics_hub::MetricsHub;

/// Synchronous spill buffer backing post-trade backpressure.
pub struct InMemoryEventStore {
    post_trade_jobs: Mutex<VecDeque<PostTradeJob>>,
}

impl InMemoryEventStore {
    pub const fn new() -> Self {
        Self {
            post_trade_jobs: Mutex::new(VecDeque::new()),
        }
    }

    /// Spill a post-trade job into the in-memory outbox (never drops).
    pub fn enqueue_sync_post_trade(
        &self,
        job: PostTradeJob,
        metrics: &MetricsHub,
    ) -> Result<(), OxideError> {
        let mut jobs = self.post_trade_jobs.lock();
        jobs.push_back(job);

        metrics
            .outbox_pending
            .set(i64::try_from(jobs.len()).unwrap_or(i64::MAX));
        drop(jobs);

        Ok(())
    }

    /// Pop one spilled job for replay (FIFO).
    pub fn try_pop_post_trade(&self) -> Option<PostTradeJob> {
        self.post_trade_jobs.lock().pop_front()
    }

    /// Drain all spilled jobs in FIFO order (shutdown path).
    pub fn drain_post_trade_jobs(&self) -> Vec<PostTradeJob> {
        self.post_trade_jobs.lock().drain(..).collect()
    }

    pub fn pending_post_trade_count(&self) -> usize {
        self.post_trade_jobs.lock().len()
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared handle used across backpressure sites and the outcome drain.
pub type SharedInMemoryEventStore = Arc<InMemoryEventStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::enums::common::ExecutionMode;
    use oxide_arb_models::enums::execution::ExecutionOutcome;
    use oxide_arb_models::types::{MarketId, Price, TokenId, TradeId, Usd};
    use rust_decimal_macros::dec;

    fn sample_job(id: &str) -> PostTradeJob {
        PostTradeJob {
            trade_id: TradeId::new(id),
            market_id: MarketId::new("m1"),
            token_id: TokenId::new("t1"),
            entry_price: Price::new(dec!(0.5)),
            net_profit: Usd::new(dec!(1)),
            outcome: ExecutionOutcome::Miss {
                reason: "test".into(),
                execution_mode: ExecutionMode::Paper,
            },
        }
    }

    #[test]
    fn spill_fifo_ordering() {
        let store = InMemoryEventStore::new();
        let metrics = MetricsHub::new();

        store
            .enqueue_sync_post_trade(sample_job("a"), &metrics)
            .expect("enqueue a");
        store
            .enqueue_sync_post_trade(sample_job("b"), &metrics)
            .expect("enqueue b");
        store
            .enqueue_sync_post_trade(sample_job("c"), &metrics)
            .expect("enqueue c");

        assert_eq!(store.try_pop_post_trade().unwrap().trade_id.as_str(), "a");
        assert_eq!(store.try_pop_post_trade().unwrap().trade_id.as_str(), "b");
        assert_eq!(store.try_pop_post_trade().unwrap().trade_id.as_str(), "c");
        assert!(store.try_pop_post_trade().is_none());
    }

    #[test]
    fn drain_preserves_fifo() {
        let store = InMemoryEventStore::new();
        let metrics = MetricsHub::new();
        for id in ["x", "y", "z"] {
            store
                .enqueue_sync_post_trade(sample_job(id), &metrics)
                .expect("enqueue");
        }
        let drained = store.drain_post_trade_jobs();
        assert_eq!(
            drained
                .iter()
                .map(|j| j.trade_id.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
    }
}
