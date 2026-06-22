//! Graceful backpressure — coalesce, dedup, evict. Never halts trading.
//!
//! Post-trade durability is NOT a backpressure concern: the venue outcome is
//! persisted on the `trade` row and replayed by the relay, so there is no
//! bounded in-memory post-trade queue to overflow.

use crate::observability::metrics_hub::MetricsHub;
use dashmap::DashMap;
use oxide_arb_models::{domain::pipeline::PipelineEvent, types::TokenId};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const COALESCER_DEDUP_WINDOW: Duration = Duration::from_micros(500);

type BookCoalesceKey = TokenId;

/// Result of applying a backpressure policy at one site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    /// Book event stored for latest-wins coalesce.
    Coalesced,
    /// Coalescer notify deduplicated within the window.
    Deduped,
    /// Event/job truly dropped (non-coalescable or below eviction threshold).
    Dropped,
}

/// Central backpressure policy for the three hot-path coalesce/evict sites.
pub struct BackpressurePolicy {
    metrics: Arc<MetricsHub>,
    book_coalesce: Vec<DashMap<BookCoalesceKey, PipelineEvent>>,
    coalescer_dedup: DashMap<TokenId, Instant>,
    coalescer_dedup_window: Duration,
}

impl BackpressurePolicy {
    pub fn new(metrics: Arc<MetricsHub>, book_shard_count: usize) -> Self {
        let shard_count = book_shard_count.max(1);
        Self {
            metrics,
            book_coalesce: (0..shard_count).map(|_| DashMap::new()).collect(),
            coalescer_dedup: DashMap::new(),
            coalescer_dedup_window: COALESCER_DEDUP_WINDOW,
        }
    }

    /// Override the dedup window — tests only, for wall-clock determinism.
    #[cfg(test)]
    #[must_use]
    const fn with_coalescer_dedup_window(mut self, window: Duration) -> Self {
        self.coalescer_dedup_window = window;
        self
    }

    /// Site 1 — `book_apply` channel full: latest-wins coalesce per (shard, token).
    pub fn on_book_channel_full(&self, shard: usize, event: PipelineEvent) -> BackpressureAction {
        let Some(token) = event.asset_id().cloned() else {
            self.record_book_drop();
            return BackpressureAction::Dropped;
        };

        if !event.is_book_coalescable() {
            self.record_book_drop();
            return BackpressureAction::Dropped;
        }

        self.book_coalesce[shard].insert(token, event);
        self.metrics.book_apply_coalesced_total.inc();
        self.record_event("book_apply", "coalesce");
        BackpressureAction::Coalesced
    }

    /// Drain coalesced book events for one shard after each worker event.
    pub fn drain_book_coalesce(&self, shard: usize, mut apply: impl FnMut(PipelineEvent)) {
        let keys: Vec<BookCoalesceKey> = self.book_coalesce[shard]
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((_, event)) = self.book_coalesce[shard].remove(&key) {
                apply(event);
            }
        }
    }

    /// Site 2 — coalescer notify succeeded: record dedup window anchor.
    pub fn on_coalescer_notify_success(&self, token: &TokenId) {
        self.coalescer_dedup.insert(token.clone(), Instant::now());
    }

    /// Site 2 — coalescer `token_tx` full: dedup within 500µs else count drop.
    pub fn on_coalescer_channel_full(&self, token: &TokenId) -> BackpressureAction {
        if let Some(last) = self.coalescer_dedup.get(token) {
            if last.elapsed() < self.coalescer_dedup_window {
                self.record_event("coalescer", "dedup");
                return BackpressureAction::Deduped;
            }
        }

        self.record_coalescer_drop();
        BackpressureAction::Dropped
    }

    /// Site 3 — execution shard backpressure: record eviction metric.
    pub fn on_execution_shard_evict(&self) {
        self.metrics.execution_shard_evicted_total.inc();
        self.record_event("execution_shard", "evict");
    }

    #[cold]
    fn record_book_drop(&self) {
        self.metrics.book_apply_dropped.inc();
        self.record_event("book_apply", "drop");
    }

    #[cold]
    fn record_coalescer_drop(&self) {
        self.metrics.coalescer_dropped.inc();
        self.record_event("coalescer", "drop");
    }

    fn record_event(&self, site: &'static str, action: &'static str) {
        self.metrics
            .backpressure_events
            .with_label_values(&[site, action])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{
        domain::pipeline::{IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta},
        enums::common::Side,
        types::{Price, Shares, TokenId},
    };
    use rust_decimal_macros::dec;
    use std::{sync::Arc, time::Instant};

    #[test]
    fn book_coalesce_does_not_halt() {
        let metrics = Arc::new(MetricsHub::new());
        let bp = BackpressurePolicy::new(Arc::clone(&metrics), 1);

        let event = PipelineEvent::PriceDelta(PriceDeltaCmd {
            asset_id: TokenId::new("t1"),
            changes: Arc::from([PriceLevelDelta {
                price: Price::new(dec!(0.5)),
                size: Shares::new(dec!(100)),
                side: Side::Buy,
            }]),
            timestamp_ms: 1,
            trace: IngressTrace::new(Instant::now(), 1),
        });

        assert_eq!(
            bp.on_book_channel_full(0, event),
            BackpressureAction::Coalesced
        );
        assert_eq!(metrics.book_apply_coalesced_total.get(), 1);
    }

    #[test]
    fn coalescer_dedup_within_window() {
        let metrics = Arc::new(MetricsHub::new());
        // A generous window keeps the assertion deterministic under loaded
        // parallel test runs (the production default is 500µs of wall clock).
        let bp = BackpressurePolicy::new(Arc::clone(&metrics), 1)
            .with_coalescer_dedup_window(Duration::from_secs(60));
        let token = TokenId::new("t1");

        bp.on_coalescer_notify_success(&token);
        assert_eq!(
            bp.on_coalescer_channel_full(&token),
            BackpressureAction::Deduped
        );
        assert_eq!(metrics.coalescer_dropped.get(), 0);
    }
}
